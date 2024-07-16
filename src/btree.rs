use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::BTreeError;
use crate::node::Node;
use crate::ORDER;

/// A generic B-tree implementation with concurrent operations support.
///
/// This B-tree supports concurrent reads and writes using tokio's asynchronous locks.
/// It employs a technique called "latch-crabbing" for efficient and safe concurrent operations.
///
/// Latch-crabbing: When locking a node, the parent node's lock is held.
/// Once operations on the child node are confirmed to be safe, the parent node's lock is released,
/// allowing other threads to access the parent node.
///
/// tokio::sync::Mutex is used instead of std::sync::Mutex because:
/// 1. It provides OwnedMutexGuard, which moves ownership to the lock guard.
/// 2. std::sync::Mutex's MutexGuard has lifetime constraints that make
///    the latch-crabbing implementation impossible.
///
/// Type parameters:
/// - `K`: The type of the keys stored in the B-tree. Must implement `Ord`, `Clone`, `Send`, and `Sync`.
/// - `V`: The type of the values associated with the keys. Must implement `Clone`, `Send`, and `Sync`.
pub struct BTree<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    root: Arc<Mutex<Node<K, V>>>,
}

impl<K, V> BTree<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Creates a new, empty B-tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_btree::BTree;
    ///
    /// let tree: BTree<i32, String> = BTree::new();
    /// ```
    pub fn new() -> Self {
        BTree {
            root: Arc::new(Mutex::new(Node::new(true))),
        }
    }

    /// Inserts a key-value pair into the B-tree.
    ///
    /// If the key already exists, the value is updated.
    /// This method uses latch-crabbing to ensure safe concurrent operations.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value associated with the key.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok(())` if the operation was successful, or an `Err` containing a `BTreeError` if not.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_btree::BTree;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let tree = Arc::new(BTree::new());
    ///     tree.clone().insert(1, "one".to_string()).await.unwrap();
    /// }
    /// ```
    pub async fn insert(self: Arc<Self>, key: K, value: V) -> Result<(), BTreeError> {
        let mut current = Arc::clone(&self.root);
        let mut parent: Option<Arc<Mutex<Node<K, V>>>> = None;
        let key = key;
        let value = value;

        loop {
            let mut node_guard = current.lock().await;

            if node_guard.is_leaf {
                let index = node_guard.keys.binary_search(&key).unwrap_or_else(|x| x);
                node_guard.keys.insert(index, key);
                node_guard.values.insert(index, value);
                return Ok(());
            }

            let index = node_guard.keys.binary_search(&key).unwrap_or_else(|x| x);
            let next = Arc::clone(&node_guard.children[index]);

            if !next.lock().await.is_safe_for_insert() {
                drop(node_guard);
                self.clone()
                    .split_child(Arc::clone(&current), index)
                    .await?;
                node_guard = current.lock().await;
                let index = node_guard.keys.binary_search(&key).unwrap_or_else(|x| x);
                let next = Arc::clone(&node_guard.children[index]);
                if let Some(parent) = parent.take() {
                    drop(parent.lock().await);
                }
                parent = Some(Arc::clone(&current));
                drop(node_guard);
                current = next;
            } else {
                if let Some(parent) = parent.take() {
                    drop(parent.lock().await);
                }
                parent = Some(Arc::clone(&current));
                drop(node_guard);
                current = next;
            }
        }
    }

    /// Searches for a value associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for.
    ///
    /// # Returns
    ///
    /// An `Option` containing the value if the key was found, or `None` if it wasn't.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_btree::BTree;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let tree = Arc::new(BTree::new());
    ///     tree.clone().insert(1, "one".to_string()).await.unwrap();
    ///     let value = tree.clone().search(&1).await;
    ///     assert_eq!(value, Some("one".to_string()));
    /// }
    /// ```
    pub async fn search(self: Arc<Self>, key: &K) -> Option<V> {
        let mut current = Arc::clone(&self.root);

        loop {
            let node_guard = current.lock().await;
            match node_guard.keys.binary_search(key) {
                Ok(index) => {
                    let result = node_guard.values[index].clone();
                    return Some(result);
                }
                Err(index) => {
                    if node_guard.is_leaf {
                        return None;
                    }
                    let next = Arc::clone(&node_guard.children[index]);
                    drop(node_guard);
                    current = next;
                }
            }
        }
    }

    /// Deletes a key-value pair from the B-tree.
    ///
    /// This method uses latch-crabbing to ensure safe concurrent operations.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete.
    ///
    /// # Returns
    ///
    /// An `Option` containing the deleted value if the key was found, or `None` if it wasn't.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_btree::BTree;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let tree = Arc::new(BTree::new());
    ///     tree.clone().insert(1, "one".to_string()).await.unwrap();
    ///     let deleted_value = tree.clone().delete(&1).await;
    ///     assert_eq!(deleted_value, Some("one".to_string()));
    /// }
    /// ```
    pub async fn delete(self: Arc<Self>, key: &K) -> Option<V> {
        let mut current = Arc::clone(&self.root);
        let mut parent: Option<Arc<Mutex<Node<K, V>>>> = None;

        loop {
            let mut node_guard = current.lock().await;
            match node_guard.keys.binary_search(key) {
                Ok(index) => {
                    if node_guard.is_leaf {
                        let value = node_guard.values.remove(index);
                        node_guard.keys.remove(index);
                        drop(node_guard);
                        if let Some(p) = parent {
                            let _ = self.clone().balance(p).await;
                        }
                        return Some(value);
                    } else {
                        let predecessor = self.clone().get_predecessor(&node_guard, index).await;
                        if let Some((pred_key, pred_value)) = predecessor {
                            node_guard.keys[index] = pred_key;
                            node_guard.values[index] = pred_value;
                            let next = Arc::clone(&node_guard.children[index]);
                            drop(node_guard);
                            parent = Some(Arc::clone(&current));
                            current = next;
                        } else {
                            return None;
                        }
                    }
                }
                Err(index) => {
                    if node_guard.is_leaf {
                        return None;
                    }
                    let next = Arc::clone(&node_guard.children[index]);
                    if !next.lock().await.is_safe_for_delete() {
                        let _ = self.clone().ensure_safe(&current, index).await;
                        node_guard = current.lock().await;
                    }
                    parent = Some(Arc::clone(&current));
                    drop(node_guard);
                    current = next;
                }
            }
        }
    }

    async fn split_child(
        self: Arc<Self>,
        parent: Arc<Mutex<Node<K, V>>>,
        index: usize,
    ) -> Result<(), BTreeError> {
        let mut parent_guard = parent.lock().await;
        let child = Arc::clone(&parent_guard.children[index]);
        let mut child_guard = child.lock().await;

        let mut new_child = Node::new(child_guard.is_leaf);
        new_child.keys = child_guard.keys.split_off(ORDER / 2);
        new_child.values = child_guard.values.split_off(ORDER / 2);

        if !child_guard.is_leaf {
            new_child.children = child_guard.children.split_off(ORDER / 2);
        }

        let middle_key = child_guard.keys.pop().ok_or(BTreeError::InvalidOperation)?;
        let middle_value = child_guard
            .values
            .pop()
            .ok_or(BTreeError::InvalidOperation)?;

        parent_guard.keys.insert(index, middle_key);
        parent_guard.values.insert(index, middle_value);
        parent_guard
            .children
            .insert(index + 1, Arc::new(Mutex::new(new_child)));

        Ok(())
    }

    async fn get_predecessor(&self, node: &Node<K, V>, index: usize) -> Option<(K, V)> {
        let mut current = Arc::clone(&node.children[index]);
        loop {
            let node_guard = current.lock().await;
            if node_guard.is_leaf {
                let last_index = node_guard.keys.len().checked_sub(1)?;
                return Some((
                    node_guard.keys[last_index].clone(),
                    node_guard.values[last_index].clone(),
                ));
            }
            let next = Arc::clone(&node_guard.children[node_guard.children.len() - 1]);
            drop(node_guard);
            current = next;
        }
    }

    async fn ensure_safe(
        &self,
        parent: &Arc<Mutex<Node<K, V>>>,
        index: usize,
    ) -> Result<(), BTreeError> {
        let mut parent_guard = parent.lock().await;
        let child = Arc::clone(&parent_guard.children[index]);
        let mut child_guard = child.lock().await;

        if index > 0 {
            let left_sibling = Arc::clone(&parent_guard.children[index - 1]);
            let mut left_sibling_guard = left_sibling.lock().await;
            if left_sibling_guard.keys.len() > ORDER / 2 {
                child_guard
                    .keys
                    .insert(0, parent_guard.keys[index - 1].clone());
                child_guard
                    .values
                    .insert(0, parent_guard.values[index - 1].clone());
                parent_guard.keys[index - 1] = left_sibling_guard
                    .keys
                    .pop()
                    .ok_or(BTreeError::InvalidOperation)?;
                parent_guard.values[index - 1] = left_sibling_guard
                    .values
                    .pop()
                    .ok_or(BTreeError::InvalidOperation)?;
                if !child_guard.is_leaf {
                    child_guard.children.insert(
                        0,
                        Arc::clone(
                            &left_sibling_guard
                                .children
                                .pop()
                                .ok_or(BTreeError::InvalidOperation)?,
                        ),
                    );
                }
                return Ok(());
            }
        }

        if index < parent_guard.children.len() - 1 {
            let right_sibling = Arc::clone(&parent_guard.children[index + 1]);
            let mut right_sibling_guard = right_sibling.lock().await;
            if right_sibling_guard.keys.len() > ORDER / 2 {
                child_guard.keys.push(parent_guard.keys[index].clone());
                child_guard.values.push(parent_guard.values[index].clone());
                parent_guard.keys[index] = right_sibling_guard.keys.remove(0);
                parent_guard.values[index] = right_sibling_guard.values.remove(0);
                if !child_guard.is_leaf {
                    child_guard
                        .children
                        .push(Arc::clone(&right_sibling_guard.children.remove(0)));
                }
                return Ok(());
            }
        }

        if index > 0 {
            self.merge_nodes(parent, index - 1).await
        } else {
            self.merge_nodes(parent, index).await
        }
    }

    async fn merge_nodes(
        &self,
        parent: &Arc<Mutex<Node<K, V>>>,
        index: usize,
    ) -> Result<(), BTreeError> {
        let mut parent_guard = parent.lock().await;
        let left = Arc::clone(&parent_guard.children[index]);
        let right = Arc::clone(&parent_guard.children[index + 1]);
        let mut left_guard = left.lock().await;
        let mut right_guard = right.lock().await;

        left_guard.keys.push(parent_guard.keys.remove(index));
        left_guard.values.push(parent_guard.values.remove(index));
        left_guard.keys.append(&mut right_guard.keys);
        left_guard.values.append(&mut right_guard.values);
        if !left_guard.is_leaf {
            left_guard.children.append(&mut right_guard.children);
        }

        parent_guard.children.remove(index + 1);
        Ok(())
    }

    async fn balance(&self, node: Arc<Mutex<Node<K, V>>>) -> Result<(), BTreeError> {
        let mut node_guard = node.lock().await;
        if node_guard.keys.is_empty() && !node_guard.is_leaf {
            let child = Arc::clone(&node_guard.children[0]);
            std::mem::swap(&mut *node_guard, &mut *child.lock().await);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_insert_and_search() {
        let tree: Arc<BTree<i32, String>> = Arc::new(BTree::new());

        tree.clone().insert(1, "one".to_string()).await.unwrap();
        tree.clone().insert(2, "two".to_string()).await.unwrap();

        assert_eq!(tree.clone().search(&1).await, Some("one".to_string()));
        assert_eq!(tree.clone().search(&2).await, Some("two".to_string()));
        assert_eq!(tree.clone().search(&3).await, None);
    }

    #[tokio::test]
    async fn test_delete() {
        let tree: Arc<BTree<i32, String>> = Arc::new(BTree::new());

        tree.clone().insert(1, "one".to_string()).await.unwrap();
        tree.clone().insert(2, "two".to_string()).await.unwrap();

        assert_eq!(tree.clone().delete(&1).await, Some("one".to_string()));
        assert_eq!(tree.clone().search(&1).await, None);
        assert_eq!(tree.clone().search(&2).await, Some("two".to_string()));
    }
}
