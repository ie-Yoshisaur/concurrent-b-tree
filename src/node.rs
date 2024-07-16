use std::sync::Arc;
use tokio::sync::Mutex;

use crate::ORDER;

pub(crate) struct Node<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) keys: Vec<K>,
    pub(crate) values: Vec<V>,
    pub(crate) children: Vec<Arc<Mutex<Node<K, V>>>>,
    pub(crate) is_leaf: bool,
}

impl<K, V> Node<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(is_leaf: bool) -> Self {
        Node {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf,
        }
    }

    pub(crate) fn is_safe_for_insert(&self) -> bool {
        self.keys.len() < ORDER - 1
    }

    pub(crate) fn is_safe_for_delete(&self) -> bool {
        self.keys.len() > ORDER / 2
    }
}
