# Concurrent B-tree

A Rust implementation of a concurrent B-tree data structure using Tokio for asynchronous operations.

## Features

- Generic implementation supporting any key and value types that implement `Ord`, `Clone`, `Send`, and `Sync` traits
- Concurrent read and write operations using Tokio's asynchronous locks
- Efficient latch-crabbing technique for safe concurrent access
- Support for insert, search, and delete operations

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
concurrent-btree = { git = "https://github.com/ie-Yoshisaur/concurrent-b-tree.git" }
tokio = { version = "1.0", features = ["full"] }
```

## Example

```rust
use concurrent_btree::BTree;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let tree = Arc::new(BTree::new());

    // Insert
    tree.clone().insert(1, "one".to_string()).await.unwrap();
    tree.clone().insert(2, "two".to_string()).await.unwrap();

    // Search
    assert_eq!(tree.clone().search(&1).await, Some("one".to_string()));
    assert_eq!(tree.clone().search(&2).await, Some("two".to_string()));
    assert_eq!(tree.clone().search(&3).await, None);

    // Delete
    assert_eq!(tree.clone().delete(&1).await, Some("one".to_string()));
    assert_eq!(tree.clone().search(&1).await, None);
}
```

## Implementation Details

This B-tree implementation uses Tokio's `Mutex` for concurrent access control. It employs a technique called "latch-crabbing" for efficient and safe concurrent operations:

1. When locking a node, the parent node's lock is held.
2. Once operations on the child node are confirmed to be safe, the parent node's lock is released.
3. This allows other threads to access the parent node while operations continue on the child node.

The tree maintains the B-tree invariants:
- All leaf nodes are at the same depth
- Internal nodes have between ORDER/2 and ORDER-1 keys
- Leaf nodes have between ORDER/2 and ORDER-1 key-value pairs

## API

- `new()`: Create a new, empty B-tree
- `insert(key, value)`: Insert a key-value pair into the B-tree
- `search(key)`: Search for a value associated with the given key
- `delete(key)`: Delete a key-value pair from the B-tree

## Performance

The time complexity for operations on this B-tree is O(log n) in the average and worst cases, where n is the number of elements in the tree. The concurrent design allows for multiple threads to operate on different parts of the tree simultaneously, potentially improving throughput in multi-threaded environments.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Acknowledgments

- This implementation was inspired by the principles of B-tree data structures and concurrent programming techniques.
- Special thanks to the Tokio project for providing robust asynchronous primitives in Rust.
