# BuffDB Advanced Features

This document describes the advanced features that have been added to BuffDB, transforming it from a simple key-value store into a comprehensive embedded database.

## 🚀 New Features Overview

### 1. Transaction Support

BuffDB now supports ACID transactions with proper isolation levels:

```rust
use buffdb::{store::KvStore, backend::Sqlite, proto::kv::*};

// Create a store with transaction support
let store = KvStore::<Sqlite>::with_transactions(
    Location::InMemory,
    Duration::from_secs(30),
)?;

// Begin a transaction
let tx_response = store.begin_transaction(Request::new(BeginTransactionRequest {
    read_only: Some(false),
    timeout_ms: Some(5000),
})).await?;

let tx_id = tx_response.into_inner().transaction_id;

// Perform operations within the transaction
// All operations accept an optional transaction_id field

// Commit or rollback
store.commit_transaction(Request::new(CommitTransactionRequest {
    transaction_id: tx_id,
})).await?;
```

### 2. Secondary Indexing

Create indexes on values for fast lookups:

```rust
use buffdb::index::{IndexConfig, IndexType};

// Create a unique hash index
store.create_index(IndexConfig {
    name: "email_index".to_string(),
    index_type: IndexType::Hash,
    unique: true,
    filter: None,
})?;

// Create a B-tree index for range queries
store.create_index(IndexConfig {
    name: "age_index".to_string(),
    index_type: IndexType::BTree,
    unique: false,
    filter: None,
})?;

// Indexes are automatically updated on set/delete operations
```

### 3. Full-Text Search

Advanced text search with BM25 ranking:

```rust
use buffdb::fts::{FtsManager, FtsIndex};

let fts_manager = FtsManager::new();
fts_manager.create_index("articles".to_string())?;

if let Some(index) = fts_manager.get_index("articles") {
    // Index documents
    index.index_document("doc1".to_string(), "Full text content...")?;
    
    // Search with relevance ranking
    let results = index.search("query terms", 10);
    
    // Phrase search
    let exact = index.phrase_search("exact phrase", 10);
}
```

### 4. JSON Document Store

Document-oriented storage with JSONPath queries:

```rust
use buffdb::json_store::{JsonStore, JsonPath};
use serde_json::json;

let store = JsonStore::<Sqlite>::at_location(Location::InMemory, "users".to_string())?;

// Insert documents
let doc = json!({
    "name": "Alice",
    "profile": {
        "age": 30,
        "city": "NYC"
    }
});
store.insert("user:1".to_string(), doc, None).await?;

// Query with JSONPath
let results = JsonPath::query(&document.data, "$.profile.city")?;

// Partial updates
store.patch("user:1", "$.profile.age", json!(31)).await?;
```

### 5. Multi-Version Concurrency Control (MVCC)

Enable multiple concurrent transactions without blocking:

```rust
use buffdb::mvcc::{MvccManager, IsolationLevel};

let mvcc = MvccManager::new();

// Begin transaction with isolation level
let version = mvcc.begin_transaction(
    "tx1".to_string(),
    IsolationLevel::RepeatableRead,
    false, // not read-only
)?;

// Read and write operations see consistent snapshots
mvcc.write(&tx_id, "key".to_string(), b"value".to_vec())?;
mvcc.read(&tx_id, "key")?;

// Commit or abort
mvcc.commit_transaction(&tx_id)?;
```

## 🧪 Testing

Run all tests:

```bash
# Run the test script
./test_all.sh

# Or run specific test categories
cargo test --features vendored-sqlite

# Test transactions
cargo test transaction --features vendored-sqlite

# Test indexing
cargo test index --features vendored-sqlite

# Test FTS
cargo test fts --features vendored-sqlite

# Test JSON store
cargo test json_store --features vendored-sqlite

# Test MVCC
cargo test mvcc --features vendored-sqlite
```

## 📝 Implementation Status

| Feature | SQLite | DuckDB | RocksDB |
|---------|--------|---------|----------|
| Transactions | ✅ Full | ✅ Basic | ✅ Basic |
| Secondary Indexes | ✅ | ✅ | ✅ |
| Full-Text Search | ✅ | ✅ | ✅ |
| JSON Store | ✅ | ✅ | ✅ |
| MVCC | ✅ | 🚧 | 🚧 |

## 🎯 Integration Notes

1. **Backward Compatibility**: All new features are backward compatible. Existing code continues to work without modifications.

2. **Feature Flags**: Features can be enabled/disabled at compile time using Cargo features.

3. **Performance**: The index system adds minimal overhead. Indexes are updated asynchronously where possible.

4. **Memory Usage**: MVCC keeps multiple versions in memory. Use garbage collection to clean up old versions.

## 🚨 Known Limitations

1. **Index Updates**: Currently, indexes must be manually created. Automatic index suggestions are not implemented.

2. **Transaction Timeouts**: Transaction timeouts are specified but not fully enforced in all backends.

3. **Cross-Backend Transactions**: Transactions cannot span multiple backend types.

4. **FTS Language Support**: Currently only English tokenization is supported.

## 🔮 Future Enhancements

- [ ] Automatic index recommendations
- [ ] Query optimizer
- [ ] Distributed transactions
- [ ] More FTS languages
- [ ] GraphQL interface
- [ ] Time-travel queries using MVCC
- [ ] Built-in replication

## 📚 Examples

See the `examples/` directory for comprehensive demonstrations:

- `advanced_features.rs` - Transaction and indexing demo
- `full_features_demo.rs` - Complete feature showcase

## 🤝 Contributing

When adding new features:

1. Implement the feature for at least SQLite backend
2. Add comprehensive tests
3. Update this documentation
4. Add examples demonstrating usage