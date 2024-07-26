# buffDB

This is an early implementation of a persistence layer for gRPC written in Rust and based on RocksDB.
The goal is to abstract a lot of the complexity associated with using protobufs and flattbuffers so
that mobile users can go fast.

## How to run

To run the server, you need to [have Rust installed](https://rustup.rs/). Then, with the repository
cloned, you can run

```bash
cargo run
```

This will start the server on `[::1]:50051`, storing the key-value pairs in `kv_store.db` and
the blob data in `blob_store.db`. All three can be configured with command line flags:
`--addr`, `--kv-store`, and `--blob-store` respectively.

To build with optimizations enabled, run `cargo build --release`. The resulting binary will be
located at `target/release/buffdb`. It is statically linked, so it can be moved anywhere on your
file system without issue.

Prefer to handle the gRPC server yourself? `buffdb` can be used as a library as well!

## Example usage in Rust

Run `cargo add buffdb tonic tokio` to add the necessary dependencies. Then you can execute the
following code:

```rust
use buffdb::kv::{Key, KeyValue, KeyValueRpc, KvStore};
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = KvStore::new_in_memory();

    store
        .set(Request::new(KeyValue {
            key: "key".to_owned(),
            value: "value".to_owned(),
        }))
        .await?;

    let response = store
        .delete(Request::new(Key {
            key: "key".to_owned(),
        }))
        .await?;
    assert_eq!(response.get_ref().value, "value");

    let response = store
        .get(Request::new(Key {
            key: "key".to_owned(),
        }))
        .await;
    assert!(response.is_err());

    Ok(())
}
```

This project is inspired by conversations with Michael Cahill, Professor of Practice, School of Computer Science, University of Sydney
