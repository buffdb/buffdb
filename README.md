# buffDB

This is an early implementation of a persistence layer for gRPC written in Rust and based on RocksDB.
The goal is to abstract a lot of the complexity associated with using protobufs and flattbuffers so
that mobile users can go fast.

## How to run

To run the server, you need to [have Rust installed](https://rustup.rs/). Then, with the repository
cloned, you can run

```bash
cargo run -- run
```

This will start the server on `[::1]:50051`, storing the key-value pairs in `kv_store.db` and
the blob data in `blob_store.db`. All three can be configured with command line flags:
`--addr`, `--kv-store`, and `--blob-store` respectively.

To build with optimizations enabled, run `cargo build --release`. The resulting binary will be
located at `target/release/buffdb`. It is statically linked, so it can be moved anywhere on your
file system without issue.

Prefer to handle the gRPC server yourself? `buffdb` can be used as a library as well!

## Example library usage in Rust

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

## Command line interface

You can use `buffdb help` to see the commands and flags permitted. The following operations are
currently supported:

- `buffdb run [ADDR]`, starting the server. The default address is `[::1]:50051`.
- `buffdb kv get <KEY>`, printing the value to stdout.
- `buffdb kv set <KEY> <VALUE>`, setting the value.
- `buffdb kv delete <KEY>`, deleting the value.
- `buffdb kv eq [KEYS]...`, exiting successfully if the values for all provided keys are equal.
  Exits with an error code if any two values are not equal.
- `buffdb kv not-eq [KEYS]...`, exiting successfully if the values for all provided keys are
  unique. Exits with an error code if any two values are equal.
- `buffdb blob get <ID>`, printing the data to stdout. Note that this is arbitrary bytes!
- `buffdb blob store <FILE> [METADATA]`, storing the file (use `-` for stdin) and printing the ID
  to stdout. Metadata is optional.
- `buffdb blob update <ID> data <FILE>`, updating the data of the blob. Use `-` for stdin. Metadata
  is unchanged.
- `buffdb blob update <ID> metadata [METADATA]`, updating the metadata of the blob. Data is
  unchanged. Omitting `[METADATA]` will set the metadata to null.
- `buffdb blob update <ID> all <FILE> [METADATA]`, updating both the data and metadata of the blob.
  For `<FILE>`, use `-` for stdin. Omitting `[METADATA]` will set the metadata to null.
- `buffdb blob delete <ID>`, deleting the blob.

Commands altering a store will exit with an error code if the key/id does not exist. An exception
to this is updating the metadata of a blob to be null, as it is not required to exist beforehand.

All commands for `kv` and `blob` can use `-s`/`--store` to specify which store to use. The defaults
are `kv_store.db` and `blob_store.db` respectively.
