<p align="center">
<a href="https://discord.com/channels/1267505649198305384/1267505649969795136"><img width="250" align="center" alt="image" src="https://sim.build/assets/images/image02.png?v=d300fa7d"></a>
</p>

# 🦁 buffdb 🦁

buffdb is experimental software. Join buffdb’s [Discord](https://discord.gg/P7KaMw3R) for help and
have a look at [things that don’t work yet](https://github.com/buffdb/buffdb/issues/). Many basic
things are not yet decided.

This is an early implementation of a persistence layer for gRPC written in Rust and based on DuckDB.
The goal is to abstract a lot of the complexity associated with using protobufs and flattbuffers so
that mobile users can go fast.

## How to run

To run the server, you need to [have Rust installed](https://rustup.rs/). Then, with the repository
cloned, you can run

```bash
cargo run --all-features -- run
```

This will start the server on `[::1]:50051`, storing the key-value pairs in `kv_store.db` and
the blob data in `blob_store.db`. All three can be configured with command line flags:
`--addr`, `--kv-store`, and `--blob-store` respectively.

To build with optimizations enabled, run `cargo build --all-features --release`. The resulting
binary will be located at `target/release/buffdb`. It is statically linked, so it can be moved
anywhere on your file system without issue.

Prefer to handle the gRPC server yourself? `buffdb` can be used as a library as well!

## Example library usage in Rust

Run `cargo add buffdb tonic tokio futures` to add the necessary dependencies. Then you can execute
the following code, which is placed in `src/main.rs`.

```rust
use buffdb::backend::DuckDb;
use buffdb::kv::{Key, KeyValue, Value};
use tonic::{Request, IntoRequest};
use futures::{stream, StreamExt as _};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = buffdb::transitive::kv_client::<_, DuckDb>("kv_store.db").await?;
    client
        .set(stream::iter([KeyValue {
            key: "key_set".to_owned(),
            value: "value_set".to_owned(),
        }]))
        .await?
        .into_inner();

    let mut stream = client
        .get(stream::iter([Key {
            key: "key_get".to_owned(),
        }]))
        .await?
        .into_inner();
    let Value { value } = stream.next().await.unwrap()?;
    assert_eq!(value, "value_get");

    Ok(())
}
```

This project is inspired by conversations with Michael Cahill, Professor of Practice, School of
Computer Science, University of Sydney

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
- `buffdb blob eq-data [IDS]...`, exiting successfully if the blobs for all provided IDs are equal.
  Exits with an error code if any two blobs are not equal.
- `buffdb blob not-eq-data [IDS]...`, exiting successfully if the blobs for all provided IDs are
    unique. Exits with an error code if any two blobs are equal.

Commands altering a store will exit with an error code if the key/id does not exist. An exception
to this is updating the metadata of a blob to be null, as it is not required to exist beforehand.

All commands for `kv` and `blob` can use `-s`/`--store` to specify which store to use. The defaults
are `kv_store.db` and `blob_store.db` respectively. To select a backend, use `-b`/`--backend`. The
default varies by which backends are enabled.

## Background

This project was inspired by our many edge customers of ours dealing with the challenges associated
with low-bandwidth and high performance. We hope that we can build a solution that is helpful for
teams tageting edge computing environments.

Today, buffdb’s primary focus is speed: we try to ensure some level of durability for which we pay a
performance penalty, but our goal is to eventually be faster than any other embedded database.

### High-level Goals

- Reducing the overhead of serialization/deserialization.
- Ensuring consistent data formats between local storage and network communication.
- Providing faster read/write operations compared to JSON or XML.
- Compact Data Storage: ProtoBufs can significantly reduce the size of stored data.
- Interoperability: Seamless integration between the app’s local storage and backend systems.

### Use Cases

- Offline Data Access: For apps that need to function offline (e.g., note-taking apps, games,
  fieldwork, airline, collaborative documents, etc.).
- IoT: For managing device configurations and states locally before syncing with cloud servers.
