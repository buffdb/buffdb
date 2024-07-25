# buffDB

This is an early implementation of a persistence layer for gRPC written in Rust and based on SQLite.
The goal is to abstract a lot of the complexity associated with using protobufs and flattbuffers so
that mobile users can go fast.

## How to run

To run the server, you need to [have Rust installed](https://rustup.rs/). Then, with the repository
cloned, you can run

```bash
cargo run
```

This will start the server on `[::1]:50051`, storing the key-value pairs in `kv_store.sqlite3` and
the blob data in `blob_store.sqlite3`. All three can be configured with command line flags:
`--addr`, `--kv-store`, and `--blob-store` respectively.

To build with optimizations enabled, run `cargo build --release`. The resulting binary will be
located at `target/release/buffdb`. It is statically linked, so it can be moved anywhere on your
file system without issue.
