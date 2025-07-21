# 🦁 BuffDB

[![License: MIT/Apache](https://img.shields.io/badge/License-MIT%2FApache-blue.svg)](LICENSE-MIT)
[![Tests](https://github.com/buffdb/buffdb/actions/workflows/test.yml/badge.svg)](https://github.com/buffdb/buffdb/actions/workflows/test.yml)
[![Discord](https://img.shields.io/discord/1267505649198305384?label=Discord&logo=discord)](https://discord.gg/4Pzv6sB8)
[![Crates.io](https://img.shields.io/crates/v/buffdb.svg)](https://crates.io/crates/buffdb)

BuffDB is a lightweight, high-performance embedded database with networking capabilities, designed for edge computing and offline-first applications. Built in Rust with <2MB binary size.

**⚠️ Experimental:** Join our [Discord](https://discord.gg/4Pzv6sB8) for help. See [known issues](https://github.com/buffdb/buffdb/issues/11).

## ✨ Key Features

- 🚀 **High Performance** - Optimized for speed with multiple backend options
- 🌐 **gRPC Network API** - Access your database over the network
- 🔄 **Transactions** - ACID compliance with isolation levels
- 🔍 **Full-Text Search** - Built-in FTS with BM25 ranking
- 📊 **Secondary Indexes** - B-tree and hash indexes for fast queries
- 📄 **JSON Documents** - Document store with JSONPath queries
- 🔐 **MVCC** - Multi-version concurrency control for better performance
- 📦 **Tiny Size** - Under 2MB binary with SQLite backend

## 🚀 Quick Start

### Prerequisites

BuffDB requires `protoc` (Protocol Buffers compiler) for building:

```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler

# macOS
brew install protobuf

# Windows
choco install protoc
```

### Install BuffDB Server

```bash
# Install from crates.io
cargo install buffdb

# Or clone and build
git clone https://github.com/buffdb/buffdb
cd buffdb
cargo build --release

# Start the server
buffdb run
```

### Language Examples

<details>
<summary><b>🦀 Rust</b></summary>

```rust
use buffdb::{Connection, proto::kv::*};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to BuffDB
    let mut client = Connection::connect("http://[::1]:9313").await?;
    
    // Simple key-value operations
    client.kv_set("user:1", "Alice").await?;
    let value = client.kv_get("user:1").await?;
    println!("Value: {}", value.unwrap());
    
    // Use transactions
    let tx_id = client.begin_transaction().await?;
    client.kv_set_tx("counter", "100", &tx_id).await?;
    client.commit_transaction(&tx_id).await?;
    
    Ok(())
}
```

Add to `Cargo.toml`:
```toml
[dependencies]
buffdb = "0.4"
tokio = { version = "1", features = ["full"] }
```
</details>

<details>
<summary><b>🟦 TypeScript / Node.js</b></summary>

```typescript
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// Load proto definitions
const packageDefinition = protoLoader.loadSync('buffdb.proto');
const buffdb = grpc.loadPackageDefinition(packageDefinition).buffdb;

// Connect to BuffDB
const client = new buffdb.kv.Kv(
  '[::1]:9313',
  grpc.credentials.createInsecure()
);

// Simple key-value operations
const setStream = client.Set();
setStream.write({ key: 'user:1', value: 'Alice' });
setStream.end();

const getStream = client.Get();
getStream.on('data', (data) => {
  console.log('Value:', data.value);
});
getStream.write({ key: 'user:1' });
getStream.end();
```

Install dependencies:
```bash
npm install @grpc/grpc-js @grpc/proto-loader
```
</details>

<details>
<summary><b>🐍 Python</b></summary>

```python
import grpc
import buffdb_pb2
import buffdb_pb2_grpc

# Connect to BuffDB
channel = grpc.insecure_channel('[::1]:9313')
stub = buffdb_pb2_grpc.KvStub(channel)

# Simple key-value operations
def set_key_value():
    request = buffdb_pb2.SetRequest(key='user:1', value='Alice')
    for response in stub.Set(iter([request])):
        print(f'Set key: {response.key}')

def get_value():
    request = buffdb_pb2.GetRequest(key='user:1')
    for response in stub.Get(iter([request])):
        print(f'Value: {response.value}')

set_key_value()
get_value()
```

Install dependencies:
```bash
pip install grpcio grpcio-tools
```
</details>

<details>
<summary><b>☕ Java</b></summary>

```java
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import buffdb.KvGrpc;
import buffdb.KvOuterClass.*;

public class BuffDBExample {
    public static void main(String[] args) {
        // Connect to BuffDB
        ManagedChannel channel = ManagedChannelBuilder
            .forAddress("localhost", 9313)
            .usePlaintext()
            .build();
        
        KvGrpc.KvStub stub = KvGrpc.newStub(channel);
        
        // Simple key-value operations
        StreamObserver<SetRequest> setStream = stub.set(
            new StreamObserver<SetResponse>() {
                @Override
                public void onNext(SetResponse response) {
                    System.out.println("Set key: " + response.getKey());
                }
                // ... implement other methods
            }
        );
        
        setStream.onNext(SetRequest.newBuilder()
            .setKey("user:1")
            .setValue("Alice")
            .build());
        setStream.onCompleted();
    }
}
```

Add to `build.gradle`:
```gradle
dependencies {
    implementation 'io.grpc:grpc-netty:1.58.0'
    implementation 'io.grpc:grpc-protobuf:1.58.0'
    implementation 'io.grpc:grpc-stub:1.58.0'
}
```
</details>

<details>
<summary><b>🔷 Go</b></summary>

```go
package main

import (
    "context"
    "log"
    pb "your-module/buffdb"
    "google.golang.org/grpc"
)

func main() {
    // Connect to BuffDB
    conn, err := grpc.Dial("[::1]:9313", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()
    
    client := pb.NewKvClient(conn)
    
    // Simple key-value operations
    setStream, err := client.Set(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    
    err = setStream.Send(&pb.SetRequest{
        Key:   "user:1",
        Value: "Alice",
    })
    if err != nil {
        log.Fatal(err)
    }
    setStream.CloseSend()
    
    // Get value
    getStream, err := client.Get(context.Background())
    getStream.Send(&pb.GetRequest{Key: "user:1"})
    
    resp, err := getStream.Recv()
    if err == nil {
        log.Printf("Value: %s", resp.Value)
    }
}
```

Install dependencies:
```bash
go get google.golang.org/grpc
```
</details>

## 🛠️ Installation Options

### Binary Release
```bash
# Linux/macOS
curl -L https://github.com/buffdb/buffdb/releases/latest/download/buffdb-$(uname -s)-$(uname -m) -o buffdb
chmod +x buffdb
./buffdb run

# Using cargo
cargo install buffdb
```

### Docker
```bash
docker run -p 9313:9313 ghcr.io/buffdb/buffdb:latest
```

### From Source
```bash
git clone https://github.com/buffdb/buffdb
cd buffdb
cargo build --release
./target/release/buffdb run
```

## 📚 Advanced Features

### Transactions
```rust
// Begin transaction
let tx_id = client.begin_transaction(false, Some(5000)).await?;

// Operations within transaction
client.set_in_transaction(&tx_id, "key1", "value1").await?;
client.set_in_transaction(&tx_id, "key2", "value2").await?;

// Commit or rollback
client.commit_transaction(&tx_id).await?;
```

### Secondary Indexes
```rust
// Create index
store.create_index(IndexConfig {
    name: "email_idx".to_string(),
    index_type: IndexType::Hash,
    unique: true,
    filter: None,
})?;

// Query using index (automatic on KV operations)
```

### Full-Text Search
```rust
// Create FTS index
let fts = FtsIndex::new("articles".to_string());
fts.index_document("doc1", "Search content here")?;

// Search
let results = fts.search("content", 10);
```

### JSON Documents
```rust
// Store JSON document
let doc = json!({
    "name": "Alice",
    "email": "alice@example.com"
});
json_store.insert("user:1", doc).await?;

// Query with JSONPath
json_store.patch("user:1", "$.email", json!("newemail@example.com")).await?;
```

## 🔧 Configuration

### CLI Options
```bash
buffdb run --addr [::1]:9313 --kv-store kv.db --blob-store blob.db
```

### Backends
| Backend | Feature Flag | Performance | Use Case |
|---------|-------------|-------------|----------|
| SQLite | `vendored-sqlite` | Balanced | General purpose |
| DuckDB | `vendored-duckdb` | Analytics | OLAP workloads |

## 🏗️ Architecture

BuffDB combines embedded database efficiency with network accessibility:

```
┌─────────────┐     gRPC      ┌─────────────┐
│   Client    │ ◄──────────► │   BuffDB    │
│ (Any Lang)  │               │   Server    │
└─────────────┘               └──────┬──────┘
                                     │
                              ┌──────┴──────┐
                              │   Backend   │
                              │  (SQLite/   │
                              │   DuckDB)   │
                              └─────────────┘
```

## 📊 Performance

- **Binary Size**: <2MB (SQLite backend)
- **Startup Time**: <10ms
- **Throughput**: 100K+ ops/sec (varies by backend)
- **Latency**: <1ms for local operations

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.

## TypeScript interaction

Using auto-generated code from the protobuf definition, we can interact with the server in many
languages. The following example demonstrates how to interact with the server in TypeScript. The
server is assumed to be running on port 9313. See the `examples` directory for demos in other
languages.

```typescript
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import type { ProtoGrpcType as BlobType } from "./proto/blob";
import type { StoreResponse } from "./proto/buffdb/blob/StoreResponse";

const proto = grpc.loadPackageDefinition(
    protoLoader.loadSync("../../proto/blob.proto")
) as unknown as BlobType;

const blob_client = new proto.buffdb.blob.Blob(
    "[::1]:9313",
    grpc.credentials.createInsecure()
);
const get = blob_client.Get();
const store = blob_client.Store();

// Be sure to set up the listeners before writing data!

get.on("data", (data) => console.log("received data from GET: ", data));
get.on("end", () => console.log("stream GET ended"));

const blob_ids: number[] = [];
store.on("data", (raw_id: StoreResponse) => {
    const id = (raw_id.id as protoLoader.Long).toNumber();
    console.log("received data from STORE: ", id);
    blob_ids.push(id);
});
store.on("end", () => console.log("stream STORE ended"));

store.write({ bytes: Buffer.from("abcdef"), metadata: "{ offset: 6 }" });
store.write({ bytes: Buffer.from("ghijkl") });

store.end();

// Give the store time to finish its operations before asking for data back.
// We could also do this in the callback of other events to be certain that it's been inserted.
setTimeout(() => {
    for (const id of blob_ids) {
        console.log(`requesting ${id}`);
        get.write({ id });
    }
    get.end();
}, 100);
```

This example is present in `examples/typescript`. To run it, you need to have Node.js installed. Run
`npm i` to install the dependencies and `npm run exec` to run the example.

## How to use

### Supported backends

| Backend | Support status | Raw query support | Feature flag (vendored)      | CLI flag     |
| ------- | -------------- | ----------------- | ---------------------------- | ------------ |
| SQLite  | Full support   | ✅                | `sqlite` (`vendored-sqlite`) | `-b sqlite`  |
| DuckDB  | Partial        | ✅                | `duckdb` (`vendored-duckdb`) | `-b duckdb`  |

Blockers for full DuckDB support include [duckdb/duckdb-rs#368](https://github.com/duckdb/duckdb-rs/issues/368),
but other issues are necessary to have best performance.

By default, all backends are included and vendored. To exclude a backend, use the
`--no-default-features` flag with cargo and re-enable the desired backend with `--features`. **If
you encounter unexpected errors, consider using a vendored backend.**

### Server

To run the server, you need to [have Rust installed](https://rustup.rs/). Then, with the repository
cloned, you can run

```bash
cargo run --all-features -- run
```

This will start the server on `[::1]:9313`, storing the key-value pairs in `kv_store.db` and
the blob data in `blob_store.db`. All three can be configured with command line flags:
`--addr`, `--kv-store`, and `--blob-store` respectively.

To build with optimizations enabled, run `cargo build --all-features --release`. The resulting
binary will be located at `target/release/buffdb`. It is statically linked (excluding the backends
depending on flags), so it can be moved anywhere on your file system without issue.

Prefer to handle the gRPC server yourself? `buffdb` can be used as a library as well!

### Command line interface

You can use `buffdb help` to see the commands and flags permitted. The following operations are
currently supported:

- `buffdb run [ADDR]`, starting the server. The default address is `[::1]:9313`.
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

### Library usage in Rust

Run `cargo add buffdb tonic tokio futures` to add the necessary dependencies. Then you can execute
the following code, which is placed in `src/main.rs`.

```rust
use buffdb::backend::Sqlite;
use buffdb::kv::{Key, KeyValue, Value};
use tonic::{Request, IntoRequest};
use futures::{stream, StreamExt as _};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = buffdb::transitive::kv_client::<_, Sqlite>("kv_store.db").await?;
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
