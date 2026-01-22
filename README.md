# 🦁 BuffDB

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Tests](https://github.com/buffdb/buffdb/actions/workflows/test.yml/badge.svg)](https://github.com/buffdb/buffdb/actions/workflows/test.yml)
[![Crates.io](https://img.shields.io/crates/v/buffdb.svg)](https://crates.io/crates/buffdb)

BuffDB is a lightweight, high-performance embedded database for model storage with networking capabilities, designed for edge computing and offline-first applications. Built in Rust with <2MB binary size.

**⚠️ Experimental:** This project is rapidly evolving. If you are trying it out and hit a roadblock, please open an [issue](https://github.com/buffdb/buffdb/issues).

## Key Features

- **High Performance** - Optimized for speed with SQLite backend
- **gRPC Network API** - Access your database over the network
- **Key-Value Store** - Fast key-value operations with streaming support
- **BLOB Storage** - Binary large object storage with metadata
- **Secondary Indexes** - Hash and B-tree indexes for value-based queries
- **Raw SQL Queries** - Execute SQL directly on the underlying database
- **Tiny Size** - Under 2MB binary with SQLite backend
-  **Pure Rust** - Safe, concurrent, and memory-efficient

## 🚀 Quick Start

### Prerequisites

BuffDB requires `protoc` (Protocol Buffers compiler):

```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler

# macOS
brew install protobuf

# Windows
choco install protoc
```

### macOS Setup

macOS users need additional dependencies due to linking requirements:

```bash
# Install required dependencies
brew install protobuf sqlite libiconv

# Clone the repository
git clone https://github.com/buffdb/buffdb
cd buffdb

# The project includes a .cargo/config.toml that sets up the correct paths
# If you still encounter linking errors, you can manually set:
export LIBRARY_PATH="/opt/homebrew/lib:$LIBRARY_PATH"
export RUSTFLAGS="-L/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/lib"
```

### Building and Running

#### Option 1: Install from crates.io
```bash
cargo install buffdb
buffdb run
```

#### Option 2: Build from source
```bash
# Build with all features (includes all backends)
cargo build --all-features --release

# Run the server
./target/release/buffdb run

# Or run directly with cargo
cargo run --all-features -- run
```

#### Option 3: Quick development build
```bash
# For development with faster compilation
cargo build --features sqlite
cargo run --features sqlite -- run
```

### Language Examples

<details>
<summary><b>🦀 Rust</b></summary>

```rust
use buffdb::client::{blob::BlobClient, kv::KvClient};
use buffdb::proto::{blob, kv};
use buffdb::inference::{ModelInfo, ModelKeys};
use tonic::transport::Channel;
use futures::StreamExt;
use serde_json;
use chrono;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to BuffDB
    let channel = Channel::from_static("http://[::1]:9313").connect().await?;
    let mut kv_client = KvClient::new(channel.clone());
    let mut blob_client = BlobClient::new(channel);
    
    // 1. Store ML model
    let model_info = ModelInfo {
        name: "llama2".to_string(),
        version: "7b-v1.0".to_string(),
        framework: "pytorch".to_string(),
        description: "LLaMA 2 7B base model".to_string(),
        input_shape: vec![1, 512], // batch_size, sequence_length
        output_shape: vec![1, 512, 32000], // batch_size, sequence_length, vocab_size
        blob_ids: vec![],
        created_at: chrono::Utc::now().to_rfc3339(),
        parameters: Default::default(),
    };
    
    // Store model weights (simulate with dummy data)
    let model_weights = vec![0u8; 1024 * 1024]; // 1MB dummy weights
    let store_request = blob::StoreRequest {
        bytes: model_weights,
        metadata: Some(serde_json::json!({
            "model": model_info.name,
            "version": model_info.version,
            "type": "weights"
        }).to_string()),
        transaction_id: None,
    };
    
    let mut blob_response = blob_client
        .store(tokio_stream::once(store_request))
        .await?
        .into_inner();
    
    let blob_id = blob_response.next().await.unwrap()?.id;
    
    // Store model metadata
    let mut model_info_with_blob = model_info.clone();
    model_info_with_blob.blob_ids = vec![blob_id];
    
    let metadata_key = ModelKeys::metadata_key(&model_info.name, &model_info.version);
    let set_request = kv::SetRequest {
        key: metadata_key,
        value: serde_json::to_string(&model_info_with_blob)?,
        transaction_id: None,
    };
    
    kv_client.set(tokio_stream::once(set_request)).await?;
    
    // 2. Load model for inference
    let get_request = kv::GetRequest {
        key: ModelKeys::metadata_key("llama2", "7b-v1.0"),
        transaction_id: None,
    };
    
    let mut response = kv_client
        .get(tokio_stream::once(get_request))
        .await?
        .into_inner();
    
    if let Some(result) = response.next().await {
        let model_info: ModelInfo = serde_json::from_str(&result?.value)?;
        println!("Loaded model: {} v{}", model_info.name, model_info.version);
        println!("Framework: {}", model_info.framework);
        println!("Parameters shape: {:?}", model_info.output_shape);
        
        // Load model weights
        for blob_id in &model_info.blob_ids {
            let get_request = blob::GetRequest {
                id: *blob_id,
                transaction_id: None,
            };
            
            let mut blob_response = blob_client
                .get(tokio_stream::once(get_request))
                .await?
                .into_inner();
            
            if let Some(result) = blob_response.next().await {
                let weights = result?.bytes;
                println!("Loaded model weights: {} bytes", weights.len());
                // Here you would load weights into your ML framework
            }
        }
    }
    
    Ok(())
}
```

Add to `Cargo.toml`:
```toml
[dependencies]
buffdb = "0.5"
tokio = { version = "1", features = ["full"] }
tonic = "0.12"
futures = "0.3"
serde_json = "1.0"
chrono = "0.4"
tokio-stream = "0.1"
```
</details>

<details>
<summary><b>🟦 TypeScript / Node.js</b></summary>

```typescript
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// Load proto definitions
const kvProto = protoLoader.loadSync('kv.proto');
const blobProto = protoLoader.loadSync('blob.proto');
const kvDef = grpc.loadPackageDefinition(kvProto).buffdb.kv;
const blobDef = grpc.loadPackageDefinition(blobProto).buffdb.blob;

// Connect to BuffDB
const kvClient = new kvDef.Kv('[::1]:9313', grpc.credentials.createInsecure());
const blobClient = new blobDef.Blob('[::1]:9313', grpc.credentials.createInsecure());

// Model metadata interface
interface ModelInfo {
  name: string;
  version: string;
  framework: string;
  description: string;
  input_shape: number[];
  output_shape: number[];
  blob_ids: number[];
  created_at: string;
  parameters: Record<string, string>;
}

// 1. Store ML model
async function storeModel() {
  const modelInfo: ModelInfo = {
    name: 'bert-base',
    version: 'uncased-v1',
    framework: 'tensorflow',
    description: 'BERT base uncased model',
    input_shape: [1, 512], // batch_size, sequence_length
    output_shape: [1, 512, 768], // batch_size, sequence_length, hidden_size
    blob_ids: [],
    created_at: new Date().toISOString(),
    parameters: { 'attention_heads': '12', 'hidden_layers': '12' }
  };

  // Store model weights (simulate with dummy data)
  const modelWeights = Buffer.alloc(1024 * 1024); // 1MB dummy weights
  
  // Store weights as blob
  const blobStream = blobClient.Store();
  const blobId = await new Promise<number>((resolve, reject) => {
    blobStream.on('data', (response) => resolve(response.id));
    blobStream.on('error', reject);
    
    blobStream.write({
      bytes: modelWeights,
      metadata: JSON.stringify({
        model: modelInfo.name,
        version: modelInfo.version,
        type: 'weights'
      })
    });
    blobStream.end();
  });

  // Update model info with blob ID
  modelInfo.blob_ids = [blobId];

  // Store model metadata
  const kvStream = kvClient.Set();
  await new Promise<void>((resolve, reject) => {
    kvStream.on('end', resolve);
    kvStream.on('error', reject);
    
    kvStream.write({
      key: `model:${modelInfo.name}:${modelInfo.version}:metadata`,
      value: JSON.stringify(modelInfo)
    });
    kvStream.end();
  });

  console.log(`Stored model ${modelInfo.name} v${modelInfo.version}`);
  return modelInfo;
}

// 2. Load model for inference
async function loadModel(name: string, version: string): Promise<void> {
  // Get model metadata
  const kvStream = kvClient.Get();
  const modelInfo = await new Promise<ModelInfo>((resolve, reject) => {
    kvStream.on('data', (response) => {
      resolve(JSON.parse(response.value) as ModelInfo);
    });
    kvStream.on('error', reject);
    
    kvStream.write({ key: `model:${name}:${version}:metadata` });
    kvStream.end();
  });

  console.log(`Loaded model: ${modelInfo.name} v${modelInfo.version}`);
  console.log(`Framework: ${modelInfo.framework}`);
  console.log(`Output shape: ${modelInfo.output_shape}`);

  // Load model weights
  for (const blobId of modelInfo.blob_ids) {
    const blobStream = blobClient.Get();
    const weights = await new Promise<Buffer>((resolve, reject) => {
      const chunks: Buffer[] = [];
      
      blobStream.on('data', (response) => {
        chunks.push(response.bytes);
      });
      blobStream.on('end', () => {
        resolve(Buffer.concat(chunks));
      });
      blobStream.on('error', reject);
      
      blobStream.write({ id: blobId });
      blobStream.end();
    });

    console.log(`Loaded model weights: ${weights.length} bytes`);
    // Here you would load weights into your ML framework (e.g., TensorFlow.js)
  }
}

// Run example
async function main() {
  await storeModel();
  await loadModel('bert-base', 'uncased-v1');
}

main().catch(console.error);
```

Install dependencies:
```bash
npm install @grpc/grpc-js @grpc/proto-loader
npm install @types/node # For TypeScript
```
</details>

<details>
<summary><b>🐍 Python</b></summary>

```python
import grpc
import json
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict
import kv_pb2
import kv_pb2_grpc
import blob_pb2
import blob_pb2_grpc

# Connect to BuffDB
channel = grpc.insecure_channel('[::1]:9313')
kv_stub = kv_pb2_grpc.KvStub(channel)
blob_stub = blob_pb2_grpc.BlobStub(channel)

@dataclass
class ModelInfo:
    name: str
    version: str
    framework: str
    description: str
    input_shape: List[int]
    output_shape: List[int]
    blob_ids: List[int]
    created_at: str
    parameters: Dict[str, str]

# 1. Store ML model
def store_model():
    model_info = ModelInfo(
        name="gpt2",
        version="medium-v1",
        framework="pytorch",
        description="GPT-2 Medium 345M parameters",
        input_shape=[1, 1024],  # batch_size, sequence_length
        output_shape=[1, 1024, 50257],  # batch_size, sequence_length, vocab_size
        blob_ids=[],
        created_at=datetime.now().isoformat(),
        parameters={"num_layers": "24", "hidden_size": "1024", "num_heads": "16"}
    )
    
    # Store model weights (simulate with dummy data)
    model_weights = b'\x00' * (1024 * 1024)  # 1MB dummy weights
    
    # Store weights as blob
    blob_request = blob_pb2.StoreRequest(
        bytes=model_weights,
        metadata=json.dumps({
            "model": model_info.name,
            "version": model_info.version,
            "type": "weights"
        })
    )
    
    blob_responses = list(blob_stub.Store(iter([blob_request])))
    blob_id = blob_responses[0].id
    
    # Update model info with blob ID
    model_info.blob_ids = [blob_id]
    
    # Store model metadata
    metadata_key = f"model:{model_info.name}:{model_info.version}:metadata"
    kv_request = kv_pb2.SetRequest(
        key=metadata_key,
        value=json.dumps(asdict(model_info))
    )
    
    list(kv_stub.Set(iter([kv_request])))
    print(f"Stored model {model_info.name} v{model_info.version}")
    return model_info

# 2. Load model for inference
def load_model(name: str, version: str):
    # Get model metadata
    metadata_key = f"model:{name}:{version}:metadata"
    kv_request = kv_pb2.GetRequest(key=metadata_key)
    
    responses = list(kv_stub.Get(iter([kv_request])))
    if not responses:
        raise ValueError(f"Model {name} v{version} not found")
    
    model_info = ModelInfo(**json.loads(responses[0].value))
    print(f"Loaded model: {model_info.name} v{model_info.version}")
    print(f"Framework: {model_info.framework}")
    print(f"Output shape: {model_info.output_shape}")
    print(f"Parameters: {model_info.parameters}")
    
    # Load model weights
    for blob_id in model_info.blob_ids:
        blob_request = blob_pb2.GetRequest(id=blob_id)
        blob_responses = list(blob_stub.Get(iter([blob_request])))
        
        if blob_responses:
            weights = blob_responses[0].bytes
            print(f"Loaded model weights: {len(weights)} bytes")
            # Here you would load weights into your ML framework (e.g., PyTorch, TensorFlow)
            
            # Example with PyTorch (pseudo-code):
            # import torch
            # import io
            # buffer = io.BytesIO(weights)
            # model_state_dict = torch.load(buffer)
            # model.load_state_dict(model_state_dict)
    
    return model_info

# 3. List available models
def list_models():
    # Get model index (you would maintain this index)
    model_names = ["gpt2", "bert-base", "llama2"]
    
    print("Available models:")
    for model_name in model_names:
        index_key = f"model:{model_name}:index"
        kv_request = kv_pb2.GetRequest(key=index_key)
        
        try:
            responses = list(kv_stub.Get(iter([kv_request])))
            if responses:
                versions = json.loads(responses[0].value)
                print(f"  {model_name}: {versions}")
        except:
            pass  # Model not in index

# Run example
if __name__ == "__main__":
    # Store a model
    stored_model = store_model()
    
    # List available models
    list_models()
    
    # Load model for inference
    loaded_model = load_model("gpt2", "medium-v1")
```

Install dependencies:
```bash
pip install grpcio grpcio-tools

# Generate Python gRPC code from proto files
python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. proto/kv.proto proto/blob.proto
```
</details>

<details>
<summary><b>☕ Java</b></summary>

```java
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import buffdb.KvGrpc;
import buffdb.BlobGrpc;
import buffdb.KvOuterClass.*;
import buffdb.BlobOuterClass.*;
import com.google.protobuf.ByteString;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class MLModelManager {
    private final KvGrpc.KvStub kvStub;
    private final BlobGrpc.BlobStub blobStub;
    private final Gson gson = new Gson();
    
    // Model metadata class
    static class ModelInfo {
        String name;
        String version;
        String framework;
        String description;
        @SerializedName("input_shape") List<Integer> inputShape;
        @SerializedName("output_shape") List<Integer> outputShape;
        @SerializedName("blob_ids") List<Long> blobIds;
        @SerializedName("created_at") String createdAt;
        Map<String, String> parameters;
        
        ModelInfo(String name, String version, String framework) {
            this.name = name;
            this.version = version;
            this.framework = framework;
            this.blobIds = new ArrayList<>();
            this.parameters = new HashMap<>();
            this.createdAt = Instant.now().toString();
        }
    }
    
    public MLModelManager(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()
            .build();
        
        this.kvStub = KvGrpc.newStub(channel);
        this.blobStub = BlobGrpc.newStub(channel);
    }
    
    // 1. Store ML model
    public void storeModel(ModelInfo modelInfo, byte[] modelWeights) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicLong blobId = new AtomicLong();
        
        // Store model weights as blob
        StreamObserver<StoreRequest> blobStream = blobStub.store(
            new StreamObserver<StoreResponse>() {
                @Override
                public void onNext(StoreResponse response) {
                    blobId.set(response.getId());
                    modelInfo.blobIds.add(response.getId());
                    System.out.println("Stored blob with ID: " + response.getId());
                }
                
                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
                
                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            }
        );
        
        // Send blob data
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("model", modelInfo.name);
        metadata.put("version", modelInfo.version);
        metadata.put("type", "weights");
        
        blobStream.onNext(StoreRequest.newBuilder()
            .setBytes(ByteString.copyFrom(modelWeights))
            .setMetadata(gson.toJson(metadata))
            .build());
        blobStream.onCompleted();
        
        // Store model metadata
        String metadataKey = String.format("model:%s:%s:metadata", 
            modelInfo.name, modelInfo.version);
        
        StreamObserver<SetRequest> kvStream = kvStub.set(
            new StreamObserver<SetResponse>() {
                @Override
                public void onNext(SetResponse response) {
                    System.out.println("Stored model metadata: " + response.getKey());
                }
                
                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
                
                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            }
        );
        
        kvStream.onNext(SetRequest.newBuilder()
            .setKey(metadataKey)
            .setValue(gson.toJson(modelInfo))
            .build());
        kvStream.onCompleted();
        
        latch.await();
        System.out.println("Stored model " + modelInfo.name + " v" + modelInfo.version);
    }
    
    // 2. Load model for inference
    public ModelInfo loadModel(String name, String version) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        final ModelInfo[] result = new ModelInfo[1];
        
        String metadataKey = String.format("model:%s:%s:metadata", name, version);
        
        StreamObserver<GetRequest> kvStream = kvStub.get(
            new StreamObserver<GetResponse>() {
                @Override
                public void onNext(GetResponse response) {
                    result[0] = gson.fromJson(response.getValue(), ModelInfo.class);
                    System.out.println("Loaded model: " + result[0].name + " v" + result[0].version);
                    System.out.println("Framework: " + result[0].framework);
                    System.out.println("Output shape: " + result[0].outputShape);
                }
                
                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
                
                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            }
        );
        
        kvStream.onNext(GetRequest.newBuilder().setKey(metadataKey).build());
        kvStream.onCompleted();
        
        latch.await();
        
        // Load model weights
        if (result[0] != null && !result[0].blobIds.isEmpty()) {
            for (Long blobId : result[0].blobIds) {
                loadBlobWeights(blobId);
            }
        }
        
        return result[0];
    }
    
    private void loadBlobWeights(Long blobId) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        
        StreamObserver<GetRequest> blobStream = blobStub.get(
            new StreamObserver<GetResponse>() {
                @Override
                public void onNext(GetResponse response) {
                    byte[] weights = response.getBytes().toByteArray();
                    System.out.println("Loaded model weights: " + weights.length + " bytes");
                    // Here you would load weights into your ML framework (e.g., DL4J, ONNX)
                }
                
                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
                
                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            }
        );
        
        blobStream.onNext(GetRequest.newBuilder().setId(blobId).build());
        blobStream.onCompleted();
        
        latch.await();
    }
    
    public static void main(String[] args) throws InterruptedException {
        MLModelManager manager = new MLModelManager("localhost", 9313);
        
        // Create model info
        ModelInfo model = new ModelInfo("mobilenet", "v3-small", "tensorflow");
        model.description = "MobileNet V3 Small for edge devices";
        model.inputShape = Arrays.asList(1, 224, 224, 3);
        model.outputShape = Arrays.asList(1, 1000);
        model.parameters.put("alpha", "1.0");
        model.parameters.put("include_top", "true");
        
        // Store model (with dummy weights)
        byte[] dummyWeights = new byte[1024 * 1024]; // 1MB
        manager.storeModel(model, dummyWeights);
        
        // Load model
        ModelInfo loaded = manager.loadModel("mobilenet", "v3-small");
    }
}
```

Add to `build.gradle`:
```gradle
dependencies {
    implementation 'io.grpc:grpc-netty:1.58.0'
    implementation 'io.grpc:grpc-protobuf:1.58.0'
    implementation 'io.grpc:grpc-stub:1.58.0'
    implementation 'com.google.code.gson:gson:2.10.1'
}
```
</details>

<details>
<summary><b>🔷 Go</b></summary>

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "time"
    
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    
    kvpb "your-module/proto/kv"
    blobpb "your-module/proto/blob"
)

// ModelInfo represents ML model metadata
type ModelInfo struct {
    Name        string            `json:"name"`
    Version     string            `json:"version"`
    Framework   string            `json:"framework"`
    Description string            `json:"description"`
    InputShape  []int             `json:"input_shape"`
    OutputShape []int             `json:"output_shape"`
    BlobIDs     []uint64          `json:"blob_ids"`
    CreatedAt   string            `json:"created_at"`
    Parameters  map[string]string `json:"parameters"`
}

type MLModelManager struct {
    kvClient   kvpb.KvClient
    blobClient blobpb.BlobClient
}

// NewMLModelManager creates a new model manager
func NewMLModelManager(addr string) (*MLModelManager, error) {
    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    
    return &MLModelManager{
        kvClient:   kvpb.NewKvClient(conn),
        blobClient: blobpb.NewBlobClient(conn),
    }, nil
}

// StoreModel stores an ML model in BuffDB
func (m *MLModelManager) StoreModel(ctx context.Context, info *ModelInfo, weights []byte) error {
    // Store model weights as blob
    blobStream, err := m.blobClient.Store(ctx)
    if err != nil {
        return err
    }
    
    metadata := map[string]interface{}{
        "model":   info.Name,
        "version": info.Version,
        "type":    "weights",
    }
    metadataJSON, _ := json.Marshal(metadata)
    
    if err := blobStream.Send(&blobpb.StoreRequest{
        Bytes:    weights,
        Metadata: string(metadataJSON),
    }); err != nil {
        return err
    }
    
    resp, err := blobStream.CloseAndRecv()
    if err != nil {
        return err
    }
    
    // Update model info with blob ID
    info.BlobIDs = append(info.BlobIDs, resp.Id)
    
    // Store model metadata
    kvStream, err := m.kvClient.Set(ctx)
    if err != nil {
        return err
    }
    
    infoJSON, _ := json.Marshal(info)
    metadataKey := fmt.Sprintf("model:%s:%s:metadata", info.Name, info.Version)
    
    if err := kvStream.Send(&kvpb.SetRequest{
        Key:   metadataKey,
        Value: string(infoJSON),
    }); err != nil {
        return err
    }
    
    if _, err := kvStream.CloseAndRecv(); err != nil {
        return err
    }
    
    log.Printf("Stored model %s v%s", info.Name, info.Version)
    return nil
}

// LoadModel loads a model from BuffDB
func (m *MLModelManager) LoadModel(ctx context.Context, name, version string) (*ModelInfo, []byte, error) {
    // Get model metadata
    metadataKey := fmt.Sprintf("model:%s:%s:metadata", name, version)
    
    kvStream, err := m.kvClient.Get(ctx)
    if err != nil {
        return nil, nil, err
    }
    
    if err := kvStream.Send(&kvpb.GetRequest{Key: metadataKey}); err != nil {
        return nil, nil, err
    }
    
    kvResp, err := kvStream.Recv()
    if err != nil {
        return nil, nil, err
    }
    kvStream.CloseSend()
    
    var info ModelInfo
    if err := json.Unmarshal([]byte(kvResp.Value), &info); err != nil {
        return nil, nil, err
    }
    
    log.Printf("Loaded model: %s v%s", info.Name, info.Version)
    log.Printf("Framework: %s", info.Framework)
    log.Printf("Output shape: %v", info.OutputShape)
    
    // Load model weights
    var weights []byte
    for _, blobID := range info.BlobIDs {
        blobStream, err := m.blobClient.Get(ctx)
        if err != nil {
            return nil, nil, err
        }
        
        if err := blobStream.Send(&blobpb.GetRequest{Id: blobID}); err != nil {
            return nil, nil, err
        }
        
        // Collect all chunks
        for {
            resp, err := blobStream.Recv()
            if err == io.EOF {
                break
            }
            if err != nil {
                return nil, nil, err
            }
            weights = append(weights, resp.Bytes...)
        }
    }
    
    log.Printf("Loaded model weights: %d bytes", len(weights))
    return &info, weights, nil
}

// ListModels lists available models
func (m *MLModelManager) ListModels(ctx context.Context) error {
    // In a real implementation, you'd maintain a proper index
    modelNames := []string{"yolov5", "efficientnet", "whisper"}
    
    fmt.Println("Available models:")
    for _, name := range modelNames {
        indexKey := fmt.Sprintf("model:%s:index", name)
        
        kvStream, err := m.kvClient.Get(ctx)
        if err != nil {
            continue
        }
        
        if err := kvStream.Send(&kvpb.GetRequest{Key: indexKey}); err != nil {
            kvStream.CloseSend()
            continue
        }
        
        resp, err := kvStream.Recv()
        if err == nil {
            var versions []string
            json.Unmarshal([]byte(resp.Value), &versions)
            fmt.Printf("  %s: %v\n", name, versions)
        }
        kvStream.CloseSend()
    }
    
    return nil
}

func main() {
    manager, err := NewMLModelManager("[::1]:9313")
    if err != nil {
        log.Fatal(err)
    }
    
    ctx := context.Background()
    
    // Create and store a model
    model := &ModelInfo{
        Name:        "yolov5",
        Version:     "s-640",
        Framework:   "pytorch",
        Description: "YOLOv5 small model for object detection",
        InputShape:  []int{1, 3, 640, 640},
        OutputShape: []int{1, 25200, 85}, // detections, (x,y,w,h,conf,classes...)
        CreatedAt:   time.Now().Format(time.RFC3339),
        Parameters: map[string]string{
            "conf_threshold": "0.25",
            "iou_threshold":  "0.45",
            "classes":        "80",
        },
    }
    
    // Store with dummy weights
    dummyWeights := make([]byte, 1024*1024) // 1MB
    if err := manager.StoreModel(ctx, model, dummyWeights); err != nil {
        log.Fatal(err)
    }
    
    // List models
    manager.ListModels(ctx)
    
    // Load model
    loaded, weights, err := manager.LoadModel(ctx, "yolov5", "s-640")
    if err != nil {
        log.Fatal(err)
    }
    
    _ = loaded
    _ = weights
    // Here you would load the model into your inference framework
}
```

Install dependencies:
```bash
go get google.golang.org/grpc
go get google.golang.org/grpc/credentials/insecure

# Generate Go code from proto files
protoc --go_out=. --go-grpc_out=. proto/kv.proto proto/blob.proto
```
</details>

<details>
<summary><b>🍎 Swift</b></summary>

```swift
import Foundation
import GRPC
import NIO
import SwiftProtobuf

// Model metadata structure
struct ModelInfo: Codable {
    let name: String
    let version: String
    let framework: String
    let description: String
    let inputShape: [Int]
    let outputShape: [Int]
    var blobIds: [UInt64]
    let createdAt: String
    let parameters: [String: String]
    
    enum CodingKeys: String, CodingKey {
        case name, version, framework, description
        case inputShape = "input_shape"
        case outputShape = "output_shape"
        case blobIds = "blob_ids"
        case createdAt = "created_at"
        case parameters
    }
}

class MLModelManager {
    private let group: EventLoopGroup
    private let channel: ClientConnection
    private let kvClient: Buffdb_Kv_KvNIOClient
    private let blobClient: Buffdb_Blob_BlobNIOClient
    
    init(host: String = "localhost", port: Int = 9313) throws {
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        self.channel = ClientConnection
            .insecure(group: group)
            .connect(host: host, port: port)
        
        self.kvClient = Buffdb_Kv_KvNIOClient(channel: channel)
        self.blobClient = Buffdb_Blob_BlobNIOClient(channel: channel)
    }
    
    // 1. Store ML model
    func storeModel(info: inout ModelInfo, weights: Data) async throws {
        // Store model weights as blob
        let blobMetadata = [
            "model": info.name,
            "version": info.version,
            "type": "weights"
        ]
        let metadataJSON = try JSONSerialization.data(withJSONObject: blobMetadata)
        
        var storeRequest = Buffdb_Blob_StoreRequest()
        storeRequest.bytes = weights
        storeRequest.metadata = String(data: metadataJSON, encoding: .utf8) ?? ""
        
        let blobCall = blobClient.store()
        try await blobCall.sendMessage(storeRequest)
        let blobResponse = try await blobCall.response.get()
        
        // Update model info with blob ID
        info.blobIds.append(blobResponse.id)
        
        // Store model metadata
        let encoder = JSONEncoder()
        let infoJSON = try encoder.encode(info)
        
        var setRequest = Buffdb_Kv_SetRequest()
        setRequest.key = "model:\(info.name):\(info.version):metadata"
        setRequest.value = String(data: infoJSON, encoding: .utf8) ?? ""
        
        let kvCall = kvClient.set()
        try await kvCall.sendMessage(setRequest)
        _ = try await kvCall.response.get()
        
        print("Stored model \(info.name) v\(info.version)")
    }
    
    // 2. Load model for inference
    func loadModel(name: String, version: String) async throws -> (ModelInfo, Data) {
        // Get model metadata
        var getRequest = Buffdb_Kv_GetRequest()
        getRequest.key = "model:\(name):\(version):metadata"
        
        let kvCall = kvClient.get()
        try await kvCall.sendMessage(getRequest)
        
        var modelInfo: ModelInfo?
        for try await response in kvCall.responseStream {
            let decoder = JSONDecoder()
            modelInfo = try decoder.decode(ModelInfo.self, from: response.value.data(using: .utf8)!)
            break
        }
        
        guard let info = modelInfo else {
            throw NSError(domain: "MLModelManager", code: 404, 
                         userInfo: [NSLocalizedDescriptionKey: "Model not found"])
        }
        
        print("Loaded model: \(info.name) v\(info.version)")
        print("Framework: \(info.framework)")
        print("Output shape: \(info.outputShape)")
        
        // Load model weights
        var weights = Data()
        for blobId in info.blobIds {
            var blobRequest = Buffdb_Blob_GetRequest()
            blobRequest.id = blobId
            
            let blobCall = blobClient.get()
            try await blobCall.sendMessage(blobRequest)
            
            for try await response in blobCall.responseStream {
                weights.append(response.bytes)
            }
        }
        
        print("Loaded model weights: \(weights.count) bytes")
        return (info, weights)
    }
    
    deinit {
        try? group.syncShutdownGracefully()
    }
}

// Usage example
@main
struct MLModelExample {
    static func main() async throws {
        let manager = try MLModelManager()
        
        // Create model info
        var model = ModelInfo(
            name: "coreml-resnet",
            version: "50-v1",
            framework: "coreml",
            description: "ResNet50 for iOS devices",
            inputShape: [1, 224, 224, 3],
            outputShape: [1, 1000],
            blobIds: [],
            createdAt: ISO8601DateFormatter().string(from: Date()),
            parameters: ["quantized": "true", "precision": "float16"]
        )
        
        // Store with dummy weights
        let dummyWeights = Data(repeating: 0, count: 1024 * 1024) // 1MB
        try await manager.storeModel(info: &model, weights: dummyWeights)
        
        // Load model
        let (loaded, weights) = try await manager.loadModel(
            name: "coreml-resnet", 
            version: "50-v1"
        )
        
        // Here you would load into CoreML
        // let mlModel = try MLModel(contentsOf: modelURL)
    }
}
```

Add to `Package.swift`:
```swift
dependencies: [
    .package(url: "https://github.com/grpc/grpc-swift.git", from: "1.15.0"),
]
```
</details>

<details>
<summary><b>🤖 Kotlin</b></summary>

```kotlin
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.*
import kotlinx.serialization.json.*
import java.time.Instant
import java.util.concurrent.CountDownLatch

@Serializable
data class ModelInfo(
    val name: String,
    val version: String,
    val framework: String,
    val description: String,
    @SerialName("input_shape")
    val inputShape: List<Int>,
    @SerialName("output_shape")
    val outputShape: List<Int>,
    @SerialName("blob_ids")
    var blobIds: MutableList<Long> = mutableListOf(),
    @SerialName("created_at")
    val createdAt: String = Instant.now().toString(),
    val parameters: Map<String, String> = emptyMap()
)

class MLModelManager(host: String = "localhost", port: Int = 9313) {
    private val channel: ManagedChannel = ManagedChannelBuilder
        .forAddress(host, port)
        .usePlaintext()
        .build()
    
    private val kvStub = KvGrpc.newStub(channel)
    private val blobStub = BlobGrpc.newStub(channel)
    private val json = Json { prettyPrint = true }
    
    // 1. Store ML model
    suspend fun storeModel(modelInfo: ModelInfo, weights: ByteArray) = coroutineScope {
        val latch = CountDownLatch(2)
        
        // Store model weights as blob
        val blobObserver = object : StreamObserver<StoreResponse> {
            override fun onNext(response: StoreResponse) {
                modelInfo.blobIds.add(response.id)
                println("Stored blob with ID: ${response.id}")
            }
            
            override fun onError(t: Throwable) {
                t.printStackTrace()
                latch.countDown()
            }
            
            override fun onCompleted() {
                latch.countDown()
            }
        }
        
        val blobStream = blobStub.store(blobObserver)
        
        val metadata = mapOf(
            "model" to modelInfo.name,
            "version" to modelInfo.version,
            "type" to "weights"
        )
        
        blobStream.onNext(
            StoreRequest.newBuilder()
                .setBytes(ByteString.copyFrom(weights))
                .setMetadata(json.encodeToString(metadata))
                .build()
        )
        blobStream.onCompleted()
        
        // Store model metadata
        val metadataKey = "model:${modelInfo.name}:${modelInfo.version}:metadata"
        
        val kvObserver = object : StreamObserver<SetResponse> {
            override fun onNext(response: SetResponse) {
                println("Stored model metadata: ${response.key}")
            }
            
            override fun onError(t: Throwable) {
                t.printStackTrace()
                latch.countDown()
            }
            
            override fun onCompleted() {
                latch.countDown()
            }
        }
        
        val kvStream = kvStub.set(kvObserver)
        kvStream.onNext(
            SetRequest.newBuilder()
                .setKey(metadataKey)
                .setValue(json.encodeToString(modelInfo))
                .build()
        )
        kvStream.onCompleted()
        
        withContext(Dispatchers.IO) {
            latch.await()
        }
        
        println("Stored model ${modelInfo.name} v${modelInfo.version}")
    }
    
    // 2. Load model for inference
    suspend fun loadModel(name: String, version: String): Pair<ModelInfo, ByteArray> = coroutineScope {
        val metadataKey = "model:$name:$version:metadata"
        val modelInfoDeferred = CompletableDeferred<ModelInfo>()
        
        // Get model metadata
        val kvObserver = object : StreamObserver<GetResponse> {
            override fun onNext(response: GetResponse) {
                val modelInfo = json.decodeFromString<ModelInfo>(response.value)
                modelInfoDeferred.complete(modelInfo)
                
                println("Loaded model: ${modelInfo.name} v${modelInfo.version}")
                println("Framework: ${modelInfo.framework}")
                println("Output shape: ${modelInfo.outputShape}")
            }
            
            override fun onError(t: Throwable) {
                modelInfoDeferred.completeExceptionally(t)
            }
            
            override fun onCompleted() {}
        }
        
        val kvStream = kvStub.get(kvObserver)
        kvStream.onNext(GetRequest.newBuilder().setKey(metadataKey).build())
        kvStream.onCompleted()
        
        val modelInfo = modelInfoDeferred.await()
        
        // Load model weights
        val weights = mutableListOf<ByteArray>()
        for (blobId in modelInfo.blobIds) {
            val blobDeferred = CompletableDeferred<ByteArray>()
            
            val blobObserver = object : StreamObserver<GetResponse> {
                private val chunks = mutableListOf<ByteArray>()
                
                override fun onNext(response: GetResponse) {
                    chunks.add(response.bytes.toByteArray())
                }
                
                override fun onError(t: Throwable) {
                    blobDeferred.completeExceptionally(t)
                }
                
                override fun onCompleted() {
                    blobDeferred.complete(chunks.flatMap { it.toList() }.toByteArray())
                }
            }
            
            val blobStream = blobStub.get(blobObserver)
            blobStream.onNext(GetRequest.newBuilder().setId(blobId).build())
            blobStream.onCompleted()
            
            weights.add(blobDeferred.await())
        }
        
        val allWeights = weights.flatMap { it.toList() }.toByteArray()
        println("Loaded model weights: ${allWeights.size} bytes")
        
        return@coroutineScope Pair(modelInfo, allWeights)
    }
    
    fun shutdown() {
        channel.shutdown()
    }
}

// Usage example
fun main() = runBlocking {
    val manager = MLModelManager()
    
    try {
        // Create model info
        val model = ModelInfo(
            name = "tflite-mobilenet",
            version = "v2-224",
            framework = "tflite",
            description = "MobileNet V2 for Android devices",
            inputShape = listOf(1, 224, 224, 3),
            outputShape = listOf(1, 1000),
            parameters = mapOf(
                "quantized" to "true",
                "input_mean" to "127.5",
                "input_std" to "127.5"
            )
        )
        
        // Store with dummy weights
        val dummyWeights = ByteArray(1024 * 1024) // 1MB
        manager.storeModel(model, dummyWeights)
        
        // Load model
        val (loaded, weights) = manager.loadModel("tflite-mobilenet", "v2-224")
        
        // Here you would load into TensorFlow Lite
        // val interpreter = Interpreter(modelByteBuffer)
        
    } finally {
        manager.shutdown()
    }
}
```

Add to `build.gradle.kts`:
```kotlin
dependencies {
    implementation("io.grpc:grpc-kotlin-stub:1.3.0")
    implementation("io.grpc:grpc-netty:1.58.0")
    implementation("com.google.protobuf:protobuf-kotlin:3.24.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")
}
```
</details>

## Installation

```bash
# Install from crates.io
cargo install buffdb

# Docker
docker run -p 9313:9313 ghcr.io/buffdb/buffdb:latest

# From source
git clone https://github.com/buffdb/buffdb
cd buffdb
cargo build --release
./target/release/buffdb run
```

## Advanced Features

### Secondary Indexes

BuffDB supports creating secondary indexes on values for efficient lookups:

```rust
use buffdb::index::{IndexConfig, IndexManager, IndexType};

// Create an index manager
let index_manager = IndexManager::new();

// Create a hash index for exact matches
index_manager.create_index(IndexConfig {
    name: "email_idx".to_string(),
    index_type: IndexType::Hash,
    unique: true,
    filter: None,
})?;

// Create a B-tree index for range queries
index_manager.create_index(IndexConfig {
    name: "age_idx".to_string(),  
    index_type: IndexType::BTree,
    unique: false,
    filter: None,
})?;
```

### Raw SQL Queries

Execute SQL directly on the underlying SQLite database:

```bash
# Via CLI
buffdb query "SELECT * FROM kv_store WHERE key LIKE 'user:%'"

# Via gRPC (using the Query service)
```

### BLOB Storage

Store binary data with metadata:

```bash
# Store a file
buffdb blob store myfile.pdf "content-type=application/pdf"

# Retrieve by ID
buffdb blob get 12345 > myfile.pdf
```

### ML Model Storage

BuffDB's combination of KV store and BLOB storage makes it ideal for storing and serving ML models:

```rust
use buffdb::inference::{ModelInfo, ModelKeys, format_blob_metadata};
use buffdb::client::{blob::BlobClient, kv::KvClient};
use buffdb::proto::{blob, kv};

// Store a model with metadata
let model_info = ModelInfo {
    name: "resnet50".to_string(),
    version: "1.0".to_string(),
    framework: "pytorch".to_string(),
    description: "ResNet50 trained on ImageNet".to_string(),
    input_shape: vec![1, 3, 224, 224],
    output_shape: vec![1, 1000],
    blob_ids: vec![],
    created_at: chrono::Utc::now().to_rfc3339(),
    parameters: HashMap::new(),
};

// Store model weights as BLOB
let weights = std::fs::read("model.pt")?;
let blob_metadata = format_blob_metadata(&model_info.name, &model_info.version, 0);
let store_request = blob::StoreRequest {
    bytes: weights,
    metadata: Some(blob_metadata),
    transaction_id: None,
};

let blob_id = blob_client.store(stream::iter(vec![Ok(store_request)]))
    .await?
    .into_inner()
    .next()
    .await
    .unwrap()?
    .id;

// Store model metadata in KV
let metadata_key = ModelKeys::metadata_key(&model_info.name, &model_info.version);
let set_request = kv::SetRequest {
    key: metadata_key,
    value: serde_json::to_string(&model_info)?,
    transaction_id: None,
};

kv_client.set(stream::iter(vec![Ok(set_request)])).await?;
```

**Example Use Cases:**
- **Edge AI**: Deploy models to edge devices with local inference
- **Model Versioning**: Track multiple versions of models
- **A/B Testing**: Serve different model versions to different users
- **Offline Inference**: Run models without network connectivity

See the [ml_model_inference example](examples/ml_model_inference.rs) for a complete implementation.

### Hugging Face Integration

BuffDB can be used as a caching layer for Hugging Face models, enabling efficient model distribution:

```rust
// Download model from Hugging Face and cache in BuffDB
use reqwest::Client;

async fn cache_huggingface_model(
    model_id: &str,
    filename: &str,
    kv_client: &mut KvClient<Channel>,
    blob_client: &mut BlobClient<Channel>,
) -> Result<()> {
    // Download from Hugging Face
    let url = format!("https://huggingface.co/{}/resolve/main/{}", model_id, filename);
    let model_data = Client::new().get(&url).send().await?.bytes().await?;
    
    // Store in BuffDB for fast local serving
    let blob_id = store_model_blob(blob_client, model_data).await?;
    store_model_metadata(kv_client, model_id, blob_id).await?;
    
    Ok(())
}
```

**Benefits:**
- **Reduced Latency**: Serve models from local BuffDB instead of downloading
- **Bandwidth Savings**: Download once, serve many times
- **Offline Support**: Models available without internet connection
- **Version Control**: Track and serve specific model versions

See the [huggingface_integration example](examples/huggingface_integration.rs) for complete implementation.

## 🔧 Configuration

BuffDB supports configuration via TOML files, command-line arguments, or a combination of both.

### Configuration File

BuffDB automatically looks for configuration files in these locations (in order):
1. `--config <path>` - Explicit config file path
2. `./buffdb.toml` - Current directory
3. `~/.config/buffdb/buffdb.toml` - User config directory

**Example `buffdb.toml`:**
```toml
[server]
address = "[::1]:9313"

[database]
backend = "sqlite"
kv_store = "kv_store.db"
blob_store = "blob_store.db"

[logging]
level = "info"

[performance]
max_connections = 1000
request_timeout = 30
```

### Configuration Precedence

Settings are applied in this order (highest to lowest):
1. **Command-line arguments** (override everything)
2. **Configuration file** 
3. **Default values**

### CLI Options

```bash
# Using config file
buffdb run --config custom.toml

# With CLI overrides (takes precedence over config file)
buffdb run --backend sqlite --addr 127.0.0.1:8080 --kv-store custom_kv.db --blob-store custom_blob.db
```

### Configuration Options

| Section | Setting | Description | Default |
|---------|----------|-------------|---------|
| `server` | `address` | gRPC server bind address | `[::1]:9313` |
| `database` | `backend` | Database backend | `sqlite` |
| `database` | `kv_store` | Key-value store path | `kv_store.db` |
| `database` | `blob_store` | BLOB store path | `blob_store.db` |
| `logging` | `level` | Log level | `info` |
| `performance` | `max_connections` | Max concurrent connections | `None` |
| `performance` | `request_timeout` | Request timeout (seconds) | `None` |
| `performance` | `keep_alive` | Keep-alive interval (seconds) | `None` |

### Backends
| Backend | Feature Flag | Performance | Use Case | Status |
|---------|-------------|-------------|----------|--------|
| SQLite | `vendored-sqlite` | Balanced | General purpose | ✅ Stable |
| DuckDB | `duckdb` | Analytics | OLAP workloads | 🚧 Temporarily disabled |

## Architecture

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

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## 🔧 Backend Support

**⚠️ DuckDB Support (Experimental)**

DuckDB backend is currently experimental due to:
- Platform-specific linking issues (especially on macOS)
- Incomplete Rust bindings ([duckdb/duckdb-rs#368](https://github.com/duckdb/duckdb-rs/issues/368))
- Performance optimizations still in progress

For production use, we recommend SQLite backend which is stable and well-tested.

By default, SQLite backend is included and vendored. To use DuckDB (experimental), enable it with `--features duckdb`.

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

## 📚 Using BuffDB as a Library

See the [Rust example](#-rust) above for library usage.

## 🎯 Use Cases

- **Offline-First Apps**: Note-taking, games, fieldwork applications, airline systems, collaborative documents
- **IoT & Edge Computing**: Managing device configurations and states locally before cloud sync
- **Low-Bandwidth Environments**: Reducing serialization overhead with Protocol Buffers
- **Embedded Analytics**: Local data processing with optional network access

## 🔧 Troubleshooting

### macOS Linking Errors

If you encounter `ld: library not found for -liconv` errors:

1. Ensure you have the `.cargo/config.toml` file in the project root:
```toml
[build]
rustflags = ["-L/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/lib"]

[env]
LIBRARY_PATH = "/opt/homebrew/lib"
```

2. For Apple Silicon (M1/M2) Macs, ensure Homebrew is in `/opt/homebrew`:
```bash
ls /opt/homebrew/lib/libiconv*
```

3. For Intel Macs, Homebrew may be in `/usr/local`:
```bash
# Update the config.toml accordingly:
LIBRARY_PATH = "/usr/local/lib"
```

4. If issues persist, add to your shell profile (`~/.zshrc` or `~/.bash_profile`):
```bash
export LIBRARY_PATH="/opt/homebrew/lib:$LIBRARY_PATH"
export RUSTFLAGS="-L/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/lib"
```

## 🙏 Acknowledgments

This project is inspired by conversations with Michael Cahill, Professor of Practice, School of Computer Science, University of Sydney, and feedback from edge computing customers dealing with low-bandwidth, high-performance challenges.
