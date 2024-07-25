#![allow(unused)] // TODO temporary

pub mod store;

use rusqlite::{Connection, Error as SqliteError, Result as SqliteResult};
use tonic::transport::Server;

type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let kv_store = store::kv::KvStore::new("kv_store.sqlite3");
    let blob_store = store::blob::BlobStore::new("blob_store.sqlite3");
    // let stream_store = store::stream::StreamStore::new("stream_store.sqlite3")?;

    Server::builder()
        .add_service(store::kv::KeyValueServer::new(kv_store))
        .add_service(store::blob::BlobServer::new(blob_store))
        // .add_service(StreamServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
