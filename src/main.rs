#![allow(unused)] // TODO temporary

pub mod store;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use rusqlite::{Connection, Error as SqliteError, Result as SqliteResult};
use tonic::transport::Server;

type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Opts {
    /// The file to store key-value pairs in.
    #[clap(long, default_value = "kv_store.sqlite3")]
    kv_store: PathBuf,
    /// The file to store blobs in.
    #[clap(long, default_value = "blob_store.sqlite3")]
    blob_store: PathBuf,
    #[clap(long, default_value = "[::1]:50051")]
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Opts {
        kv_store,
        blob_store,
        addr,
    } = Opts::parse();

    let kv_store = store::kv::KvStore::new(kv_store);
    let blob_store = store::blob::BlobStore::new(blob_store);

    Server::builder()
        .add_service(store::kv::KeyValueServer::new(kv_store))
        .add_service(store::blob::BlobServer::new(blob_store))
        .serve(addr)
        .await?;

    Ok(())
}
