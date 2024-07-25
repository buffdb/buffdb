use std::net::SocketAddr;
use std::path::PathBuf;

pub use buffdb::blob::*;
pub use buffdb::kv::*;
pub use buffdb::Location;
pub use buffdb::RpcResponse;
use clap::Parser;
use tonic::transport::Server;

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

    let kv_store = buffdb::kv::KvStore::new(kv_store);
    let blob_store = buffdb::blob::BlobStore::new(blob_store);

    Server::builder()
        .add_service(buffdb::kv::KeyValueServer::new(kv_store))
        .add_service(buffdb::blob::BlobServer::new(blob_store))
        .serve(addr)
        .await?;

    Ok(())
}
