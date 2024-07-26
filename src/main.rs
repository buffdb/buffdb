use std::fmt;
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
    #[clap(long, default_value = "kv_store.db")]
    kv_store: PathBuf,
    /// The file to store blobs in.
    #[clap(long, default_value = "blob_store.db")]
    blob_store: PathBuf,
    #[clap(long, default_value = "[::1]:50051")]
    addr: SocketAddr,
}

#[derive(Debug)]
struct StoresCannotBeAtSameLocation;

impl fmt::Display for StoresCannotBeAtSameLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "kv_store and blob_store cannot be at the same location")
    }
}

impl std::error::Error for StoresCannotBeAtSameLocation {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Opts {
        kv_store,
        blob_store,
        addr,
    } = Opts::parse();

    if kv_store == blob_store {
        return Err(Box::new(&StoresCannotBeAtSameLocation) as _);
    } else {
        // Rust's standard library has extension traits for Unix and Windows. Windows doesn't have
        // the concept of hard links, so there's no need to check an equivalent of inodes.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let kv_store_metadata = std::fs::metadata(&kv_store);
            let blob_store_metadata = std::fs::metadata(&blob_store);

            if let Some((kv_store_metadata, blob_store_metadata)) =
                kv_store_metadata.ok().zip(blob_store_metadata.ok())
            {
                if kv_store_metadata.ino() == blob_store_metadata.ino() {
                    return Err(Box::new(&StoresCannotBeAtSameLocation) as _);
                }
            }
        }
    }

    let kv_store = buffdb::kv::KvStore::new(kv_store);
    let blob_store = buffdb::blob::BlobStore::new(blob_store);

    Server::builder()
        .add_service(buffdb::kv::KeyValueServer::new(kv_store))
        .add_service(buffdb::blob::BlobServer::new(blob_store))
        .serve(addr)
        .await?;

    Ok(())
}
