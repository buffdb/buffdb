mod cli;

use anyhow::{bail, Result};
use buffdb::blob::{self, BlobRpc, BlobServer, BlobStore};
use buffdb::kv::{self, KeyValueRpc, KeyValueServer, KvStore};
use clap::Parser as _;
use tokio::io::AsyncWriteExt as _;
use tonic::transport::Server;
use tonic::Request;

use crate::cli::{BlobArgs, Command, KvArgs, RunArgs};

#[tokio::main]
async fn main() -> Result<()> {
    match Command::parse() {
        Command::Run(args) => run(args).await,
        Command::Kv(args) => kv(args).await,
        Command::Blob(args) => blob(args).await,
    }
}

async fn run(
    RunArgs {
        kv_store,
        blob_store,
        addr,
    }: RunArgs,
) -> Result<()> {
    if kv_store == blob_store {
        bail!("kv_store and blob_store cannot be at the same location");
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
                    bail!("kv_store and blob_store cannot be at the same location");
                }
            }
        }
    }

    let kv_store = KvStore::new(kv_store);
    let blob_store = BlobStore::new(blob_store);

    Server::builder()
        .add_service(KeyValueServer::new(kv_store))
        .add_service(BlobServer::new(blob_store))
        .serve(addr)
        .await?;

    Ok(())
}

async fn kv(KvArgs { store, command }: KvArgs) -> Result<()> {
    let store = KvStore::new(store);
    match command {
        cli::KvCommand::Get { key } => {
            let value = store
                .get(Request::new(kv::Key { key }))
                .await?
                .into_inner()
                .value;
            println!("{value}");
            Ok(())
        }
        cli::KvCommand::Set { key, value } => {
            store.set(Request::new(kv::KeyValue { key, value })).await?;
            Ok(())
        }
        cli::KvCommand::Delete { key } => {
            store.delete(Request::new(kv::Key { key })).await?;
            Ok(())
        }
    }
}

// TODO support `-` as a file path to read from stdin for store & update
async fn blob(BlobArgs { store, command }: BlobArgs) -> Result<()> {
    let store = BlobStore::new(store);
    match command {
        cli::BlobCommand::Get { id, mode } => {
            let blob = store
                .get(Request::new(blob::BlobId { id }))
                .await?
                .into_inner();
            match mode {
                cli::BlobGetMode::Data => tokio::io::stdout().write_all(&blob.bytes).await?,
                cli::BlobGetMode::Metadata => {
                    if let Some(metadata) = blob.metadata {
                        tokio::io::stdout().write_all(metadata.as_bytes()).await?
                    }
                }
                cli::BlobGetMode::All => todo!(), // TODO what format should this be in?
            }
            Ok(())
        }
        cli::BlobCommand::Store {
            file_path,
            metadata,
        } => todo!(),
        cli::BlobCommand::Update {
            id,
            file_path,
            metadata,
        } => todo!(),
        cli::BlobCommand::Delete { id } => {
            store.delete(Request::new(blob::BlobId { id })).await?;
            Ok(())
        }
    }
}
