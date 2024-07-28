mod cli;

use crate::cli::{BlobArgs, BlobUpdateMode, Command, KvArgs, RunArgs};
use anyhow::{bail, Result};
use buffdb::blob::{BlobData, BlobId, BlobIds, BlobRpc, BlobServer, BlobStore, UpdateRequest};
use buffdb::kv::{self, KvRpc, KvServer, KvStore};
use clap::Parser as _;
use std::path::PathBuf;
use std::process::ExitCode;
use tokio::fs;
use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _};
use tonic::transport::Server;
use tonic::IntoRequest as _;

const SUCCESS: ExitCode = ExitCode::SUCCESS;
const FAILURE: ExitCode = ExitCode::FAILURE;

fn main() -> Result<ExitCode> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed building the Runtime")
        .block_on(async {
            match Command::parse() {
                Command::Run(args) => run(args).await,
                Command::Kv(args) => kv(args).await,
                Command::Blob(args) => blob(args).await,
            }
        })
}

async fn run(
    RunArgs {
        kv_store,
        blob_store,
        addr,
    }: RunArgs,
) -> Result<ExitCode> {
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
        .add_service(KvServer::new(kv_store))
        .add_service(BlobServer::new(blob_store))
        .serve(addr)
        .await?;

    Ok(SUCCESS)
}

async fn kv(KvArgs { store, command }: KvArgs) -> Result<ExitCode> {
    let store = KvStore::new(store);
    match command {
        cli::KvCommand::Get { keys } => {
            let kv::Values { values } = store
                .get_many(kv::Keys { keys }.into_request())
                .await?
                .into_inner();
            let mut stdout = io::stdout();
            let first = values.first();
            // TODO use `intersperse` when it stabilizes
            if let Some(first) = first {
                stdout.write_all(first.as_bytes()).await?;
            }
            for value in values.into_iter().skip(1) {
                stdout.write_all(&[0]).await?;
                stdout.write_all(value.as_bytes()).await?;
            }
        }
        cli::KvCommand::Set { key, value } => {
            store
                .set(kv::KeyValue { key, value }.into_request())
                .await?;
        }
        cli::KvCommand::Delete { key } => {
            store.delete(kv::Key { key }.into_request()).await?;
        }
        cli::KvCommand::Eq { keys } => {
            let all_eq = store
                .eq(kv::Keys { keys }.into_request())
                .await?
                .into_inner()
                .value;
            if !all_eq {
                return Ok(FAILURE);
            }
        }
        cli::KvCommand::NotEq { keys } => {
            let all_neq = store
                .not_eq(kv::Keys { keys }.into_request())
                .await?
                .into_inner()
                .value;
            if !all_neq {
                return Ok(FAILURE);
            }
        }
    }

    Ok(SUCCESS)
}

async fn blob(BlobArgs { store, command }: BlobArgs) -> Result<ExitCode> {
    let store = BlobStore::new(store);
    match command {
        cli::BlobCommand::Get { id, mode } => {
            let blob = store.get(BlobId { id }.into_request()).await?.into_inner();
            match mode {
                cli::BlobGetMode::Data => io::stdout().write_all(&blob.bytes).await?,
                cli::BlobGetMode::Metadata => {
                    if let Some(metadata) = blob.metadata {
                        io::stdout().write_all(metadata.as_bytes()).await?
                    }
                }
                cli::BlobGetMode::All => {
                    let mut stdout = io::stdout();
                    if let Some(metadata) = blob.metadata {
                        stdout.write_all(metadata.as_bytes()).await?;
                    }
                    stdout.write_all(&[0]).await?;
                    stdout.write_all(&blob.bytes).await?;
                }
            }
        }
        cli::BlobCommand::Store {
            file_path,
            metadata,
        } => {
            let BlobId { id } = store
                .store(
                    BlobData {
                        bytes: read_file_or_stdin(file_path).await?,
                        metadata,
                    }
                    .into_request(),
                )
                .await?
                .into_inner();
            println!("{id}");
        }
        cli::BlobCommand::Update {
            id,
            mode: BlobUpdateMode::Data { file_path },
        } => {
            store
                .update(
                    UpdateRequest {
                        id,
                        bytes: Some(read_file_or_stdin(file_path).await?),
                        should_update_metadata: false,
                        metadata: None,
                    }
                    .into_request(),
                )
                .await?;
        }
        cli::BlobCommand::Update {
            id,
            mode: BlobUpdateMode::Metadata { metadata },
        } => {
            store
                .update(
                    UpdateRequest {
                        id,
                        bytes: None,
                        should_update_metadata: true,
                        metadata,
                    }
                    .into_request(),
                )
                .await?;
        }
        cli::BlobCommand::Update {
            id,
            mode:
                BlobUpdateMode::All {
                    file_path,
                    metadata,
                },
        } => {
            store
                .update(
                    UpdateRequest {
                        id,
                        bytes: Some(read_file_or_stdin(file_path).await?),
                        should_update_metadata: true,
                        metadata,
                    }
                    .into_request(),
                )
                .await?;
        }
        cli::BlobCommand::Delete { id } => {
            store.delete(BlobId { id }.into_request()).await?;
        }
        cli::BlobCommand::EqData { ids } => {
            let all_eq = store
                .eq_data(BlobIds { ids }.into_request())
                .await?
                .into_inner()
                .value;
            if !all_eq {
                return Ok(FAILURE);
            }
        }
        cli::BlobCommand::NotEqData { ids } => {
            let all_neq = store
                .not_eq_data(BlobIds { ids }.into_request())
                .await?
                .into_inner()
                .value;
            if !all_neq {
                return Ok(FAILURE);
            }
        }
    }
    Ok(SUCCESS)
}

async fn read_file_or_stdin(file_path: PathBuf) -> io::Result<Vec<u8>> {
    if file_path == PathBuf::from("-") {
        let mut bytes = Vec::new();
        io::stdin().read_to_end(&mut bytes).await?;
        Ok(bytes)
    } else {
        Ok(fs::read(file_path).await?)
    }
}
