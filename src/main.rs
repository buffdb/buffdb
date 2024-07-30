mod cli;

use crate::cli::{BlobArgs, BlobUpdateMode, Command, KvArgs, RunArgs};
use anyhow::{bail, Result};
use buffdb::blob::{BlobData, BlobId, BlobServer, BlobStore, UpdateRequest};
use buffdb::kv::{self, Key, KvServer, KvStore, Value};
use buffdb::transitive;
use clap::Parser as _;
use futures::{stream, StreamExt};
use std::path::PathBuf;
use std::process::ExitCode;
use tokio::fs;
use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _};
use tonic::transport::Server;

const SUCCESS: ExitCode = ExitCode::SUCCESS;
const FAILURE: ExitCode = ExitCode::FAILURE;

fn main() -> Result<ExitCode> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
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

    let kv_store = KvStore::at_path(kv_store);
    let blob_store = BlobStore::at_path(blob_store);

    Server::builder()
        .add_service(KvServer::new(kv_store))
        .add_service(BlobServer::new(blob_store))
        .serve(addr)
        .await?;

    Ok(SUCCESS)
}

async fn kv(KvArgs { store, command }: KvArgs) -> Result<ExitCode> {
    let mut client = transitive::kv_client(store).await?;
    match command {
        cli::KvCommand::Get { keys } => {
            let mut values = client
                .get(stream::iter(keys.into_iter().map(|key| kv::Key { key })))
                .await?
                .into_inner();

            let mut stdout = io::stdout();
            let first = values.message().await?;
            if let Some(Value { value }) = first {
                stdout.write_all(value.as_bytes()).await?;
            } else {
                return Ok(FAILURE); // TODO enforce this via clap?
            }
            while let Some(Value { value }) = values.message().await? {
                stdout.write_all(value.as_bytes()).await?;
            }
        }
        cli::KvCommand::Set { key, value } => {
            client
                .set(stream::iter([kv::KeyValue { key, value }]))
                .await?;
        }
        cli::KvCommand::Delete { key } => {
            client.delete(stream::iter([kv::Key { key }])).await?;
        }
        cli::KvCommand::Eq { keys } => {
            let keys = keys.into_iter().map(|key| Key { key });
            let all_eq = client.eq(stream::iter(keys)).await?.into_inner().value;
            if !all_eq {
                return Ok(FAILURE);
            }
        }
        cli::KvCommand::NotEq { keys } => {
            let keys = keys.into_iter().map(|key| Key { key });
            let all_neq = client.not_eq(stream::iter(keys)).await?.into_inner().value;
            if !all_neq {
                return Ok(FAILURE);
            }
        }
    }

    Ok(SUCCESS)
}

async fn blob(BlobArgs { store, command }: BlobArgs) -> Result<ExitCode> {
    let mut client = transitive::blob_client(store.clone()).await?;
    match command {
        cli::BlobCommand::Get { id, mode } => {
            let blob: Vec<_> = client
                .get(stream::iter([BlobId { id }]))
                .await?
                .into_inner()
                .collect()
                .await;

            let (bytes, metadata) = match blob.as_slice() {
                [Ok(BlobData { bytes, metadata })] => (bytes, metadata),
                [Err(err)] => return Err(err.clone().into()),
                _ => bail!("expected exactly one BlobId"),
            };

            match mode {
                cli::BlobGetMode::Data => io::stdout().write_all(bytes).await?,
                cli::BlobGetMode::Metadata => {
                    if let Some(metadata) = metadata {
                        io::stdout().write_all(metadata.as_bytes()).await?
                    }
                }
                cli::BlobGetMode::All => {
                    let mut stdout = io::stdout();
                    if let Some(metadata) = metadata {
                        stdout.write_all(metadata.as_bytes()).await?;
                    }
                    stdout.write_all(&[0]).await?;
                    stdout.write_all(bytes).await?;
                }
            }
        }
        cli::BlobCommand::Store {
            file_path,
            metadata,
        } => {
            let id: Vec<_> = client
                .store(stream::iter([BlobData {
                    bytes: read_file_or_stdin(file_path).await?,
                    metadata,
                }]))
                .await?
                .into_inner()
                .collect()
                .await;
            match id.as_slice() {
                [Ok(BlobId { id })] => println!("{id}"),
                [Err(err)] => return Err(err.clone().into()),
                _ => bail!("expected exactly one BlobId"),
            }
        }
        cli::BlobCommand::Update {
            id,
            mode: BlobUpdateMode::Data { file_path },
        } => {
            client
                .update(stream::iter([UpdateRequest {
                    id,
                    bytes: Some(read_file_or_stdin(file_path).await?),
                    should_update_metadata: false,
                    metadata: None,
                }]))
                .await?;
        }
        cli::BlobCommand::Update {
            id,
            mode: BlobUpdateMode::Metadata { metadata },
        } => {
            client
                .update(stream::iter([UpdateRequest {
                    id,
                    bytes: None,
                    should_update_metadata: true,
                    metadata,
                }]))
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
            client
                .update(stream::iter([UpdateRequest {
                    id,
                    bytes: Some(read_file_or_stdin(file_path).await?),
                    should_update_metadata: true,
                    metadata,
                }]))
                .await?;
        }
        cli::BlobCommand::Delete { id } => {
            client.delete(stream::iter([BlobId { id }])).await?;
        }
        cli::BlobCommand::EqData { ids } => {
            let all_eq = client
                .eq_data(stream::iter(ids.into_iter().map(|id| BlobId { id })))
                .await?
                .into_inner()
                .value;
            if !all_eq {
                return Ok(FAILURE);
            }
        }
        cli::BlobCommand::NotEqData { ids } => {
            let all_neq = client
                .not_eq_data(stream::iter(ids.into_iter().map(|id| BlobId { id })))
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
