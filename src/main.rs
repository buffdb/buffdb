//! A command-line interface for interacting with a stores provided by the BuffDB library.
//!
//! For usage, run `cargo run -- --help`.

#[cfg(not(any(feature = "duckdb", feature = "sqlite",)))]
compile_error!("at least one backend must be enabled (options are `duckdb` and `sqlite`)");

mod cli;

use crate::cli::{Args, Backend, BlobArgs, BlobUpdateMode, Command, KvArgs, RunArgs};
#[cfg(feature = "duckdb")]
use buffdb::backend::DuckDb;
#[cfg(feature = "sqlite")]
use buffdb::backend::Sqlite;
use buffdb::backend::{BlobBackend, DatabaseBackend, KvBackend};
use buffdb::interop::IntoTonicStatus;
use buffdb::proto::{blob, kv};
use buffdb::server::blob::BlobServer;
use buffdb::server::kv::KvServer;
use buffdb::store::{BlobStore, KvStore};
use buffdb::transitive;
use clap::Parser as _;
use futures::{stream, StreamExt};
use std::path::PathBuf;
use std::process::ExitCode;
use tokio::fs;
use tokio::io::{self, AsyncReadExt as _, AsyncWriteExt as _};
use tonic::transport::Server;

/// A custom error message.
#[derive(Debug)]
struct ErrStr(&'static str);

impl std::error::Error for ErrStr {}

impl std::fmt::Display for ErrStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

fn main() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let Args { backend, command } = Args::parse();

    let future = async {
        match backend {
            #[cfg(feature = "duckdb")]
            Backend::DuckDb => match command {
                Command::Run(args) => run::<DuckDb>(args).await,
                Command::Kv(args) => kv::<DuckDb>(args).await,
                Command::Blob(args) => blob::<DuckDb>(args).await,
            },
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => match command {
                Command::Run(args) => run::<Sqlite>(args).await,
                Command::Kv(args) => kv::<Sqlite>(args).await,
                Command::Blob(args) => blob::<Sqlite>(args).await,
            },
        }
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(future)
}

/// Run BuffDB as a server. This function will block until the server is shut down.
///
/// # Parameters
///
/// - `kv_store`: The location to store key-value pairs.
/// - `blob_store`: The location to store BLOBs.
/// - `addr`: The address to bind the server to.
///
/// `kv_store` and `blob_store` cannot be the same location. This is enforced at runtime to a
/// reasonable extent.
// TODO two variants: queryable and non-queryable
async fn run<Backend>(
    RunArgs {
        kv_store,
        blob_store,
        addr,
    }: RunArgs,
) -> Result<ExitCode, Box<dyn std::error::Error>>
where
    Backend: DatabaseBackend<Error: IntoTonicStatus + std::error::Error>
        // + Queryable<QueryStream: Send>
        + KvBackend<GetStream: Send, SetStream: Send, DeleteStream: Send>
        + BlobBackend<GetStream: Send, StoreStream: Send, UpdateStream: Send, DeleteStream: Send>
        + 'static,
{
    if kv_store == blob_store {
        return Err(Box::new(ErrStr(
            "kv_store and blob_store cannot be at the same location",
        )));
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
                    return Err(Box::new(ErrStr(
                        "kv_store and blob_store cannot be at the same location",
                    )));
                }
            }
        }
    }

    let kv_store = KvStore::<Backend>::at_path(kv_store)?;
    let blob_store = BlobStore::<Backend>::at_path(blob_store)?;

    Server::builder()
        .add_service(KvServer::new(kv_store))
        .add_service(BlobServer::new(blob_store))
        .serve(addr)
        .await?;

    Ok(ExitCode::SUCCESS)
}

/// Perform operations on the key-value store.
///
/// # Parameters
///
/// - `store`: The location of the key-value store.
/// - `command`: The command to execute.
///
/// # stdout
///
/// When obtaining a value for a key, the value is written to stdout. Multiple values are separated
/// by a null byte (`\0`).
async fn kv<Backend>(
    KvArgs { store, command }: KvArgs,
) -> Result<ExitCode, Box<dyn std::error::Error>>
where
    Backend: KvBackend<GetStream: Send, SetStream: Send, DeleteStream: Send, Error: IntoTonicStatus>
        + 'static,
{
    let mut client = transitive::kv_client::<_, Backend>(store).await?;
    match command {
        cli::KvCommand::Get { keys } => {
            let mut values = client
                .get(stream::iter(
                    keys.into_iter().map(|key| kv::GetRequest { key }),
                ))
                .await?
                .into_inner();

            let mut stdout = io::stdout();
            let Some(kv::GetResponse { value }) = values.message().await? else {
                return Err(Box::new(ErrStr("expected at least one value")));
            };
            stdout.write_all(value.as_bytes()).await?;
            while let Some(kv::GetResponse { value }) = values.message().await? {
                stdout.write_all(&[0]).await?;
                stdout.write_all(value.as_bytes()).await?;
            }
        }
        cli::KvCommand::Set { key, value } => {
            let _response = client
                .set(stream::iter([kv::SetRequest { key, value }]))
                .await?;
        }
        cli::KvCommand::Delete { key } => {
            let _response = client
                .delete(stream::iter([kv::DeleteRequest { key }]))
                .await?;
        }
        cli::KvCommand::Eq { keys } => {
            let keys = keys.into_iter().map(|key| kv::EqRequest { key });
            let all_eq = client.eq(stream::iter(keys)).await?.into_inner();
            drop(client);
            if !all_eq {
                return Ok(ExitCode::FAILURE);
            }
        }
        cli::KvCommand::NotEq { keys } => {
            let keys = keys.into_iter().map(|key| kv::NotEqRequest { key });
            let all_neq = client.not_eq(stream::iter(keys)).await?.into_inner();
            drop(client);
            if !all_neq {
                return Ok(ExitCode::FAILURE);
            }
        }
    }

    Ok(ExitCode::SUCCESS)
}

/// Perform operations on the BLOB store.
///
/// # Parameters
///
/// - `store`: The location of the BLOB store.
/// - `command`: The command to execute.
///
/// # stdout
///
/// When getting information for a BLOB, the data and/or metadata is written to stdout. If both are
/// requested, the metadata (if any) is printed first, followed by a null byte (`\0`), followed by
/// the data.
///
/// When storing a BLOB, the ID of the newly-created BLOB is written to stdout.
///
/// Nothing is written to stdout for other operations.
async fn blob<Backend>(
    BlobArgs { store, command }: BlobArgs,
) -> Result<ExitCode, Box<dyn std::error::Error>>
where
    Backend: BlobBackend<
            GetStream: Send,
            StoreStream: Send,
            UpdateStream: Send,
            DeleteStream: Send,
            Error: IntoTonicStatus,
        > + 'static,
{
    let mut client = transitive::blob_client::<_, Backend>(store.clone()).await?;
    match command {
        cli::BlobCommand::Get { id, mode } => {
            let blob: Vec<_> = client
                .get(stream::iter([blob::GetRequest { id }]))
                .await?
                .into_inner()
                .collect()
                .await;
            drop(client);

            let (bytes, metadata) = match blob.as_slice() {
                [Ok(blob::GetResponse { bytes, metadata })] => (bytes, metadata),
                [Err(err)] => return Err(err.clone().into()),
                _ => return Err(Box::new(ErrStr("expected exactly one BlobId"))),
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
                .store(stream::iter([blob::StoreRequest {
                    bytes: read_file_or_stdin(file_path).await?,
                    metadata,
                }]))
                .await?
                .into_inner()
                .collect()
                .await;
            drop(client);
            match id.as_slice() {
                #[allow(clippy::print_stdout)]
                [Ok(blob::StoreResponse { id })] => println!("{id}"),
                [Err(err)] => return Err(err.clone().into()),
                _ => return Err(Box::new(ErrStr("expected exactly one BlobId"))),
            }
        }
        cli::BlobCommand::Update {
            id,
            mode: BlobUpdateMode::Data { file_path },
        } => {
            let _response = client
                .update(stream::iter([blob::UpdateRequest {
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
            let _response = client
                .update(stream::iter([blob::UpdateRequest {
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
            let _response = client
                .update(stream::iter([blob::UpdateRequest {
                    id,
                    bytes: Some(read_file_or_stdin(file_path).await?),
                    should_update_metadata: true,
                    metadata,
                }]))
                .await?;
        }
        cli::BlobCommand::Delete { id } => {
            let _response = client
                .delete(stream::iter([blob::DeleteRequest { id }]))
                .await?;
        }
        cli::BlobCommand::EqData { ids } => {
            let all_eq = client
                .eq_data(stream::iter(
                    ids.into_iter().map(|id| blob::EqDataRequest { id }),
                ))
                .await?
                .into_inner();
            drop(client);
            if !all_eq {
                return Ok(ExitCode::FAILURE);
            }
        }
        cli::BlobCommand::NotEqData { ids } => {
            let all_neq = client
                .not_eq_data(stream::iter(
                    ids.into_iter().map(|id| blob::NotEqDataRequest { id }),
                ))
                .await?
                .into_inner();
            drop(client);
            if !all_neq {
                return Ok(ExitCode::FAILURE);
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Given a path, read from stdin if the path is "-". Otherwise, read the file at that path.
async fn read_file_or_stdin(file_path: PathBuf) -> io::Result<Vec<u8>> {
    if file_path == PathBuf::from("-") {
        let mut bytes = Vec::new();
        let _num_bytes = io::stdin().read_to_end(&mut bytes).await?;
        Ok(bytes)
    } else {
        Ok(fs::read(file_path).await?)
    }
}
