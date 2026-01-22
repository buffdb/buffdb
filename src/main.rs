//! A command-line interface for interacting with a stores provided by the BuffDB library.
//!
//! For usage, run `cargo run -- --help`.

#[cfg(not(any(feature = "duckdb", feature = "sqlite")))]
compile_error!("at least one backend must be enabled (options are `duckdb` and `sqlite`)");

mod cli;
mod tracing_shim;

use crate::cli::{Args, Backend, BlobArgs, BlobUpdateMode, Command, KvArgs, RunArgs};
use crate::tracing_shim::debug;
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
    let args = Args::parse();

    // Load configuration
    let config = args.load_config()?;
    tracing_shim::trace!(?config, "Loaded configuration");

    #[cfg(feature = "tracing")]
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::default())?;

    let future = async {
        match args.backend {
            #[cfg(feature = "duckdb")]
            Backend::DuckDb => match args.command {
                Command::Run(run_args) => run_with_config::<DuckDb>(config, run_args).await,
                Command::Kv(kv_args) => kv_with_config::<DuckDb>(config, kv_args).await,
                Command::Blob(blob_args) => blob_with_config::<DuckDb>(config, blob_args).await,
            },
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => match args.command {
                Command::Run(run_args) => run_with_config::<Sqlite>(config, run_args).await,
                Command::Kv(kv_args) => kv_with_config::<Sqlite>(config, kv_args).await,
                Command::Blob(blob_args) => blob_with_config::<Sqlite>(config, blob_args).await,
            },
        }
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(future)
}

/// Run BuffDB as a server. This function will block until the server is shut down.
#[cfg_attr(feature = "tracing", tracing::instrument)]
async fn run_with_config<Backend>(
    config: buffdb::config::Config,
    _run_args: RunArgs,
) -> Result<ExitCode, Box<dyn std::error::Error>>
where
    Backend: DatabaseBackend<Error: IntoTonicStatus + std::error::Error>
        + KvBackend<GetStream: Send, SetStream: Send, DeleteStream: Send>
        + BlobBackend<GetStream: Send, StoreStream: Send, UpdateStream: Send, DeleteStream: Send>
        + buffdb::transaction::TransactionalBackend
        + 'static,
    Backend::Error: std::fmt::Display + std::fmt::Debug,
{
    let kv_store_path = config.database.kv_store.clone();
    let blob_store_path = config.database.blob_store.clone();
    let addr = config.server_address()?;

    debug!(?kv_store_path, ?blob_store_path, "creating stores");
    let kv_store = KvStore::<Backend>::at_path(kv_store_path)?;
    let blob_store = BlobStore::<Backend>::at_path(blob_store_path)?;

    debug!("starting server");
    Server::builder()
        .add_service(KvServer::new(kv_store))
        .add_service(BlobServer::new(blob_store))
        .serve(addr)
        .await?;

    Ok(ExitCode::SUCCESS)
}

/// Perform operations on the key-value store.
#[cfg_attr(feature = "tracing", tracing::instrument)]
async fn kv_with_config<Backend>(
    config: buffdb::config::Config,
    kv_args: KvArgs,
) -> Result<ExitCode, Box<dyn std::error::Error>>
where
    Backend: KvBackend<GetStream: Send, SetStream: Send, DeleteStream: Send, Error: IntoTonicStatus>
        + buffdb::transaction::TransactionalBackend
        + 'static,
    Backend::Error: std::fmt::Display + std::fmt::Debug,
{
    let store = config.database.kv_store.clone();
    let mut client: buffdb::client::kv::KvClient<tonic::transport::Channel> =
        transitive::kv_client::<_, Backend>(store).await?;
    match kv_args.command {
        cli::KvCommand::Get { keys } => {
            let mut values: tonic::Streaming<kv::GetResponse> = client
                .get(stream::iter(keys.into_iter().map(|key| kv::GetRequest {
                    key,
                    transaction_id: None,
                })))
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
                .set(stream::iter([kv::SetRequest {
                    key,
                    value,
                    transaction_id: None,
                }]))
                .await?;
        }
        cli::KvCommand::Delete { key } => {
            let _response = client
                .delete(stream::iter([kv::DeleteRequest {
                    key,
                    transaction_id: None,
                }]))
                .await?;
        }
        cli::KvCommand::Eq { keys } => {
            let keys = keys.into_iter().map(|key| kv::EqRequest { key });
            let all_eq: bool = client.eq(stream::iter(keys)).await?.into_inner();
            drop(client);
            if !all_eq {
                return Ok(ExitCode::FAILURE);
            }
        }
        cli::KvCommand::NotEq { keys } => {
            let keys = keys.into_iter().map(|key| kv::NotEqRequest { key });
            let all_neq: bool = client.not_eq(stream::iter(keys)).await?.into_inner();
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
#[cfg_attr(feature = "tracing", tracing::instrument)]
async fn blob_with_config<Backend>(
    config: buffdb::config::Config,
    blob_args: BlobArgs,
) -> Result<ExitCode, Box<dyn std::error::Error>>
where
    Backend: BlobBackend<
            GetStream: Send,
            StoreStream: Send,
            UpdateStream: Send,
            DeleteStream: Send,
            Error: IntoTonicStatus,
        > + buffdb::transaction::TransactionalBackend
        + 'static,
    Backend::Error: std::fmt::Display + std::fmt::Debug,
{
    let store = config.database.blob_store.clone();
    let mut client: buffdb::client::blob::BlobClient<tonic::transport::Channel> =
        transitive::blob_client::<_, Backend>(store.clone()).await?;
    match blob_args.command {
        cli::BlobCommand::Get { id, mode } => {
            let blob: Vec<Result<blob::GetResponse, tonic::Status>> = client
                .get(stream::iter([blob::GetRequest {
                    id,
                    transaction_id: None,
                }]))
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
            let id: Vec<Result<blob::StoreResponse, tonic::Status>> = client
                .store(stream::iter([blob::StoreRequest {
                    bytes: read_file_or_stdin(file_path).await?,
                    metadata,
                    transaction_id: None,
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
                    transaction_id: None,
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
                    transaction_id: None,
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
                    transaction_id: None,
                }]))
                .await?;
        }
        cli::BlobCommand::Delete { id } => {
            let _response = client
                .delete(stream::iter([blob::DeleteRequest {
                    id,
                    transaction_id: None,
                }]))
                .await?;
        }
        cli::BlobCommand::EqData { ids } => {
            let all_eq: bool = client
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
            let all_neq: bool = client
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
#[cfg_attr(feature = "tracing", tracing::instrument)]
async fn read_file_or_stdin(file_path: PathBuf) -> io::Result<Vec<u8>> {
    if file_path == PathBuf::from("-") {
        let mut bytes = Vec::new();
        let _num_bytes = io::stdin().read_to_end(&mut bytes).await?;
        Ok(bytes)
    } else {
        Ok(fs::read(file_path).await?)
    }
}
