//! Command-line interface for BuffDB.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

/// The backend to use for BuffDB.
///
/// Note that the backend must be enabled at compile time for it to be used.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum Backend {
    #[cfg(feature = "duckdb")]
    #[clap(name = "duckdb")]
    #[allow(clippy::missing_docs_in_private_items)]
    DuckDb,
    #[cfg(feature = "sqlite")]
    #[allow(clippy::missing_docs_in_private_items)]
    Sqlite,
}

impl Default for Backend {
    #[allow(unreachable_code)] // simpler than more complex cfgs
    fn default() -> Self {
        #[cfg(feature = "sqlite")]
        return Self::Sqlite;
        #[cfg(feature = "duckdb")]
        return Self::DuckDb;

        unreachable!()
    }
}

impl Args {
    /// Load and merge configuration from config file and CLI arguments.
    /// CLI arguments have higher precedence than config file.
    pub fn load_config(&self) -> Result<buffdb::config::Config, Box<dyn std::error::Error>> {
        // Try to load config from various sources
        let mut config = if let Some(ref config_path) = self.config {
            // Explicit config file provided
            buffdb::config::Config::from_file(config_path)?
        } else {
            // Try default locations
            let current_dir_config = PathBuf::from("buffdb.toml");
            let home_dir_config = home::home_dir()
                .map(|h| h.join(".config/buffdb/buffdb.toml"))
                .filter(|p| p.exists());

            if let Some(home_config) = home_dir_config {
                buffdb::config::Config::from_file(home_config)?
            } else if current_dir_config.exists() {
                buffdb::config::Config::from_file(current_dir_config)?
            } else {
                buffdb::config::Config::default()
            }
        };

        // Apply CLI overrides for run command
        if let Command::Run(ref run_args) = self.command {
            let config_backend = match self.backend {
                Backend::Sqlite => buffdb::config::ConfigBackend::Sqlite,
                #[cfg(feature = "duckdb")]
                Backend::DuckDb => buffdb::config::ConfigBackend::DuckDb,
            };
            config.apply_cli_overrides(
                Some(config_backend),
                Some(run_args.kv_store.clone()),
                Some(run_args.blob_store.clone()),
                Some(run_args.addr),
            );
        } else {
            // Apply backend override for other commands
            let config_backend = match self.backend {
                Backend::Sqlite => buffdb::config::ConfigBackend::Sqlite,
                #[cfg(feature = "duckdb")]
                Backend::DuckDb => buffdb::config::ConfigBackend::DuckDb,
            };
            config.apply_cli_overrides(Some(config_backend), None, None, None);
        }

        // Validate final configuration
        config.validate()?;

        Ok(config)
    }
}

/// Arguments for performing operations on the key-value store.
#[derive(Debug, Parser)]
#[command(propagate_version = true)]
pub(crate) struct KvArgs {
    /// The location of the key-value store.
    #[arg(short, long, default_value = "kv_store.db")]
    pub(crate) store: PathBuf,
    /// The operation to perform.
    #[command(subcommand)]
    pub(crate) command: KvCommand,
}

/// Perform an operation on the key-value store.
#[derive(Debug, Subcommand)]
pub(crate) enum KvCommand {
    /// Get the value(s) associated with the given key(s).
    ///
    /// If multiple keys are provided, the values associated with each key will be written to stdout
    /// in the order they were provided, separated by a null byte (`\0`).
    #[clap(alias = "fetch")]
    Get {
        /// The key(s) to get the value(s) for.
        #[clap(required = true)]
        keys: Vec<String>,
    },
    /// Set a value for a given key.
    ///
    /// If the key already exists, the value is updated.
    #[clap(aliases = ["put", "save", "store"])]
    Set {
        /// The key to be inserted.
        ///
        /// If the key already exists, the value is updated.
        key: String,
        /// The value to be associated with the key.
        value: String,
    },
    /// Delete a value for a given key.
    #[clap(aliases = ["remove", "rm"])]
    Delete {
        /// The key to delete.
        key: String,
    },
    /// Determine if all provided keys have the same value.
    ///
    /// If all keys have the same value, the process exits with a successful status code. If any
    /// two keys have different values, the process exits with a failure status code.
    #[clap(aliases = ["equal", "equals"])]
    Eq {
        /// The key(s) to compare.
        #[clap(required = true)]
        keys: Vec<String>,
    },
    /// Determine if all provided keys have different values.
    ///
    /// If all keys have different values, the process exits with a successful status code. If any
    /// two keys have the same value, the process exits with a failure status code.
    #[clap(aliases = ["unique", "ne", "neq"])]
    NotEq {
        /// The key(s) to compare.
        #[clap(required = true)]
        keys: Vec<String>,
    },
}

/// Arguments for performing operations on the BLOB store.
#[derive(Debug, Parser)]
#[command(propagate_version = true)]
pub(crate) struct BlobArgs {
    /// The location of the BLOB store.
    #[arg(short, long, default_value = "blob_store.db")]
    pub(crate) store: PathBuf,
    /// The operation to perform.
    #[command(subcommand)]
    pub(crate) command: BlobCommand,
}

/// Execute a query on the blob store
#[derive(Debug, Subcommand)]
pub(crate) enum BlobCommand {
    /// Get the BLOB associated with the given ID.
    #[clap(alias = "fetch")]
    Get {
        /// The ID of the BLOB to get.
        id: u64,
        /// What information to obtain about the BLOB.
        #[arg(default_value_t)]
        mode: BlobGetMode,
    },
    /// Store a BLOB in the store.
    #[clap(aliases = ["put", "save", "set"])]
    Store {
        /// The file to store in the BLOB store.
        ///
        /// If `-`, the data is read from stdin.
        file_path: PathBuf,
        /// The metadata to associate with the BLOB, if any.
        ///
        /// Note that an empty string is considered metadata. For a null value to be stored, the
        /// metadata must be omitted entirely.
        metadata: Option<String>,
    },
    /// Update a BLOB in the store.
    Update {
        /// The ID of the BLOB to update.
        id: u64,
        /// What information to update about the BLOB.
        #[clap(subcommand)]
        mode: BlobUpdateMode,
    },
    /// Delete a BLOB from the store.
    #[clap(aliases = ["remove", "rm"])]
    Delete {
        /// The ID of the BLOB to delete.
        id: u64,
    },
    /// Determine if all provided IDs have the same data.
    EqData {
        /// The ID(s) to compare.
        #[clap(required = true)]
        ids: Vec<u64>,
    },
    /// Determine if all provided IDs have different data.
    NotEqData {
        /// The ID(s) to compare.
        #[clap(required = true)]
        ids: Vec<u64>,
    },
}

/// What information to obtain about a BLOB.
#[derive(Debug, Default, Clone, Copy, ValueEnum)]
pub(crate) enum BlobGetMode {
    /// Get the data associated with the BLOB, but not the metadata.
    #[clap(aliases = ["bytes", "blob", "content", "contents"])]
    Data,
    /// Get the metadata associated with the BLOB, but not the data.
    #[clap(alias = "meta")]
    Metadata,
    /// Get both the data and the metadata associated with the BLOB.
    ///
    /// When written to stdout, the metadata (if any) is written first, followed by a null byte (`\0`),
    /// followed by the data.
    #[clap(alias = "both")]
    #[default]
    All,
}

impl fmt::Display for BlobGetMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Data => "data",
            Self::Metadata => "metadata",
            Self::All => "all",
        })
    }
}

/// What information to update about a BLOB.
#[derive(Debug, Clone, Subcommand)]
pub(crate) enum BlobUpdateMode {
    /// Update the data for a BLOB, leaving the metadata unchanged.
    #[clap(aliases = ["bytes", "blob", "content", "contents"])]
    Data {
        /// The file to read the new data from.
        ///
        /// If `-`, the data is read from stdin.
        file_path: PathBuf,
    },
    /// Update the metadata for a BLOB, leaving the data unchanged.
    #[clap(alias = "meta")]
    Metadata {
        /// The new metadata to associate with the BLOB.
        ///
        /// Note that an empty string is considered metadata. For a null value to be stored, the
        /// metadata must be omitted entirely.
        metadata: Option<String>,
    },
    /// Update both the data and the metadata for a BLOB.
    #[clap(alias = "both")]
    All {
        /// The file to read the new data from.
        ///
        /// If `-`, the data is read from stdin.
        file_path: PathBuf,
        /// The new metadata to associate with the BLOB.
        ///
        /// Note that an empty string is considered metadata. For a null value to be stored, the
        /// metadata must be omitted entirely.
        metadata: Option<String>,
    },
}
