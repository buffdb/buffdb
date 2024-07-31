use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(version, propagate_version = true)]
pub(crate) enum Command {
    #[clap(alias = "serve")]
    Run(RunArgs),
    #[clap(aliases = ["key-value", "k-v"])]
    Kv(KvArgs),
    Blob(BlobArgs),
}

/// Run BuffDB as a server
#[derive(Debug, Parser)]
#[command(propagate_version = true)]
pub(crate) struct RunArgs {
    /// The file to store key-value pairs in.
    #[clap(long, default_value = "kv_store.db")]
    pub(crate) kv_store: PathBuf,
    /// The file to store blobs in.
    #[clap(long, default_value = "blob_store.db")]
    pub(crate) blob_store: PathBuf,
    #[clap(default_value = "[::1]:50051")]
    pub(crate) addr: SocketAddr,
}

#[derive(Debug, Parser)]
#[command(propagate_version = true)]
pub(crate) struct KvArgs {
    #[arg(short, long, default_value = "kv_store.db")]
    pub(crate) store: PathBuf,
    #[command(subcommand)]
    pub(crate) command: KvCommand,
}

/// Execute a query on the key-value store
#[derive(Debug, Subcommand)]
pub(crate) enum KvCommand {
    #[clap(alias = "fetch")]
    Get { keys: Vec<String> },
    #[clap(aliases = ["put", "save", "store"])]
    Set { key: String, value: String },
    #[clap(aliases = ["remove", "rm"])]
    Delete { key: String },
    #[clap(aliases = ["equal", "equals"])]
    Eq { keys: Vec<String> },
    #[clap(aliases = ["unique", "ne", "neq"])]
    NotEq { keys: Vec<String> },
}

#[derive(Debug, Parser)]
#[command(propagate_version = true)]
pub(crate) struct BlobArgs {
    #[arg(short, long, default_value = "blob_store.db")]
    pub(crate) store: PathBuf,
    #[command(subcommand)]
    pub(crate) command: BlobCommand,
}

/// Execute a query on the blob store
#[derive(Debug, Subcommand)]
pub(crate) enum BlobCommand {
    #[clap(alias = "fetch")]
    Get {
        id: u64,
        #[arg(default_value_t)]
        mode: BlobGetMode,
    },
    #[clap(aliases = ["put", "save", "set"])]
    Store {
        file_path: PathBuf,
        metadata: Option<String>,
    },
    Update {
        id: u64,
        #[clap(subcommand)]
        mode: BlobUpdateMode,
    },
    #[clap(aliases = ["remove", "rm"])]
    Delete {
        id: u64,
    },
    EqData {
        ids: Vec<u64>,
    },
    NotEqData {
        ids: Vec<u64>,
    },
}

#[derive(Debug, Default, Clone, Copy, ValueEnum)]
pub(crate) enum BlobGetMode {
    #[clap(aliases = ["bytes", "blob", "content", "contents"])]
    Data,
    #[clap(alias = "meta")]
    Metadata,
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

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum BlobUpdateMode {
    #[clap(aliases = ["bytes", "blob", "content", "contents"])]
    Data { file_path: PathBuf },
    #[clap(alias = "meta")]
    Metadata { metadata: Option<String> },
    #[clap(alias = "both")]
    All {
        file_path: PathBuf,
        metadata: Option<String>,
    },
}
