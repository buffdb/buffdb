//! **BuffDB is experimental software**; many things are not yet decided.
//!
//! BuffDB is an implementation of a persistence layer for gRPC. Its goal is to abstract away
//! complexity associated with using protobufs.
//!
//! To add BuffDB to your project, run `cargo add buffdb`.
//!
//! # Use as a library
//!
//! BuffDB is designed to be used as both a library and a binary. As a library, it provides
//! everything needed to spawn a server and interact with a server.
//!
//! When using `buffdb`, you will likely want to add a number of dependencies to you project,
//! including `tokio`, `tonic`, and `futures`.
//!
//! # Use as a binary
//!
//! To use BuffDB as a binary, you can install it using `cargo install buffdb`. This will install
//! the `buffdb` binary, which can be used to start a server or interact with a server. For more
//! information, see the help message for the binary.

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![allow(clippy::missing_docs_in_private_items)]

pub mod backend;
mod conv;
#[cfg(feature = "duckdb")]
mod duckdb_helper;
pub mod frontend;
pub mod handler;
mod internal_macros;
pub mod interop;
mod location;
pub mod queryable;
pub mod structs;
mod tracing_shim;
pub mod transitive;

/// Rust bindings for the gRPC schema provided by the protobufs.
// We have no control over the generated code, so silence anything non-critical.
#[allow(warnings)]
mod bindings {
    pub(crate) mod buffdb {
        pub(crate) mod blob {
            tonic::include_proto!("buffdb.blob");
        }
        pub(crate) mod kv {
            tonic::include_proto!("buffdb.kv");
        }
        pub(crate) mod query {
            tonic::include_proto!("buffdb.query");
        }
    }
}

/// Protobuf types used by BuffDB.
pub mod proto {
    /// Protobuf types needed to interact with the BLOB store.
    pub mod blob {
        pub use crate::bindings::buffdb::blob::{
            DeleteRequest, DeleteResponse, EqDataRequest, GetRequest, GetResponse,
            NotEqDataRequest, StoreRequest, StoreResponse, UpdateRequest, UpdateResponse,
        };
    }
    /// Protobuf types needed to interact with the KV store.
    pub mod kv {
        pub use crate::bindings::buffdb::kv::{
            DeleteRequest, DeleteResponse, EqRequest, GetRequest, GetResponse, NotEqRequest,
            SetRequest, SetResponse,
        };
    }
    /// Protobuf types needed to send raw queries to a given store.
    pub mod query {
        pub use crate::bindings::buffdb::query::{QueryResult, RawQuery, RowsChanged, TargetStore};
    }
}

/// gRPC server definitions.
pub mod server {
    /// gRPC server for the BLOB store.
    pub mod blob {
        pub use crate::bindings::buffdb::blob::blob_server::BlobServer;
    }
    /// gRPC server for the KV store.
    pub mod kv {
        pub use crate::bindings::buffdb::kv::kv_server::KvServer;
    }
    /// gRPC server for the raw query execution.
    pub mod query {
        pub use crate::bindings::buffdb::query::query_server::QueryServer;
    }
}

/// gRPC client definitions.
pub mod client {
    /// gRPC client for the BLOB store.
    pub mod blob {
        pub use crate::bindings::buffdb::blob::blob_client::BlobClient;
    }
    /// gRPC client for the KV store.
    pub mod kv {
        pub use crate::bindings::buffdb::kv::kv_client::KvClient;
    }
    /// gRPC client for the raw query execution.
    pub mod query {
        pub use crate::bindings::buffdb::query::query_client::QueryClient;
    }
}

/// gRPC service definitions.
pub mod service {
    /// gRPC service for the BLOB store.
    pub mod blob {
        pub use crate::bindings::buffdb::blob::blob_server::Blob as BlobRpc;
    }
    /// gRPC service for the KV store.
    pub mod kv {
        pub use crate::bindings::buffdb::kv::kv_server::Kv as KvRpc;
    }
    /// gRPC service for the raw query execution.
    pub mod query {
        pub use crate::bindings::buffdb::query::query_server::Query as QueryRpc;
    }
}

pub use crate::location::Location;
pub use prost_types;

/// A response from a gRPC server.
pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
type StreamingRequest<T> = tonic::Request<tonic::Streaming<T>>;
