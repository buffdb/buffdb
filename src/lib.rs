//! **BuffDB is experimental software**; many things are not yet decided.
//!
//! BuffDB is an implementation of a persistence layer for gRPC and is backed by [DuckDB]. Its goal
//! is to abstract away complexity associated with using protobufs.
//!
//! [DuckDB]: https://duckdb.org/
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

#![allow(clippy::missing_docs_in_private_items)]

pub mod backend;
mod blob;
mod conv;
#[cfg(feature = "duckdb")]
mod duckdb_helper;
pub mod interop;
mod kv;
mod location;
pub mod transitive;

/// Rust bindings for the gRPC schema provided by the protobufs.
// We have no control over the generated code, so silence a handful of lints.
#[allow(
    unused_qualifications,
    unreachable_pub,
    unused_results,
    clippy::future_not_send,
    clippy::missing_const_for_fn,
    clippy::unwrap_used
)]
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
        pub use crate::bindings::buffdb::query::{QueryResult, RawQuery, RowsChanged};
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
}

/// Store implementations.
pub mod store {
    pub use crate::blob::BlobStore;
    pub use crate::kv::KvStore;
}

pub use crate::location::Location;
pub use prost_types;

/// A response from a gRPC server.
pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
type StreamingRequest<T> = tonic::Request<tonic::Streaming<T>>;
#[cfg(any(feature = "duckdb", feature = "sqlite"))]
type DynStream<T> = std::pin::Pin<Box<dyn futures::Stream<Item = T> + Send + 'static>>;
