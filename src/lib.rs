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

pub mod blob;
mod conv;
mod db_connection;
mod duckdb_helper;
mod interop;
pub mod kv;
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
pub mod schema {
    pub(crate) mod blob {
        tonic::include_proto!("blob");
    }
    pub(crate) mod kv {
        tonic::include_proto!("kv");
    }
}

pub use crate::db_connection::Location;
use futures::Stream;
use std::pin::Pin;

/// A response from a gRPC server.
pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
type StreamingRequest<T> = tonic::Request<tonic::Streaming<T>>;
type DynStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;
