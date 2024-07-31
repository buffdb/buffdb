//! **BuffDB is experimental software**; many things are not yet decided.
//!
//! BuffDB is an implementation of a persistence layer for gRPC and is backed by [DuckDB]. Its goal
//! is to abstract away complexity associated with using protobufs.
//!
//! [DuckDB]: https://duckdb.org/
//!
//! # Usage as a library
//!
//! BuffDB is designed to be used as both a library and a binary. As a library, it provides
//! everything needed to spawn a server and interact with a server.

#![allow(clippy::missing_docs_in_private_items)]

pub mod blob;
mod db_connection;
mod helpers;
mod interop;
pub mod kv;
pub mod transitive;

/// Rust bindings for the gRPC schema provided by the protobufs.
pub mod schema {
    pub(crate) mod blob {
        tonic::include_proto!("blob");
    }
    /// Types used across multiple services.
    pub mod common {
        tonic::include_proto!("common");
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
