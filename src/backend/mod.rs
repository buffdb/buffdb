//! Backend implementations for various databases.
//!
//! Note that backends must be enabled at compile time using the appropriate feature flag.

mod arc;
#[cfg(feature = "duckdb")]
mod duckdb;
mod helpers;
#[cfg(feature = "sqlite")]
mod sqlite;

use std::sync::Arc;

mod sealed {
    use super::*;

    pub trait Sealed {}
    #[cfg(feature = "duckdb")]
    impl Sealed for DuckDb {}
    #[cfg(feature = "sqlite")]
    impl Sealed for Sqlite {}

    impl<T> Sealed for Arc<T> {}
}

#[cfg(feature = "duckdb")]
pub use self::duckdb::DuckDb;
#[cfg(feature = "sqlite")]
pub use self::sqlite::Sqlite;
use crate::proto::{blob, kv, query};
use crate::{Location, RpcResponse, StreamingRequest};
use futures::Stream;
use tonic::async_trait;

/// A backend for a database, permitting connections to be established at a given location.
pub trait DatabaseBackend: sealed::Sealed + Sized {
    /// The type of connection to the database.
    type Connection;
    /// The type of any errors returned by the backend.
    type Error;

    // TODO Consider a different error type such that in-memory connections can be rejected as
    // necessary.
    /// Create a new instance of the backend at the given location.
    fn at_location(location: Location) -> Result<Self, Self::Error>;

    /// The location of the database.
    fn location(&self) -> &Location;

    /// Note: Backends for specific stores provide their own versions of `connect` that initialize
    /// the store as necessary. It is recommended to **not** call this method directly unless you
    /// are implementing a new backend.
    fn connect(&self) -> Result<Self::Connection, Self::Error>;
}

/// A backend that supports executing arbitrary queries.
#[async_trait]
pub trait QueryBackend: DatabaseBackend + Send + Sync {
    /// A stream that is returned when a query is executed. Returns the result of the query.
    type QueryStream: Stream<Item = Result<query::QueryResult, tonic::Status>>;
    /// A stream that is returned when a query is executed. Returns the number of rows changed.
    type ExecuteStream: Stream<Item = Result<query::RowsChanged, tonic::Status>>;

    /// Execute the query and return the results.
    async fn query(
        &self,
        request: StreamingRequest<query::RawQuery>,
    ) -> RpcResponse<Self::QueryStream>;

    /// Execute the query and return the number of rows changed.
    async fn execute(
        &self,
        request: StreamingRequest<query::RawQuery>,
    ) -> RpcResponse<Self::ExecuteStream>;
}

/// A backend that supports key-value operations.
#[async_trait]
pub trait KvBackend: DatabaseBackend + Send + Sync {
    /// A stream for the response to a `get` command.
    type GetStream: Stream<Item = Result<kv::GetResponse, tonic::Status>>;
    /// A stream for the response to a `set` command.
    type SetStream: Stream<Item = Result<kv::SetResponse, tonic::Status>>;
    /// A stream for the response to a `delete` command.
    type DeleteStream: Stream<Item = Result<kv::DeleteResponse, tonic::Status>>;

    /// Initialize the key-value store.
    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error>;

    /// Connect to the key-value store, initializing it if necessary.
    fn connect_kv(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        self.initialize(&conn)?;
        Ok(conn)
    }

    /// Get the value associated with the given key.
    async fn get(&self, request: StreamingRequest<kv::GetRequest>) -> RpcResponse<Self::GetStream>;

    /// Insert or update a key-value pair.
    async fn set(&self, request: StreamingRequest<kv::SetRequest>) -> RpcResponse<Self::SetStream>;

    /// Delete the key-value pair associated with the given key.
    async fn delete(
        &self,
        request: StreamingRequest<kv::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream>;

    /// Determine if all provided keys have the same value.
    async fn eq(&self, request: StreamingRequest<kv::EqRequest>) -> RpcResponse<bool>;

    /// Determine if all provided keys have different values.
    async fn not_eq(&self, request: StreamingRequest<kv::NotEqRequest>) -> RpcResponse<bool>;
}

/// A backend that supports BLOB operations.
#[async_trait]
pub trait BlobBackend: DatabaseBackend + Send + Sync {
    /// A stream for the response to a `get` command.
    type GetStream: Stream<Item = Result<blob::GetResponse, tonic::Status>>;
    /// A stream for the response to a `store` command.
    type StoreStream: Stream<Item = Result<blob::StoreResponse, tonic::Status>>;
    /// A stream for the response to an `update` command.
    type UpdateStream: Stream<Item = Result<blob::UpdateResponse, tonic::Status>>;
    /// A stream for the response to a `delete` command.
    type DeleteStream: Stream<Item = Result<blob::DeleteResponse, tonic::Status>>;

    /// Initialize the BLOB store.
    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error>;

    /// Connect to the BLOB store, initializing it if necessary.
    fn connect_blob(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        self.initialize(&conn)?;
        Ok(conn)
    }

    /// Get the BLOB and associated metadata given the ID.
    async fn get(
        &self,
        request: StreamingRequest<blob::GetRequest>,
    ) -> RpcResponse<Self::GetStream>;

    /// Store a BLOB and associated metadata, returning the ID.
    async fn store(
        &self,
        request: StreamingRequest<blob::StoreRequest>,
    ) -> RpcResponse<Self::StoreStream>;

    /// Update the BLOB and/or its associated metadata.
    async fn update(
        &self,
        request: StreamingRequest<blob::UpdateRequest>,
    ) -> RpcResponse<Self::UpdateStream>;

    /// Delete the BLOB and associated metadata given the ID.
    async fn delete(
        &self,
        request: StreamingRequest<blob::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream>;

    /// Determine if all provided IDs have the same data. Metadata is not considered.
    async fn eq_data(&self, request: StreamingRequest<blob::EqDataRequest>) -> RpcResponse<bool>;

    /// Determine if all provided IDs have different data. Metadata is not considered.
    async fn not_eq_data(
        &self,
        request: StreamingRequest<blob::NotEqDataRequest>,
    ) -> RpcResponse<bool>;
}
