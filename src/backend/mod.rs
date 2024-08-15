//! Backend implementations for various databases.
//!
//! Note that backends must be enabled at compile time using the appropriate feature flag.

mod arc;
#[cfg(feature = "duckdb")]
mod duckdb;
mod helpers;
#[cfg(feature = "rocksdb")]
mod rocksdb;
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
    #[cfg(feature = "rocksdb")]
    impl Sealed for RocksDb {}

    impl<T> Sealed for Arc<T> {}
}

#[cfg(feature = "duckdb")]
pub use self::duckdb::DuckDb;
#[cfg(feature = "rocksdb")]
pub use self::rocksdb::RocksDb;
#[cfg(feature = "sqlite")]
pub use self::sqlite::Sqlite;
use crate::internal_macros::future_send;
use crate::proto::{blob, kv};
use crate::{Location, RpcResponse, StreamingRequest};
use futures::Stream;

/// A backend for a database, permitting connections to be established at a given location.
pub trait DatabaseBackend: sealed::Sealed + Sized {
    /// The type of connection to the database.
    type Connection;
    /// The type of any errors returned by the backend.
    type Error; // TODO permit custom error messages?

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

/// A backend that supports key-value operations.
pub trait KvBackend: DatabaseBackend + Send + Sync {
    /// A stream for the response to a `get` command.
    type GetStream: Stream<Item = Result<kv::GetResponse, tonic::Status>>;
    /// A stream for the response to a `set` command.
    type SetStream: Stream<Item = Result<kv::SetResponse, tonic::Status>>;
    /// A stream for the response to a `delete` command.
    type DeleteStream: Stream<Item = Result<kv::DeleteResponse, tonic::Status>>;

    /// Initialize the key-value store.
    fn initialize(
        &self,
        #[allow(unused_variables)] connection: &Self::Connection,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Connect to the key-value store, initializing it if necessary.
    fn connect_kv(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        self.initialize(&conn)?;
        Ok(conn)
    }

    /// Get the value associated with the given key.
    fn get(
        &self,
        request: StreamingRequest<kv::GetRequest>,
    ) -> future_send!(RpcResponse<Self::GetStream>);

    /// Insert or update a key-value pair.
    fn set(
        &self,
        request: StreamingRequest<kv::SetRequest>,
    ) -> future_send!(RpcResponse<Self::SetStream>);

    /// Delete the key-value pair associated with the given key.
    fn delete(
        &self,
        request: StreamingRequest<kv::DeleteRequest>,
    ) -> future_send!(RpcResponse<Self::DeleteStream>);

    /// Determine if all provided keys have the same value.
    fn eq(&self, request: StreamingRequest<kv::EqRequest>) -> future_send!(RpcResponse<bool>);

    /// Determine if all provided keys have different values.
    fn not_eq(
        &self,
        request: StreamingRequest<kv::NotEqRequest>,
    ) -> future_send!(RpcResponse<bool>);
}

/// A backend that supports BLOB operations.
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
    fn initialize(
        &self,
        #[allow(unused_variables)] connection: &Self::Connection,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Connect to the BLOB store, initializing it if necessary.
    fn connect_blob(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        self.initialize(&conn)?;
        Ok(conn)
    }

    /// Get the BLOB and associated metadata given the ID.
    fn get(
        &self,
        request: StreamingRequest<blob::GetRequest>,
    ) -> future_send!(RpcResponse<Self::GetStream>);

    /// Store a BLOB and associated metadata, returning the ID.
    fn store(
        &self,
        request: StreamingRequest<blob::StoreRequest>,
    ) -> future_send!(RpcResponse<Self::StoreStream>);

    /// Update the BLOB and/or its associated metadata.
    fn update(
        &self,
        request: StreamingRequest<blob::UpdateRequest>,
    ) -> future_send!(RpcResponse<Self::UpdateStream>);

    /// Delete the BLOB and associated metadata given the ID.
    fn delete(
        &self,
        request: StreamingRequest<blob::DeleteRequest>,
    ) -> future_send!(RpcResponse<Self::DeleteStream>);

    /// Determine if all provided IDs have the same data. Metadata is not considered.
    fn eq_data(
        &self,
        request: StreamingRequest<blob::EqDataRequest>,
    ) -> future_send!(RpcResponse<bool>);

    /// Determine if all provided IDs have different data. Metadata is not considered.
    fn not_eq_data(
        &self,
        request: StreamingRequest<blob::NotEqDataRequest>,
    ) -> future_send!(RpcResponse<bool>);
}
