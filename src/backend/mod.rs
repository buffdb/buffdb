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
use crate::structs::{blob, kv};
use crate::Location;
use futures::Stream;

/// A backend for a database, permitting connections to be established at a given location.
pub trait DatabaseBackend: sealed::Sealed + Sized {
    /// The type of connection to the database.
    type Connection;
    /// The type of any errors returned by the backend.
    type Error;

    /// Create a new instance of the backend at the given location.
    fn at_location(location: Location) -> Result<Self, Self::Error>;

    fn at_path<P>(path: P) -> Result<Self, Self::Error>
    where
        P: Into<std::path::PathBuf>,
    {
        Self::at_location(Location::OnDisk { path: path.into() })
    }

    fn in_memory() -> Result<Self, Self::Error> {
        Self::at_location(Location::InMemory)
    }

    /// The location of the database.
    fn location(&self) -> &Location;

    /// Note: Backends for specific stores provide their own versions of `connect` that initialize
    /// the store as necessary. It is recommended to **not** call this method directly unless you
    /// are implementing a new backend.
    fn connect(&self) -> Result<Self::Connection, Self::Error>;
}

/// A backend that supports key-value operations.
pub trait KvBackend<FrontendError>: DatabaseBackend + Send + Sync {
    /// A stream for the response to a `get` command.
    type GetStream: Stream<Item = Result<kv::GetResponse, FrontendError>> + Unpin;
    /// A stream for the response to a `set` command.
    type SetStream: Stream<Item = Result<kv::SetResponse, FrontendError>> + Unpin;
    /// A stream for the response to a `delete` command.
    type DeleteStream: Stream<Item = Result<kv::DeleteResponse, FrontendError>> + Unpin;

    /// Initialize the key-value store.
    fn initialize(
        &self,
        #[allow(unused_variables)] connection: &Self::Connection,
    ) -> Result<(), FrontendError> {
        Ok(())
    }

    /// Connect to the key-value store, initializing it if necessary.
    fn connect_kv(&self) -> Result<Self::Connection, FrontendError> {
        let conn = self.connect()?;
        self.initialize(&conn)?;
        Ok(conn)
    }

    /// Get the value associated with the given key.
    fn get<Req>(&self, request: Req) -> future_send!(Result<Self::GetStream, FrontendError>)
    where
        Req: Stream<Item = Result<kv::GetRequest, FrontendError>> + Unpin + Send + 'static;

    /// Insert or update a key-value pair.
    fn set<Req>(&self, request: Req) -> future_send!(Result<Self::SetStream, FrontendError>)
    where
        Req: Stream<Item = Result<kv::SetRequest, FrontendError>> + Unpin + Send + 'static;

    /// Delete the key-value pair associated with the given key.
    fn delete<Req>(&self, request: Req) -> future_send!(Result<Self::DeleteStream, FrontendError>)
    where
        Req: Stream<Item = Result<kv::DeleteRequest, FrontendError>> + Unpin + Send + 'static;

    /// Determine if all provided keys have the same value.
    fn eq<Req>(&self, request: Req) -> future_send!(Result<bool, FrontendError>)
    where
        Req: Stream<Item = Result<kv::EqRequest, FrontendError>> + Unpin + Send + 'static;

    /// Determine if all provided keys have different values.
    fn not_eq<Req>(&self, request: Req) -> future_send!(Result<bool, FrontendError>)
    where
        Req: Stream<Item = Result<kv::NotEqRequest, FrontendError>> + Unpin + Send + 'static;
}

/// A backend that supports BLOB operations.
pub trait BlobBackend<FrontendError>: DatabaseBackend + Send + Sync {
    /// A stream for the response to a `get` command.
    type GetStream: Stream<Item = Result<blob::GetResponse, FrontendError>> + Unpin;
    /// A stream for the response to a `store` command.
    type StoreStream: Stream<Item = Result<blob::StoreResponse, FrontendError>> + Unpin;
    /// A stream for the response to an `update` command.
    type UpdateStream: Stream<Item = Result<blob::UpdateResponse, FrontendError>> + Unpin;
    /// A stream for the response to a `delete` command.
    type DeleteStream: Stream<Item = Result<blob::DeleteResponse, FrontendError>> + Unpin;

    /// Initialize the BLOB store.
    fn initialize(
        &self,
        #[allow(unused_variables)] connection: &Self::Connection,
    ) -> Result<(), FrontendError> {
        Ok(())
    }

    /// Connect to the BLOB store, initializing it if necessary.
    fn connect_blob(&self) -> Result<Self::Connection, FrontendError> {
        let conn = self.connect()?;
        self.initialize(&conn)?;
        Ok(conn)
    }

    /// Get the BLOB and associated metadata given the ID.
    fn get<Req>(&self, request: Req) -> future_send!(Result<Self::GetStream, FrontendError>)
    where
        Req: Stream<Item = Result<blob::GetRequest, FrontendError>> + Unpin + Send + 'static;

    /// Store a BLOB and associated metadata, returning the ID.
    fn store<Req>(&self, request: Req) -> future_send!(Result<Self::StoreStream, FrontendError>)
    where
        Req: Stream<Item = Result<blob::StoreRequest, FrontendError>> + Unpin + Send + 'static;

    /// Update the BLOB and/or its associated metadata.
    fn update<Req>(&self, request: Req) -> future_send!(Result<Self::UpdateStream, FrontendError>)
    where
        Req: Stream<Item = Result<blob::UpdateRequest, FrontendError>> + Unpin + Send + 'static;

    /// Delete the BLOB and associated metadata given the ID.
    fn delete<Req>(&self, request: Req) -> future_send!(Result<Self::DeleteStream, FrontendError>)
    where
        Req: Stream<Item = Result<blob::DeleteRequest, FrontendError>> + Unpin + Send + 'static;

    /// Determine if all provided IDs have the same data. Metadata is not considered.
    fn eq_data<Req>(&self, request: Req) -> future_send!(Result<bool, FrontendError>)
    where
        Req: Stream<Item = Result<blob::EqDataRequest, FrontendError>> + Unpin + Send + 'static;

    /// Determine if all provided IDs have different data. Metadata is not considered.
    fn not_eq_data<Req>(&self, request: Req) -> future_send!(Result<bool, FrontendError>)
    where
        Req: Stream<Item = Result<blob::NotEqDataRequest, FrontendError>> + Unpin + Send + 'static;
}
