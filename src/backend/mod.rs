#![allow(missing_docs)] // TODO temporaray

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "sqlite")]
mod sqlite;

mod sealed {
    pub trait Sealed {}
    #[cfg(feature = "duckdb")]
    impl Sealed for super::duckdb::DuckDb {}
    #[cfg(feature = "sqlite")]
    impl Sealed for super::sqlite::Sqlite {}
}

#[cfg(feature = "duckdb")]
pub use self::duckdb::DuckDb;
#[cfg(feature = "sqlite")]
pub use self::sqlite::Sqlite;
use crate::proto::{blob, kv, query};
use crate::{Location, RpcResponse, StreamingRequest};
use futures::Stream;
use tonic::async_trait;

pub trait DatabaseBackend: sealed::Sealed + Sized {
    type Connection;
    type Error;

    // TODO Consider a different error type such that in-memory connections can be rejected as
    // necessary.
    fn at_location(location: Location) -> Result<Self, Self::Error>;
    fn location(&self) -> &Location;

    /// Note: Backends for specific stores provide their own versions of `connect` that initialize
    /// the store as necessary. It is recommended to **not** call this method directly unless you
    /// are implementing a new backend.
    fn connect(&self) -> Result<Self::Connection, Self::Error>;
}

#[async_trait]
pub trait QueryBackend: DatabaseBackend {
    type QueryStream: Stream<Item = Result<query::QueryResult, tonic::Status>>;
    type ExecuteStream: Stream<Item = Result<query::RowsChanged, tonic::Status>>;

    async fn query(
        &self,
        request: StreamingRequest<query::RawQuery>,
    ) -> RpcResponse<Self::QueryStream>;
    async fn execute(
        &self,
        request: StreamingRequest<query::RawQuery>,
    ) -> RpcResponse<Self::ExecuteStream>;
}

#[async_trait]
pub trait KvBackend: DatabaseBackend + Send + Sync {
    type GetStream: Stream<Item = Result<kv::GetResponse, tonic::Status>>;
    type SetStream: Stream<Item = Result<kv::SetResponse, tonic::Status>>;
    type DeleteStream: Stream<Item = Result<kv::DeleteResponse, tonic::Status>>;

    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error>;
    fn connect_kv(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        self.initialize(&conn)?;
        Ok(conn)
    }
    async fn get(&self, request: StreamingRequest<kv::GetRequest>) -> RpcResponse<Self::GetStream>;
    async fn set(&self, request: StreamingRequest<kv::SetRequest>) -> RpcResponse<Self::SetStream>;
    async fn delete(
        &self,
        request: StreamingRequest<kv::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream>;
    async fn eq(&self, request: StreamingRequest<kv::EqRequest>) -> RpcResponse<bool>;
    async fn not_eq(&self, request: StreamingRequest<kv::NotEqRequest>) -> RpcResponse<bool>;
}

#[async_trait]
pub trait BlobBackend: DatabaseBackend + Send + Sync {
    type GetStream: Stream<Item = Result<blob::GetResponse, tonic::Status>>;
    type StoreStream: Stream<Item = Result<blob::StoreResponse, tonic::Status>>;
    type UpdateStream: Stream<Item = Result<blob::UpdateResponse, tonic::Status>>;
    type DeleteStream: Stream<Item = Result<blob::DeleteResponse, tonic::Status>>;

    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error>;
    fn connect_blob(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        self.initialize(&conn)?;
        Ok(conn)
    }
    async fn get(
        &self,
        request: StreamingRequest<blob::GetRequest>,
    ) -> RpcResponse<Self::GetStream>;
    async fn store(
        &self,
        request: StreamingRequest<blob::StoreRequest>,
    ) -> RpcResponse<Self::StoreStream>;
    async fn update(
        &self,
        request: StreamingRequest<blob::UpdateRequest>,
    ) -> RpcResponse<Self::UpdateStream>;
    async fn delete(
        &self,
        request: StreamingRequest<blob::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream>;
    async fn eq_data(&self, request: StreamingRequest<blob::EqDataRequest>) -> RpcResponse<bool>;
    async fn not_eq_data(
        &self,
        request: StreamingRequest<blob::NotEqDataRequest>,
    ) -> RpcResponse<bool>;
}
