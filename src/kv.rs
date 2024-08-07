//! A key-value store.

use crate::backend::{DatabaseBackend, KvBackend, QueryBackend};
use crate::interop::IntoTonicStatus;
use crate::proto::kv::{DeleteRequest, EqRequest, GetRequest, NotEqRequest, SetRequest};
use crate::proto::query::RawQuery;
use crate::service::kv::KvRpc;
use crate::service::query::QueryRpc;
use crate::{Location, RpcResponse, StreamingRequest};
use std::path::PathBuf;

/// A key-value store.
///
/// This is a key-value store where both the key and value are strings. There are no restrictions on
/// the length or contents of either the key or value beyond restrictions implemented by the
/// protobuf server.
#[must_use]
#[derive(Debug)]
pub struct KvStore<Backend> {
    backend: Backend,
}

impl<Backend> KvStore<Backend>
where
    Backend: DatabaseBackend,
{
    /// Create a new key-value store at the given location. If not pre-existing, the store will not
    /// be initialized until the first connection is made.
    #[inline]
    pub fn at_location(location: Location) -> Result<Self, Backend::Error> {
        Ok(Self {
            backend: Backend::at_location(location)?,
        })
    }

    /// Create a new key-value at the given path on disk. If not pre-existing, the store will not be
    /// initialized until the first connection is made.
    #[inline]
    pub fn at_path<P>(path: P) -> Result<Self, Backend::Error>
    where
        P: Into<PathBuf>,
    {
        Self::at_location(Location::OnDisk { path: path.into() })
    }

    /// Create a new in-memory key-value store. This is useful for short-lived data.
    ///
    /// Note that all in-memory connections share the same stream, so any asynchronous calls have a
    /// nondeterministic order. This is not a problem for on-disk connections.
    #[inline]
    pub fn in_memory() -> Result<Self, Backend::Error> {
        Self::at_location(Location::InMemory)
    }
}

#[tonic::async_trait]
impl<Backend> QueryRpc for KvStore<Backend>
where
    Backend: QueryBackend<Error: IntoTonicStatus, QueryStream: Send, ExecuteStream: Send> + 'static,
{
    type QueryStream = Backend::QueryStream;
    type ExecuteStream = Backend::ExecuteStream;

    async fn query(&self, request: StreamingRequest<RawQuery>) -> RpcResponse<Self::QueryStream> {
        self.backend.query(request).await
    }

    async fn execute(
        &self,
        request: StreamingRequest<RawQuery>,
    ) -> RpcResponse<Self::ExecuteStream> {
        self.backend.execute(request).await
    }
}

#[tonic::async_trait]
impl<Backend> KvRpc for KvStore<Backend>
where
    Backend: KvBackend<Error: IntoTonicStatus, GetStream: Send, SetStream: Send, DeleteStream: Send>
        + 'static,
{
    type GetStream = Backend::GetStream;
    type SetStream = Backend::SetStream;
    type DeleteStream = Backend::DeleteStream;

    async fn get(&self, request: StreamingRequest<GetRequest>) -> RpcResponse<Self::GetStream> {
        self.backend.get(request).await
    }

    async fn set(&self, request: StreamingRequest<SetRequest>) -> RpcResponse<Self::SetStream> {
        self.backend.set(request).await
    }

    async fn delete(
        &self,
        request: StreamingRequest<DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        self.backend.delete(request).await
    }

    async fn eq(&self, request: StreamingRequest<EqRequest>) -> RpcResponse<bool> {
        self.backend.eq(request).await
    }

    async fn not_eq(&self, request: StreamingRequest<NotEqRequest>) -> RpcResponse<bool> {
        self.backend.not_eq(request).await
    }
}
