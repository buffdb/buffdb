//! A key-value handler.

use crate::backend::{DatabaseBackend, KvBackend};
use crate::interop::DatabaseError;
use crate::service::kv::KvRpc;
use crate::{proto, structs, Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::stream::BoxStream;
use futures::StreamExt as _;
use std::path::PathBuf;

/// A key-value store.
///
/// This is a key-value store where both the key and value are strings. There are no restrictions on
/// the length or contents of either the key or value beyond restrictions implemented by the
/// frontend.
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
impl<Backend> KvRpc for KvStore<Backend>
where
    Backend: KvBackend<
            tonic::Status,
            Error: Into<DatabaseError<tonic::Status>>,
            GetStream: Send,
            SetStream: Send,
            DeleteStream: Send,
        > + 'static,
{
    type GetStream = BoxStream<'static, Result<proto::kv::GetResponse, tonic::Status>>;
    type SetStream = BoxStream<'static, Result<proto::kv::SetResponse, tonic::Status>>;
    type DeleteStream = BoxStream<'static, Result<proto::kv::DeleteResponse, tonic::Status>>;

    async fn get(
        &self,
        request: StreamingRequest<proto::kv::GetRequest>,
    ) -> RpcResponse<Self::GetStream> {
        let request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::kv::GetRequest { key: request.key });
            }
        });
        let mut response = self.backend.get(request).await?;
        let response = stream!({
            while let Some(res) = response.next().await {
                yield Ok(proto::kv::GetResponse { value: res?.value });
            }
        });
        Ok(tonic::Response::new(Box::pin(response)))
    }

    async fn set(
        &self,
        request: StreamingRequest<proto::kv::SetRequest>,
    ) -> RpcResponse<Self::SetStream> {
        let request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::kv::SetRequest {
                    key: request.key,
                    value: request.value,
                });
            }
        });
        let mut response = self.backend.set(request).await?;
        let response = stream!({
            while let Some(res) = response.next().await {
                yield Ok(proto::kv::SetResponse { key: res?.key });
            }
        });
        Ok(tonic::Response::new(Box::pin(response)))
    }

    async fn delete(
        &self,
        request: StreamingRequest<proto::kv::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::kv::DeleteRequest { key: request.key });
            }
        });
        let mut response = self.backend.delete(request).await?;
        let response = stream!({
            while let Some(res) = response.next().await {
                yield Ok(proto::kv::DeleteResponse { key: res?.key });
            }
        });
        Ok(tonic::Response::new(Box::pin(response)))
    }

    async fn eq(&self, request: StreamingRequest<proto::kv::EqRequest>) -> RpcResponse<bool> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::kv::EqRequest { key: request.key });
            }
        });
        self.backend.eq(request).await
    }

    async fn not_eq(
        &self,
        request: StreamingRequest<proto::kv::NotEqRequest>,
    ) -> RpcResponse<bool> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::kv::NotEqRequest { key: request.key });
            }
        });
        self.backend.not_eq(request).await
    }
}
