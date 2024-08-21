//! A handler for binary large objects (BLOBs) with an optional metadata field.

use crate::backend::{BlobBackend, DatabaseBackend};
use crate::interop::DatabaseError;
use crate::service::blob::BlobRpc;
use crate::{proto, structs};
use crate::{Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::stream::BoxStream;
use futures::{StreamExt as _, TryStreamExt as _};
use std::path::PathBuf;

/// A handler for binary large objects (BLOBs) with an optional metadata field.
///
/// Metadata, if present, is a string that can be used to store any additional information about the
/// BLOB, such as a description or a name. Neither the BLOB or the metadata are required to be
/// unique.
#[must_use]
#[derive(Debug)]
pub struct BlobStore<Backend> {
    backend: Backend,
}

impl<Backend> BlobStore<Backend>
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
impl<Backend> BlobRpc for BlobStore<Backend>
where
    Backend: BlobBackend<
            tonic::Status,
            Error: Into<DatabaseError<tonic::Status>>,
            GetStream: Send,
            StoreStream: Send,
            UpdateStream: Send,
            DeleteStream: Send,
        > + 'static,
{
    type GetStream = BoxStream<'static, Result<proto::blob::GetResponse, tonic::Status>>;
    type StoreStream = BoxStream<'static, Result<proto::blob::StoreResponse, tonic::Status>>;
    type UpdateStream = BoxStream<'static, Result<proto::blob::UpdateResponse, tonic::Status>>;
    type DeleteStream = BoxStream<'static, Result<proto::blob::DeleteResponse, tonic::Status>>;

    async fn get(
        &self,
        request: StreamingRequest<proto::blob::GetRequest>,
    ) -> RpcResponse<Self::GetStream> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::blob::GetRequest { id: request.id });
            }
        });
        let mut response = self.backend.get(request).await?;
        let response = stream!({
            while let Some(res) = response.next().await {
                let res = res?;
                yield Ok(proto::blob::GetResponse {
                    data: res.data,
                    metadata: res.metadata,
                });
            }
        });
        Ok(tonic::Response::new(Box::pin(response)))
    }

    async fn store(
        &self,
        request: StreamingRequest<proto::blob::StoreRequest>,
    ) -> RpcResponse<Self::StoreStream> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::blob::StoreRequest {
                    data: request.data,
                    metadata: request.metadata,
                });
            }
        });
        let mut response = self.backend.store(request).await?;
        let response = stream!({
            while let Some(res) = response.next().await {
                yield Ok(proto::blob::StoreResponse { id: res?.id });
            }
        });
        Ok(tonic::Response::new(Box::pin(response)))
    }

    async fn update(
        &self,
        request: StreamingRequest<proto::blob::UpdateRequest>,
    ) -> RpcResponse<Self::UpdateStream> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::blob::UpdateRequest {
                    id: request.id,
                    data: request.data,
                    metadata: request.should_update_metadata.then_some(request.metadata),
                });
            }
        });
        let response = self
            .backend
            .update(request)
            .await?
            .map_ok(|res| proto::blob::UpdateResponse { id: res.id });
        Ok(tonic::Response::new(Box::pin(response)))
    }

    async fn delete(
        &self,
        request: StreamingRequest<proto::blob::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::blob::DeleteRequest { id: request.id });
            }
        });
        let response = self
            .backend
            .delete(request)
            .await?
            .map_ok(|res| proto::blob::DeleteResponse { id: res.id });
        Ok(tonic::Response::new(Box::pin(response)))
    }

    async fn eq_data(
        &self,
        request: StreamingRequest<proto::blob::EqDataRequest>,
    ) -> RpcResponse<bool> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::blob::EqDataRequest { id: request.id });
            }
        });
        self.backend
            .eq_data(request)
            .await
            .map(tonic::Response::new)
    }

    async fn not_eq_data(
        &self,
        request: StreamingRequest<proto::blob::NotEqDataRequest>,
    ) -> RpcResponse<bool> {
        let mut request = request.into_inner();
        let request = stream!({
            while let Some(request) = request.message().await? {
                yield Ok(structs::blob::NotEqDataRequest { id: request.id });
            }
        });
        self.backend
            .not_eq_data(request)
            .await
            .map(tonic::Response::new)
    }
}
