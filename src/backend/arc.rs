use crate::backend::{BlobBackend, DatabaseBackend, KvBackend};
use crate::proto::{blob, kv};
use crate::{Location, StreamingRequest};
use std::sync::Arc;

impl<Backend> DatabaseBackend for Arc<Backend>
where
    Backend: DatabaseBackend,
{
    type Connection = Backend::Connection;
    type Error = Backend::Error;

    fn at_location(location: Location) -> Result<Self, Self::Error> {
        Backend::at_location(location).map(Self::new)
    }

    fn location(&self) -> &Location {
        self.as_ref().location()
    }

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.as_ref().connect()
    }
}

impl<Backend> KvBackend for Arc<Backend>
where
    Backend: KvBackend,
{
    type GetStream = Backend::GetStream;
    type SetStream = Backend::SetStream;
    type DeleteStream = Backend::DeleteStream;

    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error> {
        self.as_ref().initialize(connection)
    }

    async fn get(
        &self,
        request: StreamingRequest<kv::GetRequest>,
    ) -> crate::RpcResponse<Self::GetStream> {
        self.as_ref().get(request).await
    }

    async fn set(
        &self,
        request: StreamingRequest<kv::SetRequest>,
    ) -> crate::RpcResponse<Self::SetStream> {
        self.as_ref().set(request).await
    }

    async fn delete(
        &self,
        request: StreamingRequest<kv::DeleteRequest>,
    ) -> crate::RpcResponse<Self::DeleteStream> {
        self.as_ref().delete(request).await
    }

    async fn eq(&self, request: StreamingRequest<kv::EqRequest>) -> crate::RpcResponse<bool> {
        self.as_ref().eq(request).await
    }

    async fn not_eq(
        &self,
        request: StreamingRequest<kv::NotEqRequest>,
    ) -> crate::RpcResponse<bool> {
        self.as_ref().not_eq(request).await
    }
}

impl<Backend> BlobBackend for Arc<Backend>
where
    Backend: BlobBackend,
{
    type GetStream = Backend::GetStream;
    type StoreStream = Backend::StoreStream;
    type UpdateStream = Backend::UpdateStream;
    type DeleteStream = Backend::DeleteStream;

    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error> {
        self.as_ref().initialize(connection)
    }

    async fn get(
        &self,
        request: StreamingRequest<blob::GetRequest>,
    ) -> crate::RpcResponse<Self::GetStream> {
        self.as_ref().get(request).await
    }

    async fn store(
        &self,
        request: StreamingRequest<blob::StoreRequest>,
    ) -> crate::RpcResponse<Self::StoreStream> {
        self.as_ref().store(request).await
    }

    async fn update(
        &self,
        request: StreamingRequest<blob::UpdateRequest>,
    ) -> crate::RpcResponse<Self::UpdateStream> {
        self.as_ref().update(request).await
    }

    async fn delete(
        &self,
        request: StreamingRequest<blob::DeleteRequest>,
    ) -> crate::RpcResponse<Self::DeleteStream> {
        self.as_ref().delete(request).await
    }

    async fn eq_data(
        &self,
        request: StreamingRequest<blob::EqDataRequest>,
    ) -> crate::RpcResponse<bool> {
        self.as_ref().eq_data(request).await
    }

    async fn not_eq_data(
        &self,
        request: StreamingRequest<blob::NotEqDataRequest>,
    ) -> crate::RpcResponse<bool> {
        self.as_ref().not_eq_data(request).await
    }
}
