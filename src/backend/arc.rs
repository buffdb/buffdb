use crate::backend::{BlobBackend, DatabaseBackend, KvBackend};
use crate::queryable::Queryable;
use crate::structs::{blob, kv, query};
use crate::Location;
use futures::Stream;
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

impl<FrontendError, Backend> Queryable<FrontendError> for Arc<Backend>
where
    Backend: Queryable<FrontendError>,
{
    type QueryStream = Backend::QueryStream;
    type Any = Backend::Any;

    fn query(
        query: String,
        conn: Self::Connection,
    ) -> impl std::future::Future<Output = (Self::QueryStream, Self::Connection)> + Send {
        Backend::query(query, conn)
    }

    fn execute(
        query: String,
        conn: Self::Connection,
    ) -> impl std::future::Future<
        Output = (
            Result<query::ExecuteResponse, FrontendError>,
            Self::Connection,
        ),
    > + Send {
        Backend::execute(query, conn)
    }
}

impl<FrontendError, Backend> KvBackend<FrontendError> for Arc<Backend>
where
    Backend: KvBackend<FrontendError>,
{
    type GetStream = Backend::GetStream;
    type SetStream = Backend::SetStream;
    type DeleteStream = Backend::DeleteStream;

    fn initialize(&self, connection: &Self::Connection) -> Result<(), FrontendError> {
        self.as_ref().initialize(connection)
    }

    fn connect_kv(&self) -> Result<Self::Connection, FrontendError> {
        self.as_ref().connect_kv()
    }

    async fn get<Req>(&self, stream: Req) -> Result<Self::GetStream, FrontendError>
    where
        Req: Stream<Item = Result<kv::GetRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().get(stream).await
    }

    async fn set<Req>(&self, stream: Req) -> Result<Self::SetStream, FrontendError>
    where
        Req: Stream<Item = Result<kv::SetRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().set(stream).await
    }

    async fn delete<Req>(&self, stream: Req) -> Result<Self::DeleteStream, FrontendError>
    where
        Req: Stream<Item = Result<kv::DeleteRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().delete(stream).await
    }

    async fn eq<Req>(&self, stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<kv::EqRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().eq(stream).await
    }

    async fn not_eq<Req>(&self, stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<kv::NotEqRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().not_eq(stream).await
    }
}

impl<FrontendError, Backend> BlobBackend<FrontendError> for Arc<Backend>
where
    Backend: BlobBackend<FrontendError>,
{
    type GetStream = Backend::GetStream;
    type StoreStream = Backend::StoreStream;
    type UpdateStream = Backend::UpdateStream;
    type DeleteStream = Backend::DeleteStream;

    fn initialize(&self, connection: &Self::Connection) -> Result<(), FrontendError> {
        self.as_ref().initialize(connection)
    }

    fn connect_blob(&self) -> Result<Self::Connection, FrontendError> {
        self.as_ref().connect_blob()
    }

    async fn get<Req>(&self, stream: Req) -> Result<Self::GetStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::GetRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().get(stream).await
    }

    async fn store<Req>(&self, stream: Req) -> Result<Self::StoreStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::StoreRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().store(stream).await
    }

    async fn update<Req>(&self, stream: Req) -> Result<Self::UpdateStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::UpdateRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().update(stream).await
    }

    async fn delete<Req>(&self, stream: Req) -> Result<Self::DeleteStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::DeleteRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().delete(stream).await
    }

    async fn eq_data<Req>(&self, stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<blob::EqDataRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().eq_data(stream).await
    }

    async fn not_eq_data<Req>(&self, stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<blob::NotEqDataRequest, FrontendError>> + Unpin + Send + 'static,
    {
        self.as_ref().not_eq_data(stream).await
    }
}
