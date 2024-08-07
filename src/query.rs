use crate::backend::DatabaseBackend;
use crate::interop::{into_tonic_status, IntoTonicStatus};
use crate::proto::query::{QueryResult, RawQuery, RowsChanged, TargetStore};
use crate::queryable::Queryable;
use crate::service::query::QueryRpc;
use crate::{DynStream, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::StreamExt as _;
use tonic::{Response, Status};

/// The handler for raw queries. Supports both key-value and blob stores.
#[must_use]
#[derive(Debug)]
pub struct QueryHandler<Backend> {
    kv_backend: Backend,
    blob_backend: Backend,
}

impl<Backend> QueryHandler<Backend>
where
    Backend: DatabaseBackend,
{
    // Deliberately not exposing an `at_location` and `in_memory` method for this struct, as it
    // would require additional validation to ensure not more than one location is in memory.

    /// Create a new query handler at the given paths on disk. No initialization is performed.
    pub fn at_path<P>(kv_path: P, blob_path: P) -> Result<Self, Backend::Error>
    where
        P: Into<std::path::PathBuf>,
    {
        Ok(Self {
            kv_backend: Backend::at_location(kv_path.into().into())?,
            blob_backend: Backend::at_location(blob_path.into().into())?,
        })
    }
}

#[tonic::async_trait]
impl<Backend> QueryRpc for QueryHandler<Backend>
where
    Backend: DatabaseBackend<Error: IntoTonicStatus, Connection: Send>
        + Queryable<Connection = <Backend as DatabaseBackend>::Connection, QueryStream: Send>
        + Send
        + Sync
        + 'static,
{
    type QueryStream = DynStream<Result<QueryResult, Status>>;
    type ExecuteStream = DynStream<Result<RowsChanged, Status>>;

    async fn query(&self, request: StreamingRequest<RawQuery>) -> RpcResponse<Self::QueryStream> {
        let mut request = request.into_inner();

        let mut kv_conn = self.kv_backend.connect().map_err(into_tonic_status)?;
        let mut blob_conn = self.blob_backend.connect().map_err(into_tonic_status)?;

        let stream = stream!({
            while let Some(RawQuery { query, target }) = request.message().await? {
                match target.try_into().map_err(into_tonic_status)? {
                    TargetStore::Kv => {
                        let (mut items, conn) = Backend::query(query, kv_conn).await;
                        kv_conn = conn;
                        while let Some(item) = items.next().await {
                            yield item;
                        }
                    }
                    TargetStore::Blob => {
                        let (mut items, conn) = Backend::query(query, blob_conn).await;
                        blob_conn = conn;
                        while let Some(item) = items.next().await {
                            yield item;
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn execute(
        &self,
        request: StreamingRequest<RawQuery>,
    ) -> RpcResponse<Self::ExecuteStream> {
        let mut request = request.into_inner();

        let mut kv_conn = self.kv_backend.connect().map_err(into_tonic_status)?;
        let mut blob_conn = self.blob_backend.connect().map_err(into_tonic_status)?;

        let stream = stream!({
            while let Some(RawQuery { query, target }) = request.message().await? {
                match target.try_into().map_err(into_tonic_status)? {
                    TargetStore::Kv => {
                        let (res, conn) = Backend::execute(query, kv_conn).await;
                        kv_conn = conn;
                        yield res;
                    }
                    TargetStore::Blob => {
                        let (res, conn) = Backend::execute(query, blob_conn).await;
                        blob_conn = conn;
                        yield res;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }
}
