use crate::backend::DatabaseBackend;
use crate::conv::grpc::{try_into_protobuf_any, TryIntoProtobufAny};
use crate::interop::DatabaseError;
use crate::proto::query::{QueryResult, RawQuery, RowsChanged, TargetStore};
use crate::queryable::Queryable;
use crate::service::query::QueryRpc;
use crate::{Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::stream::BoxStream;
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
    /// Create a new query handler at the given locations. No initialization is performed.
    ///
    /// **Note**: At most one location can be in memory.
    #[inline]
    pub fn at_location(
        kv_location: Location,
        blob_location: Location,
    ) -> Result<Self, Backend::Error> {
        // TODO validate that both locations are not in memory
        Ok(Self {
            kv_backend: Backend::at_location(kv_location)?,
            blob_backend: Backend::at_location(blob_location)?,
        })
    }

    /// Create a new query handler at the given paths on disk. No initialization is performed.
    #[inline]
    pub fn at_path<P1, P2>(kv_path: P1, blob_path: P2) -> Result<Self, Backend::Error>
    where
        P1: Into<std::path::PathBuf>,
        P2: Into<std::path::PathBuf>,
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
    Backend: DatabaseBackend<Error: Into<DatabaseError<Status>>, Connection: Send>
        + Queryable<Status, QueryStream: Send, Any: TryIntoProtobufAny + Send>
        + Send
        + Sync
        + 'static,
{
    type QueryStream = BoxStream<'static, Result<QueryResult, Status>>;
    type ExecuteStream = BoxStream<'static, Result<RowsChanged, Status>>;

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    async fn query(&self, request: StreamingRequest<RawQuery>) -> RpcResponse<Self::QueryStream> {
        let mut request = request.into_inner();

        let mut kv_conn = self.kv_backend.connect()?;
        let mut blob_conn = self.blob_backend.connect()?;

        let stream = stream!({
            while let Some(RawQuery { query, target }) = request.message().await? {
                match target.try_into().map_err(DatabaseError)? {
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

        let stream = stream.map(|item| {
            Ok(QueryResult {
                fields: item?
                    .fields
                    .into_iter()
                    .map(try_into_protobuf_any)
                    .collect::<Result<_, _>>()?,
            })
        });

        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    async fn execute(
        &self,
        request: StreamingRequest<RawQuery>,
    ) -> RpcResponse<Self::ExecuteStream> {
        let mut request = request.into_inner();

        let mut kv_conn = self.kv_backend.connect()?;
        let mut blob_conn = self.blob_backend.connect()?;

        let stream = stream!({
            while let Some(RawQuery { query, target }) = request.message().await? {
                match target.try_into().map_err(DatabaseError)? {
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

        let stream = stream.map(|item| {
            Ok(RowsChanged {
                rows_changed: item?.rows_changed,
            })
        });

        Ok(Response::new(Box::pin(stream)))
    }
}
