use crate::backend::{helpers, BlobBackend, DatabaseBackend, KvBackend};
use crate::interop::into_tonic_status;
use crate::proto::{blob, kv};
use crate::tracing_shim::{trace_span, Instrument as _};
use crate::{DynStream, Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use rand::{Rng, SeedableRng};
use rocksdb::TransactionDB;
use tonic::{Response, Status};

/// A backend utilizing RocksDb.
#[derive(Debug)]
pub struct RocksDb {
    location: Location,
}

fn generate_id() -> u64 {
    rand::rngs::SmallRng::from_entropy().r#gen()
}

macro_rules! cf_handle {
    ($db:expr, $name:expr) => {
        match $db.cf_handle($name) {
            Some(value) => Ok(value),
            None => Err(Status::internal("Failed to get ColumnFamily handle")),
        }
    };
}

impl DatabaseBackend for RocksDb {
    type Connection = TransactionDB;
    type Error = rocksdb::Error;

    fn at_location(location: Location) -> Result<Self, Self::Error> {
        Ok(Self { location })
    }

    fn location(&self) -> &Location {
        &self.location
    }

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match &self.location() {
            #[allow(clippy::unimplemented)]
            Location::InMemory => unimplemented!(),
            Location::OnDisk { path } => {
                let mut opts = rocksdb::Options::default();
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
                let txn_opts = rocksdb::TransactionDBOptions::default();
                let fields = vec!["data", "metadata"];
                Self::Connection::open_cf(&opts, &txn_opts, path, fields)
            }
        }
    }
}

impl KvBackend for RocksDb {
    type GetStream = DynStream<Result<kv::GetResponse, Status>>;
    type SetStream = DynStream<Result<kv::SetResponse, Status>>;
    type DeleteStream = DynStream<Result<kv::DeleteResponse, Status>>;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn get(&self, request: StreamingRequest<kv::GetRequest>) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = stream!({
            while let Some(kv::GetRequest { key }) = stream.message().await? {
                let Some(value) = db.get(&key).map_err(into_tonic_status)? else {
                    return Err(Status::not_found(format!("key {key} not found")))?;
                };
                yield Ok(kv::GetResponse {
                    value: String::from_utf8(value)
                        .expect("protobuf requires strings be valid UTF-8"),
                });
            }
        })
        .instrument(trace_span!("RocksDB kv get query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn set(&self, request: StreamingRequest<kv::SetRequest>) -> RpcResponse<Self::SetStream> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = stream!({
            while let Some(kv::SetRequest { key, value }) = stream.message().await? {
                db.put(&key, value.as_bytes()).map_err(into_tonic_status)?;
                yield Ok(kv::SetResponse { key });
            }
        })
        .instrument(trace_span!("RocksDB kv set query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn delete(
        &self,
        request: StreamingRequest<kv::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = stream!({
            while let Some(kv::DeleteRequest { key }) = stream.message().await? {
                db.delete(&key).map_err(into_tonic_status)?;
                yield Ok(kv::DeleteResponse { key });
            }
        })
        .instrument(trace_span!("RocksDB kv delete query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn eq(&self, request: StreamingRequest<kv::EqRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = Box::pin(stream!({
            while let Some(kv::EqRequest { key }) = stream.message().await? {
                let Some(value) = db.get(&key).map_err(into_tonic_status)? else {
                    return Err(Status::not_found(format!("key {key} not found")))?;
                };
                yield Ok::<_, Status>(value);
            }
        }))
        .instrument(trace_span!("RocksDB kv eq query"));
        Ok(Response::new(helpers::all_eq(stream).await?))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn not_eq(&self, request: StreamingRequest<kv::NotEqRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = Box::pin(stream!({
            while let Some(kv::NotEqRequest { key }) = stream.message().await? {
                let Some(value) = db.get(&key).map_err(into_tonic_status)? else {
                    return Err(Status::not_found(format!("key {key} not found")))?;
                };
                yield Ok::<_, Status>(value);
            }
        }))
        .instrument(trace_span!("RocksDB kv not_eq query"));
        Ok(Response::new(helpers::all_not_eq(stream).await?))
    }
}

impl BlobBackend for RocksDb {
    type GetStream = DynStream<Result<blob::GetResponse, Status>>;
    type StoreStream = DynStream<Result<blob::StoreResponse, Status>>;
    type UpdateStream = DynStream<Result<blob::UpdateResponse, Status>>;
    type DeleteStream = DynStream<Result<blob::DeleteResponse, Status>>;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn get(
        &self,
        request: StreamingRequest<blob::GetRequest>,
    ) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;
            let metadata_col = cf_handle!(db, "metadata")?;

            while let Some(blob::GetRequest { id }) = stream.message().await? {
                let Some(data) = db
                    .get_cf(data_col, id.to_le_bytes())
                    .map_err(into_tonic_status)?
                else {
                    return Err(Status::not_found(format!("id {id} not found")))?;
                };
                let metadata = db
                    .get_cf(metadata_col, id.to_le_bytes())
                    .map_err(into_tonic_status)?
                    .map(|value| {
                        String::from_utf8(value).expect("protobuf requires strings be valid UTF-8")
                    });
                yield Ok(blob::GetResponse {
                    bytes: data,
                    metadata,
                });
            }
        })
        .instrument(trace_span!("RocksDB blob get query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn store(
        &self,
        request: StreamingRequest<blob::StoreRequest>,
    ) -> RpcResponse<Self::StoreStream> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;
            let metadata_col = cf_handle!(db, "metadata")?;

            while let Some(blob::StoreRequest { bytes, metadata }) = stream.message().await? {
                let mut id = generate_id();
                let mut id_bytes = id.to_le_bytes();

                // Check for a collision of the generated identifier. If there is one, try once more
                // before erroring. Note that there is a TOCTOU issue here, but the odds of any
                // collision at all is so low that it is hardly worth worrying about that. If this
                // somehow becomes a plausible issue, the ID can be extended to 128 bits from the
                // current 64, rendering a collision all but impossible.
                if matches!(db.get_cf(data_col, id_bytes), Ok(Some(_))) {
                    id = generate_id();
                    id_bytes = id.to_le_bytes();

                    if matches!(db.get_cf(data_col, id_bytes), Ok(Some(_))) {
                        return Err(Status::internal("failed to generate unique id"))?;
                    }
                };

                let txn = db.transaction();
                txn.put_cf(data_col, id_bytes, bytes)
                    .map_err(into_tonic_status)?;

                if let Some(metadata) = metadata {
                    txn.put_cf(metadata_col, id_bytes, metadata)
                } else {
                    Ok(())
                }
                .map_err(into_tonic_status)?;

                txn.commit().map_err(into_tonic_status)?;
                yield Ok(blob::StoreResponse { id });
            }
        })
        .instrument(trace_span!("RocksDB blob get query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn update(
        &self,
        request: StreamingRequest<blob::UpdateRequest>,
    ) -> RpcResponse<Self::UpdateStream> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;
            let metadata_col = cf_handle!(db, "metadata")?;

            while let Some(blob::UpdateRequest {
                id,
                bytes,
                should_update_metadata,
                metadata,
            }) = stream.message().await?
            {
                let txn = db.transaction();
                if let Some(bytes) = bytes {
                    txn.put_cf(data_col, id.to_le_bytes(), &bytes)
                        .map_err(into_tonic_status)?;
                }

                if should_update_metadata {
                    if let Some(metadata) = metadata {
                        txn.put_cf(metadata_col, id.to_le_bytes(), metadata)
                    } else {
                        txn.delete_cf(metadata_col, id.to_le_bytes())
                    }
                    .map_err(into_tonic_status)?;
                }
                txn.commit().map_err(into_tonic_status)?;
                yield Ok(blob::UpdateResponse { id });
            }
        })
        .instrument(trace_span!("RocksDB blob update query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn delete(
        &self,
        request: StreamingRequest<blob::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;
        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;
            let metadata_col = cf_handle!(db, "metadata")?;

            while let Some(blob::DeleteRequest { id }) = stream.message().await? {
                db.delete_cf(data_col, id.to_le_bytes())
                    .and_then(|_| db.delete_cf(metadata_col, id.to_le_bytes()))
                    .map_err(into_tonic_status)?;
                yield Ok(blob::DeleteResponse { id });
            }
        })
        .instrument(trace_span!("RocksDB blob delete query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn eq_data(&self, request: StreamingRequest<blob::EqDataRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let stream = Box::pin(stream!({
            let data_col = cf_handle!(db, "data")?;

            while let Some(blob::EqDataRequest { id }) = stream.message().await? {
                let Some(value) = db
                    .get_cf(data_col, id.to_le_bytes())
                    .map_err(into_tonic_status)?
                else {
                    return Err(Status::not_found(format!("id {id} not found")))?;
                };
                yield Ok::<_, Status>(value);
            }
        }))
        .instrument(trace_span!("RocksDB blob eq_data query"));
        Ok(Response::new(helpers::all_eq(stream).await?))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn not_eq_data(
        &self,
        request: StreamingRequest<blob::NotEqDataRequest>,
    ) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let stream = Box::pin(stream!({
            let data_col = cf_handle!(db, "data")?;

            while let Some(blob::NotEqDataRequest { id }) = stream.message().await? {
                let Some(value) = db
                    .get_cf(data_col, id.to_le_bytes())
                    .map_err(into_tonic_status)?
                else {
                    return Err(Status::not_found(format!("id {id} not found")))?;
                };
                yield Ok::<_, Status>(value);
            }
        }))
        .instrument(trace_span!("RocksDB blob not_eq_data query"));
        Ok(Response::new(helpers::all_not_eq(stream).await?))
    }
}
