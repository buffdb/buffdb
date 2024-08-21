use crate::backend::{helpers, BlobBackend, DatabaseBackend, KvBackend};
use crate::interop::DatabaseError;
use crate::structs::{blob, kv};
use crate::tracing_shim::{trace_span, Instrument as _};
use crate::Location;
use async_stream::stream;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt as _};
use rand::{Rng, SeedableRng};
use rocksdb::TransactionDB;

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
            // TODO make this generic over frontend errors
            None => Err(tonic::Status::internal("Failed to get ColumnFamily handle")),
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
                let fields = ["data", "metadata"];
                Self::Connection::open_cf(&opts, &txn_opts, path, fields)
            }
        }
    }
}

impl<FrontendError> KvBackend<FrontendError> for RocksDb
where
    FrontendError: From<DatabaseError<rocksdb::Error>> + Send + 'static,
{
    type GetStream = BoxStream<'static, Result<kv::GetResponse, FrontendError>>;
    type SetStream = BoxStream<'static, Result<kv::SetResponse, FrontendError>>;
    type DeleteStream = BoxStream<'static, Result<kv::DeleteResponse, FrontendError>>;

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn get<Req>(&self, mut stream: Req) -> Result<Self::GetStream, FrontendError>
    where
        Req: Stream<Item = Result<kv::GetRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_kv()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(kv::GetRequest { key }) => {
                        let Some(value) = db.get(&key).map_err(DatabaseError)? else {
                            return Err(Status::not_found(format!("key {key} not found")))?;
                        };
                        yield Ok(kv::GetResponse {
                            value: String::from_utf8(value)
                                .expect("protobuf requires strings be valid UTF-8"),
                        });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB kv get query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn set<Req>(&self, mut stream: Req) -> Result<Self::SetStream, FrontendError>
    where
        Req: Stream<Item = Result<kv::SetRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_kv()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(kv::SetRequest { key, value }) => {
                        db.put(&key, value.as_bytes()).map_err(DatabaseError)?;
                        yield Ok(kv::SetResponse { key });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB kv set query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn delete<Req>(&self, mut stream: Req) -> Result<Self::DeleteStream, FrontendError>
    where
        Req: Stream<Item = Result<kv::DeleteRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_kv()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(kv::DeleteRequest { key }) => {
                        db.delete(&key).map_err(DatabaseError)?;
                        yield Ok(kv::DeleteResponse { key });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB kv delete query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn eq<Req>(&self, mut stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<kv::EqRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_kv()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(kv::EqRequest { key }) => {
                        let Some(value) = db.get(&key).map_err(DatabaseError)? else {
                            return Err(Status::not_found(format!("key {key} not found")))?;
                        };
                        yield Ok(value);
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB kv eq query"));
        helpers::all_eq(stream).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn not_eq<Req>(&self, mut stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<kv::NotEqRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_kv()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(kv::NotEqRequest { key }) => {
                        let Some(value) = db.get(&key).map_err(DatabaseError)? else {
                            return Err(Status::not_found(format!("key {key} not found")))?;
                        };
                        yield Ok(value);
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB kv not_eq query"));
        helpers::all_not_eq(stream).await
    }
}

impl<FrontendError> BlobBackend<FrontendError> for RocksDb
where
    FrontendError: From<DatabaseError<rocksdb::Error>> + Send + 'static,
{
    type GetStream = BoxStream<'static, Result<blob::GetResponse, FrontendError>>;
    type StoreStream = BoxStream<'static, Result<blob::StoreResponse, FrontendError>>;
    type UpdateStream = BoxStream<'static, Result<blob::UpdateResponse, FrontendError>>;
    type DeleteStream = BoxStream<'static, Result<blob::DeleteResponse, FrontendError>>;

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn get<Req>(&self, mut stream: Req) -> Result<Self::GetStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::GetRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;
            let metadata_col = cf_handle!(db, "metadata")?;

            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::GetRequest { id }) => {
                        let Some(data) = db
                            .get_cf(data_col, id.to_le_bytes())
                            .map_err(DatabaseError)?
                        else {
                            return Err(Status::not_found(format!("id {id} not found")))?;
                        };
                        let metadata = db
                            .get_cf(metadata_col, id.to_le_bytes())
                            .map_err(DatabaseError)?
                            .map(|value| {
                                String::from_utf8(value)
                                    .expect("protobuf requires strings be valid UTF-8")
                            });
                        yield Ok(blob::GetResponse { data, metadata });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB blob get query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn store<Req>(&self, mut stream: Req) -> Result<Self::StoreStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::StoreRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;
            let metadata_col = cf_handle!(db, "metadata")?;

            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::StoreRequest { data, metadata }) => {
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
                        txn.put_cf(data_col, id_bytes, data)
                            .map_err(DatabaseError)?;

                        if let Some(metadata) = metadata {
                            txn.put_cf(metadata_col, id_bytes, metadata)
                        } else {
                            Ok(())
                        }
                        .map_err(DatabaseError)?;

                        txn.commit().map_err(DatabaseError)?;
                        yield Ok(blob::StoreResponse { id });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB blob get query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn update<Req>(&self, mut stream: Req) -> Result<Self::UpdateStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::UpdateRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;
            let metadata_col = cf_handle!(db, "metadata")?;

            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::UpdateRequest { id, data, metadata }) => {
                        let txn = db.transaction();
                        if let Some(data) = data {
                            txn.put_cf(data_col, id.to_le_bytes(), &data)
                                .map_err(DatabaseError)?;
                        }

                        if let Some(metadata) = metadata {
                            if let Some(metadata) = metadata {
                                txn.put_cf(metadata_col, id.to_le_bytes(), metadata)
                            } else {
                                txn.delete_cf(metadata_col, id.to_le_bytes())
                            }
                            .map_err(DatabaseError)?;
                        }
                        txn.commit().map_err(DatabaseError)?;
                        yield Ok(blob::UpdateResponse { id });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB blob update query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn delete<Req>(&self, mut stream: Req) -> Result<Self::DeleteStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::DeleteRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;
            let metadata_col = cf_handle!(db, "metadata")?;

            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::DeleteRequest { id }) => {
                        db.delete_cf(data_col, id.to_le_bytes())
                            .and_then(|_| db.delete_cf(metadata_col, id.to_le_bytes()))
                            .map_err(DatabaseError)?;
                        yield Ok(blob::DeleteResponse { id });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB blob delete query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn eq_data<Req>(&self, mut stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<blob::EqDataRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;

            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::EqDataRequest { id }) => {
                        let Some(value) = db
                            .get_cf(data_col, id.to_le_bytes())
                            .map_err(DatabaseError)?
                        else {
                            return Err(Status::not_found(format!("id {id} not found")))?;
                        };
                        yield Ok(value);
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB blob eq_data query"));
        helpers::all_eq(stream).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn not_eq_data<Req>(&self, mut stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<blob::NotEqDataRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            let data_col = cf_handle!(db, "data")?;

            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::NotEqDataRequest { id }) => {
                        let Some(value) = db
                            .get_cf(data_col, id.to_le_bytes())
                            .map_err(DatabaseError)?
                        else {
                            return Err(Status::not_found(format!("id {id} not found")))?;
                        };
                        yield Ok(value);
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("RocksDB blob not_eq_data query"));
        helpers::all_not_eq(stream).await
    }
}
