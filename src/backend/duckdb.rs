use std::sync::atomic::{AtomicBool, Ordering};

use crate::backend::{helpers, BlobBackend, DatabaseBackend, KvBackend};
use crate::conv::try_into_protobuf_any;
use crate::duckdb_helper::{params2, params3};
use crate::interop::into_tonic_status;
use crate::proto::{blob, kv, query};
use crate::queryable::Queryable;
use crate::tracing_shim::{trace_span, Instrument};
use crate::{DynStream, Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use duckdb::Connection;
use tonic::{async_trait, Response, Status};

/// A backend utilizing DuckDB.
#[derive(Debug)]
pub struct DuckDb {
    location: Location,
    initialized: AtomicBool,
}

impl DatabaseBackend for DuckDb {
    type Connection = Connection;
    type Error = duckdb::Error;

    fn at_location(location: Location) -> Result<Self, Self::Error> {
        Ok(Self {
            location,
            initialized: AtomicBool::new(false),
        })
    }

    fn location(&self) -> &Location {
        &self.location
    }

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match &self.location() {
            Location::InMemory => Connection::open_in_memory(),
            Location::OnDisk { path } => Connection::open(path),
        }
    }
}

impl Queryable for DuckDb {
    type Connection = Connection;
    type QueryStream = DynStream<Result<query::QueryResult, Status>>;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn query(query: String, connection: Connection) -> (Self::QueryStream, Connection) {
        // Needed until rust-lang/rust#128095 is resolved. At that point, `stream!` in combination
        // with `drop(statement);` can be used.`
        let (tx, rx) = crossbeam::channel::bounded(64);

        match connection.prepare(&query) {
            Ok(mut statement) => match statement.query([]) {
                Ok(mut rows) => {
                    while let Ok(Some(row)) = rows.next() {
                        let column_count = row.as_ref().column_count();
                        let mut values = Vec::with_capacity(column_count);
                        for i in 0..column_count {
                            match row
                                .get::<_, duckdb::types::Value>(i)
                                .map(try_into_protobuf_any)
                            {
                                Ok(Ok(value)) => values.push(value),
                                Ok(Err(err)) => {
                                    let _res = tx.send(Err(into_tonic_status(err)));
                                    break;
                                }
                                Err(err) => {
                                    let _res = tx.send(Err(into_tonic_status(err)));
                                    break;
                                }
                            }
                        }
                        let _res = tx.send(Ok(query::QueryResult { fields: values }));
                    }
                }
                Err(err) => {
                    let _res = tx.send(Err(into_tonic_status(err)));
                }
            },
            Err(err) => {
                let _res = tx.send(Err(into_tonic_status(err)));
            }
        }

        let stream = stream!({
            while let Ok(result) = rx.recv() {
                yield result;
            }
        })
        .instrument(trace_span!("DuckDB raw query"));

        (Box::pin(stream), connection)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn execute(
        query: String,
        connection: Connection,
    ) -> (Result<query::RowsChanged, Status>, Connection) {
        match connection
            .prepare(&query)
            .and_then(|mut statement| statement.execute([]))
        {
            Ok(rows_changed) => (
                Ok(query::RowsChanged {
                    rows_changed: rows_changed
                        .try_into()
                        .expect("more than 10^19 rows altered"),
                }),
                connection,
            ),
            Err(err) => (Err(into_tonic_status(err)), connection),
        }
    }
}

#[async_trait]
impl KvBackend for DuckDb {
    type GetStream = DynStream<Result<kv::GetResponse, Status>>;
    type SetStream = DynStream<Result<kv::SetResponse, Status>>;
    type DeleteStream = DynStream<Result<kv::DeleteResponse, Status>>;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error> {
        let _res = connection.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)",
            [],
        )?;
        self.initialized.store(true, Ordering::Relaxed);
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn connect_kv(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        if !self.initialized.load(Ordering::Relaxed) {
            KvBackend::initialize(self, &conn)?;
        }
        Ok(conn)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn get(&self, request: StreamingRequest<kv::GetRequest>) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = stream!({
            while let Some(kv::GetRequest { key }) = stream.message().await? {
                let value = db
                    .query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                        row.get(0)
                    })
                    .map_err(into_tonic_status)?;
                yield Ok(kv::GetResponse {
                    value: String::from_utf8(value)
                        .expect("protobuf requires strings be valid UTF-8"),
                });
            }
        })
        .instrument(trace_span!("DuckDB kv get query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn set(&self, request: StreamingRequest<kv::SetRequest>) -> RpcResponse<Self::SetStream> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = stream!({
            while let Some(kv::SetRequest { key, value }) = stream.message().await? {
                db.execute(
                    "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
                    [&key, &value],
                )
                .map_err(into_tonic_status)?;
                yield Ok(kv::SetResponse { key });
            }
        })
        .instrument(trace_span!("DuckDB kv set query"));
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
                db.execute("DELETE FROM kv WHERE key = ?", [&key])
                    .map_err(into_tonic_status)?;
                yield Ok(kv::DeleteResponse { key });
            }
        })
        .instrument(trace_span!("DuckDB kv delete query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn eq(&self, request: StreamingRequest<kv::EqRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = Box::pin(stream!({
            while let Some(kv::EqRequest { key }) = stream.message().await? {
                let value = db
                    .query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                        row.get(0)
                    })
                    .map_err(into_tonic_status)?;
                yield Ok::<_, Status>(
                    String::from_utf8(value).expect("protobuf requires strings be valid UTF-8"),
                );
            }
        }))
        .instrument(trace_span!("DuckDB kv eq query"));
        Ok(Response::new(helpers::all_eq(stream).await?))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn not_eq(&self, request: StreamingRequest<kv::NotEqRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let stream = Box::pin(stream!({
            while let Some(kv::NotEqRequest { key }) = stream.message().await? {
                let value = db
                    .query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                        row.get(0)
                    })
                    .map_err(into_tonic_status)?;
                yield Ok::<_, Status>(
                    String::from_utf8(value).expect("protobuf requires strings be valid UTF-8"),
                );
            }
        }))
        .instrument(trace_span!("DuckDB kv not_eq query"));
        Ok(Response::new(helpers::all_not_eq(stream).await?))
    }
}

#[async_trait]
impl BlobBackend for DuckDb {
    type GetStream = DynStream<Result<blob::GetResponse, Status>>;
    type StoreStream = DynStream<Result<blob::StoreResponse, Status>>;
    type UpdateStream = DynStream<Result<blob::UpdateResponse, Status>>;
    type DeleteStream = DynStream<Result<blob::DeleteResponse, Status>>;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error> {
        connection.execute_batch(
            "CREATE SEQUENCE IF NOT EXISTS blob_id_seq START 1;
            CREATE TABLE IF NOT EXISTS blob(
                id INTEGER PRIMARY KEY DEFAULT nextval('blob_id_seq'),
                data BLOB,
                metadata TEXT
            );",
        )?;
        self.initialized.store(true, Ordering::Relaxed);
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn connect_blob(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        if !self.initialized.load(Ordering::Relaxed) {
            BlobBackend::initialize(self, &conn)?;
        }
        Ok(conn)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn get(
        &self,
        request: StreamingRequest<blob::GetRequest>,
    ) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let stream = stream!({
            while let Some(blob::GetRequest { id }) = stream.message().await? {
                let (data, metadata) = db
                    .query_row(
                        "SELECT data, metadata FROM blob WHERE id = ?",
                        [id],
                        |row| {
                            let data: Vec<u8> = row.get(0)?;
                            let metadata: Option<String> = row.get(1)?;
                            Ok((data, metadata))
                        },
                    )
                    .map_err(into_tonic_status)?;

                yield Ok(blob::GetResponse {
                    bytes: data,
                    metadata,
                });
            }
        })
        .instrument(trace_span!("DuckDB blob get query"));
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
            while let Some(blob::StoreRequest { bytes, metadata }) = stream.message().await? {
                let id = db
                    .query_row(
                        "INSERT INTO blob(data, metadata) VALUES(?, ?) RETURNING id",
                        params2(bytes, metadata),
                        |row| row.get(0),
                    )
                    .map_err(into_tonic_status)?;
                yield Ok(blob::StoreResponse { id });
            }
        })
        .instrument(trace_span!("DuckDB blob store query"));
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
            while let Some(blob::UpdateRequest {
                id,
                bytes,
                should_update_metadata,
                metadata,
            }) = stream.message().await?
            {
                match (bytes, should_update_metadata) {
                    (None, false) => {}
                    (Some(bytes), true) => {
                        db.execute(
                            "UPDATE blob SET data = ?, metadata = ? WHERE id = ?",
                            params3(bytes, metadata, id),
                        )
                        .map_err(into_tonic_status)?;
                    }
                    (None, true) => {
                        db.execute(
                            "UPDATE blob SET metadata = ? WHERE id = ?",
                            params2(metadata, id),
                        )
                        .map_err(into_tonic_status)?;
                    }
                    (Some(bytes), false) => {
                        db.execute("UPDATE blob SET data = ? WHERE id = ?", params2(bytes, id))
                            .map_err(into_tonic_status)?;
                    }
                }
                yield Ok(blob::UpdateResponse { id });
            }
        })
        .instrument(trace_span!("DuckDB blob update query"));
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
            while let Some(blob::DeleteRequest { id }) = stream.message().await? {
                db.execute("DELETE FROM blob WHERE id = ?", [id])
                    .map_err(into_tonic_status)?;
                yield Ok(blob::DeleteResponse { id });
            }
        })
        .instrument(trace_span!("DuckDB blob delete query"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn eq_data(&self, request: StreamingRequest<blob::EqDataRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let stream = Box::pin(stream!({
            while let Some(blob::EqDataRequest { id }) = stream.message().await? {
                let data = db
                    .query_row("SELECT data FROM blob WHERE id = ?", [id], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .map_err(into_tonic_status)?;

                yield Ok::<_, Status>(data);
            }
        }))
        .instrument(trace_span!("DuckDB blob eq_data query"));
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
            while let Some(blob::NotEqDataRequest { id }) = stream.message().await? {
                let data = db
                    .query_row("SELECT data FROM blob WHERE id = ?", [id], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .map_err(into_tonic_status)?;

                yield Ok::<_, Status>(data);
            }
        }))
        .instrument(trace_span!("DuckDB blob not_eq_data query"));
        Ok(Response::new(helpers::all_not_eq(stream).await?))
    }
}
