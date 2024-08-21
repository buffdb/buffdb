use crate::backend::{helpers, BlobBackend, DatabaseBackend, KvBackend};
use crate::duckdb_helper::{params2, params3};
use crate::interop::DatabaseError;
use crate::queryable::Queryable;
use crate::structs::{blob, kv, query};
use crate::tracing_shim::{trace_span, Instrument};
use crate::Location;
use async_stream::stream;
use duckdb::Connection;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt as _};
use std::sync::atomic::{AtomicBool, Ordering};

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

impl<FrontendError> Queryable<FrontendError> for DuckDb
where
    FrontendError: From<DatabaseError<Self::Error>> + Send + 'static,
{
    type QueryStream = BoxStream<'static, Result<query::QueryResponse<Self::Any>, FrontendError>>;
    type Any = duckdb::types::Value;

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
                            match row.get::<_, duckdb::types::Value>(i) {
                                Ok(value) => values.push(value),
                                Err(err) => {
                                    let _res = tx.send(Err(DatabaseError(err).into()));
                                    break;
                                }
                            }
                        }
                        let _res = tx.send(Ok(query::QueryResponse { fields: values }));
                    }
                }
                Err(err) => {
                    let _res = tx.send(Err(DatabaseError(err).into()));
                }
            },
            Err(err) => {
                let _res = tx.send(Err(DatabaseError(err).into()));
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
    ) -> (Result<query::ExecuteResponse, FrontendError>, Connection) {
        match connection
            .prepare(&query)
            .and_then(|mut statement| statement.execute([]))
        {
            Ok(rows_changed) => (
                Ok(query::ExecuteResponse {
                    rows_changed: rows_changed
                        .try_into()
                        .expect("more than 10^19 rows altered"),
                }),
                connection,
            ),
            Err(err) => (Err(DatabaseError(err).into()), connection),
        }
    }
}

impl<FrontendError> KvBackend<FrontendError> for DuckDb
where
    FrontendError: From<DatabaseError<duckdb::Error>> + Send + 'static,
{
    type GetStream = BoxStream<'static, Result<kv::GetResponse, FrontendError>>;
    type SetStream = BoxStream<'static, Result<kv::SetResponse, FrontendError>>;
    type DeleteStream = BoxStream<'static, Result<kv::DeleteResponse, FrontendError>>;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn initialize(&self, connection: &Self::Connection) -> Result<(), FrontendError> {
        let _res = connection.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)",
            [],
        )?;
        self.initialized.store(true, Ordering::Relaxed);
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn connect_kv(&self) -> Result<Self::Connection, FrontendError> {
        let conn = self.connect()?;
        if !self.initialized.load(Ordering::Relaxed) {
            KvBackend::initialize(self, &conn)?;
        }
        Ok(conn)
    }

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
                        let value = db
                            .query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                                row.get(0)
                            })
                            .map_err(DatabaseError)?;
                        yield Ok(kv::GetResponse { value });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB kv get query"));
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
                        db.execute(
                            "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
                            [&key, &value],
                        )
                        .map_err(DatabaseError)?;
                        yield Ok(kv::SetResponse { key });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB kv set query"));
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
                        db.execute("DELETE FROM kv WHERE key = ?", [&key])
                            .map_err(DatabaseError)?;
                        yield Ok(kv::DeleteResponse { key });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB kv delete query"));
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
                        let value = db
                            .query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                                row.get(0)
                            })
                            .map_err(DatabaseError)?;
                        yield Ok(String::from_utf8(value)
                            .expect("protobuf requires strings be valid UTF-8"));
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB kv eq query"));
        // let stream = std::pin::pin!(stream);
        Ok(helpers::all_eq(stream).await?)
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
                        let value = db
                            .query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                                row.get(0)
                            })
                            .map_err(DatabaseError)?;
                        yield Ok(String::from_utf8(value)
                            .expect("protobuf requires strings be valid UTF-8"));
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB kv not_eq query"));
        Ok(helpers::all_not_eq(stream).await?)
    }
}

impl<FrontendError> BlobBackend<FrontendError> for DuckDb
where
    FrontendError: From<DatabaseError<duckdb::Error>> + Send + 'static,
{
    type GetStream = BoxStream<'static, Result<blob::GetResponse, FrontendError>>;
    type StoreStream = BoxStream<'static, Result<blob::StoreResponse, FrontendError>>;
    type UpdateStream = BoxStream<'static, Result<blob::UpdateResponse, FrontendError>>;
    type DeleteStream = BoxStream<'static, Result<blob::DeleteResponse, FrontendError>>;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn initialize(&self, connection: &Self::Connection) -> Result<(), FrontendError> {
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
    fn connect_blob(&self) -> Result<Self::Connection, FrontendError> {
        let conn = self.connect()?;
        if !self.initialized.load(Ordering::Relaxed) {
            BlobBackend::initialize(self, &conn)?;
        }
        Ok(conn)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn get<Req>(&self, mut stream: Req) -> Result<Self::GetStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::GetRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::GetRequest { id }) => {
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
                            .map_err(DatabaseError)?;

                        yield Ok(blob::GetResponse { data, metadata });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB blob get query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn store<Req>(&self, mut stream: Req) -> Result<Self::StoreStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::StoreRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;

        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::StoreRequest { data, metadata }) => {
                        let id = db
                            .query_row(
                                "INSERT INTO blob(data, metadata) VALUES(?, ?) RETURNING id",
                                params2(data, metadata),
                                |row| row.get(0),
                            )
                            .map_err(DatabaseError)?;
                        yield Ok(blob::StoreResponse { id });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB blob store query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn update<Req>(&self, mut stream: Req) -> Result<Self::UpdateStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::UpdateRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::UpdateRequest { id, data, metadata }) => {
                        match (data, metadata) {
                            (None, None) => {}
                            (Some(data), Some(metadata)) => {
                                db.execute(
                                    "UPDATE blob SET data = ?, metadata = ? WHERE id = ?",
                                    params3(data, metadata, id),
                                )
                                .map_err(DatabaseError)?;
                            }
                            (None, Some(metadata)) => {
                                db.execute(
                                    "UPDATE blob SET metadata = ? WHERE id = ?",
                                    params2(metadata, id),
                                )
                                .map_err(DatabaseError)?;
                            }
                            (Some(data), None) => {
                                db.execute(
                                    "UPDATE blob SET data = ? WHERE id = ?",
                                    params2(data, id),
                                )
                                .map_err(DatabaseError)?;
                            }
                        }
                        yield Ok(blob::UpdateResponse { id });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB blob update query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn delete<Req>(&self, mut stream: Req) -> Result<Self::DeleteStream, FrontendError>
    where
        Req: Stream<Item = Result<blob::DeleteRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::DeleteRequest { id }) => {
                        db.execute("DELETE FROM blob WHERE id = ?", [id])
                            .map_err(DatabaseError)?;
                        yield Ok(blob::DeleteResponse { id });
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB blob delete query"));
        Ok(Box::pin(stream))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn eq_data<Req>(&self, mut stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<blob::EqDataRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::EqDataRequest { id }) => {
                        let data = db
                            .query_row("SELECT data FROM blob WHERE id = ?", [id], |row| {
                                row.get::<_, Vec<u8>>(0)
                            })
                            .map_err(DatabaseError)?;
                        yield Ok(data);
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB blob eq_data query"));
        helpers::all_eq(stream).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(stream)))]
    async fn not_eq_data<Req>(&self, mut stream: Req) -> Result<bool, FrontendError>
    where
        Req: Stream<Item = Result<blob::NotEqDataRequest, FrontendError>> + Unpin + Send + 'static,
    {
        let db = self.connect_blob()?;
        let stream = stream!({
            while let Some(request) = stream.next().await {
                match request {
                    Ok(blob::NotEqDataRequest { id }) => {
                        let data = db
                            .query_row("SELECT data FROM blob WHERE id = ?", [id], |row| {
                                row.get::<_, Vec<u8>>(0)
                            })
                            .map_err(DatabaseError)?;
                        yield Ok(data);
                    }
                    Err(err) => yield Err(err),
                }
            }
        })
        .instrument(trace_span!("DuckDB blob not_eq_data query"));
        helpers::all_not_eq(stream).await
    }
}
