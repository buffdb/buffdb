use crate::backend::{BlobBackend, DatabaseBackend, KvBackend, QueryBackend};
use crate::conv::sqlite_value_to_protobuf_any;
use crate::interop::into_tonic_status;
use crate::proto::{blob, kv, query};
use crate::{DynStream, Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::StreamExt as _;
use rusqlite::Connection;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use tonic::{async_trait, Response, Status};

#[derive(Debug)]
pub struct Sqlite {
    location: Location,
    initialized: AtomicBool,
}

impl DatabaseBackend for Sqlite {
    type Connection = Connection;
    type Error = rusqlite::Error;

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

#[async_trait]
impl QueryBackend for Sqlite {
    type QueryStream = DynStream<Result<query::QueryResult, Status>>;
    type ExecuteStream = DynStream<Result<query::RowsChanged, Status>>;

    async fn query(
        &self,
        request: StreamingRequest<query::RawQuery>,
    ) -> RpcResponse<Self::QueryStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(into_tonic_status)?;

        // Needed until rust-lang/rust#128095 is resolved. At that point, `stream!` in combination
        // with `drop(statement);` can be used.`
        let (tx, rx) = crossbeam::channel::bounded(64);

        while let Some(query::RawQuery { query }) = stream.message().await? {
            let mut statement = match db.prepare(&query) {
                Ok(statement) => statement,
                Err(err) => {
                    let _res = tx.send(Err(err));
                    break;
                }
            };
            match statement.query([]) {
                Ok(mut rows) => {
                    while let Ok(Some(row)) = rows.next() {
                        let column_count = row.as_ref().column_count();
                        let mut values = Vec::with_capacity(column_count);
                        for i in 0..column_count {
                            match row.get::<_, rusqlite::types::Value>(i) {
                                Ok(value) => values.push(sqlite_value_to_protobuf_any(value)?),
                                Err(err) => {
                                    let _res = tx.send(Err(err));
                                    break;
                                }
                            }
                        }
                        let _res = tx.send(Ok(query::QueryResult { fields: values }));
                    }
                }
                Err(err) => {
                    let _res = tx.send(Err(err));
                    break;
                }
            };
        }

        let stream = stream!({
            while let Ok(result) = rx.recv() {
                yield result.map_err(into_tonic_status);
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn execute(
        &self,
        request: StreamingRequest<query::RawQuery>,
    ) -> RpcResponse<Self::ExecuteStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(into_tonic_status)?;

        // Needed until rust-lang/rust#128095 is resolved. At that point, `stream!` in combination
        // with `drop(statement);` can be used.`
        let (tx, rx) = crossbeam::channel::bounded(64);

        while let Some(query::RawQuery { query }) = stream.message().await? {
            let mut statement = db.prepare(&query).map_err(into_tonic_status)?;
            let rows_changed = statement.execute([]).map_err(into_tonic_status)?;
            let _res = tx.send(Ok(query::RowsChanged {
                rows_changed: rows_changed
                    .try_into()
                    .expect("more than 10^19 rows altered"),
            }));
        }

        let stream = stream!({
            while let Ok(result) = rx.recv() {
                yield result;
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

#[async_trait]
impl KvBackend for Sqlite {
    type GetStream = DynStream<Result<kv::GetResponse, Status>>;
    type SetStream = DynStream<Result<kv::SetResponse, Status>>;
    type DeleteStream = DynStream<Result<kv::DeleteResponse, Status>>;

    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error> {
        let _res = connection.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)",
            [],
        )?;
        self.initialized.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn connect_kv(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        if !self.initialized.load(Ordering::Relaxed) {
            KvBackend::initialize(self, &conn)?;
        }
        Ok(conn)
    }

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
        });
        Ok(Response::new(Box::pin(stream)))
    }

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
        });
        Ok(Response::new(Box::pin(stream)))
    }

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
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn eq(&self, request: StreamingRequest<kv::EqRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let mut stream = Box::pin(stream!({
            while let Some(kv::EqRequest { key }) = stream.message().await? {
                let value = db
                    .query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                        row.get(0)
                    })
                    .map_err(into_tonic_status)?;
                yield Ok(
                    String::from_utf8(value).expect("protobuf requires strings be valid UTF-8")
                );
            }
        }));

        let value = match stream.next().await {
            Some(Ok(value)) => value,
            Some(Err(err)) => return Err(err),
            // If there are no keys, then all values are by definition equal.
            None => return Ok(Response::new(true)),
        };
        // Hash the values to avoid storing it fully in memory.
        let first_hash = Sha256::digest(value);

        while let Some(value) = stream.next().await {
            let value = value?;

            if first_hash != Sha256::digest(value) {
                return Ok(Response::new(false));
            }
        }

        Ok(Response::new(true))
    }

    async fn not_eq(&self, request: StreamingRequest<kv::NotEqRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_kv().map_err(into_tonic_status)?;
        let mut stream = Box::pin(stream!({
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
        }));

        let mut unique_values = BTreeSet::new();

        while let Some(value) = stream.next().await {
            // `insert` returns false if the value already exists.
            // Hash the values to avoid storing it fully in memory.
            if !unique_values.insert(Sha256::digest(value?)) {
                return Ok(Response::new(false));
            }
        }

        Ok(Response::new(true))
    }
}

#[async_trait]
impl BlobBackend for Sqlite {
    type GetStream = DynStream<Result<blob::GetResponse, Status>>;
    type StoreStream = DynStream<Result<blob::StoreResponse, Status>>;
    type UpdateStream = DynStream<Result<blob::UpdateResponse, Status>>;
    type DeleteStream = DynStream<Result<blob::DeleteResponse, Status>>;

    fn initialize(&self, connection: &Self::Connection) -> Result<(), Self::Error> {
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS blob(
                data BLOB,
                metadata TEXT
            );",
        )?;
        self.initialized.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn connect_blob(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connect()?;
        if !self.initialized.load(Ordering::Relaxed) {
            BlobBackend::initialize(self, &conn)?;
        }
        Ok(conn)
    }

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
                        "SELECT data, metadata FROM blob WHERE rowid = ?",
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
        });
        Ok(Response::new(Box::pin(stream)))
    }

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
                        "INSERT INTO blob(data, metadata) VALUES(?, ?) RETURNING rowid",
                        (bytes, metadata),
                        |row| row.get(0),
                    )
                    .map_err(into_tonic_status)?;
                yield Ok(blob::StoreResponse { id });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

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
                            "UPDATE blob SET data = ?, metadata = ? WHERE rowid = ?",
                            (bytes, metadata, id),
                        )
                        .map_err(into_tonic_status)?;
                    }
                    (None, true) => {
                        db.execute(
                            "UPDATE blob SET metadata = ? WHERE rowid = ?",
                            (metadata, id),
                        )
                        .map_err(into_tonic_status)?;
                    }
                    (Some(bytes), false) => {
                        db.execute("UPDATE blob SET data = ? WHERE rowid = ?", (bytes, id))
                            .map_err(into_tonic_status)?;
                    }
                }
                yield Ok(blob::UpdateResponse { id });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn delete(
        &self,
        request: StreamingRequest<blob::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;
        let stream = stream!({
            while let Some(blob::DeleteRequest { id }) = stream.message().await? {
                db.execute("DELETE FROM blob WHERE rowid = ?", [id])
                    .map_err(into_tonic_status)?;
                yield Ok(blob::DeleteResponse { id });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn eq_data(&self, request: StreamingRequest<blob::EqDataRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let mut stream = Box::pin(stream!({
            while let Some(blob::EqDataRequest { id }) = stream.message().await? {
                let data = db
                    .query_row("SELECT data FROM blob WHERE rowid = ?", [id], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .map_err(into_tonic_status)?;

                yield Ok(data);
            }
        }));

        let value = match stream.next().await {
            Some(Ok(bytes)) => bytes,
            Some(Err(err)) => return Err(err),
            // If there are no keys, then all values are by definition equal.
            None => return Ok(Response::new(true)),
        };
        // Hash the values to avoid storing it fully in memory.
        let first_hash = Sha256::digest(&value);

        while let Some(value) = stream.next().await {
            if first_hash != Sha256::digest(&value?) {
                return Ok(Response::new(false));
            }
        }

        Ok(Response::new(true))
    }

    async fn not_eq_data(
        &self,
        request: StreamingRequest<blob::NotEqDataRequest>,
    ) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect_blob().map_err(into_tonic_status)?;

        let mut stream = Box::pin(stream!({
            while let Some(blob::NotEqDataRequest { id }) = stream.message().await? {
                let data = db
                    .query_row("SELECT data FROM blob WHERE rowid = ?", [id], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .map_err(into_tonic_status)?;

                yield Ok::<_, Status>(data);
            }
        }));

        let mut unique_values = BTreeSet::new();

        while let Some(value) = stream.next().await {
            // `insert` returns false if the value already exists.
            // Hash the values to avoid storing it fully in memory.
            if !unique_values.insert(Sha256::digest(&value?)) {
                return Ok(Response::new(false));
            }
        }

        Ok(Response::new(true))
    }
}
