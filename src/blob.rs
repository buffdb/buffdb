//! A store for binary large objects (BLOBs) with an optional metadata field.

use crate::conv::duckdb_value_to_protobuf_any;
use crate::db_connection::{Database, DbConnectionInfo};
use crate::duckdb_helper::{params2, params3};
use crate::interop::duckdb_err_to_tonic_status;
use crate::proto::blob::{
    DeleteRequest, DeleteResponse, EqDataRequest, GetRequest, GetResponse, NotEqDataRequest,
    StoreRequest, StoreResponse, UpdateRequest, UpdateResponse,
};
use crate::proto::query::{QueryResult, RawQuery, RowsChanged};
use crate::service::blob::BlobRpc;
use crate::{DynStream, Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::StreamExt;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use tonic::{Response, Status};

/// A store for binary large objects (BLOBs) with an optional metadata field.
///
/// Metadata, if stored, is a string that can be used to store any additional information about the
/// BLOB, such as a description or a name. Neither the BLOB or the metadata are required to be
/// unique.
#[must_use]
#[derive(Debug)]
pub struct BlobStore {
    location: Location,
    initialized: AtomicBool,
}

impl DbConnectionInfo for BlobStore {
    fn initialize(&self, db: &Database) -> duckdb::Result<()> {
        db.execute_batch(
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

    fn was_initialized(&self) -> bool {
        self.initialized.load(Ordering::Relaxed)
    }

    fn location(&self) -> &Location {
        &self.location
    }
}

impl BlobStore {
    /// Create a new key-value store at the given location. If not pre-existing, the store will not
    /// be initialized until the first connection is made.
    #[inline]
    pub const fn at_location(location: Location) -> Self {
        Self {
            location,
            initialized: AtomicBool::new(false),
        }
    }

    /// Create a new key-value at the given path on disk. If not pre-existing, the store will not be
    /// initialized until the first connection is made.
    #[inline]
    pub fn at_path<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            location: Location::OnDisk { path: path.into() },
            initialized: AtomicBool::new(false),
        }
    }

    /// Create a new in-memory key-value store. This is useful for short-lived data.
    ///
    /// Note that all in-memory connections share the same stream, so any asynchronous calls have a
    /// nondeterministic order. This is not a problem for on-disk connections.
    #[inline]
    pub const fn in_memory() -> Self {
        Self {
            location: Location::InMemory,
            initialized: AtomicBool::new(false),
        }
    }
}

#[tonic::async_trait]
impl BlobRpc for BlobStore {
    type QueryStream = DynStream<Result<QueryResult, Status>>;
    type ExecuteStream = DynStream<Result<RowsChanged, Status>>;
    type GetStream = DynStream<Result<GetResponse, Status>>;
    type StoreStream = DynStream<Result<StoreResponse, Status>>;
    type UpdateStream = DynStream<Result<UpdateResponse, Status>>;
    type DeleteStream = DynStream<Result<DeleteResponse, Status>>;

    async fn query(&self, request: StreamingRequest<RawQuery>) -> RpcResponse<Self::QueryStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;

        // Needed until rust-lang/rust#128095 is resolved. At that point, `stream!` in combination
        // with `drop(statement);` can be used.`
        let (tx, rx) = crossbeam::channel::bounded(64);

        while let Some(RawQuery { query }) = stream.message().await? {
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
                            match row.get::<_, duckdb::types::Value>(i) {
                                Ok(value) => values.push(duckdb_value_to_protobuf_any(value)?),
                                Err(err) => {
                                    let _res = tx.send(Err(err));
                                    break;
                                }
                            }
                        }
                        let _res = tx.send(Ok(QueryResult { fields: values }));
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
                yield result.map_err(duckdb_err_to_tonic_status);
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn execute(
        &self,
        request: StreamingRequest<RawQuery>,
    ) -> RpcResponse<Self::ExecuteStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;

        // Needed until rust-lang/rust#128095 is resolved. At that point, `stream!` in combination
        // with `drop(statement);` can be used.`
        let (tx, rx) = crossbeam::channel::bounded(64);

        while let Some(RawQuery { query }) = stream.message().await? {
            let mut statement = db.prepare(&query).map_err(duckdb_err_to_tonic_status)?;
            let rows_changed = statement.execute([]).map_err(duckdb_err_to_tonic_status)?;
            let _res = tx.send(Ok(RowsChanged {
                rows_changed: rows_changed
                    .try_into()
                    .expect("more than 10^19 rows altered"),
            }));
        }

        let stream = stream!({
            while let Ok(result) = rx.recv() {
                yield result.map_err(duckdb_err_to_tonic_status);
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get(&self, request: StreamingRequest<GetRequest>) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;

        let stream = stream!({
            while let Some(GetRequest { id }) = stream.message().await? {
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
                    .map_err(duckdb_err_to_tonic_status)?;

                yield Ok(GetResponse {
                    bytes: data,
                    metadata,
                });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn store(
        &self,
        request: StreamingRequest<StoreRequest>,
    ) -> RpcResponse<Self::StoreStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;

        let stream = stream!({
            while let Some(StoreRequest { bytes, metadata }) = stream.message().await? {
                let id = db
                    .query_row(
                        "INSERT INTO blob(data, metadata) VALUES(?, ?) RETURNING id",
                        params2(bytes, metadata),
                        |row| row.get(0),
                    )
                    .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(StoreResponse { id });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn update(
        &self,
        request: StreamingRequest<UpdateRequest>,
    ) -> RpcResponse<Self::UpdateStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;

        let stream = stream!({
            while let Some(UpdateRequest {
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
                        .map_err(duckdb_err_to_tonic_status)?;
                    }
                    (None, true) => {
                        db.execute(
                            "UPDATE blob SET metadata = ? WHERE id = ?",
                            params2(metadata, id),
                        )
                        .map_err(duckdb_err_to_tonic_status)?;
                    }
                    (Some(bytes), false) => {
                        db.execute("UPDATE blob SET data = ? WHERE id = ?", params2(bytes, id))
                            .map_err(duckdb_err_to_tonic_status)?;
                    }
                }
                yield Ok(UpdateResponse { id });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn delete(
        &self,
        request: StreamingRequest<DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;
        let stream = stream!({
            while let Some(DeleteRequest { id }) = stream.message().await? {
                db.execute("DELETE FROM blob WHERE id = ?", [id])
                    .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(DeleteResponse { id });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn eq_data(&self, request: StreamingRequest<EqDataRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;

        let mut stream = Box::pin(stream!({
            while let Some(EqDataRequest { id }) = stream.message().await? {
                let data = db
                    .query_row("SELECT data FROM blob WHERE id = ?", [id], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .map_err(duckdb_err_to_tonic_status)?;

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

    async fn not_eq_data(&self, request: StreamingRequest<NotEqDataRequest>) -> RpcResponse<bool> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;

        let mut stream = Box::pin(stream!({
            while let Some(NotEqDataRequest { id }) = stream.message().await? {
                let data = db
                    .query_row("SELECT data FROM blob WHERE id = ?", [id], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .map_err(duckdb_err_to_tonic_status)?;

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
