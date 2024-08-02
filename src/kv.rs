//! A key-value store.

use crate::conv::duckdb_value_to_protobuf_any;
use crate::db_connection::{Database, DbConnectionInfo};
use crate::interop::duckdb_err_to_tonic_status;
use crate::proto::kv::{Key, KeyValue, Value};
use crate::proto::query::{QueryResult, RawQuery, RowsChanged};
use crate::service::kv::KvRpc;
use crate::{DynStream, Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::StreamExt;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use tonic::{Response, Status};

/// A key-value store.
///
/// This is a key-value store where both the key and value are strings. There are no restrictions on
/// the length or contents of either the key or value beyond restrictions implemented by the
/// protobuf server.
#[must_use]
#[derive(Debug)]
pub struct KvStore {
    location: Location,
}

impl DbConnectionInfo for KvStore {
    fn initialize(db: &Database) -> duckdb::Result<()> {
        let _rows_changed = db.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)",
            [],
        )?;
        Ok(())
    }

    fn location(&self) -> &Location {
        &self.location
    }
}

impl KvStore {
    /// Create a new key-value store at the given location. If not pre-existing, the store will not
    /// be initialized until the first connection is made.
    #[inline]
    pub const fn at_location(location: Location) -> Self {
        Self { location }
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
        }
    }
}

#[tonic::async_trait]
impl KvRpc for KvStore {
    type QueryStream = DynStream<Result<QueryResult, Status>>;
    type ExecuteStream = DynStream<Result<RowsChanged, Status>>;
    type GetStream = DynStream<Result<Value, Status>>;
    type SetStream = DynStream<Result<Key, Status>>;
    type DeleteStream = DynStream<Result<Key, Status>>;

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

    async fn get(&self, request: StreamingRequest<Key>) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;
        let stream = stream!({
            while let Some(Key { key }) = stream.message().await? {
                let value = db
                    .query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                        row.get(0)
                    })
                    .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(Value {
                    value: String::from_utf8(value)
                        .expect("protobuf requires strings be valid UTF-8"),
                });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn set(&self, request: StreamingRequest<KeyValue>) -> RpcResponse<Self::SetStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;
        let stream = stream!({
            while let Some(KeyValue { key, value }) = stream.message().await? {
                db.execute(
                    "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
                    [&key, &value],
                )
                .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(Key { key });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn delete(&self, request: StreamingRequest<Key>) -> RpcResponse<Self::DeleteStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;
        let stream = stream!({
            while let Some(Key { key }) = stream.message().await? {
                db.execute("DELETE FROM kv WHERE key = ?", [&key])
                    .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(Key { key });
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn eq(&self, request: StreamingRequest<Key>) -> RpcResponse<bool> {
        let mut values = self.get(request).await?.into_inner();

        let value = match values.next().await {
            Some(Ok(Value { value })) => value,
            Some(Err(err)) => return Err(err),
            // If there are no keys, then all values are by definition equal.
            None => return Ok(Response::new(true)),
        };
        // Hash the values to avoid storing it fully in memory.
        let first_hash = Sha256::digest(value);

        while let Some(value) = values.next().await {
            let Value { value } = value?;

            if first_hash != Sha256::digest(value) {
                return Ok(Response::new(false));
            }
        }

        Ok(Response::new(true))
    }

    async fn not_eq(&self, request: StreamingRequest<Key>) -> RpcResponse<bool> {
        let mut unique_values = BTreeSet::new();

        let mut values = self.get(request).await?.into_inner();
        while let Some(value) = values.next().await {
            let Value { value } = value?;

            // `insert` returns false if the value already exists.
            // Hash the values to avoid storing it fully in memory.
            if !unique_values.insert(Sha256::digest(value)) {
                return Ok(Response::new(false));
            }
        }

        Ok(Response::new(true))
    }
}
