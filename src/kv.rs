use crate::db_connection::{Database, DbConnectionInfo};
use crate::interop::duckdb_err_to_tonic_status;
use crate::schema::common::Bool;
pub use crate::schema::kv::kv_client::KvClient;
pub use crate::schema::kv::kv_server::{Kv as KvRpc, KvServer};
pub use crate::schema::kv::{Key, KeyValue, Value};
use crate::{Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::{Stream, StreamExt};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::pin::Pin;
use tonic::{Response, Status};

#[derive(Debug)]
pub struct KvStore {
    location: Location,
}

impl DbConnectionInfo for KvStore {
    fn initialize(db: &Database) -> duckdb::Result<()> {
        db.execute(
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
    #[inline]
    pub fn at_location(location: Location) -> Self {
        Self { location }
    }

    /// Create a new key-value at the given path.
    #[inline]
    pub fn at_path<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            location: Location::OnDisk { path: path.into() },
        }
    }

    #[inline]
    pub fn in_memory() -> Self {
        Self {
            location: Location::InMemory,
        }
    }
}

#[tonic::async_trait]
impl KvRpc for KvStore {
    type GetStream = Pin<Box<dyn Stream<Item = Result<Value, Status>> + Send + 'static>>;
    type SetStream = Pin<Box<dyn Stream<Item = Result<Key, Status>> + Send + 'static>>;
    type DeleteStream = Pin<Box<dyn Stream<Item = Result<Key, Status>> + Send + 'static>>;

    async fn get(&self, request: StreamingRequest<Key>) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;
        let stream = stream! {
            while let Some(Key { key }) = stream.message().await? {
                let value = db.query_row("SELECT value FROM kv WHERE key = ?", [&key], |row| {
                    row.get(0)
                }).map_err(duckdb_err_to_tonic_status)?;
                yield Ok(Value {
                    value: String::from_utf8(value).expect("protobuf requires strings be valid UTF-8"),
                });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::GetStream))
    }

    async fn set(&self, request: StreamingRequest<KeyValue>) -> RpcResponse<Self::SetStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;
        let stream = stream! {
            while let Some(KeyValue { key, value }) = stream.message().await? {
                db.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", [&key, &value])
                    .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(Key { key });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::SetStream))
    }

    async fn delete(&self, request: StreamingRequest<Key>) -> RpcResponse<Self::DeleteStream> {
        let mut stream = request.into_inner();
        let db = self.connect().map_err(duckdb_err_to_tonic_status)?;
        let stream = stream! {
            while let Some(Key { key }) = stream.message().await? {
                db.execute("DELETE FROM kv WHERE key = ?", [&key])
                    .map_err(duckdb_err_to_tonic_status)?;
                yield Ok(Key { key });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::DeleteStream))
    }

    async fn eq(&self, request: StreamingRequest<Key>) -> RpcResponse<Bool> {
        let mut values = self.get(request).await?.into_inner();

        let value = match values.next().await {
            Some(Ok(Value { value })) => value,
            Some(Err(err)) => return Err(err),
            // If there are no keys, then all values are by definition equal.
            None => return Ok(Response::new(Bool { value: true })),
        };
        // Hash the values to avoid storing it fully in memory.
        let first_hash = Sha256::digest(value);

        while let Some(value) = values.next().await {
            let Value { value } = value?;

            if first_hash != Sha256::digest(value) {
                return Ok(Response::new(Bool { value: false }));
            }
        }

        Ok(Response::new(Bool { value: true }))
    }

    async fn not_eq(&self, request: StreamingRequest<Key>) -> RpcResponse<Bool> {
        let mut unique_values = BTreeSet::new();

        let mut values = self.get(request).await?.into_inner();
        while let Some(value) = values.next().await {
            let Value { value } = value?;

            // `insert` returns false if the value already exists.
            // Hash the values to avoid storing it fully in memory.
            if !unique_values.insert(Sha256::digest(value)) {
                return Ok(Response::new(Bool { value: false }));
            }
        }

        Ok(Response::new(Bool { value: true }))
    }
}
