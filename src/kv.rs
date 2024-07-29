use crate::db_connection::{Database, DbConnectionInfo};
use crate::interop::rocksdb_err_to_tonic_status;
use crate::schema::common::Bool;
pub use crate::schema::kv::kv_client::KvClient;
pub use crate::schema::kv::kv_server::{Kv as KvRpc, KvServer};
pub use crate::schema::kv::{Key, KeyValue, Keys, Value, Values};
use crate::{Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::{Stream, StreamExt};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Response, Status};

#[derive(Debug)]
pub struct KvStore {
    db: Arc<Database>,
}

impl DbConnectionInfo for KvStore {
    fn adjust_opts(opts: &mut rocksdb::Options) {
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    }
}

impl KvStore {
    /// Create a new key-value at the given path.
    ///
    /// To store data in memory, use a path pointing to a tmpfs or ramfs.
    #[inline]
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            db: Arc::new(Self::connection(Location::OnDisk { path: path.into() })),
        }
    }
}

#[tonic::async_trait]
impl KvRpc for KvStore {
    type GetStream = Pin<Box<dyn Stream<Item = Result<Value, Status>> + Send + 'static>>;
    type GetManyStream = Pin<Box<dyn Stream<Item = Result<Values, Status>> + Send + 'static>>;
    type SetStream = Pin<Box<dyn Stream<Item = Result<Key, Status>> + Send + 'static>>;
    type DeleteStream = Pin<Box<dyn Stream<Item = Result<Key, Status>> + Send + 'static>>;

    async fn get(&self, request: StreamingRequest<Key>) -> RpcResponse<Self::GetStream> {
        let mut stream = request.into_inner();
        let db = self.db.clone();
        let stream = stream! {
            while let Some(Key { key }) = stream.message().await? {
                let value = db.get(&key).map_err(rocksdb_err_to_tonic_status)?;
                match value {
                    Some(value) => {
                        yield Ok(Value {
                            value: String::from_utf8(value).expect("protobuf requires strings be valid UTF-8"),
                        });
                    }
                    None => {
                        return Err(Status::not_found(format!("key {key} not found")))?;
                    }
                }
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::GetStream))
    }

    async fn get_many(&self, request: StreamingRequest<Keys>) -> RpcResponse<Self::GetManyStream> {
        let mut stream = request.into_inner();
        let db = self.db.clone();
        let stream = stream! {
            while let Some(Keys { keys }) = stream.message().await? {
                let res = db
                    .multi_get(&keys)
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>();
                let raw_values = res.map_err(rocksdb_err_to_tonic_status)?;

                let mut values = Vec::new();
                for (idx, value) in raw_values.into_iter().enumerate() {
                    match value {
                        Some(value) => values.push(
                            String::from_utf8(value).expect("protobuf requires strings be valid UTF-8"),
                        ),
                        None => {
                            Err(Status::not_found(format!("key {} not found", keys[idx])))?;
                        }
                    }
                }
                yield Ok(Values { values: values.clone() });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::GetManyStream))
    }

    async fn set(&self, request: StreamingRequest<KeyValue>) -> RpcResponse<Self::SetStream> {
        let mut stream = request.into_inner();
        let db = self.db.clone();
        let stream = stream! {
            while let Some(KeyValue { key, value }) = stream.message().await? {
                db.put(&key, value.as_bytes())
                    .map_err(rocksdb_err_to_tonic_status)?;
                yield Ok(Key { key });
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::SetStream))
    }

    async fn delete(&self, request: StreamingRequest<Key>) -> RpcResponse<Self::DeleteStream> {
        let mut stream = request.into_inner();
        let db = self.db.clone();
        let stream = stream! {
            while let Some(Key { key }) = stream.message().await? {
                db.delete(&key).map_err(rocksdb_err_to_tonic_status)?;
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
