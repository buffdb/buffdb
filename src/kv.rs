use crate::db_connection::{Database, DbConnectionInfo};
use crate::interop::rocksdb_err_to_tonic_status;
use crate::schema::common::Bool;
pub use crate::schema::kv::kv_client::KvClient;
pub use crate::schema::kv::kv_server::{Kv as KvRpc, KvServer};
pub use crate::schema::kv::{Key, KeyValue, Keys, Value, Values};
use crate::{Location, RpcResponse, StreamingRequest};
use async_stream::stream;
use futures::Stream;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{IntoRequest, Request, Response, Status};

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
    type GetManyStream = Pin<Box<dyn Stream<Item = Result<Values, Status>> + Send + 'static>>;

    async fn get(&self, request: Request<Key>) -> RpcResponse<Value> {
        let Key { key } = request.into_inner();
        let value = self.db.get(key);
        match value.map_err(rocksdb_err_to_tonic_status)? {
            // https://protobuf.dev/programming-guides/proto3/#scalar
            Some(value) => Ok(Response::new(Value {
                value: String::from_utf8(value).expect("protobuf requires strings be valid UTF-8"),
            })),
            None => Err(Status::new(tonic::Code::NotFound, "key not found")),
        }
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

    async fn set(&self, request: Request<KeyValue>) -> RpcResponse<Key> {
        let KeyValue { key, value } = request.into_inner();
        self.db
            .put(&key, value.as_bytes())
            .map_err(rocksdb_err_to_tonic_status)?;
        Ok(Response::new(Key { key }))
    }

    async fn delete(&self, request: Request<Key>) -> RpcResponse<Key> {
        let Key { key } = request.into_inner();
        self.db.delete(&key).map_err(rocksdb_err_to_tonic_status)?;
        Ok(Response::new(Key { key }))
    }

    async fn eq(&self, request: StreamingRequest<Key>) -> RpcResponse<Bool> {
        let mut stream = request.into_inner();

        let Some(key) = stream.message().await? else {
            // If there are no keys, then all values are by definition equal.
            return Ok(Response::new(Bool { value: true }));
        };
        let Value { value } = self.get(key.into_request()).await?.into_inner();
        // Hash the values to avoid storing it fully in memory.
        let first_hash = Sha256::digest(value);

        while let Some(key) = stream.message().await? {
            let Value { value } = self.get(key.into_request()).await?.into_inner();

            if first_hash != Sha256::digest(value) {
                return Ok(Response::new(Bool { value: false }));
            }
        }

        Ok(Response::new(Bool { value: true }))
    }

    async fn not_eq(&self, request: StreamingRequest<Key>) -> RpcResponse<Bool> {
        let mut stream = request.into_inner();

        let mut unique_values = BTreeSet::new();
        while let Some(key) = stream.message().await? {
            let Value { value } = self.get(key.into_request()).await?.into_inner();

            // `insert` returns false if the value already exists.
            // Hash the values to avoid storing it fully in memory.
            if !unique_values.insert(Sha256::digest(value)) {
                return Ok(Response::new(Bool { value: false }));
            }
        }

        Ok(Response::new(Bool { value: true }))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::LazyLock;
    use tonic::IntoRequest;

    static KV_STORE: LazyLock<KvStore> = LazyLock::new(|| KvStore::new("test_kv_store"));

    #[tokio::test]
    async fn test_get() -> Result<(), Box<dyn std::error::Error>> {
        KV_STORE
            .set(
                KeyValue {
                    key: "key_get".to_owned(),
                    value: "value_get".to_owned(),
                }
                .into_request(),
            )
            .await?;

        let Value { value } = KV_STORE
            .get(
                Key {
                    key: "key_get".to_owned(),
                }
                .into_request(),
            )
            .await?
            .into_inner();
        assert_eq!(value, "value_get");

        Ok(())
    }

    #[tokio::test]
    async fn test_set() -> Result<(), Box<dyn std::error::Error>> {
        let Key { key } = KV_STORE
            .set(
                KeyValue {
                    key: "key_set".to_owned(),
                    value: "value_set".to_owned(),
                }
                .into_request(),
            )
            .await?
            .into_inner();
        assert_eq!(key, "key_set");

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<(), Box<dyn std::error::Error>> {
        KV_STORE
            .set(
                KeyValue {
                    key: "key_delete".to_owned(),
                    value: "value_delete".to_owned(),
                }
                .into_request(),
            )
            .await?;

        let Key { key } = KV_STORE
            .delete(
                Key {
                    key: "key_delete".to_owned(),
                }
                .into_request(),
            )
            .await?
            .into_inner();
        assert_eq!(key, "key_delete");

        let response = KV_STORE
            .get(
                Key {
                    key: "key_delete".to_owned(),
                }
                .into_request(),
            )
            .await;
        assert!(response.is_err());

        Ok(())
    }
}
