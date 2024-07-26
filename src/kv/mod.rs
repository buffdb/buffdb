mod kv_store {
    tonic::include_proto!("kvstore");
}

pub use crate::kv::kv_store::kv_server::{Kv as KeyValueRpc, KvServer as KeyValueServer};
pub use crate::kv::kv_store::{Key, KeyValue, Value};
use crate::location::{Connect, Database};
use crate::{Location, RpcResponse};
use std::path::PathBuf;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct KvStore {
    db: Database,
}

impl Connect for KvStore {
    fn adjust_opts(opts: &mut rocksdb::Options) {
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    }
}

impl KvStore {
    /// To store data in memory, use a path pointing to a tmpfs or ramfs.
    #[inline]
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            db: Location::OnDisk { path: path.into() }.to_connection::<Self>(),
        }
    }
}

#[tonic::async_trait]
impl KeyValueRpc for KvStore {
    async fn get(&self, request: Request<Key>) -> RpcResponse<Value> {
        let Key { key } = request.into_inner();
        let value = self.db.get(key);
        match value {
            // https://protobuf.dev/programming-guides/proto3/#scalar
            Ok(Some(value)) => Ok(Response::new(Value {
                value: String::from_utf8(value).expect("protobuf requires strings be valid UTF-8"),
            })),
            Ok(None) => Err(Status::new(tonic::Code::NotFound, "key not found")),
            // TODO handle errors more gracefully
            Err(_) => Err(Status::new(
                tonic::Code::Internal,
                "failed to get key-value pair",
            )),
        }
    }

    async fn set(&self, request: Request<KeyValue>) -> RpcResponse<Key> {
        let KeyValue { key, value } = request.into_inner();
        let res = self.db.put(&key, value.as_bytes());
        match res {
            Ok(_) => Ok(Response::new(Key { key })),
            // TODO handle errors more gracefully
            Err(_) => Err(Status::new(
                tonic::Code::Internal,
                "failed to set key-value pair",
            )),
        }
    }

    async fn delete(&self, request: Request<Key>) -> RpcResponse<Key> {
        let Key { key } = request.into_inner();
        let res = self.db.delete(&key);
        match res {
            Ok(()) => Ok(Response::new(Key { key })),
            // TODO handle errors more gracefully
            Err(_) => Err(Status::new(
                tonic::Code::Internal,
                "failed to delete key-value pair",
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::LazyLock;

    static KV_STORE: LazyLock<KvStore> = LazyLock::new(|| KvStore::new("test_kv_store"));

    #[tokio::test]
    async fn test_get() -> Result<(), Box<dyn std::error::Error>> {
        KV_STORE
            .set(Request::new(KeyValue {
                key: "key_get".to_owned(),
                value: "value_get".to_owned(),
            }))
            .await?;

        let response = KV_STORE
            .get(Request::new(Key {
                key: "key_get".to_owned(),
            }))
            .await?;
        assert_eq!(response.get_ref().value, "value_get");

        Ok(())
    }

    #[tokio::test]
    async fn test_set() -> Result<(), Box<dyn std::error::Error>> {
        let response = KV_STORE
            .set(Request::new(KeyValue {
                key: "key_set".to_owned(),
                value: "value_set".to_owned(),
            }))
            .await?;
        assert_eq!(
            response.get_ref(),
            &Key {
                key: "key_set".to_owned()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<(), Box<dyn std::error::Error>> {
        KV_STORE
            .set(Request::new(KeyValue {
                key: "key_delete".to_owned(),
                value: "value_delete".to_owned(),
            }))
            .await?;

        let response = KV_STORE
            .delete(Request::new(Key {
                key: "key_delete".to_owned(),
            }))
            .await?;
        assert_eq!(response.get_ref().key, "key_delete");

        let response = KV_STORE
            .get(Request::new(Key {
                key: "key_delete".to_owned(),
            }))
            .await;
        assert!(response.is_err());

        Ok(())
    }
}
