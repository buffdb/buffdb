mod kv_store {
    tonic::include_proto!("kvstore");
}

use crate::db_connection::{Database, DbConnectionInfo};
pub use crate::kv::kv_store::kv_server::{Kv as KeyValueRpc, KvServer as KeyValueServer};
pub use crate::kv::kv_store::{Bool, Key, KeyValue, Keys, Value};
use crate::{Location, RpcResponse};
use std::collections::HashSet;
use std::path::PathBuf;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct KvStore {
    db: Database,
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

    async fn eq(&self, request: Request<Keys>) -> RpcResponse<Bool> {
        let Keys { keys } = request.into_inner();
        let res = self
            .db
            .multi_get(keys)
            .into_iter()
            .collect::<Result<Vec<_>, _>>();

        let values = match res {
            Ok(values) => values,
            // TODO handle errors more gracefully
            Err(_) => {
                return Err(Status::new(
                    tonic::Code::Internal,
                    "failed to check if all keys exist",
                ))
            }
        };

        let all_eq = match values.first() {
            Some(first @ Some(_)) => values.iter().skip(1).all(|v| v == first),
            // If the first key does not exist, then all values do not exist and cannot be equal.
            Some(None) => false,
            // If there are no keys, then all values are by definition equal.
            None => true,
        };
        Ok(Response::new(Bool { value: all_eq }))
    }

    async fn not_eq(&self, request: Request<Keys>) -> RpcResponse<Bool> {
        let Keys { keys } = request.into_inner();
        let res = self
            .db
            .multi_get(keys)
            .into_iter()
            .collect::<Result<Vec<_>, _>>();

        let values = match res {
            Ok(values) => values,
            // TODO handle errors more gracefully
            Err(_) => {
                return Err(Status::new(
                    tonic::Code::Internal,
                    "failed to check if all keys exist",
                ))
            }
        };

        let mut unique_values = HashSet::new();
        for value in values {
            // Each key requested must exist.
            let Some(value) = value else {
                return Ok(Response::new(Bool { value: false }));
            };
            // `insert` returns false if the value already exists.
            if !unique_values.insert(value) {
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

        let response = KV_STORE
            .get(
                Key {
                    key: "key_get".to_owned(),
                }
                .into_request(),
            )
            .await?;
        assert_eq!(response.get_ref().value, "value_get");

        Ok(())
    }

    #[tokio::test]
    async fn test_set() -> Result<(), Box<dyn std::error::Error>> {
        let response = KV_STORE
            .set(
                KeyValue {
                    key: "key_set".to_owned(),
                    value: "value_set".to_owned(),
                }
                .into_request(),
            )
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
            .set(
                KeyValue {
                    key: "key_delete".to_owned(),
                    value: "value_delete".to_owned(),
                }
                .into_request(),
            )
            .await?;

        let response = KV_STORE
            .delete(
                Key {
                    key: "key_delete".to_owned(),
                }
                .into_request(),
            )
            .await?;
        assert_eq!(response.get_ref().key, "key_delete");

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

    #[tokio::test]
    async fn test_eq() -> Result<(), Box<dyn std::error::Error>> {
        for key in ["key_a_eq", "key_b_eq", "key_c_eq", "key_d_eq"] {
            KV_STORE
                .set(
                    KeyValue {
                        key: key.to_owned(),
                        value: "value_eq".to_owned(),
                    }
                    .into_request(),
                )
                .await?;
        }

        let all_eq = KV_STORE
            .eq(Keys {
                keys: vec![
                    "key_a_eq".to_owned(),
                    "key_b_eq".to_owned(),
                    "key_c_eq".to_owned(),
                    "key_d_eq".to_owned(),
                ],
            }
            .into_request())
            .await?
            .into_inner()
            .value;
        assert!(all_eq);

        KV_STORE
            .set(
                KeyValue {
                    key: "key_e_eq".to_owned(),
                    value: "value2_eq".to_owned(),
                }
                .into_request(),
            )
            .await?;

        let all_eq = KV_STORE
            .eq(Keys {
                keys: vec![
                    "key_a_eq".to_owned(),
                    "key_b_eq".to_owned(),
                    "key_c_eq".to_owned(),
                    "key_d_eq".to_owned(),
                    "key_e_eq".to_owned(),
                ],
            }
            .into_request())
            .await?
            .into_inner()
            .value;
        assert!(!all_eq);

        Ok(())
    }

    #[tokio::test]
    async fn test_not_eq() -> Result<(), Box<dyn std::error::Error>> {
        for (idx, key) in ["key_a_neq", "key_b_neq", "key_c_neq", "key_d_neq"]
            .into_iter()
            .enumerate()
        {
            KV_STORE
                .set(
                    KeyValue {
                        key: key.to_owned(),
                        value: format!("value{idx}_neq"),
                    }
                    .into_request(),
                )
                .await?;
        }

        let all_neq = KV_STORE
            .not_eq(
                Keys {
                    keys: vec![
                        "key_a_neq".to_owned(),
                        "key_b_neq".to_owned(),
                        "key_c_neq".to_owned(),
                        "key_d_neq".to_owned(),
                    ],
                }
                .into_request(),
            )
            .await?
            .into_inner()
            .value;
        assert!(all_neq);

        KV_STORE
            .set(
                KeyValue {
                    key: "key_e_neq".to_owned(),
                    value: "value2_neq".to_owned(),
                }
                .into_request(),
            )
            .await?;

        let all_neq = KV_STORE
            .not_eq(
                Keys {
                    keys: vec![
                        "key_a_neq".to_owned(),
                        "key_b_neq".to_owned(),
                        "key_c_neq".to_owned(),
                        "key_d_neq".to_owned(),
                        "key_e_neq".to_owned(),
                    ],
                }
                .into_request(),
            )
            .await?
            .into_inner()
            .value;
        assert!(!all_neq);

        Ok(())
    }
}
