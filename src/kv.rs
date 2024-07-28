use crate::db_connection::{Database, DbConnectionInfo};
use crate::interop::rocksdb_err_to_tonic_status;
use crate::schema::common::Bool;
pub use crate::schema::kv::kv_server::{Kv as KvRpc, KvServer};
pub use crate::schema::kv::{Key, KeyValue, Keys, Value, Values};
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
            db: Self::connection(Location::OnDisk { path: path.into() }),
        }
    }
}

#[tonic::async_trait]
impl KvRpc for KvStore {
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

    async fn get_many(&self, request: Request<Keys>) -> RpcResponse<Values> {
        let Keys { keys } = request.into_inner();
        let res = self
            .db
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
                    return Err(Status::new(
                        tonic::Code::NotFound,
                        format!("key {} not found", keys[idx]),
                    ))
                }
            }
        }
        Ok(Response::new(Values { values }))
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

    async fn eq(&self, request: Request<Keys>) -> RpcResponse<Bool> {
        let Values { values } = self.get_many(request).await?.into_inner();
        let all_eq = match values.first() {
            Some(first) => values.iter().skip(1).all(|v| v == first),
            // If there are no keys, then all values are by definition equal.
            None => true,
        };
        Ok(Response::new(Bool { value: all_eq }))
    }

    async fn not_eq(&self, request: Request<Keys>) -> RpcResponse<Bool> {
        let Values { values } = self.get_many(request).await?.into_inner();

        let mut unique_values = HashSet::new();
        for value in values {
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
    async fn get_many() -> Result<(), Box<dyn std::error::Error>> {
        KV_STORE
            .set(
                KeyValue {
                    key: "key_a_get_many".to_owned(),
                    value: "value_a_get_many".to_owned(),
                }
                .into_request(),
            )
            .await?;
        KV_STORE
            .set(
                KeyValue {
                    key: "key_b_get_many".to_owned(),
                    value: "value_b_get_many".to_owned(),
                }
                .into_request(),
            )
            .await?;

        let Values { values } = KV_STORE
            .get_many(
                Keys {
                    keys: vec!["key_a_get_many".to_owned(), "key_b_get_many".to_owned()],
                }
                .into_request(),
            )
            .await?
            .into_inner();
        assert_eq!(values, vec!["value_a_get_many", "value_b_get_many"]);

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

    #[tokio::test]
    async fn test_eq_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let res = KV_STORE
            .eq(Keys {
                keys: vec!["this-key-should-not-exist".to_owned()],
            }
            .into_request())
            .await;
        assert!(res.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_not_eq_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let res = KV_STORE
            .not_eq(
                Keys {
                    keys: vec!["this-key-should-not-exist".to_owned()],
                }
                .into_request(),
            )
            .await;
        assert!(res.is_err());
        Ok(())
    }
}
