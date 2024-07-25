mod kv_store {
    tonic::include_proto!("kvstore");
}

pub use crate::store::kv::kv_store::kv_server::{Kv as KeyValueRpc, KvServer as KeyValueServer};
pub use crate::store::kv::kv_store::{Key, KeyValue, Value};
use crate::{store, RpcResponse};
use std::collections::HashMap;
use std::path::PathBuf;
use tonic::{Request, Response, Status};

#[derive(Debug, PartialEq, Eq)]
pub struct KvStore {
    location: store::Location,
}

impl KvStore {
    #[inline]
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            location: store::Location::OnDisk { path: path.into() },
        }
    }

    #[inline]
    pub fn new_in_memory() -> Self {
        Self {
            location: store::Location::InMemory,
        }
    }

    fn with_db<T, E>(
        &self,
        mut f: impl FnMut(&mut rusqlite::Connection) -> Result<T, E>,
    ) -> Result<T, E> {
        store::DB.with_borrow_mut(|map| {
            let db = map
                .entry(self.location.clone())
                .or_insert_with(|| self.location.to_connection());

            db.execute(
                "CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )",
                [],
            );

            f(db)
        })
    }
}

#[tonic::async_trait]
impl KeyValueRpc for KvStore {
    async fn get(&self, request: Request<Key>) -> RpcResponse<Value> {
        self.with_db(|db| {
            let value = db.query_row(
                "SELECT value FROM kv_store WHERE key = ?1",
                [&request.get_ref().key],
                |row| row.get(0),
            );

            match value {
                Ok(value) => Ok(Response::new(Value { value })),
                // TODO Errors are possible even if the key is found. Handle them appropriately.
                Err(_) => Err(Status::new(tonic::Code::NotFound, "key not found")),
            }
        })
    }

    async fn set(&self, request: Request<KeyValue>) -> RpcResponse<Key> {
        let request = request.get_ref();
        self.with_db(|db| {
            let key = db.query_row(
                "REPLACE INTO kv_store (key, value) VALUES (?1, ?2) RETURNING key",
                [&request.key, &request.value],
                |row| row.get(0),
            );

            match key {
                Ok(key) => Ok(Response::new(Key { key })),
                Err(_) => Err(Status::new(
                    tonic::Code::Internal,
                    "failed to set key-value pair",
                )),
            }
        })
    }

    async fn delete(&self, request: Request<Key>) -> RpcResponse<Value> {
        self.with_db(|db| {
            let value = db.query_row(
                "DELETE FROM kv_store WHERE key = ?1 RETURNING value",
                [&request.get_ref().key],
                |row| row.get(0),
            );

            match value {
                Ok(value) => Ok(Response::new(Value { value })),
                // TODO Errors are possible even if the key is found. Handle them appropriately.
                Err(_) => Err(Status::new(tonic::Code::NotFound, "key not found")),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_get() -> Result<(), Box<dyn std::error::Error>> {
        let store = KvStore::new_in_memory();

        store
            .set(Request::new(KeyValue {
                key: "key".to_owned(),
                value: "value".to_owned(),
            }))
            .await?;

        let response = store
            .get(Request::new(Key {
                key: "key".to_owned(),
            }))
            .await?;
        assert_eq!(response.get_ref().value, "value");

        Ok(())
    }

    #[tokio::test]
    async fn test_set() -> Result<(), Box<dyn std::error::Error>> {
        let store = KvStore::new_in_memory();

        let response = store
            .set(Request::new(KeyValue {
                key: "key".to_owned(),
                value: "value".to_owned(),
            }))
            .await?;
        assert_eq!(
            response.get_ref(),
            &Key {
                key: "key".to_owned()
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<(), Box<dyn std::error::Error>> {
        let store = KvStore::new_in_memory();

        store
            .set(Request::new(KeyValue {
                key: "key".to_owned(),
                value: "value".to_owned(),
            }))
            .await?;

        let response = store
            .delete(Request::new(Key {
                key: "key".to_owned(),
            }))
            .await?;
        assert_eq!(response.get_ref().value, "value");

        let response = store
            .get(Request::new(Key {
                key: "key".to_owned(),
            }))
            .await;
        assert!(response.is_err());

        Ok(())
    }
}
