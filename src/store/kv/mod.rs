mod kv_store {
    tonic::include_proto!("kvstore");
}

use crate::store;
pub use crate::store::kv::kv_store::key_value_server::KeyValue;
pub use crate::store::kv::kv_store::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};
use crate::RpcResponse;
use std::collections::HashMap;
use std::path::PathBuf;
use tonic::{Request, Response, Status};

#[derive(Debug, PartialEq, Eq)]
pub struct KvStore {
    location: store::Location,
}

impl KvStore {
    #[inline]
    pub fn new(path: PathBuf) -> Self {
        Self {
            location: store::Location::OnDisk { path },
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
impl KeyValue for KvStore {
    async fn get(&self, request: Request<GetRequest>) -> RpcResponse<GetResponse> {
        self.with_db(|db| {
            let value = db.query_row(
                "SELECT value FROM kv_store WHERE key = ?1",
                [&request.get_ref().key],
                |row| row.get(0),
            );

            match value {
                Ok(value) => Ok(Response::new(GetResponse { value })),
                // TODO Errors are possible even if the key is found. Handle them appropriately.
                Err(_) => Err(Status::new(tonic::Code::NotFound, "key not found")),
            }
        })
    }

    async fn set(&self, request: Request<SetRequest>) -> RpcResponse<SetResponse> {
        let request = request.get_ref();
        self.with_db(|db| {
            let value = db.execute(
                "REPLACE INTO kv_store (key, value) VALUES (?1, ?2)",
                [&request.key, &request.value],
            );

            match value {
                Ok(_) => Ok(Response::new(SetResponse {})),
                Err(_) => Err(Status::new(
                    tonic::Code::Internal,
                    "failed to set key-value pair",
                )),
            }
        })
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> RpcResponse<DeleteResponse> {
        self.with_db(|db| {
            let value = db.query_row(
                "DELETE FROM kv_store WHERE key = ?1 RETURNING value",
                [&request.get_ref().key],
                |row| row.get(0),
            );

            match value {
                Ok(old_value) => Ok(Response::new(DeleteResponse { old_value })),
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
            .set(Request::new(SetRequest {
                key: "key".to_owned(),
                value: "value".to_owned(),
            }))
            .await?;

        let response = store
            .get(Request::new(GetRequest {
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
            .set(Request::new(SetRequest {
                key: "key".to_owned(),
                value: "value".to_owned(),
            }))
            .await?;
        assert_eq!(response.get_ref(), &SetResponse {});

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<(), Box<dyn std::error::Error>> {
        let store = KvStore::new_in_memory();

        store
            .set(Request::new(SetRequest {
                key: "key".to_owned(),
                value: "value".to_owned(),
            }))
            .await?;

        let response = store
            .delete(Request::new(DeleteRequest {
                key: "key".to_owned(),
            }))
            .await?;
        assert_eq!(response.get_ref().old_value, "value");

        let response = store
            .get(Request::new(GetRequest {
                key: "key".to_owned(),
            }))
            .await;
        assert!(response.is_err());

        Ok(())
    }
}
