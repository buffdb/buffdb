mod kv_store {
    tonic::include_proto!("kvstore");
}

use std::collections::HashMap;
use std::path::PathBuf;

use crate::store;
pub use crate::store::kv::kv_store::key_value_server::KeyValue;
pub use crate::store::kv::kv_store::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};
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
                "CREATE TABLE IF NOT EXISTS kv (
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
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        self.with_db(|db| {
            let value = db.query_row(
                "SELECT value FROM kv WHERE key = ?1",
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

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request = request.get_ref();
        self.with_db(|db| {
            let value = db.execute(
                "REPLACE INTO kv (key, value) VALUES (?1, ?2)",
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

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.with_db(|db| {
            let value = db.query_row(
                "DELETE FROM kv WHERE key = ?1 RETURNING value",
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
