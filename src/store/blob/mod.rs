mod blob_store {
    tonic::include_proto!("blobstore");
}

use std::path::PathBuf;

use crate::{store, RpcResponse};

pub use self::blob_store::blob_server::{Blob, BlobServer};
pub use self::blob_store::{BlobData, BlobId, UpdateRequest};
use tonic::{Request, Response, Status};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BlobStore {
    location: store::Location,
}

impl BlobStore {
    #[inline]
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        let store = Self {
            location: store::Location::OnDisk { path: path.into() },
        };
        store.initialize_table();
        store
    }

    #[inline]
    pub fn new_in_memory() -> Self {
        let store = Self {
            location: store::Location::InMemory,
        };
        store.initialize_table();
        store
    }

    fn initialize_table(&self) {
        store::DB.with_borrow_mut(|map| {
            let db = map
                .entry(self.location.clone())
                .or_insert_with(|| self.location.to_connection());

            // rowid is automatically present in SQLite tables
            db.execute(
                "CREATE TABLE IF NOT EXISTS blob_store (
                    bytes BLOB NOT NULL,
                    metadata TEXT
                )",
                [],
            );
        });
    }

    fn with_db<T, E>(
        &self,
        mut f: impl FnMut(&mut rusqlite::Connection) -> Result<T, E>,
    ) -> Result<T, E> {
        store::DB.with_borrow_mut(|map| {
            let db = map
                .entry(self.location.clone())
                .or_insert_with(|| self.location.to_connection());
            f(db)
        })
    }
}

#[tonic::async_trait]
impl Blob for BlobStore {
    async fn get(&self, request: Request<BlobId>) -> RpcResponse<BlobData> {
        self.with_db(|db| {
            let value = db.query_row(
                "SELECT bytes, metadata FROM blob_store WHERE rowid = ?1",
                [&request.get_ref().id],
                |row| {
                    let data = row.get(0)?;
                    let metadata = row.get(1)?;
                    Ok((data, metadata))
                },
            );

            match value {
                Ok((bytes, metadata)) => Ok(Response::new(BlobData { bytes, metadata })),
                // TODO Errors are possible even if the key is found. Handle them appropriately.
                Err(_) => Err(Status::new(tonic::Code::NotFound, "id not found")),
            }
        })
    }

    async fn store(&self, request: Request<BlobData>) -> RpcResponse<BlobId> {
        let request = request.get_ref();
        self.with_db(|db| {
            let value = db.query_row(
                "INSERT INTO blob_store (bytes, metadata) VALUES (?1, ?2) RETURNING rowid",
                (&request.bytes, &request.metadata),
                |row| row.get(0),
            );

            match value {
                Ok(id) => Ok(Response::new(BlobId { id })),
                // TODO Numerous errors are possible. Handle them appropriately.
                Err(_) => Err(Status::new(tonic::Code::Internal, "failed to store blob")),
            }
        })
    }

    async fn update(&self, request: Request<UpdateRequest>) -> RpcResponse<BlobId> {
        let request = request.get_ref();
        // TODO Would a query builder be worth the added overhead?
        let value = match (&request.bytes, request.should_update_metadata) {
            (None, false) => {
                // No update requested
                Ok(request.id)
            }
            (Some(bytes), false) => {
                // Update bytes but not metadata
                self.with_db(|db| {
                    db.query_row(
                        "UPDATE blob_store SET bytes = ?1 WHERE rowid = ?2 RETURNING rowid",
                        (bytes, request.id),
                        |row| row.get(0),
                    )
                })
            }
            (None, true) => {
                // Update metadata but not bytes
                self.with_db(|db| {
                    db.query_row(
                        "UPDATE blob_store SET metadata = ?1 WHERE rowid = ?2 RETURNING rowid",
                        (&request.metadata, request.id),
                        |row| row.get(0),
                    )
                })
            }
            (Some(bytes), true) => {
                // Update both bytes and metadata
                self.with_db(|db| {
                    db.query_row(
                        "UPDATE blob_store SET bytes = ?1, metadata = ?2 WHERE rowid = ?3 RETURNING rowid",
                        (bytes, &request.metadata, request.id),
                        |row| row.get(0),
                    )
                })
            }
        };

        match value {
            Ok(id) => Ok(Response::new(BlobId { id })),
            // TODO Numerous errors are possible. Handle them appropriately.
            Err(_) => Err(Status::new(tonic::Code::Internal, "failed to update blob")),
        }
    }

    async fn delete(&self, request: Request<BlobId>) -> RpcResponse<BlobData> {
        self.with_db(|db| {
            let value = db.query_row(
                "DELETE FROM blob_store WHERE rowid = ?1 RETURNING bytes, metadata",
                [&request.get_ref().id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            );

            match value {
                Ok((bytes, metadata)) => Ok(Response::new(BlobData { bytes, metadata })),
                // TODO Errors are possible even if the key is found. Handle them appropriately.
                Err(err) => Err(Status::new(tonic::Code::NotFound, "key not found")),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_get() -> Result<(), Box<dyn std::error::Error>> {
        let store = BlobStore::new_in_memory();

        let response = store
            .store(Request::new(BlobData {
                bytes: b"abcdef".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = store.get(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"abcdef");
        assert_eq!(response.metadata, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_store() -> Result<(), Box<dyn std::error::Error>> {
        let store = BlobStore::new_in_memory();

        let response = store
            .store(Request::new(BlobData {
                bytes: b"abcdef".to_vec(),
                metadata: None,
            }))
            .await;
        // As test order is nondeterministic, don't assert on the ID.
        assert!(response.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_update_both() -> Result<(), Box<dyn std::error::Error>> {
        let store = BlobStore::new_in_memory();

        let response = store
            .store(Request::new(BlobData {
                bytes: b"abc".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = store
            .update(Request::new(UpdateRequest {
                id,
                bytes: Some(b"def".to_vec()),
                should_update_metadata: true,
                metadata: Some("{}".to_owned()),
            }))
            .await?;
        let id = response.get_ref().id;

        let response = store.get(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"def");
        assert_eq!(response.metadata, Some("{}".to_owned()));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let store = BlobStore::new_in_memory();

        let response = store
            .store(Request::new(BlobData {
                bytes: b"abc".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = store
            .update(Request::new(UpdateRequest {
                id,
                bytes: Some(b"def".to_vec()),
                should_update_metadata: false,
                metadata: Some("{}".to_owned()),
            }))
            .await?;
        let id = response.get_ref().id;

        let response = store.get(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"def");
        assert_eq!(response.metadata, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let store = BlobStore::new_in_memory();

        let response = store
            .store(Request::new(BlobData {
                bytes: b"abc".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = store
            .update(Request::new(UpdateRequest {
                id,
                bytes: None,
                should_update_metadata: true,
                metadata: Some("{}".to_owned()),
            }))
            .await?;
        let id = response.get_ref().id;

        let response = store.get(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"abc");
        assert_eq!(response.metadata, Some("{}".to_owned()));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<(), Box<dyn std::error::Error>> {
        let store = BlobStore::new_in_memory();

        let response = store
            .store(Request::new(BlobData {
                bytes: b"abcdef".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = store.delete(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"abcdef");
        assert_eq!(response.metadata, None);

        let response = store.get(Request::new(BlobId { id })).await;
        assert!(response.is_err());

        Ok(())
    }
}
