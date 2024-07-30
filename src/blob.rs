use crate::db_connection::{Database, DbConnectionInfo};
use crate::interop::duckdb_err_to_tonic_status;
pub use crate::schema::blob::blob_client::BlobClient;
pub use crate::schema::blob::blob_server::{Blob as BlobRpc, BlobServer};
pub use crate::schema::blob::{BlobData, BlobId, BlobIds, UpdateRequest};
use crate::schema::common::Bool;
use crate::{Location, RpcResponse};
use duckdb::params;
use std::path::PathBuf;
use tonic::{Request, Response};

#[derive(Debug)]
pub struct BlobStore {
    location: Location,
}

impl DbConnectionInfo for BlobStore {
    fn initialize(db: &Database) -> duckdb::Result<()> {
        db.execute_batch(
            "CREATE SEQUENCE IF NOT EXISTS blob_id_seq START 1;
            CREATE TABLE IF NOT EXISTS blob(
                id INTEGER PRIMARY KEY DEFAULT nextval('blob_id_seq'),
                data BLOB,
                metadata TEXT
            );",
        )
    }

    fn location(&self) -> &Location {
        &self.location
    }
}

impl BlobStore {
    #[inline]
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            location: Location::OnDisk { path: path.into() },
        }
    }

    #[inline]
    pub fn new_in_memory() -> Self {
        Self {
            location: Location::InMemory,
        }
    }
}

#[tonic::async_trait]
impl BlobRpc for BlobStore {
    async fn get(&self, request: Request<BlobId>) -> RpcResponse<BlobData> {
        let BlobId { id } = request.into_inner();
        let db = self.connect();

        let (data, metadata) = db
            .query_row(
                "SELECT data, metadata FROM blob WHERE id = ?",
                [id],
                |row| {
                    let data: Vec<u8> = row.get(0)?;
                    let metadata: Option<String> = row.get(1)?;
                    Ok((data, metadata))
                },
            )
            .map_err(duckdb_err_to_tonic_status)?;

        Ok(Response::new(BlobData {
            bytes: data,
            metadata,
        }))
    }

    async fn store(&self, request: Request<BlobData>) -> RpcResponse<BlobId> {
        let BlobData { bytes, metadata } = request.into_inner();
        let db = self.connect();

        let id = db
            .query_row(
                "INSERT INTO blob(data, metadata) VALUES(?, ?) RETURNING id",
                params![bytes, metadata],
                |row| row.get(0),
            )
            .map_err(duckdb_err_to_tonic_status)?;

        Ok(Response::new(BlobId { id }))
    }

    async fn update(&self, request: Request<UpdateRequest>) -> RpcResponse<BlobId> {
        let UpdateRequest {
            id,
            bytes,
            should_update_metadata,
            metadata,
        } = request.into_inner();
        let mut db = self.connect();

        let transaction = db.transaction().map_err(duckdb_err_to_tonic_status)?;

        if let Some(bytes) = bytes {
            transaction
                .execute("UPDATE blob SET data = ? WHERE id = ?", params![bytes, id])
                .map_err(duckdb_err_to_tonic_status)?;
        }
        if should_update_metadata {
            transaction
                .execute(
                    "UPDATE blob SET metadata = ? WHERE id = ?",
                    params![metadata, id],
                )
                .map_err(duckdb_err_to_tonic_status)?;
        }

        transaction.commit().map_err(duckdb_err_to_tonic_status)?;

        Ok(Response::new(BlobId { id }))
    }

    async fn delete(&self, request: Request<BlobId>) -> RpcResponse<BlobId> {
        let BlobId { id } = request.into_inner();
        let db = self.connect();

        db.execute("DELETE FROM blob WHERE id = ?", params![id])
            .map_err(duckdb_err_to_tonic_status)?;
        Ok(Response::new(BlobId { id }))
    }

    async fn eq_data(&self, request: Request<BlobIds>) -> RpcResponse<Bool> {
        todo!()
    }

    async fn not_eq_data(&self, request: Request<BlobIds>) -> RpcResponse<Bool> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::LazyLock;
    use tonic::IntoRequest as _;

    static BLOB_STORE: LazyLock<BlobStore> = LazyLock::new(|| BlobStore::new("test_blob_store"));

    #[tokio::test]
    async fn test_get() -> Result<(), Box<dyn std::error::Error>> {
        let id = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abcdef".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE.get(id.into_request()).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"abcdef");
        assert_eq!(response.metadata, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_store() -> Result<(), Box<dyn std::error::Error>> {
        let id = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abcdef".to_vec(),
                    metadata: Some("{}".to_owned()),
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE.get(id.into_request()).await?.into_inner();
        assert_eq!(response.bytes, b"abcdef");
        assert_eq!(response.metadata, Some("{}".to_owned()));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_both() -> Result<(), Box<dyn std::error::Error>> {
        let BlobId { id } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abc".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let id = BLOB_STORE
            .update(
                UpdateRequest {
                    id,
                    bytes: Some(b"def".to_vec()),
                    should_update_metadata: true,
                    metadata: Some("{}".to_owned()),
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE.get(id.into_request()).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"def");
        assert_eq!(response.metadata, Some("{}".to_owned()));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let BlobId { id } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abc".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let id = BLOB_STORE
            .update(
                UpdateRequest {
                    id,
                    bytes: Some(b"def".to_vec()),
                    should_update_metadata: false,
                    metadata: Some("{}".to_owned()),
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE.get(id.into_request()).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"def");
        assert_eq!(response.metadata, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let BlobId { id } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abc".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let id = BLOB_STORE
            .update(
                UpdateRequest {
                    id,
                    bytes: None,
                    should_update_metadata: true,
                    metadata: Some("{}".to_owned()),
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE.get(id.into_request()).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"abc");
        assert_eq!(response.metadata, Some("{}".to_owned()));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_with_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let blob_id @ BlobId { id } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abcdef".to_vec(),
                    metadata: Some("{}".to_owned()),
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE
            .delete(blob_id.into_request())
            .await?
            .into_inner();
        assert_eq!(response.id, id);

        let response = BLOB_STORE.get(blob_id.into_request()).await;
        assert!(response.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_no_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let blob_id @ BlobId { id } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abcdef".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE
            .delete(blob_id.into_request())
            .await?
            .into_inner();
        assert_eq!(response.id, id);

        let response = BLOB_STORE.get(blob_id.into_request()).await;
        assert!(response.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_eq_data() -> Result<(), Box<dyn std::error::Error>> {
        let BlobId { id: id1 } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abcdef".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();
        let BlobId { id: id2 } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abcdef".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();
        let BlobId { id: id3 } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"ghijkl".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE
            .eq_data(
                BlobIds {
                    ids: vec![id1, id2],
                }
                .into_request(),
            )
            .await?
            .into_inner();
        assert!(response.value);

        let response = BLOB_STORE
            .eq_data(
                BlobIds {
                    ids: vec![id1, id2, id3],
                }
                .into_request(),
            )
            .await?
            .into_inner();
        assert!(!response.value);

        Ok(())
    }

    #[tokio::test]
    async fn test_not_eq_data() -> Result<(), Box<dyn std::error::Error>> {
        let BlobId { id: id1 } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abcdef".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();
        let BlobId { id: id2 } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"abcdef".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();
        let BlobId { id: id3 } = BLOB_STORE
            .store(
                BlobData {
                    bytes: b"ghijkl".to_vec(),
                    metadata: None,
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let response = BLOB_STORE
            .not_eq_data(
                BlobIds {
                    ids: vec![id1, id2],
                }
                .into_request(),
            )
            .await?
            .into_inner();
        assert!(!response.value);

        let response = BLOB_STORE
            .not_eq_data(
                BlobIds {
                    ids: vec![id1, id3],
                }
                .into_request(),
            )
            .await?
            .into_inner();
        assert!(response.value);

        Ok(())
    }

    #[tokio::test]
    async fn test_eq_data_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let res = BLOB_STORE
            .eq_data(
                BlobIds {
                    // If all four of these keys somehow exist, then a test failure is deserved.
                    ids: vec![0, 1, 2, 3],
                }
                .into_request(),
            )
            .await;
        assert!(res.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_not_eq_data_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let res = BLOB_STORE
            .not_eq_data(
                BlobIds {
                    // If all four of these keys somehow exist, then a test failure is deserved.
                    ids: vec![0, 1, 2, 3],
                }
                .into_request(),
            )
            .await;
        assert!(res.is_err());
        Ok(())
    }
}
