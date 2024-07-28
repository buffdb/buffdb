use crate::db_connection::{Database, DbConnectionInfo};
use crate::interop::rocksdb_err_to_tonic_status;
pub use crate::schema::blob::blob_server::{Blob as BlobRpc, BlobServer};
pub use crate::schema::blob::{BlobData, BlobId, BlobIds, UpdateRequest};
use crate::schema::common::Bool;
use crate::{Location, RpcResponse};
use std::collections::HashSet;
use std::path::PathBuf;
use tonic::{Request, Response, Status};

fn generate_id() -> u64 {
    rand::random()
}

#[derive(Debug)]
pub struct BlobStore {
    db: Database,
}

impl DbConnectionInfo for BlobStore {
    fn fields() -> Option<impl Iterator<Item = &'static str>> {
        Some(["data", "metadata"].into_iter())
    }
}

impl BlobStore {
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
impl BlobRpc for BlobStore {
    async fn get(&self, request: Request<BlobId>) -> RpcResponse<BlobData> {
        let BlobId { id } = request.into_inner();

        let data_col = self.db.cf_handle("data").unwrap();
        let metadata_col = self.db.cf_handle("metadata").unwrap();

        let data = self.db.get_cf(&data_col, id.to_le_bytes());
        let metadata = self.db.get_cf(&metadata_col, id.to_le_bytes());

        let Some(data) = data.map_err(rocksdb_err_to_tonic_status)? else {
            return Err(Status::not_found("id not found"));
        };
        let metadata = metadata.map_err(rocksdb_err_to_tonic_status)?.map(|value| {
            String::from_utf8(value).expect("protobuf requires strings be valid UTF-8")
        });

        Ok(Response::new(BlobData {
            bytes: data,
            metadata,
        }))
    }

    async fn store(&self, request: Request<BlobData>) -> RpcResponse<BlobId> {
        let mut id = generate_id();
        let mut id_bytes = id.to_le_bytes();
        let BlobData { bytes, metadata } = request.into_inner();

        let data_col = self.db.cf_handle("data").unwrap();
        let metadata_col = self.db.cf_handle("metadata").unwrap();

        // Check for a collision of the generated identifier. If there is one, try once more before
        // erroring. Note that there is a TOCTOU issue here, but the odds of any collision at all
        // is so low that it is hardly worth worrying about. If this somehow becomes a plausible
        // issue, the ID can be extended to 128 bits from the current 64, rendering a collision all
        // but impossible.
        if matches!(self.db.get_cf(&data_col, id_bytes), Ok(Some(_))) {
            id = generate_id();
            id_bytes = id.to_le_bytes();

            // Theoretically it is possible for there to be *another* collision, but it incredibly
            // unlikely to have back-to-back collisions. If it does happen, return an error instead
            // of continuing to retry.
            if matches!(self.db.get_cf(&data_col, id_bytes), Ok(Some(_))) {
                return Err(Status::internal("failed to generate unique id"));
            }
        }

        // TODO Put these in a transaction to ensure all-or-nothing behavior. This is blocked on
        // rust-rocksdb/rust-rocksdb#868 being released.
        let data_res = self.db.put_cf(&data_col, id_bytes, bytes);
        let metadata_res = if let Some(metadata) = metadata {
            self.db.put_cf(&metadata_col, id_bytes, metadata)
        } else {
            Ok(())
        };

        // TODO until transactions are used, handle failure of one operation by undoing the other if
        // necessary

        data_res
            .and(metadata_res)
            .map_err(rocksdb_err_to_tonic_status)?;
        Ok(Response::new(BlobId { id }))
    }

    async fn update(&self, request: Request<UpdateRequest>) -> RpcResponse<BlobId> {
        let UpdateRequest {
            id,
            bytes,
            should_update_metadata,
            metadata,
        } = request.into_inner();

        if let Some(bytes) = bytes {
            let data_col = self.db.cf_handle("data").unwrap();
            self.db
                .put_cf(&data_col, id.to_le_bytes(), &bytes)
                .map_err(rocksdb_err_to_tonic_status)?;
        }

        if should_update_metadata {
            let metadata_col = self.db.cf_handle("metadata").unwrap();

            if let Some(metadata) = metadata {
                self.db.put_cf(&metadata_col, id.to_le_bytes(), metadata)
            } else {
                self.db.delete_cf(&metadata_col, id.to_le_bytes())
            }
            .map_err(rocksdb_err_to_tonic_status)?;
        }

        Ok(Response::new(BlobId { id }))
    }

    async fn delete(&self, request: Request<BlobId>) -> RpcResponse<BlobId> {
        let BlobId { id } = request.into_inner();

        let data_col = self.db.cf_handle("data").unwrap();
        let metadata_col = self.db.cf_handle("metadata").unwrap();

        self.db
            .delete_cf(&data_col, id.to_le_bytes())
            .and_then(|_| self.db.delete_cf(&metadata_col, id.to_le_bytes()))
            .map_err(rocksdb_err_to_tonic_status)?;

        Ok(Response::new(BlobId { id }))
    }

    async fn eq_data(&self, request: Request<BlobIds>) -> RpcResponse<Bool> {
        let BlobIds { ids } = request.into_inner();
        let data_col = self.db.cf_handle("data").unwrap();
        let blobs = self
            .db
            .multi_get_cf(ids.iter().map(|id| (&data_col, id.to_le_bytes())))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(rocksdb_err_to_tonic_status)?;

        let all_eq = match blobs.first() {
            Some(first @ Some(_)) => blobs.iter().skip(1).all(|value| value == first),
            // The first ID does not exist, so all blobs cannot be equal.
            Some(None) => return Err(Status::not_found(format!("id {} not found", ids[0]))),
            // If there are no IDs, then all blobs are by definition equal.
            None => true,
        };

        Ok(Response::new(Bool { value: all_eq }))
    }

    async fn not_eq_data(&self, request: Request<BlobIds>) -> RpcResponse<Bool> {
        let BlobIds { ids } = request.into_inner();
        let data_col = self.db.cf_handle("data").unwrap();
        let blobs = self
            .db
            .multi_get_cf(ids.iter().map(|id| (&data_col, id.to_le_bytes())))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(rocksdb_err_to_tonic_status)?;

        let mut unique_values = HashSet::new();
        for (blob, id) in blobs.into_iter().zip(ids.iter()) {
            // Each requested key must exist.
            let Some(blob) = blob else {
                return Err(Status::not_found(format!("id {id} not found")));
            };
            // `insert` returns false if the value already exists.
            if !unique_values.insert(blob) {
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
