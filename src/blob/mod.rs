mod blob_store {
    tonic::include_proto!("blobstore");
}

pub use self::blob_store::blob_server::{Blob as BlobRpc, BlobServer};
pub use self::blob_store::{BlobData, BlobId, UpdateRequest};
use crate::db_connection::{Database, DbConnectionInfo};
use crate::{Location, RpcResponse};
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
            db: Location::OnDisk { path: path.into() }.to_connection::<Self>(),
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

        let data = match data {
            Ok(Some(value)) => value,
            Ok(None) => return Err(Status::new(tonic::Code::NotFound, "id not found")),
            // TODO handle errors more gracefully
            Err(_) => return Err(Status::new(tonic::Code::Internal, "failed to get blob")),
        };
        let metadata = match metadata {
            Ok(Some(value)) => {
                Some(String::from_utf8(value).expect("protobuf requires strings be valid UTF-8"))
            }
            Ok(None) => None,
            // TODO handle errors more gracefully
            Err(_) => return Err(Status::new(tonic::Code::Internal, "failed to get blob")),
        };

        Ok(Response::new(BlobData {
            bytes: data,
            metadata,
        }))
    }

    async fn store(&self, request: Request<BlobData>) -> RpcResponse<BlobId> {
        let mut id = generate_id();
        let mut id_bytes = id.to_ne_bytes();
        let BlobData { bytes, metadata } = request.into_inner();

        let data_col = self.db.cf_handle("data").unwrap();
        let metadata_col = self.db.cf_handle("metadata").unwrap();

        if self.db.key_may_exist_cf(&data_col, id_bytes) {
            id = generate_id();
            id_bytes = id.to_ne_bytes();

            // Theoretically it is possible for there to be *another* collision, but it incredibly
            // unlikely to have back-to-back collisions. If it does happen, return an error instead
            // of continuing to retry.
            if self.db.key_may_exist_cf(&data_col, id_bytes) {
                return Err(Status::new(
                    tonic::Code::Internal,
                    "failed to generate unique id",
                ));
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

        match data_res.and(metadata_res) {
            Ok(()) => Ok(Response::new(BlobId { id })),
            // TODO handle errors more gracefully
            Err(_) => Err(Status::new(tonic::Code::Internal, "failed to store blob")),
        }
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
            let res = self.db.put_cf(&data_col, id.to_le_bytes(), &bytes);
            match res {
                Ok(()) => (),
                // TODO handle errors more gracefully
                Err(_) => return Err(Status::new(tonic::Code::Internal, "failed to update blob")),
            }
        }

        if should_update_metadata {
            let metadata_col = self.db.cf_handle("metadata").unwrap();

            let res = if let Some(metadata) = metadata {
                self.db.put_cf(&metadata_col, id.to_le_bytes(), metadata)
            } else {
                self.db.delete_cf(&metadata_col, id.to_le_bytes())
            };

            match res {
                Ok(()) => Ok(Response::new(BlobId { id })),
                // TODO handle errors more gracefully
                Err(_) => Err(Status::new(tonic::Code::Internal, "failed to update blob")),
            }
        } else {
            Ok(Response::new(BlobId { id }))
        }
    }

    async fn delete(&self, request: Request<BlobId>) -> RpcResponse<BlobId> {
        let BlobId { id } = request.into_inner();

        let data_col = self.db.cf_handle("data").unwrap();
        let metadata_col = self.db.cf_handle("metadata").unwrap();

        let data_res = self.db.delete_cf(&data_col, id.to_le_bytes());
        let metadata_res = self.db.delete_cf(&metadata_col, id.to_le_bytes());

        match data_res {
            Ok(()) => {}
            // TODO handle errors more gracefully
            Err(_) => return Err(Status::new(tonic::Code::Internal, "failed to delete blob")),
        }
        match metadata_res {
            Ok(()) => Ok(Response::new(BlobId { id })),
            // TODO handle errors more gracefully
            Err(_) => Err(Status::new(tonic::Code::Internal, "failed to delete blob")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::LazyLock;

    static BLOB_STORE: LazyLock<BlobStore> = LazyLock::new(|| BlobStore::new("test_blob_store"));

    #[tokio::test]
    async fn test_get() -> Result<(), Box<dyn std::error::Error>> {
        let response = BLOB_STORE
            .store(Request::new(BlobData {
                bytes: b"abcdef".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE.get(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"abcdef");
        assert_eq!(response.metadata, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_store() -> Result<(), Box<dyn std::error::Error>> {
        let response = BLOB_STORE
            .store(Request::new(BlobData {
                bytes: b"abcdef".to_vec(),
                metadata: Some("{}".to_owned()),
            }))
            .await?;

        let id = response.into_inner().id;
        let response = BLOB_STORE
            .get(Request::new(BlobId { id }))
            .await?
            .into_inner();
        assert_eq!(response.bytes, b"abcdef");
        assert_eq!(response.metadata, Some("{}".to_owned()));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_both() -> Result<(), Box<dyn std::error::Error>> {
        let response = BLOB_STORE
            .store(Request::new(BlobData {
                bytes: b"abc".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE
            .update(Request::new(UpdateRequest {
                id,
                bytes: Some(b"def".to_vec()),
                should_update_metadata: true,
                metadata: Some("{}".to_owned()),
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE.get(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"def");
        assert_eq!(response.metadata, Some("{}".to_owned()));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let response = BLOB_STORE
            .store(Request::new(BlobData {
                bytes: b"abc".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE
            .update(Request::new(UpdateRequest {
                id,
                bytes: Some(b"def".to_vec()),
                should_update_metadata: false,
                metadata: Some("{}".to_owned()),
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE.get(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"def");
        assert_eq!(response.metadata, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let response = BLOB_STORE
            .store(Request::new(BlobData {
                bytes: b"abc".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE
            .update(Request::new(UpdateRequest {
                id,
                bytes: None,
                should_update_metadata: true,
                metadata: Some("{}".to_owned()),
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE.get(Request::new(BlobId { id })).await?;
        let response = response.get_ref();
        assert_eq!(response.bytes, b"abc");
        assert_eq!(response.metadata, Some("{}".to_owned()));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_with_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let response = BLOB_STORE
            .store(Request::new(BlobData {
                bytes: b"abcdef".to_vec(),
                metadata: Some("{}".to_owned()),
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE.delete(Request::new(BlobId { id })).await?;
        assert_eq!(response.get_ref().id, id);

        let response = BLOB_STORE.get(Request::new(BlobId { id })).await;
        assert!(response.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_no_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let response = BLOB_STORE
            .store(Request::new(BlobData {
                bytes: b"abcdef".to_vec(),
                metadata: None,
            }))
            .await?;
        let id = response.get_ref().id;

        let response = BLOB_STORE.delete(Request::new(BlobId { id })).await?;
        assert_eq!(response.get_ref().id, id);

        let response = BLOB_STORE.get(Request::new(BlobId { id })).await;
        assert!(response.is_err());

        Ok(())
    }
}
