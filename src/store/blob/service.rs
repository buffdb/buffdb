pub mod blob_store {
    tonic::include_proto!("blobstore");
}

use crate::store::blob::service::blob_store::Blob;
use once_cell::sync::Lazy;

use self::blob_store::blob_service_server::BlobService;
use self::blob_store::{DeleteBlobRequest, GetBlobRequest, StoreBlobResponse};
use tonic::{Request, Response, Status};

thread_local! {
    static BLOB_STORE_DB: Lazy<rusqlite::Connection> = Lazy::new(|| {
        // TODO back by something on the file system, not memory
        rusqlite::Connection::open_in_memory().unwrap()
    });
}

#[derive(Default)]
pub struct BlobStore {}

#[tonic::async_trait]
impl BlobService for BlobStore {
    async fn get_blob(&self, request: Request<GetBlobRequest>) -> Result<Response<Blob>, Status> {
        todo!()
    }

    async fn store_blob(
        &self,
        request: Request<Blob>,
    ) -> Result<Response<StoreBlobResponse>, Status> {
        todo!()
    }

    async fn delete_blob(
        &self,
        request: Request<DeleteBlobRequest>,
    ) -> Result<Response<Blob>, Status> {
        todo!()
    }
}
