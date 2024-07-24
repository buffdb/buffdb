pub mod blob_store {
    tonic::include_proto!("blobstore");
}

use crate::store::blob::service::blob_store::Blob;

use self::blob_store::blob_service_server::BlobService;
use self::blob_store::{DeleteBlobRequest, GetBlobRequest, StoreBlobResponse};
use tonic::{Request, Response, Status};

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
