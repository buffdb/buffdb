pub mod kv_store {
    tonic::include_proto!("kvstore");
}

use self::kv_store::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};
use crate::store::kv::service::kv_store::key_value_server::KeyValue;
use once_cell::sync::Lazy;
use tonic::{Request, Response, Status};

thread_local! {
    static KV_STORE_DB: Lazy<rusqlite::Connection> = Lazy::new(|| {
        // TODO back by something on the file system, not memory
        rusqlite::Connection::open_in_memory().unwrap()
    });
}

#[derive(Default)]
pub struct KvStore {}

#[tonic::async_trait]
impl KeyValue for KvStore {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        todo!()
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        todo!()
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        todo!()
    }
}
