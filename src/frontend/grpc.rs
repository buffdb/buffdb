use crate::backend::KvBackend;
use crate::handler::KvHandler;
use crate::internal_macros::future_send;
use crate::structs::kv::{
    DeleteRequest as KvDeleteRequest, DeleteResponse as KvDeleteResponse, EqRequest as KvEqRequest,
    GetRequest as KvGetRequest, GetResponse as KvGetResponse, NotEqRequest as KvNotEqRequest,
    SetRequest as KvSetRequest, SetResponse as KvSetResponse,
};
use futures::Stream;

#[derive(Debug)]
pub struct Grpc<Store>(Store);

impl<Store> KvHandler<Store> for Grpc<Store>
where
    Store: KvBackend<tonic::Status>,
{
    fn from_store(store: Store) -> Self {
        Self(store)
    }

    fn get(&self, request: impl Stream<Item = KvGetRequest>) -> future_send!(KvGetResponse) {
        self.0.get(request)
    }

    fn set(&self, request: impl Stream<Item = KvSetRequest>) -> future_send!(KvSetResponse) {
        todo!()
    }

    fn delete(
        &self,
        request: impl Stream<Item = KvDeleteRequest>,
    ) -> future_send!(KvDeleteResponse) {
        todo!()
    }

    fn eq(&self, request: KvEqRequest) -> future_send!(bool) {
        todo!()
    }

    fn not_eq(&self, request: KvNotEqRequest) -> future_send!(bool) {
        todo!()
    }
}
