//! Handler implementations.

mod blob;
mod kv;
mod query;

pub use self::blob::BlobStore;
pub use self::kv::KvStore;
pub use self::query::QueryHandler;
use crate::internal_macros::future_send;
pub use crate::structs::blob::{
    DeleteRequest as BlobDeleteRequest, DeleteResponse as BlobDeleteResponse,
    EqDataRequest as BlobEqDataRequest, GetRequest as BlobGetRequest,
    GetResponse as BlobGetResponse, NotEqDataRequest as BlobNotEqDataRequest,
    StoreRequest as BlobStoreRequest, StoreResponse as BlobStoreResponse,
    UpdateRequest as BlobUpdateRequest, UpdateResponse as BlobUpdateResponse,
};
use crate::structs::kv::{
    DeleteRequest as KvDeleteRequest, DeleteResponse as KvDeleteResponse, EqRequest as KvEqRequest,
    GetRequest as KvGetRequest, GetResponse as KvGetResponse, NotEqRequest as KvNotEqRequest,
    SetRequest as KvSetRequest, SetResponse as KvSetResponse,
};
use crate::structs::query::{ExecuteResponse, QueryResponse, RawQuery};
use futures::Stream;

/// Handle key-value operations.
pub trait KvHandler<Store> {
    fn from_store(store: Store) -> Self;
    fn get(&self, request: impl Stream<Item = KvGetRequest>) -> future_send!(KvGetResponse);
    fn set(&self, request: impl Stream<Item = KvSetRequest>) -> future_send!(KvSetResponse);
    fn delete(
        &self,
        request: impl Stream<Item = KvDeleteRequest>,
    ) -> future_send!(KvDeleteResponse);
    fn eq(&self, request: KvEqRequest) -> future_send!(bool);
    fn not_eq(&self, request: KvNotEqRequest) -> future_send!(bool);
}

/// Handle BLOB operations.
pub trait BlobHandler<Store> {
    fn from_store(store: Store) -> Self;
    fn get(&self, request: BlobGetRequest) -> future_send!(BlobGetResponse);
    fn store(&self, request: BlobStoreRequest) -> future_send!(BlobStoreResponse);
    fn update(&self, request: BlobUpdateRequest) -> future_send!(BlobUpdateResponse);
    fn delete(&self, request: BlobDeleteRequest) -> future_send!(BlobDeleteResponse);
    fn eq_data(&self, request: BlobEqDataRequest) -> future_send!(bool);
    fn not_eq_data(&self, request: BlobNotEqDataRequest) -> future_send!(bool);
}

/// Handle query operations for all stores.
pub trait QueryHandleTrait<KvStore, BlobStore = KvStore> {
    fn from_stores(kv_store: KvStore, blob_store: BlobStore) -> Self;
    fn query(&self, request: RawQuery) -> future_send!(QueryResponse);
    fn execute(&self, request: RawQuery) -> future_send!(ExecuteResponse);
}
