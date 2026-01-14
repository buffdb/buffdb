//! A key-value store.

use crate::backend::error::BackendError;
use crate::backend::{DatabaseBackend, KvBackend};
use crate::index::{IndexConfig, IndexManager};
use crate::proto::kv::{
    BeginTransactionRequest, BeginTransactionResponse, CommitTransactionRequest,
    CommitTransactionResponse, DeleteRequest, EqRequest, GetRequest, NotEqRequest,
    RollbackTransactionRequest, RollbackTransactionResponse, SetRequest,
};
use crate::service::kv::KvRpc;
use crate::transaction::{TransactionManager, TransactionalBackend};
use crate::{Location, RpcResponse, StreamingRequest};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
// use tokio_stream::StreamExt; // Unused
use tonic::{Request, Response, Status};

/// A key-value store.
///
/// This is a key-value store where both the key and value are strings. There are no restrictions on
/// the length or contents of either the key or value beyond restrictions implemented by the
/// protobuf server.
#[must_use]
#[derive(Debug)]
pub struct KvStore<Backend: TransactionalBackend> {
    backend: Backend,
    #[allow(dead_code)]
    transaction_manager: Option<Arc<TransactionManager<Backend>>>,
    index_manager: Arc<IndexManager>,
}

impl<Backend> KvStore<Backend>
where
    Backend: DatabaseBackend + TransactionalBackend,
{
    /// Create a new key-value store at the given location. If not pre-existing, the store will not
    /// be initialized until the first connection is made.
    #[inline]
    pub fn at_location(location: Location) -> Result<Self, BackendError> {
        Ok(Self {
            backend: Backend::at_location(location)?,
            transaction_manager: None,
            index_manager: Arc::new(IndexManager::new()),
        })
    }

    /// Create a new key-value at the given path on disk. If not pre-existing, the store will not be
    /// initialized until the first connection is made.
    #[inline]
    pub fn at_path<P>(path: P) -> Result<Self, BackendError>
    where
        P: Into<PathBuf>,
    {
        Self::at_location(Location::OnDisk { path: path.into() })
    }

    /// Create a new in-memory key-value store. This is useful for short-lived data.
    ///
    /// Note that all in-memory connections share the same stream, so any asynchronous calls have a
    /// nondeterministic order. This is not a problem for on-disk connections.
    #[inline]
    pub fn in_memory() -> Result<Self, BackendError> {
        Self::at_location(Location::InMemory)
    }

    /// Create a secondary index on the key-value store
    pub fn create_index(&self, config: IndexConfig) -> Result<(), crate::index::IndexError> {
        self.index_manager.create_index(config)
    }

    /// Drop a secondary index
    pub fn drop_index(&self, name: &str) -> Result<(), crate::index::IndexError> {
        self.index_manager.drop_index(name)
    }

    /// Get the index manager
    pub const fn index_manager(&self) -> &Arc<IndexManager> {
        &self.index_manager
    }
}

impl<Backend> KvStore<Backend>
where
    Backend: TransactionalBackend,
{
    /// Create a new key-value store with transaction support.
    ///
    /// DEPRECATED: Transaction support over gRPC is being phased out.
    /// Use batch operations or client-side transaction management instead.
    #[deprecated(
        since = "0.5.0",
        note = "Transaction support over gRPC is problematic and will be removed. Use batch operations instead."
    )]
    pub fn with_transactions(
        location: Location,
        transaction_timeout: Duration,
    ) -> Result<Self, BackendError> {
        Ok(Self {
            backend: Backend::at_location(location)?,
            transaction_manager: Some(Arc::new(TransactionManager::new(transaction_timeout))),
            index_manager: Arc::new(IndexManager::new()),
        })
    }
}

#[tonic::async_trait]
impl<Backend> KvRpc for KvStore<Backend>
where
    Backend: KvBackend<GetStream: Send, SetStream: Send, DeleteStream: Send>
        + TransactionalBackend
        + 'static,
{
    type GetStream = Backend::GetStream;
    type SetStream = Backend::SetStream;
    type DeleteStream = Backend::DeleteStream;

    async fn get(&self, request: StreamingRequest<GetRequest>) -> RpcResponse<Self::GetStream> {
        // For now, just pass through to the backend
        // Transaction support would need a different approach
        self.backend.get(request).await
    }

    async fn set(&self, request: StreamingRequest<SetRequest>) -> RpcResponse<Self::SetStream> {
        // For now, just pass through to the backend
        // Index updates would need to be handled differently without Clone trait
        self.backend.set(request).await
    }

    async fn delete(
        &self,
        request: StreamingRequest<DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        // For now, just pass through to the backend
        // Index updates would need to be handled differently without Clone trait
        self.backend.delete(request).await
    }

    async fn eq(&self, request: StreamingRequest<EqRequest>) -> RpcResponse<bool> {
        self.backend.eq(request).await
    }

    async fn not_eq(&self, request: StreamingRequest<NotEqRequest>) -> RpcResponse<bool> {
        self.backend.not_eq(request).await
    }

    async fn begin_transaction(
        &self,
        _request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        Err(Status::unimplemented(
            "Transaction support over gRPC is deprecated. Use batch operations or manage transactions client-side.",
        ))
    }

    async fn commit_transaction(
        &self,
        _request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionResponse>, Status> {
        Err(Status::unimplemented(
            "Transaction support over gRPC is deprecated. Use batch operations or manage transactions client-side.",
        ))
    }

    async fn rollback_transaction(
        &self,
        _request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionResponse>, Status> {
        Err(Status::unimplemented(
            "Transaction support over gRPC is deprecated. Use batch operations or manage transactions client-side.",
        ))
    }
}
