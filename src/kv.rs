//! A key-value store.

use crate::backend::{DatabaseBackend, KvBackend};
use crate::index::{IndexConfig, IndexManager};
use crate::interop::IntoTonicStatus;
use crate::proto::kv::{
    BeginTransactionRequest, BeginTransactionResponse, CommitTransactionRequest,
    CommitTransactionResponse, DeleteRequest, EqRequest, GetRequest, NotEqRequest,
    RollbackTransactionRequest, RollbackTransactionResponse, SetRequest,
};
use crate::service::kv::KvRpc;
use crate::transaction::{TransactionManager, TransactionalBackend, TransactionalKvBackend};
use crate::{Location, RpcResponse, StreamingRequest};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};
use tokio_stream::StreamExt;

/// A key-value store.
///
/// This is a key-value store where both the key and value are strings. There are no restrictions on
/// the length or contents of either the key or value beyond restrictions implemented by the
/// protobuf server.
#[must_use]
#[derive(Debug)]
pub struct KvStore<Backend> {
    backend: Backend,
    transaction_manager: Option<Arc<TransactionManager<Backend>>>,
    index_manager: Arc<IndexManager>,
}

impl<Backend> KvStore<Backend>
where
    Backend: DatabaseBackend,
{
    /// Create a new key-value store at the given location. If not pre-existing, the store will not
    /// be initialized until the first connection is made.
    #[inline]
    pub fn at_location(location: Location) -> Result<Self, Backend::Error> {
        Ok(Self {
            backend: Backend::at_location(location)?,
            transaction_manager: None,
            index_manager: Arc::new(IndexManager::new()),
        })
    }

    /// Create a new key-value at the given path on disk. If not pre-existing, the store will not be
    /// initialized until the first connection is made.
    #[inline]
    pub fn at_path<P>(path: P) -> Result<Self, Backend::Error>
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
    pub fn in_memory() -> Result<Self, Backend::Error> {
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
    pub fn index_manager(&self) -> &Arc<IndexManager> {
        &self.index_manager
    }
}

impl<Backend> KvStore<Backend>
where
    Backend: TransactionalBackend,
{
    /// Create a new key-value store with transaction support.
    pub fn with_transactions(
        location: Location,
        transaction_timeout: Duration,
    ) -> Result<Self, Backend::Error> {
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
    Backend: KvBackend<Error: IntoTonicStatus, GetStream: Send, SetStream: Send, DeleteStream: Send>
        + 'static,
{
    type GetStream = Backend::GetStream;
    type SetStream = Backend::SetStream;
    type DeleteStream = Backend::DeleteStream;

    async fn get(&self, request: StreamingRequest<GetRequest>) -> RpcResponse<Self::GetStream> {
        // Check if we need to handle transactions
        if let Some(transaction_manager) = &self.transaction_manager {
            // Peek at the first message to check for transaction_id
            let mut stream = request.into_inner();
            if let Some(first_msg) = stream.message().await.map_err(|e| e.into_tonic_status())? {
                if let Some(transaction_id) = &first_msg.transaction_id {
                    // If backend supports transactions, use transactional get
                    if let Some(backend) = (&self.backend as &dyn std::any::Any).downcast_ref::<Backend>() 
                        where Backend: TransactionalKvBackend 
                    {
                        // Recreate the stream with the first message
                        let (tx, rx) = tokio::sync::mpsc::channel(1);
                        tx.send(Ok(first_msg)).await.unwrap();
                        tokio::spawn(async move {
                            while let Some(msg) = stream.message().await.transpose() {
                                if tx.send(msg).await.is_err() {
                                    break;
                                }
                            }
                        });
                        let recreated_stream = tonic::Streaming::new(tokio_stream::wrappers::ReceiverStream::new(rx));
                        return backend.get_in_transaction(transaction_id, recreated_stream).await;
                    }
                }
                // Recreate the stream for normal processing
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                tx.send(Ok(first_msg)).await.unwrap();
                tokio::spawn(async move {
                    while let Some(msg) = stream.message().await.transpose() {
                        if tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                });
                let recreated_stream = tonic::Streaming::new(tokio_stream::wrappers::ReceiverStream::new(rx));
                return self.backend.get(recreated_stream).await;
            }
        }
        self.backend.get(request).await
    }

    async fn set(&self, request: StreamingRequest<SetRequest>) -> RpcResponse<Self::SetStream> {
        // For index integration, we need to intercept the set operations
        let mut stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        
        let backend = self.backend.clone();
        let index_manager = Arc::clone(&self.index_manager);
        
        tokio::spawn(async move {
            while let Some(result) = stream.message().await.transpose() {
                match result {
                    Ok(msg) => {
                        // Before setting, get the old value for index updates
                        let old_value = if index_manager.indexes.read().unwrap().len() > 0 {
                            // Only fetch old value if we have indexes
                            let get_stream = tokio_stream::iter(vec![GetRequest {
                                key: msg.key.clone(),
                                transaction_id: msg.transaction_id.clone(),
                            }].into_iter().map(Ok));
                            
                            if let Ok(mut resp) = backend.get(tonic::Request::new(get_stream)).await {
                                if let Some(Ok(old)) = resp.message().await {
                                    Some(old.value)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        
                        // Update indexes
                        if let Err(e) = index_manager.update_indexes(&msg.key, old_value.as_deref(), &msg.value) {
                            let _ = tx.send(Err(Status::internal(format!("Index update failed: {}", e)))).await;
                            break;
                        }
                        
                        // Forward the message
                        if tx.send(Ok(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        
        let recreated_stream = tonic::Streaming::new(tokio_stream::wrappers::ReceiverStream::new(rx));
        self.backend.set(recreated_stream).await
    }

    async fn delete(
        &self,
        request: StreamingRequest<DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        // For index integration, we need to intercept the delete operations
        let mut stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        
        let backend = self.backend.clone();
        let index_manager = Arc::clone(&self.index_manager);
        
        tokio::spawn(async move {
            while let Some(result) = stream.message().await.transpose() {
                match result {
                    Ok(msg) => {
                        // Get the value before deletion for index removal
                        if index_manager.indexes.read().unwrap().len() > 0 {
                            let get_stream = tokio_stream::iter(vec![GetRequest {
                                key: msg.key.clone(),
                                transaction_id: msg.transaction_id.clone(),
                            }].into_iter().map(Ok));
                            
                            if let Ok(mut resp) = backend.get(tonic::Request::new(get_stream)).await {
                                if let Some(Ok(old)) = resp.message().await {
                                    // Remove from indexes
                                    if let Err(e) = index_manager.remove_from_indexes(&msg.key, &old.value) {
                                        let _ = tx.send(Err(Status::internal(format!("Index removal failed: {}", e)))).await;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Forward the message
                        if tx.send(Ok(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        
        let recreated_stream = tonic::Streaming::new(tokio_stream::wrappers::ReceiverStream::new(rx));
        self.backend.delete(recreated_stream).await
    }

    async fn eq(&self, request: StreamingRequest<EqRequest>) -> RpcResponse<bool> {
        self.backend.eq(request).await
    }

    async fn not_eq(&self, request: StreamingRequest<NotEqRequest>) -> RpcResponse<bool> {
        self.backend.not_eq(request).await
    }
    
    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        if let Some(transaction_manager) = &self.transaction_manager {
            let req = request.into_inner();
            let transaction_id = transaction_manager
                .begin_transaction(
                    &self.backend,
                    req.read_only.unwrap_or(false),
                    req.timeout_ms,
                )
                .map_err(|e| Status::internal(format!("Failed to begin transaction: {:?}", e)))?;
            
            Ok(Response::new(BeginTransactionResponse { transaction_id }))
        } else {
            Err(Status::unimplemented("Transactions are not enabled for this store"))
        }
    }
    
    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionResponse>, Status> {
        if let Some(transaction_manager) = &self.transaction_manager {
            let req = request.into_inner();
            match transaction_manager.commit_transaction(&req.transaction_id) {
                Ok(()) => Ok(Response::new(CommitTransactionResponse {
                    success: true,
                    error_message: None,
                })),
                Err(e) => Ok(Response::new(CommitTransactionResponse {
                    success: false,
                    error_message: Some(e),
                })),
            }
        } else {
            Err(Status::unimplemented("Transactions are not enabled for this store"))
        }
    }
    
    async fn rollback_transaction(
        &self,
        request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionResponse>, Status> {
        if let Some(transaction_manager) = &self.transaction_manager {
            let req = request.into_inner();
            match transaction_manager.rollback_transaction(&req.transaction_id) {
                Ok(()) => Ok(Response::new(RollbackTransactionResponse {
                    success: true,
                    error_message: None,
                })),
                Err(e) => Ok(Response::new(RollbackTransactionResponse {
                    success: false,
                    error_message: Some(e),
                })),
            }
        } else {
            Err(Status::unimplemented("Transactions are not enabled for this store"))
        }
    }
}
