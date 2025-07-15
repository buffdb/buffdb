use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

/// Represents an active database transaction
pub trait Transaction: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    
    /// Commit the transaction, making all changes permanent
    fn commit(self) -> Result<(), Self::Error>;
    
    /// Rollback the transaction, discarding all changes
    fn rollback(self) -> Result<(), Self::Error>;
}

/// Extended trait for database backends that support transactions
pub trait TransactionalBackend: crate::backend::DatabaseBackend {
    type Transaction: Transaction<Error = Self::Error>;
    
    /// Begin a new transaction
    fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error>;
    
    /// Begin a new read-only transaction
    fn begin_read_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        // Default implementation just creates a regular transaction
        // Backends can override for optimization
        self.begin_transaction()
    }
}

/// Manages transaction lifecycle and state
pub struct TransactionManager<Backend: TransactionalBackend> {
    transactions: Arc<Mutex<HashMap<String, Backend::Transaction>>>,
    default_timeout: Duration,
}

impl<Backend: TransactionalBackend> TransactionManager<Backend> {
    /// Create a new transaction manager
    pub fn new(default_timeout: Duration) -> Self {
        Self {
            transactions: Arc::new(Mutex::new(HashMap::new())),
            default_timeout,
        }
    }
    
    /// Begin a new transaction
    pub fn begin_transaction(
        &self, 
        backend: &Backend,
        read_only: bool,
        timeout_ms: Option<i32>,
    ) -> Result<String, Backend::Error> {
        let transaction = if read_only {
            backend.begin_read_transaction()?
        } else {
            backend.begin_transaction()?
        };
        
        let transaction_id = Uuid::new_v4().to_string();
        
        self.transactions.lock().unwrap()
            .insert(transaction_id.clone(), transaction);
        
        // TODO: Implement timeout handling
        if let Some(_timeout) = timeout_ms {
            // Schedule transaction timeout
        }
        
        Ok(transaction_id)
    }
    
    /// Get a transaction by ID, removing it from the manager
    pub fn take_transaction(&self, transaction_id: &str) -> Option<Backend::Transaction> {
        self.transactions.lock().unwrap()
            .remove(transaction_id)
    }
    
    /// Commit a transaction
    pub fn commit_transaction(&self, transaction_id: &str) -> Result<(), String> {
        if let Some(transaction) = self.take_transaction(transaction_id) {
            transaction.commit()
                .map_err(|e| format!("Failed to commit transaction: {}", e))
        } else {
            Err("Transaction not found".to_string())
        }
    }
    
    /// Rollback a transaction
    pub fn rollback_transaction(&self, transaction_id: &str) -> Result<(), String> {
        if let Some(transaction) = self.take_transaction(transaction_id) {
            transaction.rollback()
                .map_err(|e| format!("Failed to rollback transaction: {}", e))
        } else {
            Err("Transaction not found".to_string())
        }
    }
    
    /// Check if a transaction exists
    pub fn has_transaction(&self, transaction_id: &str) -> bool {
        self.transactions.lock().unwrap()
            .contains_key(transaction_id)
    }
    
    /// Get the number of active transactions
    pub fn active_transaction_count(&self) -> usize {
        self.transactions.lock().unwrap().len()
    }
    
    /// Clean up expired transactions
    pub fn cleanup_expired(&self) {
        // TODO: Implement cleanup based on timeouts
    }
}

/// Extended KV backend trait with transaction support
#[async_trait::async_trait]
pub trait TransactionalKvBackend: crate::backend::KvBackend + TransactionalBackend {
    /// Get a value within a transaction
    async fn get_in_transaction(
        &self,
        transaction_id: &str,
        request: tonic::Streaming<crate::kv::GetRequest>,
    ) -> crate::RpcResponse<Self::GetStream>;
    
    /// Set a value within a transaction
    async fn set_in_transaction(
        &self,
        transaction_id: &str,
        request: tonic::Streaming<crate::kv::SetRequest>,
    ) -> crate::RpcResponse<Self::SetStream>;
    
    /// Delete a value within a transaction
    async fn delete_in_transaction(
        &self,
        transaction_id: &str,
        request: tonic::Streaming<crate::kv::DeleteRequest>,
    ) -> crate::RpcResponse<Self::DeleteStream>;
}

/// Extended Blob backend trait with transaction support
#[async_trait::async_trait]
pub trait TransactionalBlobBackend: crate::backend::BlobBackend + TransactionalBackend {
    /// Get a blob within a transaction
    async fn get_in_transaction(
        &self,
        transaction_id: &str,
        request: tonic::Streaming<crate::blob::GetRequest>,
    ) -> crate::RpcResponse<Self::GetStream>;
    
    /// Store a blob within a transaction
    async fn store_in_transaction(
        &self,
        transaction_id: &str,
        request: tonic::Streaming<crate::blob::StoreRequest>,
    ) -> crate::RpcResponse<Self::StoreStream>;
    
    /// Update a blob within a transaction
    async fn update_in_transaction(
        &self,
        transaction_id: &str,
        request: tonic::Streaming<crate::blob::UpdateRequest>,
    ) -> crate::RpcResponse<Self::UpdateStream>;
    
    /// Delete a blob within a transaction
    async fn delete_in_transaction(
        &self,
        transaction_id: &str,
        request: tonic::Streaming<crate::blob::DeleteRequest>,
    ) -> crate::RpcResponse<Self::DeleteStream>;
}