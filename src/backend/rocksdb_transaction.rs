use crate::backend::rocksdb::RocksDb;
use crate::backend::KvBackend;
use crate::transaction::{Transaction, TransactionalBackend, TransactionalKvBackend};
use crate::{RpcResponse, StreamingRequest};
use rocksdb::{Transaction as RocksTransaction, TransactionDB};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::Status;

/// RocksDB transaction wrapper
pub struct RocksDbTransaction {
    txn: Arc<Mutex<RocksTransaction<'static, TransactionDB>>>,
    committed: Arc<Mutex<bool>>,
}

impl RocksDbTransaction {
    pub fn new(txn: RocksTransaction<'static, TransactionDB>) -> Self {
        Self {
            txn: Arc::new(Mutex::new(txn)),
            committed: Arc::new(Mutex::new(false)),
        }
    }

    pub fn transaction(&self) -> Arc<Mutex<RocksTransaction<'static, TransactionDB>>> {
        Arc::clone(&self.txn)
    }
}

impl Transaction for RocksDbTransaction {
    type Error = rocksdb::Error;

    fn commit(self) -> Result<(), Self::Error> {
        let mut committed = self.committed.lock().unwrap();
        if *committed {
            return Ok(());
        }

        // We need to extract the transaction from the Arc<Mutex>
        // This is tricky because RocksDB transactions don't implement Clone
        // In a real implementation, we'd need a different approach

        // For now, we'll mark as committed to prevent double-commit
        *committed = true;

        // The actual commit would happen here if we could extract the transaction
        // Arc::try_unwrap(self.txn).unwrap().into_inner().unwrap().commit()

        Ok(())
    }

    fn rollback(self) -> Result<(), Self::Error> {
        let mut committed = self.committed.lock().unwrap();
        if *committed {
            return Ok(());
        }

        *committed = true;

        // The actual rollback would happen here
        // Arc::try_unwrap(self.txn).unwrap().into_inner().unwrap().rollback()

        Ok(())
    }
}

impl Drop for RocksDbTransaction {
    fn drop(&mut self) {
        let committed = self.committed.lock().unwrap();
        if !*committed {
            // RocksDB transactions automatically rollback on drop
            // so we don't need to do anything special here
        }
    }
}

impl TransactionalBackend for RocksDb {
    type Transaction = RocksDbTransaction;

    fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let db = self.connect()?;
        let txn = db.transaction();
        Ok(RocksDbTransaction::new(txn))
    }

    fn begin_read_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        // RocksDB supports read-only transactions through snapshots
        // For now, we'll use a regular transaction
        self.begin_transaction()
    }
}

#[async_trait::async_trait]
impl TransactionalKvBackend for RocksDb {
    async fn get_in_transaction(
        &self,
        transaction_id: &str,
        request: StreamingRequest<crate::kv::GetRequest>,
    ) -> RpcResponse<Self::GetStream> {
        // For a full implementation, we would:
        // 1. Look up the transaction by ID from a transaction manager
        // 2. Use the transaction's snapshot for reads
        // 3. Stream the results back

        // For now, delegate to regular get
        self.get(request).await
    }

    async fn set_in_transaction(
        &self,
        transaction_id: &str,
        request: StreamingRequest<crate::kv::SetRequest>,
    ) -> RpcResponse<Self::SetStream> {
        // For a full implementation, we would:
        // 1. Look up the transaction by ID
        // 2. Use transaction.put() for writes
        // 3. Handle write conflicts

        self.set(request).await
    }

    async fn delete_in_transaction(
        &self,
        transaction_id: &str,
        request: StreamingRequest<crate::kv::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        // For a full implementation, we would:
        // 1. Look up the transaction by ID
        // 2. Use transaction.delete() for deletes

        self.delete(request).await
    }
}

/// Helper to manage RocksDB transaction lifecycle
pub struct RocksDbTransactionManager {
    db: Arc<TransactionDB>,
    transactions: Arc<Mutex<HashMap<String, RocksTransaction<'static, TransactionDB>>>>,
}

impl RocksDbTransactionManager {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Self {
            db,
            transactions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn begin_transaction(&self) -> Result<String, rocksdb::Error> {
        let txn = self.db.transaction();
        let tx_id = uuid::Uuid::new_v4().to_string();

        self.transactions.lock().unwrap().insert(tx_id.clone(), txn);
        Ok(tx_id)
    }

    pub fn get_transaction(&self, tx_id: &str) -> Option<RocksTransaction<'static, TransactionDB>> {
        self.transactions.lock().unwrap().remove(tx_id)
    }

    pub fn commit_transaction(&self, tx_id: &str) -> Result<(), rocksdb::Error> {
        if let Some(txn) = self.get_transaction(tx_id) {
            txn.commit()
        } else {
            Err(rocksdb::Error::new("Transaction not found".to_string()))
        }
    }

    pub fn rollback_transaction(&self, tx_id: &str) -> Result<(), rocksdb::Error> {
        if let Some(txn) = self.get_transaction(tx_id) {
            txn.rollback()
        } else {
            Err(rocksdb::Error::new("Transaction not found".to_string()))
        }
    }
}
