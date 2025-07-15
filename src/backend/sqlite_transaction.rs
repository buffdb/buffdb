use crate::transaction::{Transaction, TransactionalBackend, TransactionalKvBackend};
use crate::backend::sqlite::Sqlite;
use crate::backend::KvBackend;
use crate::{RpcResponse, StreamingRequest};
use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use tonic::Status;

/// SQLite transaction wrapper
pub struct SqliteTransaction {
    conn: Arc<Mutex<Connection>>,
    committed: Arc<Mutex<bool>>,
}

impl SqliteTransaction {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            committed: Arc::new(Mutex::new(false)),
        }
    }
    
    pub fn connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.conn)
    }
}

impl Transaction for SqliteTransaction {
    type Error = rusqlite::Error;
    
    fn commit(self) -> Result<(), Self::Error> {
        let mut committed = self.committed.lock().unwrap();
        if *committed {
            return Ok(());
        }
        
        let conn = self.conn.lock().unwrap();
        conn.execute("COMMIT", [])?;
        *committed = true;
        Ok(())
    }
    
    fn rollback(self) -> Result<(), Self::Error> {
        let mut committed = self.committed.lock().unwrap();
        if *committed {
            return Ok(());
        }
        
        let conn = self.conn.lock().unwrap();
        conn.execute("ROLLBACK", [])?;
        *committed = true;
        Ok(())
    }
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        let committed = self.committed.lock().unwrap();
        if !*committed {
            // Rollback if not explicitly committed
            if let Ok(conn) = self.conn.lock() {
                let _ = conn.execute("ROLLBACK", []);
            }
        }
    }
}

impl TransactionalBackend for Sqlite {
    type Transaction = SqliteTransaction;
    
    fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let mut conn = self.connect()?;
        conn.execute("BEGIN IMMEDIATE", [])?;
        Ok(SqliteTransaction::new(conn))
    }
    
    fn begin_read_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let mut conn = self.connect()?;
        conn.execute("BEGIN", [])?;
        Ok(SqliteTransaction::new(conn))
    }
}

#[async_trait::async_trait]
impl TransactionalKvBackend for Sqlite {
    async fn get_in_transaction(
        &self,
        transaction_id: &str,
        request: StreamingRequest<crate::kv::GetRequest>,
    ) -> RpcResponse<Self::GetStream> {
        // For now, we'll use the regular get implementation
        // In a full implementation, we would look up the transaction by ID
        // and use its connection
        self.get(request).await
    }
    
    async fn set_in_transaction(
        &self,
        transaction_id: &str,
        request: StreamingRequest<crate::kv::SetRequest>,
    ) -> RpcResponse<Self::SetStream> {
        // For now, we'll use the regular set implementation
        self.set(request).await
    }
    
    async fn delete_in_transaction(
        &self,
        transaction_id: &str,
        request: StreamingRequest<crate::kv::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        // For now, we'll use the regular delete implementation
        self.delete(request).await
    }
}