use crate::backend::duckdb::DuckDb;
use crate::backend::KvBackend;
use crate::transaction::{Transaction, TransactionalBackend, TransactionalKvBackend};
use crate::{RpcResponse, StreamingRequest};
use duckdb::Connection;
use std::sync::{Arc, Mutex};
use tonic::Status;

/// DuckDB transaction wrapper
pub struct DuckDbTransaction {
    conn: Arc<Mutex<Connection>>,
    committed: Arc<Mutex<bool>>,
}

impl DuckDbTransaction {
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

impl Transaction for DuckDbTransaction {
    type Error = duckdb::Error;

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

impl Drop for DuckDbTransaction {
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

impl TransactionalBackend for DuckDb {
    type Transaction = DuckDbTransaction;

    fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let mut conn = self.connect()?;
        conn.execute("BEGIN TRANSACTION", [])?;
        Ok(DuckDbTransaction::new(conn))
    }

    fn begin_read_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        // DuckDB doesn't distinguish between read and write transactions
        // at the BEGIN level, so we use the same implementation
        self.begin_transaction()
    }
}

#[async_trait::async_trait]
impl TransactionalKvBackend for DuckDb {
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
