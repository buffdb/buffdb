use crate::backend::sqlite::Sqlite;
use crate::backend::KvBackend;
use crate::transaction::{
    Transaction, TransactionError, TransactionalBackend, TransactionalKvBackend,
};
use crate::RpcResponse;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// SQLite transaction wrapper
#[derive(Debug)]
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
    // type Error = rusqlite::Error;

    fn commit(self) -> Result<(), TransactionError> {
        let mut committed = self.committed.lock().map_err(|_| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some("Failed to acquire lock on committed flag".to_string()),
            )
        })?;
        if *committed {
            return Ok(());
        }

        let conn = self.conn.lock().map_err(|_| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some("Failed to acquire lock on connection".to_string()),
            )
        })?;
        let _ = conn.execute("COMMIT", [])?;
        *committed = true;
        drop(conn);
        drop(committed);
        Ok(())
    }

    fn rollback(self) -> Result<(), TransactionError> {
        let mut committed = self.committed.lock().map_err(|_| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some("Failed to acquire lock on committed flag".to_string()),
            )
        })?;
        if *committed {
            return Ok(());
        }

        let conn = self.conn.lock().map_err(|_| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some("Failed to acquire lock on connection".to_string()),
            )
        })?;
        let _ = conn.execute("ROLLBACK", [])?;
        *committed = true;
        drop(conn);
        drop(committed);
        Ok(())
    }
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if let Ok(committed) = self.committed.lock() {
            if !*committed {
                // Rollback if not explicitly committed
                if let Ok(conn) = self.conn.lock() {
                    drop(conn.execute("ROLLBACK", []));
                }
            }
        }
    }
}

impl TransactionalBackend for Sqlite {
    type Transaction = SqliteTransaction;

    fn begin_transaction(&self) -> Result<Self::Transaction, TransactionError> {
        let conn = self.connect_kv()?;
        let _ = conn.execute("BEGIN IMMEDIATE", [])?;
        Ok(SqliteTransaction::new(conn))
    }

    fn begin_read_transaction(&self) -> Result<Self::Transaction, TransactionError> {
        let conn = self.connect_kv()?;
        let _ = conn.execute("BEGIN", [])?;
        Ok(SqliteTransaction::new(conn))
    }
}

#[async_trait::async_trait]
impl TransactionalKvBackend for Sqlite {
    async fn get_in_transaction(
        &self,
        _transaction_id: &str,
        request: tonic::Streaming<crate::proto::kv::GetRequest>,
    ) -> RpcResponse<Self::GetStream> {
        // For now, we'll use the regular get implementation
        // In a full implementation, we would look up the transaction by ID
        // and use its connection
        self.get(tonic::Request::new(request)).await
    }

    async fn set_in_transaction(
        &self,
        _transaction_id: &str,
        request: tonic::Streaming<crate::proto::kv::SetRequest>,
    ) -> RpcResponse<Self::SetStream> {
        // For now, we'll use the regular set implementation
        self.set(tonic::Request::new(request)).await
    }

    async fn delete_in_transaction(
        &self,
        _transaction_id: &str,
        request: tonic::Streaming<crate::proto::kv::DeleteRequest>,
    ) -> RpcResponse<Self::DeleteStream> {
        // For now, we'll use the regular delete implementation
        self.delete(tonic::Request::new(request)).await
    }
}
