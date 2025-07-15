//! Multi-Version Concurrency Control (MVCC) implementation
//!
//! This module provides MVCC support for BuffDB, enabling multiple concurrent
//! transactions to operate without blocking each other for reads.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};

/// MVCC errors
#[derive(Debug, thiserror::Error)]
pub enum MvccError {
    #[error("Write conflict: key {key} was modified by another transaction")]
    WriteConflict { key: String },
    
    #[error("Transaction {id} not found")]
    TransactionNotFound { id: String },
    
    #[error("Transaction {id} already committed or aborted")]
    TransactionInactive { id: String },
    
    #[error("Snapshot too old: requested version {version} has been garbage collected")]
    SnapshotTooOld { version: u64 },
    
    #[error("Deadlock detected")]
    DeadlockDetected,
}

/// Version number type
pub type Version = u64;

/// Transaction ID type
pub type TxId = String;

/// Value version information
#[derive(Debug, Clone)]
pub struct VersionedValue {
    /// The actual value
    pub value: Vec<u8>,
    /// Version when this value was created
    pub version: Version,
    /// Transaction that created this version
    pub tx_id: TxId,
    /// Timestamp when created
    pub timestamp: DateTime<Utc>,
    /// Whether this version has been deleted
    pub deleted: bool,
}

/// Transaction isolation level
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    /// Read uncommitted - can see uncommitted changes
    ReadUncommitted,
    /// Read committed - only sees committed changes
    ReadCommitted,
    /// Repeatable read - sees a consistent snapshot
    RepeatableRead,
    /// Serializable - full serializability
    Serializable,
}

/// Transaction state
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Transaction metadata
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub id: TxId,
    pub start_version: Version,
    pub commit_version: Option<Version>,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
    pub read_only: bool,
    pub start_time: Instant,
    pub commit_time: Option<Instant>,
    /// Keys read by this transaction (for conflict detection)
    pub read_set: HashSet<String>,
    /// Keys written by this transaction
    pub write_set: HashSet<String>,
}

/// MVCC manager
pub struct MvccManager {
    /// Current version counter
    current_version: Arc<RwLock<Version>>,
    /// All versions of all keys
    versions: Arc<RwLock<HashMap<String, BTreeMap<Version, VersionedValue>>>>,
    /// Active transactions
    transactions: Arc<RwLock<HashMap<TxId, TransactionInfo>>>,
    /// Committed transaction versions (for garbage collection)
    committed_versions: Arc<RwLock<BTreeMap<Version, TxId>>>,
    /// Minimum active version (for garbage collection)
    min_active_version: Arc<RwLock<Version>>,
    /// Lock table for pessimistic locking
    lock_table: Arc<RwLock<HashMap<String, TxId>>>,
}

impl MvccManager {
    /// Create a new MVCC manager
    pub fn new() -> Self {
        Self {
            current_version: Arc::new(RwLock::new(1)),
            versions: Arc::new(RwLock::new(HashMap::new())),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            committed_versions: Arc::new(RwLock::new(BTreeMap::new())),
            min_active_version: Arc::new(RwLock::new(1)),
            lock_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Begin a new transaction
    pub fn begin_transaction(
        &self,
        tx_id: TxId,
        isolation_level: IsolationLevel,
        read_only: bool,
    ) -> Result<Version, MvccError> {
        let mut current_version = self.current_version.write().unwrap();
        let start_version = *current_version;
        
        if !read_only {
            *current_version += 1;
        }
        
        let tx_info = TransactionInfo {
            id: tx_id.clone(),
            start_version,
            commit_version: None,
            state: TransactionState::Active,
            isolation_level,
            read_only,
            start_time: Instant::now(),
            commit_time: None,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
        };
        
        self.transactions.write().unwrap().insert(tx_id, tx_info);
        self.update_min_active_version();
        
        Ok(start_version)
    }
    
    /// Read a value at a specific version
    pub fn read(
        &self,
        tx_id: &TxId,
        key: &str,
    ) -> Result<Option<Vec<u8>>, MvccError> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions.get_mut(tx_id)
            .ok_or_else(|| MvccError::TransactionNotFound { id: tx_id.clone() })?;
        
        if tx_info.state != TransactionState::Active {
            return Err(MvccError::TransactionInactive { id: tx_id.clone() });
        }
        
        // Record read in read set
        tx_info.read_set.insert(key.to_string());
        
        let read_version = match tx_info.isolation_level {
            IsolationLevel::ReadUncommitted => {
                // Can read latest version including uncommitted
                self.get_latest_version()
            }
            IsolationLevel::ReadCommitted => {
                // Read latest committed version
                self.get_latest_committed_version()
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Read from transaction's snapshot
                tx_info.start_version
            }
        };
        
        // Get the value at the appropriate version
        let versions = self.versions.read().unwrap();
        if let Some(key_versions) = versions.get(key) {
            // Find the latest version <= read_version
            for (version, versioned_value) in key_versions.iter().rev() {
                if *version <= read_version {
                    if versioned_value.deleted {
                        return Ok(None);
                    }
                    return Ok(Some(versioned_value.value.clone()));
                }
            }
        }
        
        Ok(None)
    }
    
    /// Write a value
    pub fn write(
        &self,
        tx_id: &TxId,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), MvccError> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions.get_mut(tx_id)
            .ok_or_else(|| MvccError::TransactionNotFound { id: tx_id.clone() })?;
        
        if tx_info.state != TransactionState::Active {
            return Err(MvccError::TransactionInactive { id: tx_id.clone() });
        }
        
        if tx_info.read_only {
            return Err(MvccError::TransactionInactive { id: tx_id.clone() });
        }
        
        // Check for write conflicts in serializable isolation
        if tx_info.isolation_level == IsolationLevel::Serializable {
            self.check_write_conflict(&key, tx_info.start_version)?;
        }
        
        // Acquire lock for pessimistic locking
        let mut lock_table = self.lock_table.write().unwrap();
        if let Some(lock_holder) = lock_table.get(&key) {
            if lock_holder != tx_id {
                return Err(MvccError::WriteConflict { key: key.clone() });
            }
        } else {
            lock_table.insert(key.clone(), tx_id.clone());
        }
        
        // Record write
        tx_info.write_set.insert(key.clone());
        
        // Create new version (will be visible after commit)
        let versioned_value = VersionedValue {
            value,
            version: tx_info.start_version,
            tx_id: tx_id.clone(),
            timestamp: Utc::now(),
            deleted: false,
        };
        
        let mut versions = self.versions.write().unwrap();
        versions.entry(key)
            .or_insert_with(BTreeMap::new)
            .insert(tx_info.start_version, versioned_value);
        
        Ok(())
    }
    
    /// Delete a value
    pub fn delete(
        &self,
        tx_id: &TxId,
        key: String,
    ) -> Result<(), MvccError> {
        // Delete is implemented as a write with deleted flag
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions.get_mut(tx_id)
            .ok_or_else(|| MvccError::TransactionNotFound { id: tx_id.clone() })?;
        
        if tx_info.state != TransactionState::Active {
            return Err(MvccError::TransactionInactive { id: tx_id.clone() });
        }
        
        // Record write
        tx_info.write_set.insert(key.clone());
        
        // Create tombstone version
        let versioned_value = VersionedValue {
            value: Vec::new(),
            version: tx_info.start_version,
            tx_id: tx_id.clone(),
            timestamp: Utc::now(),
            deleted: true,
        };
        
        let mut versions = self.versions.write().unwrap();
        versions.entry(key)
            .or_insert_with(BTreeMap::new)
            .insert(tx_info.start_version, versioned_value);
        
        Ok(())
    }
    
    /// Commit a transaction
    pub fn commit_transaction(&self, tx_id: &TxId) -> Result<Version, MvccError> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions.get_mut(tx_id)
            .ok_or_else(|| MvccError::TransactionNotFound { id: tx_id.clone() })?;
        
        if tx_info.state != TransactionState::Active {
            return Err(MvccError::TransactionInactive { id: tx_id.clone() });
        }
        
        // Validate transaction (check for conflicts)
        if tx_info.isolation_level == IsolationLevel::Serializable {
            self.validate_transaction(tx_info)?;
        }
        
        // Get commit version
        let commit_version = if tx_info.read_only {
            tx_info.start_version
        } else {
            let mut current = self.current_version.write().unwrap();
            let v = *current;
            *current += 1;
            v
        };
        
        // Update transaction state
        tx_info.commit_version = Some(commit_version);
        tx_info.state = TransactionState::Committed;
        tx_info.commit_time = Some(Instant::now());
        
        // Record committed version
        self.committed_versions.write().unwrap()
            .insert(commit_version, tx_id.clone());
        
        // Release locks
        let mut lock_table = self.lock_table.write().unwrap();
        for key in &tx_info.write_set {
            lock_table.remove(key);
        }
        
        // Update min active version
        self.update_min_active_version();
        
        Ok(commit_version)
    }
    
    /// Abort a transaction
    pub fn abort_transaction(&self, tx_id: &TxId) -> Result<(), MvccError> {
        let mut transactions = self.transactions.write().unwrap();
        let tx_info = transactions.get_mut(tx_id)
            .ok_or_else(|| MvccError::TransactionNotFound { id: tx_id.clone() })?;
        
        if tx_info.state != TransactionState::Active {
            return Err(MvccError::TransactionInactive { id: tx_id.clone() });
        }
        
        // Update state
        tx_info.state = TransactionState::Aborted;
        
        // Remove any versions created by this transaction
        let mut versions = self.versions.write().unwrap();
        for key in &tx_info.write_set {
            if let Some(key_versions) = versions.get_mut(key) {
                key_versions.retain(|_, v| v.tx_id != *tx_id);
            }
        }
        
        // Release locks
        let mut lock_table = self.lock_table.write().unwrap();
        for key in &tx_info.write_set {
            lock_table.remove(key);
        }
        
        Ok(())
    }
    
    /// Garbage collect old versions
    pub fn garbage_collect(&self, keep_duration: Duration) -> usize {
        let min_version = *self.min_active_version.read().unwrap();
        let cutoff_time = Instant::now() - keep_duration;
        
        let mut versions = self.versions.write().unwrap();
        let mut removed_count = 0;
        
        for key_versions in versions.values_mut() {
            let before_count = key_versions.len();
            
            // Keep at least one version per key
            if key_versions.len() > 1 {
                key_versions.retain(|version, versioned_value| {
                    // Keep if:
                    // 1. Version is >= min active version
                    // 2. Version is recent (within keep_duration)
                    // 3. It's the latest version for the key
                    *version >= min_version || 
                    versioned_value.timestamp.timestamp() as u64 > cutoff_time.elapsed().as_secs()
                });
            }
            
            removed_count += before_count - key_versions.len();
        }
        
        // Clean up empty keys
        versions.retain(|_, v| !v.is_empty());
        
        // Clean up old committed versions
        self.committed_versions.write().unwrap()
            .retain(|v, _| *v >= min_version);
        
        removed_count
    }
    
    fn get_latest_version(&self) -> Version {
        *self.current_version.read().unwrap()
    }
    
    fn get_latest_committed_version(&self) -> Version {
        self.committed_versions.read().unwrap()
            .keys()
            .next_back()
            .copied()
            .unwrap_or(1)
    }
    
    fn check_write_conflict(&self, key: &str, start_version: Version) -> Result<(), MvccError> {
        let versions = self.versions.read().unwrap();
        if let Some(key_versions) = versions.get(key) {
            // Check if any version was created after our start version
            for (version, _) in key_versions.iter() {
                if *version > start_version {
                    return Err(MvccError::WriteConflict { key: key.to_string() });
                }
            }
        }
        Ok(())
    }
    
    fn validate_transaction(&self, tx_info: &TransactionInfo) -> Result<(), MvccError> {
        // Validate read-write conflicts
        for key in &tx_info.read_set {
            self.check_write_conflict(key, tx_info.start_version)?;
        }
        
        // Additional validation for serializable isolation
        // In a full implementation, this would include:
        // - Checking for write-write conflicts
        // - Detecting serialization anomalies
        // - Ensuring predicate reads are still valid
        
        Ok(())
    }
    
    fn update_min_active_version(&self) {
        let transactions = self.transactions.read().unwrap();
        let min_version = transactions.values()
            .filter(|tx| tx.state == TransactionState::Active)
            .map(|tx| tx.start_version)
            .min()
            .unwrap_or_else(|| self.get_latest_version());
        
        *self.min_active_version.write().unwrap() = min_version;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_mvcc_operations() {
        let mvcc = MvccManager::new();
        
        // Start transaction 1
        let tx1_id = "tx1".to_string();
        let v1 = mvcc.begin_transaction(tx1_id.clone(), IsolationLevel::ReadCommitted, false).unwrap();
        
        // Write value
        mvcc.write(&tx1_id, "key1".to_string(), b"value1".to_vec()).unwrap();
        
        // Start transaction 2
        let tx2_id = "tx2".to_string();
        mvcc.begin_transaction(tx2_id.clone(), IsolationLevel::ReadCommitted, false).unwrap();
        
        // Tx2 shouldn't see uncommitted changes
        let val = mvcc.read(&tx2_id, "key1").unwrap();
        assert!(val.is_none());
        
        // Commit tx1
        mvcc.commit_transaction(&tx1_id).unwrap();
        
        // Now tx2 should see the value
        let val = mvcc.read(&tx2_id, "key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }
    
    #[test]
    fn test_repeatable_read() {
        let mvcc = MvccManager::new();
        
        // Write initial value
        let tx0 = "tx0".to_string();
        mvcc.begin_transaction(tx0.clone(), IsolationLevel::ReadCommitted, false).unwrap();
        mvcc.write(&tx0, "key1".to_string(), b"value0".to_vec()).unwrap();
        mvcc.commit_transaction(&tx0).unwrap();
        
        // Start repeatable read transaction
        let tx1 = "tx1".to_string();
        mvcc.begin_transaction(tx1.clone(), IsolationLevel::RepeatableRead, false).unwrap();
        
        // Read value
        let val1 = mvcc.read(&tx1, "key1").unwrap();
        assert_eq!(val1, Some(b"value0".to_vec()));
        
        // Another transaction modifies the value
        let tx2 = "tx2".to_string();
        mvcc.begin_transaction(tx2.clone(), IsolationLevel::ReadCommitted, false).unwrap();
        mvcc.write(&tx2, "key1".to_string(), b"value2".to_vec()).unwrap();
        mvcc.commit_transaction(&tx2).unwrap();
        
        // Tx1 should still see the old value (repeatable read)
        let val1_again = mvcc.read(&tx1, "key1").unwrap();
        assert_eq!(val1_again, Some(b"value0".to_vec()));
    }
    
    #[test]
    fn test_write_conflict() {
        let mvcc = MvccManager::new();
        
        // Two transactions try to write the same key
        let tx1 = "tx1".to_string();
        let tx2 = "tx2".to_string();
        
        mvcc.begin_transaction(tx1.clone(), IsolationLevel::Serializable, false).unwrap();
        mvcc.begin_transaction(tx2.clone(), IsolationLevel::Serializable, false).unwrap();
        
        // Both read the key (it doesn't exist yet)
        mvcc.read(&tx1, "key1").unwrap();
        mvcc.read(&tx2, "key1").unwrap();
        
        // Tx1 writes
        mvcc.write(&tx1, "key1".to_string(), b"value1".to_vec()).unwrap();
        mvcc.commit_transaction(&tx1).unwrap();
        
        // Tx2 tries to write - should fail due to conflict
        let result = mvcc.write(&tx2, "key1".to_string(), b"value2".to_vec());
        assert!(matches!(result, Err(MvccError::WriteConflict { .. })));
    }
}