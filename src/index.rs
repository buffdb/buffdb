//! Secondary indexing support for BuffDB
//!
//! This module provides secondary indexing capabilities for the key-value store,
//! allowing efficient queries on value content in addition to key lookups.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Types of indexes supported
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    /// Hash index for exact matches
    Hash,
    /// B-tree index for range queries
    BTree,
    /// Full-text search index
    FullText,
    /// Composite index on multiple fields
    Composite(Vec<String>),
}

/// Index configuration
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Name of the index
    pub name: String,
    /// Type of index
    pub index_type: IndexType,
    /// Whether the index enforces uniqueness
    pub unique: bool,
    /// Optional filter expression (for partial indexes)
    pub filter: Option<String>,
}

/// Trait for values that can be indexed
pub trait Indexable {
    /// Extract the indexable value
    fn index_value(&self) -> IndexValue;
}

/// Value types that can be indexed
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IndexValue {
    String(String),
    Integer(i64),
    Float(ordered_float::OrderedFloat<f64>),
    Boolean(bool),
    Null,
}

impl From<String> for IndexValue {
    fn from(s: String) -> Self {
        IndexValue::String(s)
    }
}

impl From<&str> for IndexValue {
    fn from(s: &str) -> Self {
        IndexValue::String(s.to_string())
    }
}

impl From<i64> for IndexValue {
    fn from(i: i64) -> Self {
        IndexValue::Integer(i)
    }
}

impl From<f64> for IndexValue {
    fn from(f: f64) -> Self {
        IndexValue::Float(ordered_float::OrderedFloat(f))
    }
}

impl From<bool> for IndexValue {
    fn from(b: bool) -> Self {
        IndexValue::Boolean(b)
    }
}

/// Secondary index structure
#[derive(Debug)]
pub struct SecondaryIndex {
    config: IndexConfig,
    // For hash indexes: value -> set of keys
    hash_index: Arc<RwLock<HashMap<IndexValue, HashSet<String>>>>,
    // For btree indexes: sorted map of value -> set of keys
    btree_index: Arc<RwLock<BTreeMap<IndexValue, HashSet<String>>>>,
}

impl SecondaryIndex {
    /// Create a new secondary index
    pub fn new(config: IndexConfig) -> Self {
        Self {
            config,
            hash_index: Arc::new(RwLock::new(HashMap::new())),
            btree_index: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Add an entry to the index
    pub fn insert(&self, key: &str, value: IndexValue) -> Result<(), IndexError> {
        match self.config.index_type {
            IndexType::Hash => {
                let mut index = self.hash_index.write().unwrap();

                if self.config.unique {
                    if let Some(existing_keys) = index.get(&value) {
                        if !existing_keys.is_empty() && !existing_keys.contains(&key.to_string()) {
                            return Err(IndexError::UniqueConstraintViolation {
                                index: self.config.name.clone(),
                                value: format!("{:?}", value),
                            });
                        }
                    }
                }

                index
                    .entry(value)
                    .or_insert_with(HashSet::new)
                    .insert(key.to_string());
            }
            IndexType::BTree => {
                let mut index = self.btree_index.write().unwrap();

                if self.config.unique {
                    if let Some(existing_keys) = index.get(&value) {
                        if !existing_keys.is_empty() && !existing_keys.contains(&key.to_string()) {
                            return Err(IndexError::UniqueConstraintViolation {
                                index: self.config.name.clone(),
                                value: format!("{:?}", value),
                            });
                        }
                    }
                }

                index
                    .entry(value)
                    .or_insert_with(HashSet::new)
                    .insert(key.to_string());
            }
            _ => {
                // TODO: Implement other index types
                return Err(IndexError::UnsupportedIndexType);
            }
        }

        Ok(())
    }

    /// Remove an entry from the index
    pub fn remove(&self, key: &str, value: &IndexValue) -> Result<(), IndexError> {
        match self.config.index_type {
            IndexType::Hash => {
                let mut index = self.hash_index.write().unwrap();
                if let Some(keys) = index.get_mut(value) {
                    keys.remove(key);
                    if keys.is_empty() {
                        index.remove(value);
                    }
                }
            }
            IndexType::BTree => {
                let mut index = self.btree_index.write().unwrap();
                if let Some(keys) = index.get_mut(value) {
                    keys.remove(key);
                    if keys.is_empty() {
                        index.remove(value);
                    }
                }
            }
            _ => return Err(IndexError::UnsupportedIndexType),
        }

        Ok(())
    }

    /// Find all keys with the given value
    pub fn find_exact(&self, value: &IndexValue) -> Result<HashSet<String>, IndexError> {
        match self.config.index_type {
            IndexType::Hash => {
                let index = self.hash_index.read().unwrap();
                Ok(index.get(value).cloned().unwrap_or_default())
            }
            IndexType::BTree => {
                let index = self.btree_index.read().unwrap();
                Ok(index.get(value).cloned().unwrap_or_default())
            }
            _ => Err(IndexError::UnsupportedIndexType),
        }
    }

    /// Find all keys with values in the given range (inclusive)
    pub fn find_range(
        &self,
        start: &IndexValue,
        end: &IndexValue,
    ) -> Result<HashSet<String>, IndexError> {
        match self.config.index_type {
            IndexType::BTree => {
                let index = self.btree_index.read().unwrap();
                let mut result = HashSet::new();

                for (_, keys) in index.range(start.clone()..=end.clone()) {
                    result.extend(keys.iter().cloned());
                }

                Ok(result)
            }
            _ => Err(IndexError::OperationNotSupported {
                operation: "range query".to_string(),
                index_type: format!("{:?}", self.config.index_type),
            }),
        }
    }
}

/// Index manager that maintains all indexes for a store
#[derive(Debug)]
pub struct IndexManager {
    indexes: Arc<RwLock<HashMap<String, SecondaryIndex>>>,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new index
    pub fn create_index(&self, config: IndexConfig) -> Result<(), IndexError> {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.contains_key(&config.name) {
            return Err(IndexError::IndexAlreadyExists {
                name: config.name.clone(),
            });
        }

        indexes.insert(config.name.clone(), SecondaryIndex::new(config));
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> Result<(), IndexError> {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.remove(name).is_none() {
            return Err(IndexError::IndexNotFound {
                name: name.to_string(),
            });
        }

        Ok(())
    }

    /// Get an index by name
    pub fn get_index(&self, name: &str) -> Option<SecondaryIndex> {
        let indexes = self.indexes.read().unwrap();
        indexes.get(name).map(|idx| SecondaryIndex {
            config: idx.config.clone(),
            hash_index: Arc::clone(&idx.hash_index),
            btree_index: Arc::clone(&idx.btree_index),
        })
    }

    /// Update all indexes when a key-value pair is inserted or updated
    pub fn update_indexes(
        &self,
        key: &str,
        old_value: Option<&str>,
        new_value: &str,
    ) -> Result<(), IndexError> {
        let indexes = self.indexes.read().unwrap();

        for (_, index) in indexes.iter() {
            // Remove old value from index if it exists
            if let Some(old) = old_value {
                let old_index_value = IndexValue::String(old.to_string());
                index.remove(key, &old_index_value)?;
            }

            // Add new value to index
            let new_index_value = IndexValue::String(new_value.to_string());
            index.insert(key, new_index_value)?;
        }

        Ok(())
    }

    /// Remove a key from all indexes
    pub fn remove_from_indexes(&self, key: &str, value: &str) -> Result<(), IndexError> {
        let indexes = self.indexes.read().unwrap();

        for (_, index) in indexes.iter() {
            let index_value = IndexValue::String(value.to_string());
            index.remove(key, &index_value)?;
        }

        Ok(())
    }
}

/// Errors that can occur during index operations
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("Index '{name}' already exists")]
    IndexAlreadyExists { name: String },

    #[error("Index '{name}' not found")]
    IndexNotFound { name: String },

    #[error("Unique constraint violation on index '{index}' for value '{value}'")]
    UniqueConstraintViolation { index: String, value: String },

    #[error("Unsupported index type")]
    UnsupportedIndexType,

    #[error("Operation '{operation}' not supported for index type '{index_type}'")]
    OperationNotSupported {
        operation: String,
        index_type: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_index_basic() {
        let config = IndexConfig {
            name: "test_index".to_string(),
            index_type: IndexType::Hash,
            unique: false,
            filter: None,
        };

        let index = SecondaryIndex::new(config);

        // Insert some values
        index
            .insert("key1", IndexValue::String("value1".to_string()))
            .unwrap();
        index
            .insert("key2", IndexValue::String("value1".to_string()))
            .unwrap();
        index
            .insert("key3", IndexValue::String("value2".to_string()))
            .unwrap();

        // Find by exact value
        let keys = index
            .find_exact(&IndexValue::String("value1".to_string()))
            .unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains("key1"));
        assert!(keys.contains("key2"));

        // Remove a key
        index
            .remove("key1", &IndexValue::String("value1".to_string()))
            .unwrap();
        let keys = index
            .find_exact(&IndexValue::String("value1".to_string()))
            .unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains("key2"));
    }

    #[test]
    fn test_btree_index_range_queries() {
        let config = IndexConfig {
            name: "test_btree".to_string(),
            index_type: IndexType::BTree,
            unique: false,
            filter: None,
        };

        let index = SecondaryIndex::new(config);

        // Insert numeric values
        index.insert("key1", IndexValue::Integer(10)).unwrap();
        index.insert("key2", IndexValue::Integer(20)).unwrap();
        index.insert("key3", IndexValue::Integer(30)).unwrap();
        index.insert("key4", IndexValue::Integer(40)).unwrap();

        // Range query
        let keys = index
            .find_range(&IndexValue::Integer(15), &IndexValue::Integer(35))
            .unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains("key2"));
        assert!(keys.contains("key3"));
    }

    #[test]
    fn test_unique_constraint() {
        let config = IndexConfig {
            name: "unique_index".to_string(),
            index_type: IndexType::Hash,
            unique: true,
            filter: None,
        };

        let index = SecondaryIndex::new(config);

        // Insert first value
        index
            .insert("key1", IndexValue::String("unique_value".to_string()))
            .unwrap();

        // Try to insert duplicate value with different key
        let result = index.insert("key2", IndexValue::String("unique_value".to_string()));
        assert!(result.is_err());

        // Same key should be allowed
        index
            .insert("key1", IndexValue::String("unique_value".to_string()))
            .unwrap();
    }
}
