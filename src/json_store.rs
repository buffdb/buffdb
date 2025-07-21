//! JSON document store with JSONPath query support
//!
//! This module provides a document-oriented storage layer on top of BuffDB's
//! key-value store, with support for JSONPath queries and partial updates.

use crate::backend::{DatabaseBackend, KvBackend};
use crate::fts::{FtsManager, Tokenizer};
use crate::index::{IndexConfig, IndexManager, IndexType, IndexValue};
use crate::interop::IntoTonicStatus;
use crate::transaction::{TransactionManager, TransactionalBackend};
use crate::{Location, RpcResponse, StreamingRequest};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::Status;

/// JSON document store errors
#[derive(Debug, thiserror::Error)]
pub enum JsonStoreError {
    #[error("Document not found: {id}")]
    DocumentNotFound { id: String },

    #[error("Invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("Invalid JSONPath expression: {0}")]
    InvalidJsonPath(String),

    #[error("Type mismatch at path {path}: expected {expected}, got {actual}")]
    TypeMismatch {
        path: String,
        expected: String,
        actual: String,
    },

    #[error("Index error: {0}")]
    IndexError(#[from] crate::index::IndexError),

    #[error("Backend error: {0}")]
    BackendError(String),
}

/// JSON document metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    pub id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub version: u64,
    pub schema: Option<String>,
}

/// A JSON document with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonDocument {
    #[serde(flatten)]
    pub metadata: DocumentMetadata,
    pub data: Value,
}

/// JSON document store
pub struct JsonStore<Backend> {
    backend: Backend,
    transaction_manager: Option<Arc<TransactionManager<Backend>>>,
    index_manager: Arc<IndexManager>,
    fts_manager: Arc<FtsManager>,
    /// Collection name to prefix for keys
    collection: String,
}

impl<Backend> JsonStore<Backend>
where
    Backend: DatabaseBackend + KvBackend,
{
    /// Create a new JSON document store
    pub fn new(backend: Backend, collection: String) -> Self {
        Self {
            backend,
            transaction_manager: None,
            index_manager: Arc::new(IndexManager::new()),
            fts_manager: Arc::new(FtsManager::new()),
            collection,
        }
    }

    /// Create a new JSON document store at the given location
    pub fn at_location(location: Location, collection: String) -> Result<Self, Backend::Error> {
        Ok(Self::new(Backend::at_location(location)?, collection))
    }

    /// Create a JSON path index
    pub fn create_json_index(
        &self,
        name: String,
        _json_path: String,
        index_type: IndexType,
        unique: bool,
    ) -> Result<(), crate::index::IndexError> {
        let config = IndexConfig {
            name: format!("{}_{}", self.collection, name),
            index_type,
            unique,
            filter: Some(json_path),
        };

        self.index_manager.create_index(config)
    }

    /// Create a full-text search index on a JSON path
    pub fn create_fts_index(
        &self,
        name: String,
        _json_path: String,
    ) -> Result<(), crate::index::IndexError> {
        let index_name = format!("{}_{}", self.collection, name);
        self.fts_manager.create_index(index_name)?;

        // Store the JSON path mapping
        // In a real implementation, we'd store this persistently
        Ok(())
    }

    /// Generate document key
    fn doc_key(&self, id: &str) -> String {
        format!("{}:doc:{}", self.collection, id)
    }

    /// Generate metadata key
    fn meta_key(&self, id: &str) -> String {
        format!("{}:meta:{}", self.collection, id)
    }
}

/// JSONPath query evaluation
pub struct JsonPath;

impl JsonPath {
    /// Evaluate a JSONPath expression against a JSON value
    pub fn query<'a>(json: &'a Value, path: &str) -> Result<Vec<&'a Value>, JsonStoreError> {
        // Simple JSONPath implementation
        // Full implementation would use a proper JSONPath parser

        let parts: Vec<&str> = path.split('.').collect();
        if parts.is_empty() || parts[0] != "$" {
            return Err(JsonStoreError::InvalidJsonPath(
                "Path must start with $".to_string(),
            ));
        }

        let mut current = vec![json];

        for part in parts.iter().skip(1) {
            let mut next = Vec::new();

            for value in current {
                if part.ends_with(']') {
                    // Array access
                    let (field, index_str) = part.split_once('[').unwrap();
                    let index_str = index_str.trim_end_matches(']');

                    if let Some(obj) = value.get(field) {
                        if let Some(arr) = obj.as_array() {
                            if index_str == "*" {
                                // All elements
                                next.extend(arr.iter());
                            } else if let Ok(index) = index_str.parse::<usize>() {
                                // Specific index
                                if let Some(elem) = arr.get(index) {
                                    next.push(elem);
                                }
                            }
                        }
                    }
                } else if part == "*" {
                    // Wildcard
                    match value {
                        Value::Object(map) => next.extend(map.values()),
                        Value::Array(arr) => next.extend(arr.iter()),
                        _ => {}
                    }
                } else {
                    // Object field access
                    if let Some(field_value) = value.get(part) {
                        next.push(field_value);
                    }
                }
            }

            current = next;
        }

        Ok(current)
    }

    /// Set a value at a JSONPath location
    pub fn set(json: &mut Value, path: &str, new_value: Value) -> Result<(), JsonStoreError> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.is_empty() || parts[0] != "$" {
            return Err(JsonStoreError::InvalidJsonPath(
                "Path must start with $".to_string(),
            ));
        }

        let mut current = json;

        for (i, part) in parts.iter().skip(1).enumerate() {
            let is_last = i == parts.len() - 2;

            if part.ends_with(']') {
                // Array access
                let (field, index_str) = part.split_once('[').unwrap();
                let index_str = index_str.trim_end_matches(']');

                // Ensure field exists and is an array
                if !current.get(field).map(|v| v.is_array()).unwrap_or(false) {
                    current[field] = Value::Array(Vec::new());
                }

                if let Some(arr) = current.get_mut(field).and_then(|v| v.as_array_mut()) {
                    if let Ok(index) = index_str.parse::<usize>() {
                        // Extend array if needed
                        while arr.len() <= index {
                            arr.push(Value::Null);
                        }

                        if is_last {
                            arr[index] = new_value;
                            return Ok(());
                        } else {
                            current = &mut arr[index];
                        }
                    }
                }
            } else {
                // Object field access
                if !current.is_object() {
                    *current = Value::Object(Map::new());
                }

                if let Some(obj) = current.as_object_mut() {
                    if is_last {
                        obj.insert(part.to_string(), new_value);
                        return Ok(());
                    } else {
                        obj.entry(part.to_string())
                            .or_insert(Value::Object(Map::new()));
                        current = obj.get_mut(part).unwrap();
                    }
                }
            }
        }

        Ok(())
    }
}

// Document store operations
impl<Backend> JsonStore<Backend>
where
    Backend: KvBackend<Error: IntoTonicStatus> + 'static,
{
    /// Insert a new document
    pub async fn insert(
        &self,
        id: String,
        data: Value,
        schema: Option<String>,
    ) -> Result<JsonDocument, JsonStoreError> {
        let now = chrono::Utc::now();

        let metadata = DocumentMetadata {
            id: id.clone(),
            created_at: now,
            updated_at: now,
            version: 1,
            schema,
        };

        let document = JsonDocument {
            metadata: metadata.clone(),
            data: data.clone(),
        };

        // Store document
        let doc_key = self.doc_key(&id);
        let doc_value = serde_json::to_string(&data)?;

        let set_request = tokio_stream::iter(
            vec![crate::proto::kv::SetRequest {
                key: doc_key,
                value: doc_value,
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        self.backend
            .set(tonic::Request::new(set_request))
            .await
            .map_err(|e| JsonStoreError::BackendError(e.to_string()))?;

        // Store metadata
        let meta_key = self.meta_key(&id);
        let meta_value = serde_json::to_string(&metadata)?;

        let meta_request = tokio_stream::iter(
            vec![crate::proto::kv::SetRequest {
                key: meta_key,
                value: meta_value,
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        self.backend
            .set(tonic::Request::new(meta_request))
            .await
            .map_err(|e| JsonStoreError::BackendError(e.to_string()))?;

        // Update indexes
        self.update_indexes(&id, None, &data)?;

        Ok(document)
    }

    /// Get a document by ID
    pub async fn get(&self, id: &str) -> Result<JsonDocument, JsonStoreError> {
        // Get document data
        let doc_key = self.doc_key(id);
        let get_request = tokio_stream::iter(
            vec![crate::proto::kv::GetRequest {
                key: doc_key,
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        let mut response = self
            .backend
            .get(tonic::Request::new(get_request))
            .await
            .map_err(|e| JsonStoreError::BackendError(e.to_string()))?
            .into_inner();

        let doc_value = if let Some(Ok(resp)) = response.message().await {
            resp.value
        } else {
            return Err(JsonStoreError::DocumentNotFound { id: id.to_string() });
        };

        let data: Value = serde_json::from_str(&doc_value)?;

        // Get metadata
        let meta_key = self.meta_key(id);
        let meta_request = tokio_stream::iter(
            vec![crate::proto::kv::GetRequest {
                key: meta_key,
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        let mut meta_response = self
            .backend
            .get(tonic::Request::new(meta_request))
            .await
            .map_err(|e| JsonStoreError::BackendError(e.to_string()))?
            .into_inner();

        let meta_value = if let Some(Ok(resp)) = meta_response.message().await {
            resp.value
        } else {
            return Err(JsonStoreError::DocumentNotFound { id: id.to_string() });
        };

        let metadata: DocumentMetadata = serde_json::from_str(&meta_value)?;

        Ok(JsonDocument { metadata, data })
    }

    /// Update a document
    pub async fn update(&self, id: &str, data: Value) -> Result<JsonDocument, JsonStoreError> {
        let mut document = self.get(id).await?;

        document.data = data;
        document.metadata.updated_at = chrono::Utc::now();
        document.metadata.version += 1;

        // Update document
        let doc_key = self.doc_key(id);
        let doc_value = serde_json::to_string(&document.data)?;

        let set_request = tokio_stream::iter(
            vec![crate::proto::kv::SetRequest {
                key: doc_key,
                value: doc_value,
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        self.backend
            .set(tonic::Request::new(set_request))
            .await
            .map_err(|e| JsonStoreError::BackendError(e.to_string()))?;

        // Update metadata
        let meta_key = self.meta_key(id);
        let meta_value = serde_json::to_string(&document.metadata)?;

        let meta_request = tokio_stream::iter(
            vec![crate::proto::kv::SetRequest {
                key: meta_key,
                value: meta_value,
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        self.backend
            .set(tonic::Request::new(meta_request))
            .await
            .map_err(|e| JsonStoreError::BackendError(e.to_string()))?;

        // Update indexes
        self.update_indexes(id, Some(&document.data), &data)?;

        Ok(document)
    }

    /// Partially update a document using JSONPath
    pub async fn patch(
        &self,
        id: &str,
        path: &str,
        value: Value,
    ) -> Result<JsonDocument, JsonStoreError> {
        let mut document = self.get(id).await?;

        // Apply the patch
        JsonPath::set(&mut document.data, path, value)?;

        // Save the updated document
        self.update(id, document.data).await
    }

    /// Delete a document
    pub async fn delete(&self, id: &str) -> Result<(), JsonStoreError> {
        let document = self.get(id).await?;

        // Remove from indexes
        self.remove_from_indexes(id, &document.data)?;

        // Delete document
        let doc_key = self.doc_key(id);
        let del_request = tokio_stream::iter(
            vec![crate::proto::kv::DeleteRequest {
                key: doc_key,
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        self.backend
            .delete(tonic::Request::new(del_request))
            .await
            .map_err(|e| JsonStoreError::BackendError(e.to_string()))?;

        // Delete metadata
        let meta_key = self.meta_key(id);
        let meta_del_request = tokio_stream::iter(
            vec![crate::proto::kv::DeleteRequest {
                key: meta_key,
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        self.backend
            .delete(tonic::Request::new(meta_del_request))
            .await
            .map_err(|e| JsonStoreError::BackendError(e.to_string()))?;

        Ok(())
    }

    /// Query documents using JSONPath
    pub async fn query(
        &self,
        json_path: &str,
        value: &Value,
    ) -> Result<Vec<String>, JsonStoreError> {
        // In a real implementation, this would use the index system
        // For now, return empty results
        Ok(Vec::new())
    }

    fn update_indexes(
        &self,
        _id: &str,
        _old_data: Option<&Value>,
        _new_data: &Value,
    ) -> Result<(), JsonStoreError> {
        // In a real implementation, we would:
        // 1. Extract values from JSON paths configured in indexes
        // 2. Update the index entries
        // For now, this is a placeholder
        Ok(())
    }

    fn remove_from_indexes(&self, _id: &str, _data: &Value) -> Result<(), JsonStoreError> {
        // In a real implementation, we would remove from all indexes
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_path_query() {
        let json = serde_json::json!({
            "user": {
                "name": "Alice",
                "age": 30,
                "emails": ["alice@example.com", "alice@work.com"],
                "address": {
                    "city": "New York",
                    "zip": "10001"
                }
            }
        });

        // Simple field access
        let results = JsonPath::query(&json, "$.user.name").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], &Value::String("Alice".to_string()));

        // Nested field access
        let results = JsonPath::query(&json, "$.user.address.city").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], &Value::String("New York".to_string()));

        // Array access
        let results = JsonPath::query(&json, "$.user.emails[0]").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], &Value::String("alice@example.com".to_string()));

        // Array wildcard
        let results = JsonPath::query(&json, "$.user.emails[*]").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_json_path_set() {
        let mut json = serde_json::json!({
            "user": {
                "name": "Alice"
            }
        });

        // Set simple field
        JsonPath::set(&mut json, "$.user.age", Value::Number(30.into())).unwrap();
        assert_eq!(json["user"]["age"], 30);

        // Set nested field (creates path)
        JsonPath::set(
            &mut json,
            "$.user.address.city",
            Value::String("NYC".to_string()),
        )
        .unwrap();
        assert_eq!(json["user"]["address"]["city"], "NYC");

        // Set array element
        JsonPath::set(
            &mut json,
            "$.user.tags[0]",
            Value::String("admin".to_string()),
        )
        .unwrap();
        assert_eq!(json["user"]["tags"][0], "admin");
    }
}
