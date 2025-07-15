//! Integration tests for transaction support

use buffdb::{
    backend::Sqlite,
    store::KvStore,
    proto::kv::*,
    Location,
};
use std::time::Duration;
use tokio_stream::StreamExt;

#[tokio::test]
async fn test_basic_transaction_commit() {
    // Create a store with transaction support
    let store = KvStore::<Sqlite>::with_transactions(
        Location::InMemory,
        Duration::from_secs(30),
    ).unwrap();
    
    // Begin transaction
    let begin_resp = store.begin_transaction(tonic::Request::new(BeginTransactionRequest {
        read_only: Some(false),
        timeout_ms: Some(5000),
    })).await.unwrap();
    
    let transaction_id = begin_resp.into_inner().transaction_id;
    
    // Set a value within the transaction
    let set_stream = tokio_stream::iter(vec![SetRequest {
        key: "test_key".to_string(),
        value: "test_value".to_string(),
        transaction_id: Some(transaction_id.clone()),
    }].into_iter().map(Ok));
    
    let mut set_resp = store.set(tonic::Request::new(set_stream)).await.unwrap().into_inner();
    
    // Verify set response
    if let Some(resp) = set_resp.next().await {
        assert_eq!(resp.unwrap().key, "test_key");
    }
    
    // Commit transaction
    let commit_resp = store.commit_transaction(tonic::Request::new(CommitTransactionRequest {
        transaction_id: transaction_id.clone(),
    })).await.unwrap();
    
    assert!(commit_resp.into_inner().success);
    
    // Verify the value persists after commit
    let get_stream = tokio_stream::iter(vec![GetRequest {
        key: "test_key".to_string(),
        transaction_id: None,
    }].into_iter().map(Ok));
    
    let mut get_resp = store.get(tonic::Request::new(get_stream)).await.unwrap().into_inner();
    
    if let Some(resp) = get_resp.next().await {
        assert_eq!(resp.unwrap().value, "test_value");
    }
}

#[tokio::test]
async fn test_transaction_rollback() {
    let store = KvStore::<Sqlite>::with_transactions(
        Location::InMemory,
        Duration::from_secs(30),
    ).unwrap();
    
    // Begin transaction
    let begin_resp = store.begin_transaction(tonic::Request::new(BeginTransactionRequest {
        read_only: Some(false),
        timeout_ms: Some(5000),
    })).await.unwrap();
    
    let transaction_id = begin_resp.into_inner().transaction_id;
    
    // Set a value within the transaction
    let set_stream = tokio_stream::iter(vec![SetRequest {
        key: "rollback_key".to_string(),
        value: "rollback_value".to_string(),
        transaction_id: Some(transaction_id.clone()),
    }].into_iter().map(Ok));
    
    store.set(tonic::Request::new(set_stream)).await.unwrap();
    
    // Rollback transaction
    let rollback_resp = store.rollback_transaction(tonic::Request::new(RollbackTransactionRequest {
        transaction_id: transaction_id.clone(),
    })).await.unwrap();
    
    assert!(rollback_resp.into_inner().success);
    
    // Verify the value does not exist after rollback
    let get_stream = tokio_stream::iter(vec![GetRequest {
        key: "rollback_key".to_string(),
        transaction_id: None,
    }].into_iter().map(Ok));
    
    let mut get_resp = store.get(tonic::Request::new(get_stream)).await.unwrap().into_inner();
    
    // Should not receive any value
    assert!(get_resp.next().await.is_none());
}

#[tokio::test]
async fn test_read_only_transaction() {
    let store = KvStore::<Sqlite>::with_transactions(
        Location::InMemory,
        Duration::from_secs(30),
    ).unwrap();
    
    // First, set some initial data
    let set_stream = tokio_stream::iter(vec![SetRequest {
        key: "readonly_test".to_string(),
        value: "initial_value".to_string(),
        transaction_id: None,
    }].into_iter().map(Ok));
    
    store.set(tonic::Request::new(set_stream)).await.unwrap();
    
    // Begin read-only transaction
    let begin_resp = store.begin_transaction(tonic::Request::new(BeginTransactionRequest {
        read_only: Some(true),
        timeout_ms: Some(5000),
    })).await.unwrap();
    
    let transaction_id = begin_resp.into_inner().transaction_id;
    
    // Read value within transaction
    let get_stream = tokio_stream::iter(vec![GetRequest {
        key: "readonly_test".to_string(),
        transaction_id: Some(transaction_id.clone()),
    }].into_iter().map(Ok));
    
    let mut get_resp = store.get(tonic::Request::new(get_stream)).await.unwrap().into_inner();
    
    if let Some(resp) = get_resp.next().await {
        assert_eq!(resp.unwrap().value, "initial_value");
    }
    
    // Commit read-only transaction
    let commit_resp = store.commit_transaction(tonic::Request::new(CommitTransactionRequest {
        transaction_id,
    })).await.unwrap();
    
    assert!(commit_resp.into_inner().success);
}