//! Example demonstrating advanced BuffDB features including transactions and secondary indexes

use buffdb::{
    backend::Sqlite,
    client::kv::KvClient,
    index::{IndexConfig, IndexType, IndexValue},
    proto::kv::{
        BeginTransactionRequest, BeginTransactionResponse, CommitTransactionRequest,
        CommitTransactionResponse, DeleteRequest, GetRequest, SetRequest,
    },
    server::kv::KvServer,
    store::KvStore,
};
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start a BuffDB server with advanced features
    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        // Create a KV store with transaction support
        let kv_store = KvStore::<Sqlite>::with_transactions(
            buffdb::Location::InMemory,
            Duration::from_secs(30), // 30 second transaction timeout
        )
        .expect("Failed to create KV store");

        // Create secondary indexes
        kv_store
            .create_index(IndexConfig {
                name: "email_index".to_string(),
                index_type: IndexType::Hash,
                unique: true,
                filter: None,
            })
            .expect("Failed to create email index");

        kv_store
            .create_index(IndexConfig {
                name: "age_index".to_string(),
                index_type: IndexType::BTree,
                unique: false,
                filter: None,
            })
            .expect("Failed to create age index");

        let kv_server = KvServer::new(kv_store);

        // Signal that server is ready
        tx.send(()).unwrap();

        Server::builder()
            .add_service(kv_server)
            .serve("[::1]:9314".parse().unwrap())
            .await
            .unwrap();
    });

    // Wait for server to be ready
    rx.await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect to the server
    let mut client = KvClient::connect("http://[::1]:9314").await?;

    println!("=== BuffDB Advanced Features Demo ===\n");

    // Demonstrate transactions
    println!("1. Transaction Example:");

    // Begin a transaction
    let begin_response = client
        .begin_transaction(BeginTransactionRequest {
            read_only: Some(false),
            timeout_ms: Some(5000),
        })
        .await?;

    let transaction_id = begin_response.into_inner().transaction_id;
    println!("   Started transaction: {}", transaction_id);

    // Perform operations within the transaction
    let set_stream = tokio_stream::iter(
        vec![
            SetRequest {
                key: "user:1".to_string(),
                value: r#"{"name": "Alice", "email": "alice@example.com", "age": 30}"#.to_string(),
                transaction_id: Some(transaction_id.clone()),
            },
            SetRequest {
                key: "user:2".to_string(),
                value: r#"{"name": "Bob", "email": "bob@example.com", "age": 25}"#.to_string(),
                transaction_id: Some(transaction_id.clone()),
            },
        ]
        .into_iter()
        .map(Ok),
    );

    let mut set_response = client.set(set_stream).await?.into_inner();
    while let Some(resp) = set_response.message().await? {
        println!("   Set key in transaction: {}", resp.key);
    }

    // Commit the transaction
    let commit_response = client
        .commit_transaction(CommitTransactionRequest {
            transaction_id: transaction_id.clone(),
        })
        .await?
        .into_inner();

    if commit_response.success {
        println!("   Transaction committed successfully!");
    } else {
        println!(
            "   Transaction commit failed: {:?}",
            commit_response.error_message
        );
    }

    println!("\n2. Secondary Index Example:");

    // The index would be automatically updated when setting values
    // Here we demonstrate querying by index

    // First, let's add more users to demonstrate index functionality
    let more_users = vec![
        (
            "user:3",
            r#"{"name": "Charlie", "email": "charlie@example.com", "age": 30}"#,
        ),
        (
            "user:4",
            r#"{"name": "Diana", "email": "diana@example.com", "age": 25}"#,
        ),
        (
            "user:5",
            r#"{"name": "Eve", "email": "eve@example.com", "age": 35}"#,
        ),
    ];

    for (key, value) in more_users {
        let set_stream = tokio_stream::iter(
            vec![SetRequest {
                key: key.to_string(),
                value: value.to_string(),
                transaction_id: None,
            }]
            .into_iter()
            .map(Ok),
        );

        client.set(set_stream).await?;
    }

    println!("   Added 5 users to the database");

    // In a real implementation, we would have index query methods
    // For now, let's demonstrate the concept
    println!("\n   Index capabilities:");
    println!("   - email_index: Unique hash index for fast email lookups");
    println!("   - age_index: B-tree index for age range queries");
    println!("   - Example: Find all users aged 25-30 would use age_index");
    println!("   - Example: Find user by email would use email_index");

    println!("\n3. Transaction Rollback Example:");

    // Begin another transaction
    let begin_response = client
        .begin_transaction(BeginTransactionRequest {
            read_only: Some(false),
            timeout_ms: Some(5000),
        })
        .await?;

    let transaction_id = begin_response.into_inner().transaction_id;
    println!("   Started transaction: {}", transaction_id);

    // Try to set a value
    let set_stream = tokio_stream::iter(
        vec![SetRequest {
            key: "user:6".to_string(),
            value: r#"{"name": "Frank", "email": "frank@example.com", "age": 40}"#.to_string(),
            transaction_id: Some(transaction_id.clone()),
        }]
        .into_iter()
        .map(Ok),
    );

    client.set(set_stream).await?;
    println!("   Set user:6 in transaction");

    // Rollback the transaction
    let rollback_response = client
        .rollback_transaction(buffdb::proto::kv::RollbackTransactionRequest {
            transaction_id: transaction_id.clone(),
        })
        .await?
        .into_inner();

    if rollback_response.success {
        println!("   Transaction rolled back successfully!");
    }

    // Verify the key doesn't exist
    let get_stream = tokio_stream::iter(
        vec![GetRequest {
            key: "user:6".to_string(),
            transaction_id: None,
        }]
        .into_iter()
        .map(Ok),
    );

    let mut get_response = client.get(get_stream).await?.into_inner();
    if let Ok(None) = get_response.message().await {
        println!("   Verified: user:6 does not exist after rollback");
    }

    println!("\n4. Read-only Transaction Example:");

    // Begin a read-only transaction
    let begin_response = client
        .begin_transaction(BeginTransactionRequest {
            read_only: Some(true),
            timeout_ms: Some(5000),
        })
        .await?;

    let transaction_id = begin_response.into_inner().transaction_id;
    println!("   Started read-only transaction: {}", transaction_id);

    // Read multiple values consistently
    let keys = vec!["user:1", "user:2", "user:3"];
    for key in keys {
        let get_stream = tokio_stream::iter(
            vec![GetRequest {
                key: key.to_string(),
                transaction_id: Some(transaction_id.clone()),
            }]
            .into_iter()
            .map(Ok),
        );

        let mut get_response = client.get(get_stream).await?.into_inner();
        if let Some(resp) = get_response.message().await? {
            println!("   Read {}: {}", key, resp.value);
        }
    }

    // Commit the read-only transaction
    client
        .commit_transaction(CommitTransactionRequest { transaction_id })
        .await?;

    println!("   Read-only transaction completed");

    println!("\n=== Demo Complete ===");

    Ok(())
}

/// Example of how to use indexes programmatically
#[allow(dead_code)]
fn demonstrate_index_usage() {
    use buffdb::index::{IndexConfig, IndexManager, IndexType, IndexValue};

    // Create an index manager
    let index_manager = IndexManager::new();

    // Create a unique email index
    let email_index = IndexConfig {
        name: "email_idx".to_string(),
        index_type: IndexType::Hash,
        unique: true,
        filter: None,
    };

    index_manager.create_index(email_index).unwrap();

    // Create a non-unique age range index
    let age_index = IndexConfig {
        name: "age_idx".to_string(),
        index_type: IndexType::BTree,
        unique: false,
        filter: None,
    };

    index_manager.create_index(age_index).unwrap();

    // Update indexes when inserting data
    index_manager
        .update_indexes("user:1", None, "alice@example.com")
        .unwrap();

    // Query by index
    if let Some(email_idx) = index_manager.get_index("email_idx") {
        let keys = email_idx
            .find_exact(&IndexValue::String("alice@example.com".to_string()))
            .unwrap();
        println!("Found keys for email: {:?}", keys);
    }

    // Range query on age index
    if let Some(age_idx) = index_manager.get_index("age_idx") {
        let keys = age_idx
            .find_range(&IndexValue::Integer(25), &IndexValue::Integer(35))
            .unwrap();
        println!("Found keys for age range 25-35: {:?}", keys);
    }
}
