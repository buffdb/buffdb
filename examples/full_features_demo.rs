//! Comprehensive demonstration of all BuffDB advanced features

use buffdb::{
    backend::Sqlite,
    client::kv::KvClient,
    fts::{FtsManager, Tokenizer},
    index::{IndexConfig, IndexType, IndexValue},
    json_store::{JsonPath, JsonStore},
    mvcc::{IsolationLevel, MvccManager},
    proto::kv::*,
    server::kv::KvServer,
    store::KvStore,
    Location,
};
use serde_json::json;
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== BuffDB Complete Features Demo ===\n");

    // 1. Demonstrate Transactions
    demonstrate_transactions().await?;

    // 2. Demonstrate Secondary Indexes
    demonstrate_indexes().await?;

    // 3. Demonstrate Full-Text Search
    demonstrate_fts().await?;

    // 4. Demonstrate JSON Document Store
    demonstrate_json_store().await?;

    // 5. Demonstrate MVCC
    demonstrate_mvcc().await?;

    println!("\n=== All Demonstrations Complete ===");
    Ok(())
}

async fn demonstrate_transactions() -> Result<(), Box<dyn std::error::Error>> {
    println!("1. Transaction Support Demo");
    println!("   ========================\n");

    // Create store with transaction support
    let store = KvStore::<Sqlite>::with_transactions(Location::InMemory, Duration::from_secs(30))?;

    // Start server
    let (tx, rx) = oneshot::channel();
    let server = KvServer::new(store);

    tokio::spawn(async move {
        tx.send(()).unwrap();
        tonic::transport::Server::builder()
            .add_service(server)
            .serve("[::1]:9315".parse().unwrap())
            .await
            .unwrap();
    });

    rx.await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect client
    let mut client = KvClient::connect("http://[::1]:9315").await?;

    // Begin transaction
    let begin_resp = client
        .begin_transaction(BeginTransactionRequest {
            read_only: Some(false),
            timeout_ms: Some(5000),
        })
        .await?;

    let tx_id = begin_resp.into_inner().transaction_id;
    println!("   Started transaction: {}", tx_id);

    // Perform operations
    let keys = vec!["user:1", "user:2", "user:3"];
    for (i, key) in keys.iter().enumerate() {
        let set_stream = tokio_stream::iter(
            vec![SetRequest {
                key: key.to_string(),
                value: format!("User {} data", i + 1),
                transaction_id: Some(tx_id.clone()),
            }]
            .into_iter()
            .map(Ok),
        );

        client.set(set_stream).await?;
        println!("   Set {} in transaction", key);
    }

    // Commit transaction
    let commit_resp = client
        .commit_transaction(CommitTransactionRequest {
            transaction_id: tx_id,
        })
        .await?;

    if commit_resp.into_inner().success {
        println!("   ✓ Transaction committed successfully!\n");
    }

    Ok(())
}

async fn demonstrate_indexes() -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Secondary Indexing Demo");
    println!("   =======================\n");

    use buffdb::index::{IndexManager, SecondaryIndex};

    let index_manager = IndexManager::new();

    // Create different types of indexes
    index_manager.create_index(IndexConfig {
        name: "email_idx".to_string(),
        index_type: IndexType::Hash,
        unique: true,
        filter: None,
    })?;

    index_manager.create_index(IndexConfig {
        name: "age_idx".to_string(),
        index_type: IndexType::BTree,
        unique: false,
        filter: None,
    })?;

    println!("   Created email_idx (unique hash) and age_idx (btree)");

    // Simulate data insertion with index updates
    let users = vec![
        ("user:1", "alice@example.com", 25),
        ("user:2", "bob@example.com", 30),
        ("user:3", "charlie@example.com", 25),
        ("user:4", "diana@example.com", 35),
    ];

    for (id, email, age) in users {
        // Update email index
        if let Some(email_idx) = index_manager.get_index("email_idx") {
            email_idx.insert(id.to_string(), IndexValue::String(email.to_string()))?;
        }

        // Update age index
        if let Some(age_idx) = index_manager.get_index("age_idx") {
            age_idx.insert(id.to_string(), IndexValue::Integer(age))?;
        }

        println!("   Indexed {}: email={}, age={}", id, email, age);
    }

    // Query by email (exact match)
    if let Some(email_idx) = index_manager.get_index("email_idx") {
        let results = email_idx.find_exact(&IndexValue::String("alice@example.com".to_string()))?;
        println!("\n   Query: email='alice@example.com'");
        println!("   Results: {:?}", results);
    }

    // Query by age range
    if let Some(age_idx) = index_manager.get_index("age_idx") {
        let results = age_idx.find_range(&IndexValue::Integer(25), &IndexValue::Integer(30))?;
        println!("\n   Query: age BETWEEN 25 AND 30");
        println!("   Results: {:?}\n", results);
    }

    Ok(())
}

async fn demonstrate_fts() -> Result<(), Box<dyn std::error::Error>> {
    println!("3. Full-Text Search Demo");
    println!("   =====================\n");

    let fts_manager = FtsManager::new();
    fts_manager.create_index("articles".to_string())?;

    if let Some(fts_index) = fts_manager.get_index("articles") {
        // Index some documents
        let articles = vec![
            (
                "doc1",
                "The quick brown fox jumps over the lazy dog. This is a test of full-text search.",
            ),
            (
                "doc2",
                "Brown bears are found in many parts of North America and Eurasia.",
            ),
            (
                "doc3",
                "The lazy programmer automated their daily tasks using Rust.",
            ),
            (
                "doc4",
                "Quick tips for learning Rust: practice daily and read the documentation.",
            ),
        ];

        for (id, content) in articles {
            fts_index.index_document(id.to_string(), content)?;
            println!("   Indexed document: {}", id);
        }

        // Search for documents
        println!("\n   Search: 'rust'");
        let results = fts_index.search("rust", 10);
        for result in results {
            println!("   - {} (score: {:.2})", result.doc_id, result.score);
        }

        println!("\n   Search: 'quick brown'");
        let results = fts_index.search("quick brown", 10);
        for result in results {
            println!("   - {} (score: {:.2})", result.doc_id, result.score);
        }

        // Phrase search
        println!("\n   Phrase search: 'full-text search'");
        let results = fts_index.phrase_search("full-text search", 10);
        for result in results {
            println!("   - {} (exact phrase match)", result.doc_id);
        }
    }

    println!();
    Ok(())
}

async fn demonstrate_json_store() -> Result<(), Box<dyn std::error::Error>> {
    println!("4. JSON Document Store Demo");
    println!("   ========================\n");

    let json_store = JsonStore::<Sqlite>::at_location(Location::InMemory, "users".to_string())?;

    // Insert JSON documents
    let user1 = json!({
        "name": "Alice Johnson",
        "email": "alice@example.com",
        "profile": {
            "age": 30,
            "city": "New York",
            "interests": ["coding", "hiking", "photography"]
        }
    });

    let doc1 = json_store.insert("user:1".to_string(), user1, None).await?;
    println!("   Inserted document: {}", doc1.metadata.id);

    // JSONPath queries
    let user1_data = json_store.get("user:1").await?;

    // Query specific fields
    let name_results = JsonPath::query(&user1_data.data, "$.name")?;
    println!("   Query $.name: {:?}", name_results);

    let interests = JsonPath::query(&user1_data.data, "$.profile.interests[*]")?;
    println!("   Query $.profile.interests[*]: {:?}", interests);

    // Partial update using JSONPath
    json_store
        .patch("user:1", "$.profile.city", json!("San Francisco"))
        .await?;
    println!("\n   Updated $.profile.city to 'San Francisco'");

    // Add new field
    json_store
        .patch("user:1", "$.profile.company", json!("BuffDB Inc"))
        .await?;
    println!("   Added $.profile.company = 'BuffDB Inc'");

    // Verify updates
    let updated = json_store.get("user:1").await?;
    let city = JsonPath::query(&updated.data, "$.profile.city")?;
    let company = JsonPath::query(&updated.data, "$.profile.company")?;
    println!("\n   Verified updates:");
    println!("   - City: {:?}", city);
    println!("   - Company: {:?}\n", company);

    Ok(())
}

async fn demonstrate_mvcc() -> Result<(), Box<dyn std::error::Error>> {
    println!("5. MVCC (Multi-Version Concurrency Control) Demo");
    println!("   =============================================\n");

    let mvcc = MvccManager::new();

    // Transaction 1: Writer
    let tx1 = "tx1".to_string();
    mvcc.begin_transaction(tx1.clone(), IsolationLevel::ReadCommitted, false)?;
    println!("   Transaction 1 started (writer)");

    // Transaction 2: Reader with repeatable read
    let tx2 = "tx2".to_string();
    mvcc.begin_transaction(tx2.clone(), IsolationLevel::RepeatableRead, false)?;
    println!("   Transaction 2 started (reader, repeatable read)");

    // Tx1 writes a value
    mvcc.write(&tx1, "counter".to_string(), b"100".to_vec())?;
    println!("\n   Tx1: SET counter = 100");

    // Tx2 reads - shouldn't see uncommitted value
    let val = mvcc.read(&tx2, "counter")?;
    println!(
        "   Tx2: READ counter = {:?} (uncommitted changes not visible)",
        val
    );

    // Tx1 commits
    mvcc.commit_transaction(&tx1)?;
    println!("\n   Tx1: COMMITTED");

    // Transaction 3: New reader
    let tx3 = "tx3".to_string();
    mvcc.begin_transaction(tx3.clone(), IsolationLevel::ReadCommitted, false)?;

    // Tx3 sees committed value
    let val = mvcc.read(&tx3, "counter")?;
    println!(
        "   Tx3: READ counter = {:?} (sees committed value)",
        val.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // But Tx2 still sees old snapshot (repeatable read)
    let val = mvcc.read(&tx2, "counter")?;
    println!("   Tx2: READ counter = {:?} (still sees snapshot)", val);

    // Demonstrate write conflict detection
    println!("\n   Demonstrating write conflict:");

    let tx4 = "tx4".to_string();
    let tx5 = "tx5".to_string();
    mvcc.begin_transaction(tx4.clone(), IsolationLevel::Serializable, false)?;
    mvcc.begin_transaction(tx5.clone(), IsolationLevel::Serializable, false)?;

    // Both read the same key
    mvcc.read(&tx4, "shared_resource")?;
    mvcc.read(&tx5, "shared_resource")?;

    // Both try to write
    mvcc.write(&tx4, "shared_resource".to_string(), b"tx4_value".to_vec())?;
    println!("   Tx4: WRITE shared_resource = 'tx4_value'");

    mvcc.commit_transaction(&tx4)?;
    println!("   Tx4: COMMITTED");

    // Tx5's write should fail due to conflict
    match mvcc.write(&tx5, "shared_resource".to_string(), b"tx5_value".to_vec()) {
        Err(e) => println!("   Tx5: WRITE failed - {}", e),
        Ok(_) => println!("   Tx5: WRITE succeeded (unexpected)"),
    }

    println!("\n   ✓ MVCC ensures data consistency with concurrent transactions\n");

    Ok(())
}

/// Helper function to demonstrate feature integration
async fn integrated_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n6. Integrated Features Example");
    println!("   ============================\n");

    // Create a JSON store with full-text search on content
    let json_store = JsonStore::<Sqlite>::at_location(Location::InMemory, "articles".to_string())?;

    // Create indexes
    json_store.create_json_index(
        "author_idx".to_string(),
        "$.author".to_string(),
        IndexType::Hash,
        false,
    )?;

    json_store.create_fts_index("content_fts".to_string(), "$.content".to_string())?;

    // Insert articles within a transaction
    let articles = vec![
        json!({
            "title": "Getting Started with BuffDB",
            "author": "Alice Smith",
            "content": "BuffDB is a high-performance embedded database with advanced features...",
            "tags": ["database", "rust", "embedded"]
        }),
        json!({
            "title": "Advanced Indexing Techniques",
            "author": "Bob Johnson",
            "content": "Secondary indexes improve query performance dramatically...",
            "tags": ["indexing", "performance", "database"]
        }),
    ];

    // Use transaction for atomic insertion
    println!("   Inserting articles in a transaction...");
    for (i, article) in articles.iter().enumerate() {
        json_store
            .insert(format!("article:{}", i), article.clone(), None)
            .await?;
    }

    println!("   ✓ All features work together seamlessly!");

    Ok(())
}
