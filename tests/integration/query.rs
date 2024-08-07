use super::{Backend, BLOB_PATH, KV_PATH};
use anyhow::Result;
use buffdb::proto::blob::StoreRequest;
use buffdb::proto::kv::SetRequest;
use buffdb::proto::query::{QueryResult, RawQuery, RowsChanged, TargetStore};
use buffdb::transitive::{blob_client, kv_client, query_client};
use buffdb::Location;
use futures::{stream, StreamExt as _};
use serial_test::serial;
use std::sync::LazyLock;

static KV_STORE_LOC: LazyLock<Location> = LazyLock::new(|| Location::OnDisk {
    path: KV_PATH.into(),
});
static BLOB_STORE_LOC: LazyLock<Location> = LazyLock::new(|| Location::OnDisk {
    path: BLOB_PATH.into(),
});

#[tokio::test]
#[serial]
async fn test_kv_query() -> Result<()> {
    let mut kv_client = kv_client::<_, Backend>(KV_STORE_LOC.clone()).await?;
    let mut query_client = query_client::<_, _, Backend>(KV_PATH, BLOB_PATH).await?;

    let _response = kv_client
        .set(stream::iter([SetRequest {
            key: "key_raw_query".to_owned(),
            value: "value_raw_query".to_owned(),
        }]))
        .await;

    let mut response = query_client
        .query(stream::iter([RawQuery {
            query: "SELECT COUNT(*) FROM kv".to_owned(),
            target: TargetStore::Kv as i32,
        }]))
        .await?
        .into_inner();
    drop((kv_client, query_client));

    let QueryResult { fields } = response
        .next()
        .await
        .expect("one result should be present")?;
    assert_eq!(fields.len(), 1);

    assert!(response.next().await.is_none());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_kv_execute() -> Result<()> {
    let mut query_client = query_client::<_, _, Backend>(KV_PATH, BLOB_PATH).await?;

    let response = query_client
        .execute(stream::iter([RawQuery {
            query: "CREATE TABLE IF NOT EXISTS test_kv_execute_table (value TEXT);".to_owned(),
            target: TargetStore::Kv as i32,
        }]))
        .await?;
    drop(query_client);

    let mut response = response.into_inner();
    assert!(matches!(
        response.next().await,
        Some(Ok(RowsChanged { rows_changed: 0 }))
    ));

    assert!(response.next().await.is_none());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_blob_query() -> Result<()> {
    let mut client = blob_client::<_, Backend>(BLOB_STORE_LOC.clone()).await?;
    let mut query_client = query_client::<_, _, Backend>(KV_PATH, BLOB_PATH).await?;

    let _id = client
        .store(stream::iter([StoreRequest {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        }]))
        .await?
        .into_inner()
        .collect::<Vec<_>>()
        .await;

    let mut response = query_client
        .query(stream::iter([RawQuery {
            query: "SELECT COUNT(*) FROM blob".to_owned(),
            target: TargetStore::Blob as i32,
        }]))
        .await?
        .into_inner();
    drop((client, query_client));

    let QueryResult { fields } = response
        .next()
        .await
        .expect("one result should be present")?;
    assert_eq!(fields.len(), 1);

    assert!(response.next().await.is_none());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_blob_execute() -> Result<()> {
    let mut query_client = query_client::<_, _, Backend>(KV_PATH, BLOB_PATH).await?;

    let response = query_client
        .execute(stream::iter([RawQuery {
            query: "CREATE TABLE IF NOT EXISTS test_blob_execute_table (value TEXT);".to_owned(),
            target: TargetStore::Blob as i32,
        }]))
        .await?;
    drop(query_client);
    let mut response = response.into_inner();
    assert!(matches!(
        response.next().await,
        Some(Ok(RowsChanged { rows_changed: 0 }))
    ));

    assert!(response.next().await.is_none());

    Ok(())
}
