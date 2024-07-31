use anyhow::{bail, Result};
use buffdb::blob::{BlobClient, BlobData, BlobId, UpdateRequest};
use buffdb::transitive::blob_client;
use buffdb::Location;
use futures::{stream, StreamExt as _};
use serial_test::serial;
use std::sync::LazyLock;
use tonic::transport::Channel;

use crate::helpers::assert_stream_eq;

static BLOB_STORE_LOC: LazyLock<Location> = LazyLock::new(|| Location::OnDisk {
    path: "blob_store.test.db".into(),
});

async fn insert_one(client: &mut BlobClient<Channel>, value: BlobData) -> Result<u64> {
    let id = client
        .store(stream::iter([value]))
        .await?
        .into_inner()
        .collect::<Vec<_>>()
        .await;
    match id.as_slice() {
        [Ok(BlobId { id })] => Ok(*id),
        [Err(e)] => Err(e.clone().into()),
        _ => bail!("expected exactly one BlobId"),
    }
}

#[tokio::test]
#[serial]
async fn test_get() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        },
    )
    .await?;

    let response = client
        .get(stream::iter([BlobId { id }]))
        .await?
        .into_inner();
    drop(client);
    assert_stream_eq(
        response,
        [BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        }],
    )
    .await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_store() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: Some("{}".to_owned()),
        },
    )
    .await?;

    let response = client
        .get(stream::iter([BlobId { id }]))
        .await?
        .into_inner();
    drop(client);
    assert_stream_eq(
        response,
        [BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: Some("{}".to_owned()),
        }],
    )
    .await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_update_both() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        },
    )
    .await?;

    let stream = client
        .update(stream::iter([UpdateRequest {
            id,
            bytes: Some(b"def".to_vec()),
            should_update_metadata: true,
            metadata: Some("{}".to_owned()),
        }]))
        .await?
        .into_inner();
    assert_stream_eq(stream, [BlobId { id }]).await;

    let response = client
        .get(stream::iter([BlobId { id }]))
        .await?
        .into_inner();
    drop(client);
    assert_stream_eq(
        response,
        [BlobData {
            bytes: b"def".to_vec(),
            metadata: Some("{}".to_owned()),
        }],
    )
    .await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_update_bytes() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        },
    )
    .await?;

    let stream = client
        .update(stream::iter([UpdateRequest {
            id,
            bytes: Some(b"def".to_vec()),
            should_update_metadata: false,
            metadata: Some("{}".to_owned()),
        }]))
        .await?
        .into_inner();
    assert_stream_eq(stream, [BlobId { id }]).await;

    let response = client.get(stream::iter([BlobId { id }])).await?;
    drop(client);
    assert_stream_eq(
        response.into_inner(),
        [BlobData {
            bytes: b"def".to_vec(),
            metadata: None,
        }],
    )
    .await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_update_metadata() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"def".to_vec(),
            metadata: Some("{}".to_owned()),
        },
    )
    .await?;

    let stream = client
        .update(stream::iter([UpdateRequest {
            id,
            bytes: None,
            should_update_metadata: true,
            metadata: Some("{}".to_owned()),
        }]))
        .await?
        .into_inner();
    assert_stream_eq(stream, [BlobId { id }]).await;

    let response = client.get(stream::iter([BlobId { id }])).await?;
    drop(client);
    assert_stream_eq(
        response.into_inner(),
        [BlobData {
            bytes: b"def".to_vec(),
            metadata: Some("{}".to_owned()),
        }],
    )
    .await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_delete_with_metadata() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: Some("{}".to_owned()),
        },
    )
    .await?;

    let response = client
        .delete(stream::iter([BlobId { id }]))
        .await?
        .into_inner();
    assert_stream_eq(response, [BlobId { id }]).await;

    let mut response = client
        .get(stream::iter([BlobId { id }]))
        .await?
        .into_inner();
    drop(client);
    let msg = response.message().await;
    assert!(msg.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_delete_no_metadata() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        },
    )
    .await?;

    let response = client
        .delete(stream::iter([BlobId { id }]))
        .await?
        .into_inner();
    assert_stream_eq(response, [BlobId { id }]).await;

    let mut response = client
        .get(stream::iter([BlobId { id }]))
        .await?
        .into_inner();
    drop(client);
    let msg = response.message().await;
    assert!(msg.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_eq_data() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        },
    )
    .await?;
    let id2 = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        },
    )
    .await?;
    let id3 = insert_one(
        &mut client,
        BlobData {
            bytes: b"ghijkl".to_vec(),
            metadata: None,
        },
    )
    .await?;

    let response = client
        .eq_data(stream::iter([BlobId { id }, BlobId { id: id2 }]))
        .await?
        .into_inner();
    assert!(response.value);

    let response = client
        .eq_data(stream::iter([BlobId { id }, BlobId { id: id3 }]))
        .await?
        .into_inner();
    drop(client);
    assert!(!response.value);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_not_eq_data() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;

    let id = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        },
    )
    .await?;
    let id2 = insert_one(
        &mut client,
        BlobData {
            bytes: b"abcdef".to_vec(),
            metadata: None,
        },
    )
    .await?;
    let id3 = insert_one(
        &mut client,
        BlobData {
            bytes: b"ghijkl".to_vec(),
            metadata: None,
        },
    )
    .await?;

    let response = client
        .not_eq_data(stream::iter([BlobId { id }, BlobId { id: id2 }]))
        .await?
        .into_inner();
    assert!(!response.value);

    let response = client
        .not_eq_data(stream::iter([BlobId { id }, BlobId { id: id3 }]))
        .await?
        .into_inner();
    drop(client);
    assert!(response.value);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_eq_data_not_found() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;
    // If all four of these keys somehow exist, then a test failure is deserved.
    let res = client
        .eq_data(stream::iter([
            BlobId { id: u64::MAX },
            BlobId { id: u64::MAX - 1 },
            BlobId { id: u64::MAX - 2 },
            BlobId { id: u64::MAX - 3 },
        ]))
        .await;
    drop(client);
    assert!(res.is_err());
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_not_eq_data_not_found() -> Result<()> {
    let mut client = blob_client(BLOB_STORE_LOC.clone()).await?;
    // If all four of these keys somehow exist, then a test failure is deserved.
    let res = client
        .not_eq_data(stream::iter([
            BlobId { id: u64::MAX },
            BlobId { id: u64::MAX - 1 },
            BlobId { id: u64::MAX - 2 },
            BlobId { id: u64::MAX - 3 },
        ]))
        .await;
    drop(client);
    assert!(res.is_err());
    Ok(())
}
