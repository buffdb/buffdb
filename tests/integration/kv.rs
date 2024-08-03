use crate::helpers::assert_stream_eq;
use anyhow::Result;
use buffdb::proto::kv::{
    DeleteRequest, DeleteResponse, EqRequest, GetRequest, GetResponse, NotEqRequest, SetRequest,
    SetResponse,
};
use buffdb::proto::query::{QueryResult, RawQuery, RowsChanged};
use buffdb::transitive::kv_client;
use buffdb::Location;
use futures::{stream, StreamExt as _};
use serial_test::serial;
use std::sync::LazyLock;

static KV_STORE_LOC: LazyLock<Location> = LazyLock::new(|| Location::OnDisk {
    path: "kv_store.test.db".into(),
});

#[tokio::test]
#[serial]
async fn test_query() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    let _response = client
        .set(stream::iter([SetRequest {
            key: "key_raw_query".to_owned(),
            value: "value_raw_query".to_owned(),
        }]))
        .await;

    let mut response = client
        .query(stream::iter([RawQuery {
            query: "SELECT COUNT(*) FROM kv".to_owned(),
        }]))
        .await?
        .into_inner();
    drop(client);

    let QueryResult { fields } = response
        .next()
        .await
        .expect("one result should be present")?;
    assert_eq!(fields.len(), 1);
    // TODO check the actual value

    assert!(response.next().await.is_none());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_execute() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    let response = client
        .execute(stream::iter([RawQuery {
            query: "CREATE SEQUENCE IF NOT EXISTS kv_pointless_sequence START 1".to_owned(),
        }]))
        .await?;
    drop(client);

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
async fn test_get() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    let _response = client
        .set(stream::iter([SetRequest {
            key: "key_get".to_owned(),
            value: "value_get".to_owned(),
        }]))
        .await?;

    let stream = client
        .get(stream::iter([GetRequest {
            key: "key_get".to_owned(),
        }]))
        .await?
        .into_inner();
    drop(client);
    assert_stream_eq(
        stream,
        [GetResponse {
            value: "value_get".to_owned(),
        }],
    )
    .await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    let stream = client
        .set(stream::iter([SetRequest {
            key: "key_set".to_owned(),
            value: "value_set".to_owned(),
        }]))
        .await?
        .into_inner();
    drop(client);
    assert_stream_eq(
        stream,
        [SetResponse {
            key: "key_set".to_owned(),
        }],
    )
    .await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_delete() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    let _response = client
        .set(stream::iter([SetRequest {
            key: "key_delete".to_owned(),
            value: "value_delete".to_owned(),
        }]))
        .await?;

    let stream = client
        .delete(stream::iter([DeleteRequest {
            key: "key_delete".to_owned(),
        }]))
        .await?
        .into_inner();
    assert_stream_eq(
        stream,
        [DeleteResponse {
            key: "key_delete".to_owned(),
        }],
    )
    .await;

    let mut response = client
        .get(stream::iter([GetRequest {
            key: "key_delete".to_owned(),
        }]))
        .await?
        .into_inner();
    drop(client);
    let next = response.next().await;
    assert!(matches!(next, Some(Err(_))));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_eq() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    for key in ["key_a_eq", "key_b_eq", "key_c_eq", "key_d_eq"] {
        let _response = client
            .set(stream::iter([SetRequest {
                key: key.to_owned(),
                value: "value_eq".to_owned(),
            }]))
            .await?;
    }

    let all_eq = client
        .eq(stream::iter([
            EqRequest {
                key: "key_a_eq".to_owned(),
            },
            EqRequest {
                key: "key_b_eq".to_owned(),
            },
            EqRequest {
                key: "key_c_eq".to_owned(),
            },
            EqRequest {
                key: "key_d_eq".to_owned(),
            },
        ]))
        .await?
        .into_inner();
    assert!(all_eq);

    let _response = client
        .set(stream::iter([SetRequest {
            key: "key_e_eq".to_owned(),
            value: "value2_eq".to_owned(),
        }]))
        .await?;

    let all_eq = client
        .eq(stream::iter([
            EqRequest {
                key: "key_a_eq".to_owned(),
            },
            EqRequest {
                key: "key_b_eq".to_owned(),
            },
            EqRequest {
                key: "key_c_eq".to_owned(),
            },
            EqRequest {
                key: "key_d_eq".to_owned(),
            },
            EqRequest {
                key: "key_e_eq".to_owned(),
            },
        ]))
        .await?
        .into_inner();
    drop(client);
    assert!(!all_eq);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_not_eq() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    for (idx, key) in ["key_a_neq", "key_b_neq", "key_c_neq", "key_d_neq"]
        .into_iter()
        .enumerate()
    {
        let _response = client
            .set(stream::iter([SetRequest {
                key: key.to_owned(),
                value: format!("value{idx}_neq"),
            }]))
            .await?;
    }

    let all_neq = client
        .not_eq(stream::iter([
            NotEqRequest {
                key: "key_a_neq".to_owned(),
            },
            NotEqRequest {
                key: "key_b_neq".to_owned(),
            },
            NotEqRequest {
                key: "key_c_neq".to_owned(),
            },
            NotEqRequest {
                key: "key_d_neq".to_owned(),
            },
        ]))
        .await?
        .into_inner();
    assert!(all_neq);

    let _response = client
        .set(stream::iter([SetRequest {
            key: "key_e_neq".to_owned(),
            value: "value2_neq".to_owned(),
        }]))
        .await?;

    let all_neq = client
        .not_eq(stream::iter([
            NotEqRequest {
                key: "key_a_neq".to_owned(),
            },
            NotEqRequest {
                key: "key_b_neq".to_owned(),
            },
            NotEqRequest {
                key: "key_c_neq".to_owned(),
            },
            NotEqRequest {
                key: "key_d_neq".to_owned(),
            },
            NotEqRequest {
                key: "key_e_neq".to_owned(),
            },
        ]))
        .await?
        .into_inner();
    drop(client);
    assert!(!all_neq);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_eq_not_found() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;
    let res = client
        .eq(stream::iter([EqRequest {
            key: "this-key-should-not-exist".to_owned(),
        }]))
        .await;
    drop(client);
    assert!(res.is_err());
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_not_eq_not_found() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;
    let res = client
        .not_eq(stream::iter([NotEqRequest {
            key: "this-key-should-not-exist".to_owned(),
        }]))
        .await;
    drop(client);
    assert!(res.is_err());
    Ok(())
}
