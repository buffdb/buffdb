use crate::helpers::assert_stream_eq;
use anyhow::Result;
use buffdb::kv::{Key, KeyValue, Value};
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
async fn test_get() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    client
        .set(stream::iter([KeyValue {
            key: "key_get".to_owned(),
            value: "value_get".to_owned(),
        }]))
        .await?;

    let stream = client
        .get(stream::iter([Key {
            key: "key_get".to_owned(),
        }]))
        .await?
        .into_inner();
    assert_stream_eq(
        stream,
        [Value {
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
        .set(stream::iter([KeyValue {
            key: "key_set".to_owned(),
            value: "value_set".to_owned(),
        }]))
        .await?
        .into_inner();
    assert_stream_eq(
        stream,
        [Key {
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

    client
        .set(stream::iter([KeyValue {
            key: "key_delete".to_owned(),
            value: "value_delete".to_owned(),
        }]))
        .await?;

    let stream = client
        .delete(stream::iter([Key {
            key: "key_delete".to_owned(),
        }]))
        .await?
        .into_inner();
    assert_stream_eq(
        stream,
        [Key {
            key: "key_delete".to_owned(),
        }],
    )
    .await;

    let mut response = client
        .get(stream::iter([Key {
            key: "key_delete".to_owned(),
        }]))
        .await?
        .into_inner();
    let next = response.next().await;
    assert!(matches!(next, Some(Err(_))));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_eq() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;

    for key in ["key_a_eq", "key_b_eq", "key_c_eq", "key_d_eq"] {
        client
            .set(stream::iter([KeyValue {
                key: key.to_owned(),
                value: "value_eq".to_owned(),
            }]))
            .await?;
    }

    let all_eq = client
        .eq(stream::iter([
            Key {
                key: "key_a_eq".to_owned(),
            },
            Key {
                key: "key_b_eq".to_owned(),
            },
            Key {
                key: "key_c_eq".to_owned(),
            },
            Key {
                key: "key_d_eq".to_owned(),
            },
        ]))
        .await?
        .into_inner()
        .value;
    assert!(all_eq);

    client
        .set(stream::iter([KeyValue {
            key: "key_e_eq".to_owned(),
            value: "value2_eq".to_owned(),
        }]))
        .await?;

    let all_eq = client
        .eq(stream::iter([
            Key {
                key: "key_a_eq".to_owned(),
            },
            Key {
                key: "key_b_eq".to_owned(),
            },
            Key {
                key: "key_c_eq".to_owned(),
            },
            Key {
                key: "key_d_eq".to_owned(),
            },
            Key {
                key: "key_e_eq".to_owned(),
            },
        ]))
        .await?
        .into_inner()
        .value;
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
        client
            .set(stream::iter([KeyValue {
                key: key.to_owned(),
                value: format!("value{idx}_neq"),
            }]))
            .await?;
    }

    let all_neq = client
        .not_eq(stream::iter([
            Key {
                key: "key_a_neq".to_owned(),
            },
            Key {
                key: "key_b_neq".to_owned(),
            },
            Key {
                key: "key_c_neq".to_owned(),
            },
            Key {
                key: "key_d_neq".to_owned(),
            },
        ]))
        .await?
        .into_inner()
        .value;
    assert!(all_neq);

    client
        .set(stream::iter([KeyValue {
            key: "key_e_neq".to_owned(),
            value: "value2_neq".to_owned(),
        }]))
        .await?;

    let all_neq = client
        .not_eq(stream::iter([
            Key {
                key: "key_a_neq".to_owned(),
            },
            Key {
                key: "key_b_neq".to_owned(),
            },
            Key {
                key: "key_c_neq".to_owned(),
            },
            Key {
                key: "key_d_neq".to_owned(),
            },
            Key {
                key: "key_e_neq".to_owned(),
            },
        ]))
        .await?
        .into_inner()
        .value;
    assert!(!all_neq);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_eq_not_found() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;
    let res = client
        .eq(stream::iter([Key {
            key: "this-key-should-not-exist".to_owned(),
        }]))
        .await;
    assert!(res.is_err());
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_not_eq_not_found() -> Result<()> {
    let mut client = kv_client(KV_STORE_LOC.clone()).await?;
    let res = client
        .not_eq(stream::iter([Key {
            key: "this-key-should-not-exist".to_owned(),
        }]))
        .await;
    assert!(res.is_err());
    Ok(())
}
