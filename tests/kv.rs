use anyhow::Result;
use buffdb::kv::{Key, KeyValue, Keys, Values};
use buffdb::transitive::kv_client;
use futures::{stream, StreamExt as _};
use serial_test::serial;
use std::future;
use std::path::PathBuf;
use std::sync::LazyLock;

static KV_STORE_PATH: LazyLock<PathBuf> = LazyLock::new(|| PathBuf::from("test_kv_store"));

fn key(key: impl ToString) -> Key {
    Key {
        key: key.to_string(),
    }
}

#[tokio::test]
#[serial]
async fn test_get_many() -> Result<()> {
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;

    client
        .set(KeyValue {
            key: "key_a_get_many".to_owned(),
            value: "value_a_get_many".to_owned(),
        })
        .await?;
    client
        .set(KeyValue {
            key: "key_b_get_many".to_owned(),
            value: "value_b_get_many".to_owned(),
        })
        .await?;

    let values_stream = client
        .get_many(stream::once(future::ready(Keys {
            keys: vec!["key_a_get_many".to_owned(), "key_b_get_many".to_owned()],
        })))
        .await?
        .into_inner();

    let values = values_stream.collect::<Vec<_>>().await;
    match values.as_slice() {
        [Ok(Values { ref values })] => {
            assert_eq!(values, &["value_a_get_many", "value_b_get_many"]);
        }
        _ => assert_eq!(values.len(), 1, "exactly one value was expected"),
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_eq() -> Result<()> {
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;

    for key in ["key_a_eq", "key_b_eq", "key_c_eq", "key_d_eq"] {
        client
            .set(KeyValue {
                key: key.to_owned(),
                value: "value_eq".to_owned(),
            })
            .await?;
    }

    let all_eq = client
        .eq(stream::iter([
            key("key_a_eq"),
            key("key_b_eq"),
            key("key_c_eq"),
            key("key_d_eq"),
        ]))
        .await?
        .into_inner()
        .value;
    assert!(all_eq);

    client
        .set(KeyValue {
            key: "key_e_eq".to_owned(),
            value: "value2_eq".to_owned(),
        })
        .await?;

    let all_eq = client
        .eq(stream::iter([
            key("key_a_eq"),
            key("key_b_eq"),
            key("key_c_eq"),
            key("key_d_eq"),
            key("key_e_eq"),
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
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;

    for (idx, key) in ["key_a_neq", "key_b_neq", "key_c_neq", "key_d_neq"]
        .into_iter()
        .enumerate()
    {
        client
            .set(KeyValue {
                key: key.to_owned(),
                value: format!("value{idx}_neq"),
            })
            .await?;
    }

    let all_neq = client
        .not_eq(stream::iter([
            key("key_a_neq"),
            key("key_b_neq"),
            key("key_c_neq"),
            key("key_d_neq"),
        ]))
        .await?
        .into_inner()
        .value;
    assert!(all_neq);

    client
        .set(KeyValue {
            key: "key_e_neq".to_owned(),
            value: "value2_neq".to_owned(),
        })
        .await?;

    let all_neq = client
        .not_eq(stream::iter([
            key("key_a_neq"),
            key("key_b_neq"),
            key("key_c_neq"),
            key("key_d_neq"),
            key("key_e_neq"),
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
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;
    let res = client
        .eq(stream::iter([key("this-key-should-not-exist")]))
        .await;
    assert!(res.is_err());
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_not_eq_not_found() -> Result<()> {
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;
    let res = client
        .not_eq(stream::iter([key("this-key-should-not-exist")]))
        .await;
    assert!(res.is_err());
    Ok(())
}
