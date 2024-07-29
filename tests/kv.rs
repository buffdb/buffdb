use anyhow::Result;
use buffdb::kv::{Key, KeyValue, Keys, Value, Values};
use buffdb::transitive::kv_client;
use futures::{stream, Stream, StreamExt as _};
use serial_test::serial;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{future, iter};

static KV_STORE_PATH: LazyLock<PathBuf> = LazyLock::new(|| PathBuf::from("test_kv_store"));

fn key(key: impl ToString) -> Key {
    Key {
        key: key.to_string(),
    }
}

fn value(value: impl ToString) -> Value {
    Value {
        value: value.to_string(),
    }
}

fn values(values: impl IntoIterator<Item = impl ToString>) -> Values {
    Values {
        values: values.into_iter().map(|v| v.to_string()).collect(),
    }
}

async fn stream_eq<S, ST, X>(mut stream: S, expected: X) -> Result<bool, String>
where
    S: Stream<Item = Result<ST, tonic::Status>> + Unpin,
    ST: PartialEq<X::Item>,
    X: IntoIterator,
{
    let mut expected = expected.into_iter();

    while let Some(stream_item) = stream.next().await {
        match (stream_item, expected.next()) {
            (Ok(stream_item), Some(expected_item)) => {
                if stream_item != expected_item {
                    return Ok(false);
                }
            }
            (Ok(_), None) => {
                return Err("stream has more items than expected".to_owned());
            }
            (Err(err), _) => {
                return Err(format!("stream error: {err:?}"));
            }
        }
    }

    Ok(expected.next().is_none())
}

#[tokio::test]
#[serial]
async fn test_get() -> Result<()> {
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;

    client
        .set(stream::iter([KeyValue {
            key: "key_get".to_owned(),
            value: "value_get".to_owned(),
        }]))
        .await?;

    let stream = client
        .get(stream::iter([key("key_get")]))
        .await?
        .into_inner();
    assert_eq!(stream_eq(stream, [value("value_get")]).await, Ok(true));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_many() -> Result<()> {
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;

    client
        .set(stream::iter([KeyValue {
            key: "key_a_get_many".to_owned(),
            value: "value_a_get_many".to_owned(),
        }]))
        .await?;
    client
        .set(stream::iter([KeyValue {
            key: "key_b_get_many".to_owned(),
            value: "value_b_get_many".to_owned(),
        }]))
        .await?;

    let values_stream = client
        .get_many(stream::once(future::ready(Keys {
            keys: vec!["key_a_get_many".to_owned(), "key_b_get_many".to_owned()],
        })))
        .await?
        .into_inner();
    assert_eq!(
        stream_eq(
            values_stream,
            iter::once(values(["value_a_get_many", "value_b_get_many"])),
        )
        .await,
        Ok(true)
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set() -> Result<()> {
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;

    let stream = client
        .set(stream::iter([KeyValue {
            key: "key_set".to_owned(),
            value: "value_set".to_owned(),
        }]))
        .await?
        .into_inner();
    assert_eq!(stream_eq(stream, [key("key_set")]).await, Ok(true));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_delete() -> Result<()> {
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;

    client
        .set(stream::iter([KeyValue {
            key: "key_delete".to_owned(),
            value: "value_delete".to_owned(),
        }]))
        .await?;

    let stream = client
        .delete(stream::iter([key("key_delete")]))
        .await?
        .into_inner();
    assert_eq!(stream_eq(stream, [key("key_delete")]).await, Ok(true));

    let mut response = client
        .get(stream::iter([key("key_delete")]))
        .await?
        .into_inner();
    let next = response.next().await;
    assert!(matches!(next, Some(Err(_))));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_eq() -> Result<()> {
    let mut client = kv_client(KV_STORE_PATH.clone()).await?;

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
        .set(stream::iter([KeyValue {
            key: "key_e_eq".to_owned(),
            value: "value2_eq".to_owned(),
        }]))
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
            .set(stream::iter([KeyValue {
                key: key.to_owned(),
                value: format!("value{idx}_neq"),
            }]))
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
        .set(stream::iter([KeyValue {
            key: "key_e_neq".to_owned(),
            value: "value2_neq".to_owned(),
        }]))
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
