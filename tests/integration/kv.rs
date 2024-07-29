use crate::{BoxFuture, Test};
use anyhow::Result;
use buffdb::kv::{Key, KeyValue, Keys, KvClient, Values};
use futures::{stream, StreamExt as _};
use std::future::{self, Future};
use std::pin::Pin;
use tonic::transport::Channel;

fn key(key: impl ToString) -> Key {
    Key {
        key: key.to_string(),
    }
}

fn test_get_many(mut client: KvClient<Channel>) -> crate::BoxFuture<Result<()>> {
    Box::pin(async move {
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
            _ => assert!(false),
        }

        Ok(())
    })
}

// #[tokio::test]
fn test_eq(mut client: KvClient<Channel>) -> BoxFuture<Result<()>> {
    Box::pin(async move {
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
    })
}

// #[tokio::test]
fn test_not_eq(mut client: KvClient<Channel>) -> BoxFuture<Result<()>> {
    Box::pin(async move {
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
    })
}

// #[tokio::test]
fn test_eq_not_found(
    mut client: KvClient<Channel>,
) -> Pin<Box<dyn Future<Output = anyhow::Result<()>>>> {
    Box::pin(async move {
        let res = client
            .eq(stream::iter([key("this-key-should-not-exist")]))
            .await;
        assert!(res.is_err());
        Ok(())
    })
}

// #[tokio::test]
fn test_not_eq_not_found(
    mut client: KvClient<Channel>,
) -> Pin<Box<dyn Future<Output = anyhow::Result<()>>>> {
    Box::pin(async move {
        let res = client
            .not_eq(stream::iter([key("this-key-should-not-exist")]))
            .await;
        assert!(res.is_err());
        Ok(())
    })
}

inventory::submit!(Test {
    name: "test_get_many",
    file: file!(),
    f: test_get_many
});
inventory::submit!(Test {
    name: "test_eq",
    file: file!(),
    f: test_eq
});
inventory::submit!(Test {
    name: "test_not_eq",
    file: file!(),
    f: test_not_eq
});
inventory::submit!(Test {
    name: "test_eq_not_found",
    file: file!(),
    f: test_eq_not_found
});
inventory::submit!(Test {
    name: "test_not_eq_not_found",
    file: file!(),
    f: test_not_eq_not_found,
});
