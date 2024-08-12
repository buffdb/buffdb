#![allow(
    missing_docs,
    clippy::missing_docs_in_private_items,
    unused_results,
    clippy::unwrap_used
)]

use buffdb::client::kv::KvClient;
use buffdb::client::query::QueryClient;
use buffdb::proto::kv::{DeleteRequest, GetRequest, SetRequest};
use buffdb::proto::query::{RawQuery, TargetStore};
use buffdb::{backend, transitive, Location};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::stream;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use std::iter;

const INSERT_QUERIES_PER_BATCH: usize = 1_000;
const GET_QUERIES_PER_BATCH: usize = 1_000;
const DELETE_QUERIES_PER_BATCH: usize = 250;

const PREFILL_ROW_COUNT: usize = 100_000;

fn generate_key() -> String {
    let mut rng = thread_rng();
    let len = rng.gen_range(5..=25);
    Alphanumeric.sample_string(&mut rng, len)
}

fn generate_value() -> String {
    let mut rng = thread_rng();
    let len = rng.gen_range(50..=1_000);
    Alphanumeric.sample_string(&mut rng, len)
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

/// Insert a large number of rows into the database, returning `RETURN_COUNT` key-value pairs. All
/// other pairs are effectively discarded, only being present in the database to ensure a real-world
/// load.
async fn prefill_rows_raw<T, const RETURN_COUNT: usize>(
    conn: &mut QueryClient<T>,
) -> Vec<(String, String)>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody, Future: Send> + Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    T::ResponseBody: tonic::transport::Body<Data = prost::bytes::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::transport::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send,
{
    let requests: Vec<_> = iter::repeat_with(|| {
        let key = generate_key();
        let value = generate_value();
        (
            RawQuery {
                query: format!("INSERT INTO kv (key, value) VALUES ('{key}', '{value}')"),
                target: TargetStore::Kv as _,
            },
            (key, value),
        )
    })
    .take(PREFILL_ROW_COUNT)
    .collect();

    let ret = requests
        .iter()
        .cloned()
        .map(|req| req.1)
        .take(RETURN_COUNT)
        .collect();

    let requests = requests.into_iter().map(|(req, _)| req).collect::<Vec<_>>();
    conn.execute(stream::iter(requests)).await.unwrap();

    ret
}

/// Insert a large number of rows into the database, returning `RETURN_COUNT` key-value pairs. All
/// other pairs are effectively discarded, only being present in the database to ensure a real-world
/// load.
async fn prefill_rows<T, const RETURN_COUNT: usize>(conn: &mut KvClient<T>) -> Vec<(String, String)>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody, Future: Send> + Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    T::ResponseBody: tonic::transport::Body<Data = prost::bytes::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::transport::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send,
{
    let requests: Vec<_> = iter::repeat_with(|| SetRequest {
        key: generate_key(),
        value: generate_value(),
    })
    .take(PREFILL_ROW_COUNT)
    .collect();

    let ret = requests
        .iter()
        .cloned()
        .map(|req| (req.key, req.value))
        .take(RETURN_COUNT)
        .collect();

    conn.set(stream::iter(requests)).await.unwrap();

    ret
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_insert_raw(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let client = runtime.block_on(async {
        let mut client =
            transitive::query_client::<_, _, backend::Sqlite>(Location::InMemory, "/dev/null")
                .await
                .unwrap();
        client
            .execute(stream::iter([RawQuery {
                query: "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)"
                    .to_owned(),
                target: TargetStore::Kv as _,
            }]))
            .await
            .unwrap();
        client
    });

    c.bench_function("sqlite_kv_insert_raw", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let queries: Vec<_> = iter::repeat_with(|| {
                    let key = generate_key();
                    let value = generate_value();
                    RawQuery {
                        // No risk of SQL injection here as the values are known to be alphanumeric.
                        // Even if they weren't, it's a transitive in-memory database.
                        query: format!("INSERT INTO kv (key, value) VALUES ('{key}', '{value}')"),
                        target: TargetStore::Kv as _,
                    }
                })
                .take(INSERT_QUERIES_PER_BATCH)
                .collect();
                (stream::iter(queries), client.clone())
            },
            |(queries, mut client)| async move {
                client.execute(queries).await.unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_insert(c: &mut Criterion) {
    let runtime = create_runtime();

    let client = runtime.block_on(async {
        transitive::kv_client::<_, backend::Sqlite>(Location::InMemory)
            .await
            .unwrap()
    });

    // Initializing the client before the first query should not be necessary, as it will be done
    // during the warmup phase.

    c.bench_function("sqlite_kv_insert", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let requests: Vec<_> = iter::repeat_with(|| SetRequest {
                    key: generate_key(),
                    value: generate_value(),
                })
                .take(INSERT_QUERIES_PER_BATCH)
                .collect();
                (stream::iter(requests), client.clone())
            },
            |(requests, mut client)| async move {
                client.set(requests).await.unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_get_raw(c: &mut Criterion) {
    let runtime = create_runtime();

    let (client, keys) = runtime.block_on(async {
        let mut client =
            transitive::query_client::<_, _, backend::Sqlite>(Location::InMemory, "/dev/null")
                .await
                .unwrap();
        let keys = prefill_rows_raw::<_, GET_QUERIES_PER_BATCH>(&mut client).await;
        (client, keys)
    });

    let queries = keys
        .into_iter()
        .map(|(key, _)| RawQuery {
            query: format!("SELECT (key, value) FROM kv WHERE key = '{key}'"),
            target: TargetStore::Kv as _,
        })
        .collect::<Vec<_>>();

    c.bench_function("sqlite_kv_get_raw", |b| {
        b.to_async(&runtime).iter_batched(
            || (stream::iter(queries.clone()), client.clone()),
            |(queries, mut client)| async move { client.query(queries).await.unwrap() },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_get(c: &mut Criterion) {
    let runtime = create_runtime();

    let (client, keys) = runtime.block_on(async {
        let mut client = transitive::kv_client::<_, backend::Sqlite>(Location::InMemory)
            .await
            .unwrap();
        let keys = prefill_rows::<_, GET_QUERIES_PER_BATCH>(&mut client).await;
        (client, keys)
    });

    let keys = keys
        .into_iter()
        .map(|(key, _)| GetRequest { key })
        .collect::<Vec<_>>();

    c.bench_function("sqlite_kv_get", |b| {
        b.to_async(&runtime).iter_batched(
            || (stream::iter(keys.clone()), client.clone()),
            |(keys, mut client)| async move {
                client.get(keys).await.unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_delete_raw(c: &mut Criterion) {
    let runtime = create_runtime();

    let (mut client, kv_pairs) = runtime.block_on(async {
        let mut client =
            transitive::query_client::<_, _, backend::Sqlite>(Location::InMemory, "/dev/null")
                .await
                .unwrap();
        let keys = prefill_rows_raw::<_, DELETE_QUERIES_PER_BATCH>(&mut client).await;
        (client, keys)
    });

    let queries = kv_pairs
        .iter()
        .map(|(key, _)| RawQuery {
            query: format!("DELETE FROM kv WHERE key = '{key}'"),
            target: TargetStore::Kv as _,
        })
        .collect::<Vec<_>>();

    c.bench_function("sqlite_kv_delete_raw", |b| {
        b.iter_batched(
            || {
                // Reinsert the deleted keys to avoid trying to delete non-existent keys on
                // iterations after the first.
                let mut insert_queries = vec![];
                for (key, value) in &kv_pairs {
                    insert_queries.push(RawQuery {
                        query: format!("INSERT INTO kv (key, value) VALUES ('{key}', '{value}')"),
                        target: TargetStore::Kv as _,
                    });
                }
                runtime.block_on(async {
                    client.query(stream::iter(insert_queries)).await.unwrap();
                });

                (stream::iter(queries.clone()), client.clone())
            },
            |(queries, mut client)| {
                runtime.block_on(async { client.query(queries).await.unwrap() })
            },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_delete(c: &mut Criterion) {
    let runtime = create_runtime();

    let (mut client, kv_pairs) = runtime.block_on(async {
        let mut client = transitive::kv_client::<_, backend::Sqlite>(Location::InMemory)
            .await
            .unwrap();
        let keys = prefill_rows::<_, DELETE_QUERIES_PER_BATCH>(&mut client).await;
        (client, keys)
    });

    let queries = kv_pairs
        .iter()
        .cloned()
        .map(|(key, _)| DeleteRequest { key })
        .collect::<Vec<_>>();

    c.bench_function("sqlite_kv_delete", |b| {
        b.iter_batched(
            || {
                // Reinsert the deleted keys to avoid trying to delete non-existent keys on
                // iterations after the first.
                let mut insert_queries = vec![];
                for (key, value) in kv_pairs.iter().cloned() {
                    insert_queries.push(SetRequest { key, value });
                }
                runtime.block_on(async {
                    client.set(stream::iter(insert_queries)).await.unwrap();
                });

                (stream::iter(queries.clone()), client.clone())
            },
            |(queries, mut client)| {
                runtime.block_on(async { client.delete(queries).await.unwrap() })
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    buffdb,
    sqlite_insert,
    sqlite_insert_raw,
    sqlite_get,
    sqlite_get_raw,
    sqlite_delete,
    sqlite_delete_raw,
);
criterion_main!(buffdb);
