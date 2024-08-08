#![allow(
    missing_docs,
    clippy::missing_docs_in_private_items,
    unused_results,
    clippy::unwrap_used
)]

use buffdb::client::kv::KvClient;
use buffdb::client::query::QueryClient;
use buffdb::proto::kv::{GetRequest, SetRequest};
use buffdb::proto::query::{RawQuery, TargetStore};
use buffdb::{backend, transitive, Location};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::stream;
use rand::distributions::{Alphanumeric, DistString};
use std::iter;

// TODO randomize the length of generated strings

const INSERT_QUERIES_PER_BATCH: usize = 1_000;
const GET_QUERIES_PER_BATCH: usize = 1_000;

/// Insert one million rows into the database, returning `RETURN_COUNT` keys. All other
/// keys are effectively discarded, only being present in the database to ensure a real-world load.
async fn insert_1m_rows_raw<T, const RETURN_COUNT: usize>(conn: &mut QueryClient<T>) -> Vec<String>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody, Future: Send> + Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    T::ResponseBody: tonic::transport::Body<Data = prost::bytes::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::transport::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send,
{
    let mut rng = rand::rngs::OsRng;
    let requests: Vec<_> = iter::from_fn(|| {
        let key = Alphanumeric.sample_string(&mut rng, 20);
        let value = Alphanumeric.sample_string(&mut rng, 20);
        Some((
            RawQuery {
                query: format!("INSERT INTO kv (key, value) VALUES ('{key}', '{value}')"),
                target: TargetStore::Kv as _,
            },
            key,
        ))
    })
    .take(1_000_000)
    .collect();

    let ret = requests
        .iter()
        .map(|req| req.1.clone())
        .take(RETURN_COUNT)
        .collect();

    let requests = requests.into_iter().map(|(req, _)| req).collect::<Vec<_>>();
    conn.execute(stream::iter(requests)).await.unwrap();

    ret
}

/// Insert one million rows into the database, returning `RETURN_COUNT` keys. All other
/// keys are effectively discarded, only being present in the database to ensure a real-world load.
async fn insert_1m_rows<T, const RETURN_COUNT: usize>(conn: &mut KvClient<T>) -> Vec<String>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody, Future: Send> + Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    T::ResponseBody: tonic::transport::Body<Data = prost::bytes::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::transport::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send,
{
    let mut rng = rand::rngs::OsRng;
    let requests: Vec<_> = iter::from_fn(|| {
        let key = Alphanumeric.sample_string(&mut rng, 20);
        let value = Alphanumeric.sample_string(&mut rng, 20);
        Some(SetRequest { key, value })
    })
    .take(1_000_000)
    .collect();

    let ret = requests
        .iter()
        .map(|req| req.key.clone())
        .take(RETURN_COUNT)
        .collect();

    conn.set(stream::iter(requests)).await.unwrap();

    ret
}

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
    let mut rng = rand::thread_rng();

    c.bench_function("sqlite_kv_insert_raw", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let queries: Vec<_> = iter::from_fn(|| {
                    let key = Alphanumeric.sample_string(&mut rng, 20);
                    let value = Alphanumeric.sample_string(&mut rng, 20);
                    Some(RawQuery {
                        // No risk of SQL injection here as the values are known to be alphanumeric.
                        // Even if they weren't, it's a transitive in-memory database.
                        query: format!("INSERT INTO kv (key, value) VALUES ('{key}', '{value}')"),
                        target: TargetStore::Kv as _,
                    })
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

fn sqlite_insert(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let client = runtime.block_on(async {
        transitive::kv_client::<_, backend::Sqlite>(Location::InMemory)
            .await
            .unwrap()
    });
    let mut rng = rand::thread_rng();

    // Initializing the client before the first query should not be necessary, as it will be done
    // during the warmup phase.

    c.bench_function("sqlite_kv_insert", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let requests: Vec<_> = iter::from_fn(|| {
                    let key = Alphanumeric.sample_string(&mut rng, 20);
                    let value = Alphanumeric.sample_string(&mut rng, 20);
                    Some(SetRequest { key, value })
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

fn sqlite_get_raw(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let (client, keys) = runtime.block_on(async {
        let mut client =
            transitive::query_client::<_, _, backend::Sqlite>(Location::InMemory, "/dev/null")
                .await
                .unwrap();
        let keys = insert_1m_rows_raw::<_, GET_QUERIES_PER_BATCH>(&mut client).await;
        (client, keys)
    });

    let queries = keys
        .into_iter()
        .map(|key| RawQuery {
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

fn sqlite_get(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let (client, keys) = runtime.block_on(async {
        let mut client = transitive::kv_client::<_, backend::Sqlite>(Location::InMemory)
            .await
            .unwrap();
        let keys = insert_1m_rows::<_, GET_QUERIES_PER_BATCH>(&mut client).await;
        (client, keys)
    });

    let keys = keys
        .into_iter()
        .map(|key| GetRequest { key })
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

criterion_group!(
    buffdb,
    sqlite_insert,
    sqlite_insert_raw,
    sqlite_get,
    sqlite_get_raw
);
criterion_main!(buffdb);
