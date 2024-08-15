use crate::create_runtime;
use buffdb::backend::{DatabaseBackend, KvBackend};
use buffdb::client::kv::KvClient;
use buffdb::client::query::QueryClient;
use buffdb::interop::IntoTonicStatus;
use buffdb::proto::kv::{DeleteRequest, GetRequest, SetRequest};
use buffdb::proto::query::{RawQuery, TargetStore};
use buffdb::queryable::Queryable;
use buffdb::transitive::Transitive;
use buffdb::{backend, transitive, Location};
use criterion::measurement::Measurement;
use criterion::{criterion_group, BatchSize, Criterion};
use futures::stream;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use std::iter;

const INSERT_QUERIES_PER_BATCH: usize = 1_000;
const GET_QUERIES_PER_BATCH: usize = 1_000;
const DELETE_QUERIES_PER_BATCH: usize = 250;

const PREFILL_ROW_COUNT: usize = 10_000;

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

async fn create_query_client<Backend, const RETURN_COUNT: usize>() -> (
    Transitive<QueryClient<tonic::transport::Channel>>,
    Vec<(String, String)>,
)
where
    Backend: DatabaseBackend<Error: IntoTonicStatus + Send, Connection: Send>
        + Queryable<QueryStream: Send, Connection = <Backend as DatabaseBackend>::Connection>
        + Send
        + Sync
        + 'static,
{
    let mut client = transitive::query_client::<_, _, Backend>(Location::InMemory, "/dev/null")
        .await
        .unwrap();
    client
        .execute(stream::iter([RawQuery {
            query: "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)".to_owned(),
            target: TargetStore::Kv as _,
        }]))
        .await
        .unwrap();
    let contents = insert_rows_raw::<_, PREFILL_ROW_COUNT, RETURN_COUNT>(&mut client).await;
    (client, contents)
}

async fn create_kv_client<Backend, const RETURN_COUNT: usize>() -> (
    Transitive<KvClient<tonic::transport::Channel>>,
    Vec<(String, String)>,
)
where
    Backend: KvBackend<
            Error: IntoTonicStatus + Send,
            GetStream: Send,
            SetStream: Send,
            DeleteStream: Send,
        > + 'static,
{
    let mut client = transitive::kv_client::<_, Backend>(Location::InMemory)
        .await
        .unwrap();
    let contents = insert_rows::<_, PREFILL_ROW_COUNT, RETURN_COUNT>(&mut client).await;
    (client, contents)
}

/// Insert a large number of rows into the database, returning `RETURN_COUNT` key-value pairs. All
/// other pairs are effectively discarded, only being present in the database to ensure a real-world
/// load.
async fn insert_rows_raw<T, const INSERT_COUNT: usize, const RETURN_COUNT: usize>(
    conn: &mut QueryClient<T>,
) -> Vec<(String, String)>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody, Future: Send> + Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    T::ResponseBody: tonic::transport::Body<Data = prost::bytes::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::transport::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send,
{
    assert!(
        RETURN_COUNT <= INSERT_COUNT,
        "cannot return {RETURN_COUNT} rows while inserting {INSERT_COUNT}"
    );

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
    .take(INSERT_COUNT)
    .collect();

    let ret = requests
        .iter()
        .cloned()
        .map(|req| req.1)
        .choose_multiple(&mut thread_rng(), RETURN_COUNT);

    let requests = requests.into_iter().map(|(req, _)| req).collect::<Vec<_>>();
    conn.execute(stream::iter(requests)).await.unwrap();

    ret
}

/// Insert a number of rows into the database, returning `RETURN_COUNT` key-value pairs. All other
/// pairs are effectively discarded, only being present in the database to ensure a real-world load.
async fn insert_rows<T, const INSERT_COUNT: usize, const RETURN_COUNT: usize>(
    conn: &mut KvClient<T>,
) -> Vec<(String, String)>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody, Future: Send> + Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    T::ResponseBody: tonic::transport::Body<Data = prost::bytes::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::transport::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send,
{
    assert!(
        RETURN_COUNT <= INSERT_COUNT,
        "cannot return {RETURN_COUNT} rows while inserting {INSERT_COUNT}"
    );

    let requests: Vec<_> = iter::repeat_with(|| SetRequest {
        key: generate_key(),
        value: generate_value(),
    })
    .take(INSERT_COUNT)
    .collect();

    conn.set(stream::iter(requests.clone())).await.unwrap();

    requests
        .into_iter()
        .map(|req| (req.key, req.value))
        .choose_multiple(&mut thread_rng(), RETURN_COUNT)
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_connection_only<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, _) = runtime.block_on(create_kv_client::<backend::Sqlite, 0>());

    c.bench_function("sqlite_connection_only", |b| {
        b.to_async(&runtime).iter_batched(
            || (stream::empty(), client.clone()),
            |(queries, mut client)| async move { client.get(queries).await.unwrap() },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_insert_raw<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, _) = runtime.block_on(create_query_client::<backend::Sqlite, 0>());

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
            |(queries, mut client)| async move { client.execute(queries).await.unwrap() },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_insert<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, _) = runtime.block_on(create_kv_client::<backend::Sqlite, 0>());

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
            |(requests, mut client)| async move { client.set(requests).await.unwrap() },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_get_raw<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, kv_pairs) =
        runtime.block_on(create_query_client::<backend::Sqlite, GET_QUERIES_PER_BATCH>());

    let queries = kv_pairs
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
fn sqlite_get<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, kv_pairs) =
        runtime.block_on(create_kv_client::<backend::Sqlite, GET_QUERIES_PER_BATCH>());

    let keys = kv_pairs
        .into_iter()
        .map(|(key, _)| GetRequest { key })
        .collect::<Vec<_>>();

    c.bench_function("sqlite_kv_get", |b| {
        b.to_async(&runtime).iter_batched(
            || (stream::iter(keys.clone()), client.clone()),
            |(keys, mut client)| async move { client.get(keys).await.unwrap() },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_delete_raw<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (mut client, kv_pairs) = runtime.block_on(create_query_client::<
        backend::Sqlite,
        DELETE_QUERIES_PER_BATCH,
    >());

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
                runtime
                    .block_on(async { client.query(stream::iter(insert_queries)).await.unwrap() });

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
fn sqlite_delete<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (mut client, kv_pairs) =
        runtime.block_on(create_kv_client::<backend::Sqlite, DELETE_QUERIES_PER_BATCH>());

    let queries: Vec<_> = kv_pairs
        .iter()
        .cloned()
        .map(|(key, _)| DeleteRequest { key })
        .collect();

    c.bench_function("sqlite_kv_delete", |b| {
        b.iter_batched(
            || {
                // Reinsert the deleted keys to avoid trying to delete non-existent keys on
                // iterations after the first.
                let mut insert_queries = vec![];
                for (key, value) in kv_pairs.iter().cloned() {
                    insert_queries.push(SetRequest { key, value });
                }
                runtime.block_on(async { client.set(stream::iter(insert_queries)).await.unwrap() });

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
    name = kv;
    config = super::criterion_config();
    targets =
        sqlite_connection_only,
        sqlite_insert,
        sqlite_insert_raw,
        sqlite_get,
        sqlite_get_raw,
        sqlite_delete,
        sqlite_delete_raw,
);
