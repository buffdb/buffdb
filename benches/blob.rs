#![allow(
    missing_docs,
    clippy::missing_docs_in_private_items,
    unused_results,
    clippy::unwrap_used
)]

use buffdb::backend::BlobBackend;
use buffdb::client::blob::BlobClient;
use buffdb::interop::IntoTonicStatus;
use buffdb::proto::blob::{DeleteRequest, GetRequest, StoreRequest, UpdateRequest};
use buffdb::transitive::Transitive;
use buffdb::{backend, transitive, Location};
use criterion::measurement::Measurement;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::stream;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use std::iter;
use std::time::Duration;

const INSERT_QUERIES_PER_BATCH: usize = 1_000;
const GET_QUERIES_PER_BATCH: usize = 1_000;
const UPDATE_QUERIES_PER_BATCH: usize = 1_000;
const DELETE_QUERIES_PER_BATCH: usize = 250;

const PREFILL_ROW_COUNT: usize = 10_000;

fn generate_bytes() -> Vec<u8> {
    let mut rng = thread_rng();
    let mut v = vec![0; rng.gen_range(500..=250_000)];
    rng.fill_bytes(&mut v);
    v
}

fn generate_metadata() -> Option<String> {
    let mut rng = thread_rng();
    rng.gen_bool(0.75).then(|| {
        let len = rng.gen_range(50..=1_000);
        Alphanumeric.sample_string(&mut rng, len)
    })
}

/// Insert a number of rows into the database, returning `RETURN_COUNT` id-blob-metadata tuples. All
/// other values are effectively discarded, only being present in the database to ensure a
/// real-world load.
async fn insert_rows<T, const INSERT_COUNT: usize, const RETURN_COUNT: usize>(
    conn: &mut BlobClient<T>,
) -> Vec<(u64, Vec<u8>, Option<String>)>
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

    let requests: Vec<_> = iter::repeat_with(|| StoreRequest {
        bytes: generate_bytes(),
        metadata: generate_metadata(),
    })
    .take(INSERT_COUNT)
    .collect();

    let mut responses = conn
        .store(stream::iter(requests.clone()))
        .await
        .unwrap()
        .into_inner();
    let mut ids = Vec::new();
    while let Ok(Some(response)) = responses.message().await {
        ids.push(response.id);
    }

    ids.into_iter()
        .zip(requests.into_iter())
        .map(|(id, request)| (id, request.bytes, request.metadata))
        .choose_multiple(&mut thread_rng(), RETURN_COUNT)
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

async fn create_blob_client<Backend, const RETURN_COUNT: usize>() -> (
    Transitive<BlobClient<tonic::transport::Channel>>,
    Vec<(u64, Vec<u8>, Option<String>)>,
)
where
    Backend: BlobBackend<
            Error: IntoTonicStatus + Send,
            GetStream: Send,
            StoreStream: Send,
            UpdateStream: Send,
            DeleteStream: Send,
        > + 'static,
{
    let mut client = transitive::blob_client::<_, Backend>(Location::InMemory)
        .await
        .unwrap();
    let contents = insert_rows::<_, PREFILL_ROW_COUNT, RETURN_COUNT>(&mut client).await;
    (client, contents)
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_insert<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, _) = runtime.block_on(create_blob_client::<backend::Sqlite, 0>());

    c.bench_function("sqlite_blob_insert", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let requests: Vec<_> = iter::repeat_with(|| StoreRequest {
                    bytes: generate_bytes(),
                    metadata: generate_metadata(),
                })
                .take(INSERT_QUERIES_PER_BATCH)
                .collect();
                (stream::iter(requests), client.clone())
            },
            |(requests, mut client)| async move { client.store(requests).await.unwrap() },
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
    let (client, contents) =
        runtime.block_on(create_blob_client::<backend::Sqlite, GET_QUERIES_PER_BATCH>());
    let ids: Vec<_> = contents
        .into_iter()
        .map(|(id, _, _)| GetRequest { id })
        .collect();

    c.bench_function("sqlite_blob_get", |b| {
        b.to_async(&runtime).iter_batched(
            || (stream::iter(ids.clone()), client.clone()),
            |(id, mut client)| async move { client.get(id).await.unwrap() },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_update_data<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, contents) = runtime.block_on(create_blob_client::<
        backend::Sqlite,
        UPDATE_QUERIES_PER_BATCH,
    >());
    let ids: Vec<_> = contents.into_iter().map(|(id, _, _)| id).collect();

    c.bench_function("sqlite_blob_update_data", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let requests: Vec<_> = ids
                    .iter()
                    .map(|id| UpdateRequest {
                        id: *id,
                        bytes: Some(generate_bytes()),
                        should_update_metadata: false,
                        metadata: None,
                    })
                    .collect();
                (stream::iter(requests), client.clone())
            },
            |(requests, mut client)| async move { client.update(requests).await.unwrap() },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_update_metadata<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, contents) = runtime.block_on(create_blob_client::<
        backend::Sqlite,
        UPDATE_QUERIES_PER_BATCH,
    >());
    let ids: Vec<_> = contents.into_iter().map(|(id, _, _)| id).collect();

    c.bench_function("sqlite_blob_update_metadata", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let requests: Vec<_> = ids
                    .iter()
                    .map(|id| UpdateRequest {
                        id: *id,
                        bytes: None,
                        should_update_metadata: true,
                        metadata: generate_metadata(),
                    })
                    .collect();
                (stream::iter(requests), client.clone())
            },
            |(requests, mut client)| async move { client.update(requests).await.unwrap() },
            BatchSize::SmallInput,
        );
    });
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(c)))]
fn sqlite_update_both<M>(c: &mut Criterion<M>)
where
    M: Measurement + 'static,
{
    let runtime = create_runtime();
    let (client, contents) = runtime.block_on(create_blob_client::<
        backend::Sqlite,
        UPDATE_QUERIES_PER_BATCH,
    >());
    let ids: Vec<_> = contents.into_iter().map(|(id, _, _)| id).collect();

    c.bench_function("sqlite_blob_update_both", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let requests: Vec<_> = ids
                    .iter()
                    .map(|id| UpdateRequest {
                        id: *id,
                        bytes: Some(generate_bytes()),
                        should_update_metadata: true,
                        metadata: generate_metadata(),
                    })
                    .collect();
                (stream::iter(requests), client.clone())
            },
            |(requests, mut client)| async move { client.update(requests).await.unwrap() },
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
    let (client, _) = runtime.block_on(create_blob_client::<backend::Sqlite, 0>());

    c.bench_function("sqlite_blob_delete", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    // Insert rows that will be deleted in the benchmark.
                    let ids: Vec<_> =
                        insert_rows::<_, DELETE_QUERIES_PER_BATCH, DELETE_QUERIES_PER_BATCH>(
                            &mut client.clone(),
                        )
                        .await
                        .into_iter()
                        .map(|(id, _, _)| DeleteRequest { id })
                        .collect();
                    (stream::iter(ids), client.clone())
                })
            },
            |(queries, mut client)| {
                runtime.block_on(async { client.delete(queries).await.unwrap() })
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group! {
    name = buffdb;
    config = Criterion::default()
        .with_measurement(criterion_cycles_per_byte::CyclesPerByte)
        .measurement_time(Duration::from_secs(10));
    targets =
        sqlite_insert,
        sqlite_get,
        sqlite_update_data,
        sqlite_update_metadata,
        sqlite_update_both,
        sqlite_delete,
}
criterion_main!(buffdb);
