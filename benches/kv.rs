#![allow(
    missing_docs,
    clippy::missing_docs_in_private_items,
    unused_results,
    clippy::unwrap_used
)]

use buffdb::proto::kv::SetRequest;
use buffdb::proto::query::{RawQuery, TargetStore};
use buffdb::{backend, transitive, Location};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::stream;
use rand::distributions::{Alphanumeric, DistString};
use std::iter;

// TODO randomize the length of generated strings

const QUERIES_PER_BATCH: usize = 1_000;

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
                .take(QUERIES_PER_BATCH)
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
                .take(QUERIES_PER_BATCH)
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

criterion_group!(buffdb, sqlite_insert, sqlite_insert_raw);
criterion_main!(buffdb);
