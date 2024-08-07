#![allow(
    missing_docs,
    clippy::missing_docs_in_private_items,
    unused_results,
    clippy::unwrap_used
)]

use buffdb::proto::kv::SetRequest;
use buffdb::{backend, transitive, Location};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::stream;
use rand::distributions::{Alphanumeric, DistString};
use rusqlite::Connection;
use std::hint::black_box;

fn sqlite_insert(c: &mut Criterion) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)", [])
        .unwrap();
    let mut query = conn
        .prepare("INSERT INTO kv (key, value) VALUES (?, ?)")
        .unwrap();
    let mut rng = rand::thread_rng();

    c.bench_function("sqlite_kv_insert", |b| {
        b.iter_batched(
            || {
                (
                    // TODO randomize the length?
                    Alphanumeric.sample_string(&mut rng, 20),
                    Alphanumeric.sample_string(&mut rng, 20),
                )
            },
            |key_value_pair| {
                query.execute(black_box(key_value_pair)).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn buffdb_insert(c: &mut Criterion) {
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

    // TODO possibly initialize the client before the first query?

    c.bench_function("buffdb_kv_insert", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                (
                    // TODO randomize the length?
                    Alphanumeric.sample_string(&mut rng, 20),
                    Alphanumeric.sample_string(&mut rng, 20),
                    client.clone(),
                )
            },
            |(key, value, mut client)| async move {
                client
                    .set(stream::iter([SetRequest { key, value }]))
                    .await
                    .unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(sqlite, sqlite_insert);
criterion_group!(buffdb, buffdb_insert);
criterion_main!(sqlite, buffdb);
