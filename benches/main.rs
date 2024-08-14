#![allow(
    missing_docs,
    clippy::missing_docs_in_private_items,
    unused_results,
    clippy::unwrap_used
)]

mod blob;
mod kv;

use criterion::measurement::Measurement;
use criterion::Criterion;
use std::time::Duration;

fn criterion_config() -> Criterion<impl Measurement> {
    Criterion::default()
        .with_measurement(criterion_cycles_per_byte::CyclesPerByte)
        .measurement_time(Duration::from_secs(10))
        .configure_from_args()
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

criterion::criterion_main!(blob::blob, kv::kv);
