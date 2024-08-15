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
use tracing::Dispatch;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_timing::{Builder, Histogram};

fn criterion_config() -> Criterion<impl Measurement> {
    Criterion::default()
        .with_measurement(criterion_cycles_per_byte::CyclesPerByte)
        .measurement_time(Duration::from_secs(5))
        .configure_from_args()
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

#[allow(clippy::significant_drop_tightening)] // false positive
fn main() {
    let timing_layer = Builder::default().layer(|| Histogram::new(1).unwrap());
    let downcaster = timing_layer.downcaster();

    let subscriber = tracing_subscriber::registry()
        // Without filtering to `buffdb`, all tracing events from all dependencies are included.
        // Unless you have infinite RAM, this will quickly exhaust your memory.
        .with(Layer::new().with_filter(EnvFilter::from_default_env()))
        .with(timing_layer);

    let dispatch = Dispatch::new(subscriber);
    tracing::dispatcher::set_global_default(dispatch.clone())
        .expect("setting tracing default failed");

    blob::blob();
    kv::kv();

    Criterion::default().configure_from_args().final_summary();

    let timings = downcaster.downcast(&dispatch).unwrap();
    timings.force_synchronize();
    timings.with_histograms(|hs| {
        let span_groups = [
            "query_client",
            "sqlite_delete_raw",
            "sqlite_delete",
            "SQLite kv get query",
            "set",
            "sqlite_insert",
            "blob_client",
            "sqlite_update_both",
            "sqlite_connection_only",
            "sqlite_get",
            "kv_client",
            "store",
            "SQLite blob store query",
            "sqlite_insert_raw",
            "sqlite_update_metadata",
            "sqlite_get_raw",
        ];

        for (span_group, hs) in span_groups
            .into_iter()
            .filter_map(|span_group| Some((span_group, hs.get(span_group)?)))
        {
            #[allow(clippy::print_stdout)]
            for (event_group, h) in hs {
                println!(
                    "[{span_group}:{event_group}] mean: {:.1}ns, min: {:.1}ns, \
                        max: {:.1}ns, stdev: {:.1}ns",
                    h.mean(),
                    h.min(),
                    h.max(),
                    h.stdev(),
                );
            }
        }
    });
}
