/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Two-runtime + S3 + per-engine async fixtures push the type-checker over
// the default 128 query depth limit on stable rustc.
#![recursion_limit = "256"]

//! Integration tests for `refresh_mode: snapshot`.
//!
//! These tests exercise the full snapshot-refresh flow with two cooperating
//! `Runtime` instances:
//!
//! * a **writer** that owns the federated source, runs `refresh_mode: full`,
//!   and is configured to create a new snapshot on every refresh, and
//! * a **reader** that runs `refresh_mode: snapshot`, bootstraps from the
//!   shared snapshot store, polls for newer snapshots, and reloads its
//!   accelerator under a `SwappableTableProvider` swap.
//!
//! The shared snapshot store is backed by a real S3 bucket (mirroring how
//! customers actually deploy snapshot refresh in production). Tests therefore
//! require AWS credentials — set `AWS_SNAPSHOT_KEY`/`AWS_SNAPSHOT_SECRET`, or
//! configure `AWS_PROFILE` with read/write access to the test bucket. See
//! `snapshot_refresh/mod.rs` for the full setup. Each accelerator that
//! advertises `supports_snapshot_reload()` (`DuckDB`, `SQLite`, `Cayenne`, `Turso`)
//! gets its own end-to-end test.

mod snapshot_refresh;
mod utils;

use runtime::Runtime;
use std::sync::Once;
use tracing_subscriber::EnvFilter;

static INIT_TRACING: Once = Once::new();

fn init_tracing(default_level: Option<&str>) {
    INIT_TRACING.call_once(|| {
        let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
            (_, Some(log)) => EnvFilter::new(log),
            (Some(level), None) => EnvFilter::new(level),
            _ => EnvFilter::new(
                "runtime=DEBUG,runtime_acceleration=DEBUG,datafusion-federation=INFO,info",
            ),
        };

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(filter)
            .with_ansi(true)
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

/// Re-export so the `snapshot_refresh` submodule can run queries through the
/// shared helpers without re-implementing them.
pub(crate) async fn run_query(
    rt: &std::sync::Arc<Runtime>,
    query: &str,
) -> Result<Vec<arrow::array::RecordBatch>, anyhow::Error> {
    use futures::StreamExt;
    let mut result = rt.datafusion().query_builder(query).build().run().await?;
    let mut results = Vec::new();
    while let Some(batch) = result.data.next().await {
        results.push(batch?);
    }
    Ok(results)
}
