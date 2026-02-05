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

//! Performance integration tests for measuring the effect of snapshot creation
//! on accelerator performance under concurrent insert and query load.
//!
//! These tests measure the lock contention effects when snapshots are created
//! while queries and inserts are running concurrently.

// Allow test-specific lint exceptions for metrics calculations
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::unused_self)]
#![allow(clippy::wrong_self_convention)]
#![allow(clippy::trivially_copy_pass_by_ref)]

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::init_tracing;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_components::arrow::write::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use futures::future::join_all;
use runtime::Runtime;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_acceleration::snapshot::{
    AccelerationEngine, ForceCreate, SnapshotBehavior as RuntimeSnapshotBehavior, SnapshotManager,
};
use spicepod::acceleration::SnapshotsCompaction;
use spicepod::component::snapshot::Snapshots;
use tokio::sync::Mutex;

/// Mock checkpointer that introduces configurable delay to simulate real checkpoint behavior
struct DelayedMockCheckpointer {
    checkpoint_delay: Duration,
}

impl DelayedMockCheckpointer {
    fn new(checkpoint_delay: Duration) -> Self {
        Self { checkpoint_delay }
    }
}

#[async_trait]
impl DatasetCheckpointer for DelayedMockCheckpointer {
    async fn exists(&self) -> bool {
        true
    }

    async fn checkpoint(
        &self,
        _schema: &arrow::datatypes::SchemaRef,
    ) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        // Simulate checkpoint work
        tokio::time::sleep(self.checkpoint_delay).await;
        Ok(())
    }

    async fn get_schema(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<arrow::datatypes::SchemaRef>> {
        Ok(None)
    }

    async fn last_checkpoint_time(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<SystemTime>> {
        Ok(None)
    }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}"))
}

/// Metrics collected during contention testing
#[derive(Debug, Default)]
struct ContentionMetrics {
    /// All individual operation latencies in milliseconds
    latencies_ms: Vec<u64>,
    /// Number of successful operations
    successful_ops: u64,
    /// Number of failed operations
    failed_ops: u64,
}

impl ContentionMetrics {
    fn new() -> Self {
        Self::default()
    }

    fn record_success(&mut self, duration: Duration) {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "duration in ms fits in u64"
        )]
        self.latencies_ms.push(duration.as_millis() as u64);
        self.successful_ops += 1;
    }

    fn record_failure(&mut self) {
        self.failed_ops += 1;
    }

    fn merge(&mut self, other: ContentionMetrics) {
        self.latencies_ms.extend(other.latencies_ms);
        self.successful_ops += other.successful_ops;
        self.failed_ops += other.failed_ops;
    }

    fn percentile(&self, p: f64) -> Option<u64> {
        if self.latencies_ms.is_empty() {
            return None;
        }
        let mut sorted = self.latencies_ms.clone();
        sorted.sort_unstable();
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "percentile index calculation is safe for typical latency array sizes"
        )]
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        Some(sorted[idx.min(sorted.len() - 1)])
    }

    fn median(&self) -> Option<u64> {
        self.percentile(50.0)
    }

    fn p95(&self) -> Option<u64> {
        self.percentile(95.0)
    }

    fn p99(&self) -> Option<u64> {
        self.percentile(99.0)
    }

    fn max(&self) -> Option<u64> {
        self.latencies_ms.iter().max().copied()
    }

    fn min(&self) -> Option<u64> {
        self.latencies_ms.iter().min().copied()
    }

    fn avg(&self) -> Option<f64> {
        if self.latencies_ms.is_empty() {
            return None;
        }
        let sum: u64 = self.latencies_ms.iter().sum();
        #[expect(
            clippy::cast_precision_loss,
            reason = "precision loss acceptable for average"
        )]
        Some(sum as f64 / self.latencies_ms.len() as f64)
    }
}

/// Results from a contention test comparing baseline vs under-load performance
#[derive(Debug)]
struct ContentionTestResults {
    /// Metrics from queries without any snapshot operations
    query_baseline: ContentionMetrics,
    /// Metrics from queries while snapshots are being created
    query_under_load: ContentionMetrics,
    /// Metrics from snapshot creation operations
    snapshot_metrics: ContentionMetrics,
}

impl ContentionTestResults {
    fn report(&self) {
        println!("\n=== Snapshot Lock Contention Test Results ===\n");

        println!("Query Baseline (no snapshots):");
        Self::print_metrics(&self.query_baseline);

        println!("\nQuery Under Load (with concurrent snapshots):");
        Self::print_metrics(&self.query_under_load);

        println!("\nSnapshot Creation:");
        Self::print_metrics(&self.snapshot_metrics);

        // Calculate degradation
        if let (Some(baseline_p50), Some(load_p50)) =
            (self.query_baseline.median(), self.query_under_load.median())
        {
            #[expect(
                clippy::cast_precision_loss,
                reason = "precision loss acceptable for percentage"
            )]
            let degradation = if baseline_p50 > 0 {
                ((load_p50 as f64 - baseline_p50 as f64) / baseline_p50 as f64) * 100.0
            } else {
                0.0
            };
            println!("\nP50 Latency Degradation: {degradation:.1}%");
        }

        if let (Some(baseline_p99), Some(load_p99)) =
            (self.query_baseline.p99(), self.query_under_load.p99())
        {
            #[expect(
                clippy::cast_precision_loss,
                reason = "precision loss acceptable for percentage"
            )]
            let degradation = if baseline_p99 > 0 {
                ((load_p99 as f64 - baseline_p99 as f64) / baseline_p99 as f64) * 100.0
            } else {
                0.0
            };
            println!("P99 Latency Degradation: {degradation:.1}%");
        }
    }

    fn print_metrics(metrics: &ContentionMetrics) {
        println!(
            "  Operations: {} successful, {} failed",
            metrics.successful_ops, metrics.failed_ops
        );
        if let Some(min) = metrics.min() {
            println!("  Min:    {min}ms");
        }
        if let Some(median) = metrics.median() {
            println!("  Median: {median}ms");
        }
        if let Some(avg) = metrics.avg() {
            println!("  Avg:    {avg:.2}ms");
        }
        if let Some(p95) = metrics.p95() {
            println!("  P95:    {p95}ms");
        }
        if let Some(p99) = metrics.p99() {
            println!("  P99:    {p99}ms");
        }
        if let Some(max) = metrics.max() {
            println!("  Max:    {max}ms");
        }
    }
}

/// Create a test accelerator table with sample data
fn create_test_accelerator(num_rows: usize) -> anyhow::Result<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "test data size is small"
    )]
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let names: Vec<String> = (0..num_rows).map(|i| format!("item_{i}")).collect();
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "test data size is small"
    )]
    let values: Vec<i32> = (0..num_rows as i32).map(|i| i * 10).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(values)),
        ],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

/// Run queries against the accelerator, measuring latency
async fn run_query_workload(
    accelerator: &Arc<dyn TableProvider>,
    accelerator_write_mutex: &Arc<Mutex<()>>,
    stop_signal: &Arc<AtomicBool>,
    require_lock: bool,
) -> ContentionMetrics {
    let mut metrics = ContentionMetrics::new();
    let queries = [
        "SELECT COUNT(*) FROM test_table",
        "SELECT SUM(value) FROM test_table",
        "SELECT name, value FROM test_table WHERE id < 100",
        "SELECT id, name FROM test_table ORDER BY value DESC LIMIT 10",
    ];

    let ctx = SessionContext::new();
    if ctx
        .register_table("test_table", Arc::clone(accelerator))
        .is_err()
    {
        return metrics;
    }

    let mut query_idx = 0;
    while !stop_signal.load(Ordering::Relaxed) {
        let query = queries[query_idx % queries.len()];
        let start = Instant::now();

        // Optionally acquire the write mutex to simulate read contention
        let _lock_guard = if require_lock {
            Some(accelerator_write_mutex.lock().await)
        } else {
            None
        };

        match ctx.sql(query).await {
            Ok(df) => match df.collect().await {
                Ok(_) => metrics.record_success(start.elapsed()),
                Err(_) => metrics.record_failure(),
            },
            Err(_) => metrics.record_failure(),
        }

        query_idx += 1;

        // Small delay to prevent tight spinning
        tokio::time::sleep(Duration::from_micros(100)).await;
    }

    metrics
}

/// Run snapshot creation workload
#[expect(clippy::too_many_arguments, reason = "test helper function")]
async fn run_snapshot_workload(
    snapshot_manager: &Arc<SnapshotManager>,
    checkpointer: &Arc<dyn DatasetCheckpointer>,
    accelerator: &Arc<dyn TableProvider>,
    accelerator_write_mutex: &Arc<Mutex<()>>,
    federated_schema: &Arc<Schema>,
    stop_signal: &Arc<AtomicBool>,
    snapshot_interval: Duration,
) -> ContentionMetrics {
    let mut metrics = ContentionMetrics::new();
    let last_updated_at = Arc::new(std::sync::atomic::AtomicI64::new(0));

    while !stop_signal.load(Ordering::Relaxed) {
        let start = Instant::now();

        // Acquire the write mutex (this is where contention happens)
        let lock_guard = Arc::clone(accelerator_write_mutex).lock_owned().await;

        // Create checkpoint
        if checkpointer.checkpoint(federated_schema).await.is_err() {
            metrics.record_failure();
            continue;
        }

        // Get row count (simulating what the real code does)
        let row_count = {
            let ctx = SessionContext::new();
            if ctx
                .register_table("snapshot_table", Arc::clone(accelerator))
                .is_ok()
            {
                match ctx.table("snapshot_table").await {
                    Ok(df) => df.count().await.ok().and_then(|c| u64::try_from(c).ok()),
                    Err(_) => None,
                }
            } else {
                None
            }
        };

        // Create snapshot
        let result = snapshot_manager
            .create_snapshot(
                federated_schema,
                lock_guard,
                Some(last_updated_at.load(Ordering::Relaxed)),
                row_count,
                ForceCreate(false),
            )
            .await;

        match result {
            Ok(_) => metrics.record_success(start.elapsed()),
            Err(_) => metrics.record_failure(),
        }

        // Wait before next snapshot
        tokio::time::sleep(snapshot_interval).await;
    }

    metrics
}

/// Test that measures query latency degradation when snapshots are being created.
///
/// This test:
/// 1. Runs a baseline of queries without any snapshot operations
/// 2. Runs queries while concurrent snapshot operations are happening
/// 3. Compares the latency distributions to measure lock contention impact
#[tokio::test]
#[ignore = "Performance test - run manually with: cargo test snapshot_lock_contention -- --ignored --nocapture"]
async fn test_snapshot_lock_contention_effect_on_queries() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_root = unique_temp_dir("lock_contention_test");
    let snapshot_dir = temp_root.join("snapshots");
    let local_snapshot_file = temp_root.join("acceleration.db");

    tokio::fs::create_dir_all(&snapshot_dir).await?;
    tokio::fs::write(&local_snapshot_file, b"snapshot-data").await?;

    let snapshots = Snapshots {
        enabled: true,
        location: Some(format!("file://{}", snapshot_dir.display())),
        ..Snapshots::default()
    };

    let runtime = Runtime::builder().build().await;
    let snapshot_behavior = RuntimeSnapshotBehavior::create_only(
        Arc::new(snapshots),
        runtime.secrets_weak(),
        runtime.tokio_io_runtime(),
        SnapshotsCompaction::Disabled,
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    // Create accelerator with 10k rows
    let accelerator = create_test_accelerator(10_000)?;
    let accelerator_write_mutex = Arc::new(Mutex::new(()));

    let snapshot_manager = Arc::new(
        SnapshotManager::try_new(
            "lock_contention_test".to_string(),
            snapshot_behavior,
            runtime_acceleration::snapshot::AccelerationLayout::file(local_snapshot_file.clone()),
            AccelerationEngine::Cayenne,
        )
        .await
        .expect("Failed to create snapshot manager"),
    );

    let checkpointer: Arc<dyn DatasetCheckpointer> =
        Arc::new(DelayedMockCheckpointer::new(Duration::from_millis(10)));

    // Configuration
    let baseline_duration = Duration::from_secs(5);
    let load_test_duration = Duration::from_secs(10);
    let snapshot_interval = Duration::from_millis(100); // Create snapshots every 100ms
    let num_query_workers = 4;

    println!("\n=== Phase 1: Baseline (queries only, no snapshots) ===");
    let stop_signal = Arc::new(AtomicBool::new(false));

    // Spawn query workers for baseline
    let mut baseline_handles = Vec::new();
    for _ in 0..num_query_workers {
        let accel = Arc::clone(&accelerator);
        let mutex = Arc::clone(&accelerator_write_mutex);
        let stop = Arc::clone(&stop_signal);
        baseline_handles.push(tokio::spawn(async move {
            run_query_workload(&accel, &mutex, &stop, false).await
        }));
    }

    tokio::time::sleep(baseline_duration).await;
    stop_signal.store(true, Ordering::Relaxed);

    let baseline_results: Vec<ContentionMetrics> = join_all(baseline_handles)
        .await
        .into_iter()
        .filter_map(std::result::Result::ok)
        .collect();

    let mut query_baseline = ContentionMetrics::new();
    for result in baseline_results {
        query_baseline.merge(result);
    }

    println!(
        "Baseline complete: {} queries executed",
        query_baseline.successful_ops
    );

    println!("\n=== Phase 2: Under Load (queries + concurrent snapshots) ===");
    let stop_signal = Arc::new(AtomicBool::new(false));

    // Spawn query workers
    let mut load_handles = Vec::new();
    for _ in 0..num_query_workers {
        let accel = Arc::clone(&accelerator);
        let mutex = Arc::clone(&accelerator_write_mutex);
        let stop = Arc::clone(&stop_signal);
        load_handles.push(tokio::spawn(async move {
            run_query_workload(&accel, &mutex, &stop, false).await
        }));
    }

    // Spawn snapshot worker
    let snap_manager = Arc::clone(&snapshot_manager);
    let snap_checkpointer = Arc::clone(&checkpointer);
    let snap_accel = Arc::clone(&accelerator);
    let snap_mutex = Arc::clone(&accelerator_write_mutex);
    let snap_schema = Arc::clone(&schema);
    let snap_stop = Arc::clone(&stop_signal);
    let snapshot_handle = tokio::spawn(async move {
        run_snapshot_workload(
            &snap_manager,
            &snap_checkpointer,
            &snap_accel,
            &snap_mutex,
            &snap_schema,
            &snap_stop,
            snapshot_interval,
        )
        .await
    });

    tokio::time::sleep(load_test_duration).await;
    stop_signal.store(true, Ordering::Relaxed);

    let load_results: Vec<ContentionMetrics> = join_all(load_handles)
        .await
        .into_iter()
        .filter_map(std::result::Result::ok)
        .collect();

    let snapshot_metrics = snapshot_handle.await?;

    let mut query_under_load = ContentionMetrics::new();
    for result in load_results {
        query_under_load.merge(result);
    }

    println!(
        "Load test complete: {} queries, {} snapshots",
        query_under_load.successful_ops, snapshot_metrics.successful_ops
    );

    // Report results
    let results = ContentionTestResults {
        query_baseline,
        query_under_load,
        snapshot_metrics,
    };
    results.report();

    // Cleanup
    tokio::fs::remove_dir_all(&temp_root).await?;

    // Assertions - these are soft limits that can be tuned
    // The main purpose is to detect severe regressions
    if let (Some(baseline_p99), Some(load_p99)) =
        (results.query_baseline.p99(), results.query_under_load.p99())
    {
        #[expect(
            clippy::cast_precision_loss,
            reason = "precision loss acceptable for percentage"
        )]
        let degradation_pct = if baseline_p99 > 0 {
            ((load_p99 as f64 - baseline_p99 as f64) / baseline_p99 as f64) * 100.0
        } else {
            0.0
        };

        // Allow up to 500% P99 degradation (5x slower) under snapshot load
        // This is a high threshold to avoid flaky tests; real monitoring would use tighter bounds
        assert!(
            degradation_pct < 500.0,
            "P99 latency degradation of {degradation_pct:.1}% exceeds 500% threshold"
        );
    }

    Ok(())
}

/// Test that runs against different accelerator engines to compare lock behavior
///
/// This test creates real accelerator instances for each supported engine type
/// and measures the lock contention impact when snapshots are created during query load.
///
/// Run with: `cargo test -p runtime --features "snapshots duckdb sqlite" test_lock_contention_across_accelerator_engines -- --ignored --nocapture`
#[tokio::test]
#[ignore = "Performance test - requires accelerator features and manual execution"]
async fn test_lock_contention_across_accelerator_engines() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    println!("\n=== Lock Contention Comparison Across Accelerator Engines ===\n");

    #[cfg(feature = "duckdb")]
    {
        println!("--- Testing DuckDB Engine ---");
        if let Err(e) = run_engine_contention_test(EngineType::DuckDB).await {
            println!("DuckDB test failed: {e}");
        }
    }

    #[cfg(feature = "sqlite")]
    {
        println!("\n--- Testing SQLite Engine ---");
        if let Err(e) = run_engine_contention_test(EngineType::Sqlite).await {
            println!("SQLite test failed: {e}");
        }
    }

    // Cayenne (Arrow) is always available
    {
        println!("\n--- Testing Cayenne (Arrow) Engine ---");
        if let Err(e) = run_engine_contention_test(EngineType::Cayenne).await {
            println!("Cayenne test failed: {e}");
        }
    }

    println!("\n=== Engine Comparison Complete ===\n");

    Ok(())
}

/// Supported engine types for contention testing
#[derive(Debug, Clone, Copy)]
enum EngineType {
    #[cfg(feature = "duckdb")]
    DuckDB,
    #[cfg(feature = "sqlite")]
    Sqlite,
    Cayenne,
}

impl std::fmt::Display for EngineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "duckdb")]
            Self::DuckDB => write!(f, "DuckDB"),
            #[cfg(feature = "sqlite")]
            Self::Sqlite => write!(f, "SQLite"),
            Self::Cayenne => write!(f, "Cayenne"),
        }
    }
}

impl EngineType {
    fn to_acceleration_engine(self) -> AccelerationEngine {
        match self {
            #[cfg(feature = "duckdb")]
            Self::DuckDB => AccelerationEngine::DuckDB,
            #[cfg(feature = "sqlite")]
            Self::Sqlite => AccelerationEngine::Sqlite,
            Self::Cayenne => AccelerationEngine::Cayenne,
        }
    }
}

/// Run contention test for a specific engine type
async fn run_engine_contention_test(engine_type: EngineType) -> anyhow::Result<()> {
    use datafusion::common::{Constraints, ToDFSchema};
    use datafusion_expr::CreateExternalTable;
    use runtime::dataaccelerator::DataAccelerator;
    use std::collections::HashMap;

    let temp_root = unique_temp_dir(&format!("lock_contention_{engine_type}"));
    let snapshot_dir = temp_root.join("snapshots");
    let db_file = temp_root.join(format!("acceleration.{engine_type}"));

    tokio::fs::create_dir_all(&snapshot_dir).await?;
    tokio::fs::create_dir_all(&temp_root).await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema))?;

    // Create engine-specific accelerator
    let accelerator: Arc<dyn TableProvider> = match engine_type {
        #[cfg(feature = "duckdb")]
        EngineType::DuckDB => {
            use runtime::dataaccelerator::duckdb::DuckDBAccelerator;

            let mut options = HashMap::new();
            options.insert("open".to_string(), db_file.display().to_string());

            let cmd = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_table"),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                or_replace: false,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options,
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };

            let engine = DuckDBAccelerator::new();
            engine
                .create_external_table(cmd, None, vec![])
                .await
                .map_err(|e| anyhow::anyhow!("DuckDB table creation failed: {e}"))?
        }
        #[cfg(feature = "sqlite")]
        EngineType::Sqlite => {
            use runtime::dataaccelerator::sqlite::SqliteAccelerator;

            let mut options = HashMap::new();
            options.insert("file".to_string(), db_file.display().to_string());

            let cmd = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_table"),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                or_replace: false,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options,
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };

            let engine = SqliteAccelerator::new();
            engine
                .create_external_table(cmd, None, vec![])
                .await
                .map_err(|e| anyhow::anyhow!("SQLite table creation failed: {e}"))?
        }
        EngineType::Cayenne => {
            // For Cayenne/Arrow, use MemTable as it's in-memory
            create_test_accelerator(10_000)?
        }
    };

    // Insert test data for file-based engines
    #[cfg(any(feature = "duckdb", feature = "sqlite"))]
    match engine_type {
        #[cfg(feature = "duckdb")]
        EngineType::DuckDB => {
            insert_test_data(&accelerator, &schema, 10_000).await?;
        }
        #[cfg(feature = "sqlite")]
        EngineType::Sqlite => {
            insert_test_data(&accelerator, &schema, 10_000).await?;
        }
        _ => {}
    }

    // Set up snapshot infrastructure
    let snapshots = Snapshots {
        enabled: true,
        location: Some(format!("file://{}", snapshot_dir.display())),
        ..Snapshots::default()
    };

    let runtime = Runtime::builder().build().await;
    let snapshot_behavior = RuntimeSnapshotBehavior::create_only(
        Arc::new(snapshots),
        runtime.secrets_weak(),
        runtime.tokio_io_runtime(),
        SnapshotsCompaction::Disabled,
    );

    // Create a placeholder file for snapshot to copy
    let local_snapshot_file = temp_root.join("snapshot_source.db");
    tokio::fs::write(&local_snapshot_file, b"snapshot-data-placeholder").await?;

    let snapshot_manager = Arc::new(
        SnapshotManager::try_new(
            format!("lock_contention_{engine_type}"),
            snapshot_behavior,
            runtime_acceleration::snapshot::AccelerationLayout::file(local_snapshot_file),
            engine_type.to_acceleration_engine(),
        )
        .await
        .ok_or_else(|| anyhow::anyhow!("Failed to create snapshot manager for {engine_type}"))?,
    );

    let checkpointer: Arc<dyn DatasetCheckpointer> =
        Arc::new(DelayedMockCheckpointer::new(Duration::from_millis(5)));
    let accelerator_write_mutex = Arc::new(Mutex::new(()));

    // Run baseline (no snapshots)
    let baseline_duration = Duration::from_secs(3);
    let load_duration = Duration::from_secs(5);
    let snapshot_interval = Duration::from_millis(50);
    let num_workers = 4;

    println!(
        "  Running baseline ({:.0}s, {num_workers} workers)...",
        baseline_duration.as_secs_f64()
    );
    let stop_signal = Arc::new(AtomicBool::new(false));

    let mut baseline_handles = Vec::new();
    for _ in 0..num_workers {
        let accel = Arc::clone(&accelerator);
        let mutex = Arc::clone(&accelerator_write_mutex);
        let stop = Arc::clone(&stop_signal);
        baseline_handles.push(tokio::spawn(async move {
            run_query_workload(&accel, &mutex, &stop, false).await
        }));
    }

    tokio::time::sleep(baseline_duration).await;
    stop_signal.store(true, Ordering::Relaxed);

    let mut query_baseline = ContentionMetrics::new();
    for handle in baseline_handles {
        if let Ok(metrics) = handle.await {
            query_baseline.merge(metrics);
        }
    }

    // Run under load (with snapshots)
    println!(
        "  Running under load ({:.0}s, {num_workers} workers + snapshots)...",
        load_duration.as_secs_f64()
    );
    let stop_signal = Arc::new(AtomicBool::new(false));

    let mut load_handles = Vec::new();
    for _ in 0..num_workers {
        let accel = Arc::clone(&accelerator);
        let mutex = Arc::clone(&accelerator_write_mutex);
        let stop = Arc::clone(&stop_signal);
        load_handles.push(tokio::spawn(async move {
            run_query_workload(&accel, &mutex, &stop, false).await
        }));
    }

    let snap_manager = Arc::clone(&snapshot_manager);
    let snap_checkpointer = Arc::clone(&checkpointer);
    let snap_accel = Arc::clone(&accelerator);
    let snap_mutex = Arc::clone(&accelerator_write_mutex);
    let snap_schema = Arc::clone(&schema);
    let snap_stop = Arc::clone(&stop_signal);
    let snapshot_handle = tokio::spawn(async move {
        run_snapshot_workload(
            &snap_manager,
            &snap_checkpointer,
            &snap_accel,
            &snap_mutex,
            &snap_schema,
            &snap_stop,
            snapshot_interval,
        )
        .await
    });

    tokio::time::sleep(load_duration).await;
    stop_signal.store(true, Ordering::Relaxed);

    let mut query_under_load = ContentionMetrics::new();
    for handle in load_handles {
        if let Ok(metrics) = handle.await {
            query_under_load.merge(metrics);
        }
    }

    let snapshot_metrics = snapshot_handle.await?;

    // Report results
    let results = ContentionTestResults {
        query_baseline,
        query_under_load,
        snapshot_metrics,
    };

    println!("\n  Results for {engine_type}:");
    println!(
        "    Baseline: {} ops, median {}ms, p99 {}ms",
        results.query_baseline.successful_ops,
        results.query_baseline.median().unwrap_or(0),
        results.query_baseline.p99().unwrap_or(0),
    );
    println!(
        "    Under Load: {} ops, median {}ms, p99 {}ms",
        results.query_under_load.successful_ops,
        results.query_under_load.median().unwrap_or(0),
        results.query_under_load.p99().unwrap_or(0),
    );
    println!(
        "    Snapshots: {} created, median {}ms",
        results.snapshot_metrics.successful_ops,
        results.snapshot_metrics.median().unwrap_or(0),
    );

    if let (Some(baseline_p99), Some(load_p99)) =
        (results.query_baseline.p99(), results.query_under_load.p99())
    {
        #[expect(
            clippy::cast_precision_loss,
            reason = "precision loss acceptable for percentage"
        )]
        let degradation = if baseline_p99 > 0 {
            ((load_p99 as f64 - baseline_p99 as f64) / baseline_p99 as f64) * 100.0
        } else {
            0.0
        };
        println!("    P99 Degradation: {degradation:.1}%");
    }

    // Cleanup
    let _ = tokio::fs::remove_dir_all(&temp_root).await;

    Ok(())
}

/// Insert test data into an accelerator table
#[cfg(any(feature = "duckdb", feature = "sqlite"))]
async fn insert_test_data(
    table: &Arc<dyn TableProvider>,
    schema: &Arc<Schema>,
    num_rows: usize,
) -> anyhow::Result<()> {
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;
    use datafusion_table_providers::util::test::MockExec;

    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "test data size is small"
    )]
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let names: Vec<String> = (0..num_rows).map(|i| format!("item_{i}")).collect();
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "test data size is small"
    )]
    let values: Vec<i32> = (0..num_rows as i32).map(|i| i * 10).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(values)),
        ],
    )?;

    let ctx = SessionContext::new();
    let mock_exec = Arc::new(MockExec::new(vec![Ok(batch)], Arc::clone(schema)));

    let insert_exec = table
        .insert_into(&ctx.state(), mock_exec, InsertOp::Append)
        .await?;
    collect(insert_exec, ctx.task_ctx()).await?;

    Ok(())
}
