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

//! Benchmarks for acceleration snapshot operations across different engines.
//!
//! These benchmarks measure the performance of snapshot operations using
//! real S3 storage with AWS SSO credentials. To run:
//!
//! 1. Authenticate with AWS SSO: `aws sso login`
//! 2. Run benchmarks: `cargo bench -p runtime-acceleration --bench snapshot_operations --features "duckdb,sqlite,turso"`
//!
//! Benchmarks include:
//! - S3 upload/download operations
//! - Metadata read/write/parse operations
//! - SHA256 checksum computation
//! - Local file copy operations (lock-release pattern)
//! - End-to-end snapshot workflow

#![allow(clippy::expect_used)]

use anyhow::{Context, Result, anyhow};
use aws_sdk_credential_bridge::{S3CredentialProvider, get_or_init_sdk_config};
use bytes::Bytes;
use chrono::Utc;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use object_store::{ClientOptions, ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath};
use runtime_acceleration::snapshot::AccelerationEngine;
use std::{hint::black_box, path::PathBuf, sync::Arc};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use uuid::Uuid;

const SNAPSHOT_BUCKET: &str = "spiceai-snapshot-integration-tests";
const SNAPSHOT_REGION: &str = "us-west-2";
const METADATA_FILE_NAME: &str = "metadata.json";
const SNAPSHOT_CHECKSUM_ALGORITHM: &str = "SHA256";

/// S3 bucket with public TPC-H demo data.
const TPCH_BUCKET: &str = "spiceai-demo-datasets";

type SchemaRef = Arc<Schema>;

/// Build an S3 object store using AWS SSO credentials from the environment.
async fn build_s3_store() -> Result<Arc<dyn ObjectStore>> {
    let config = get_or_init_sdk_config()
        .await
        .map_err(|err| anyhow!("Failed to initialize AWS credentials: {err}"))?;

    let Some(config) = config else {
        return Err(anyhow!(
            "AWS credentials are required. Run `aws sso login` first."
        ));
    };

    let builder = AmazonS3Builder::from_env()
        .with_bucket_name(SNAPSHOT_BUCKET)
        .with_region(SNAPSHOT_REGION)
        .with_client_options(ClientOptions::default())
        .with_credentials(Arc::new(
            S3CredentialProvider::from_config(config.as_ref())
                .context("Loading AWS credentials from environment")?,
        ));

    Ok(Arc::new(
        builder
            .build()
            .context("Building Amazon S3 object store client")?,
    ))
}

/// Build an anonymous S3 object store for accessing public TPC-H data.
fn build_tpch_store() -> Result<Arc<dyn ObjectStore>> {
    let builder = AmazonS3Builder::from_env()
        .with_bucket_name(TPCH_BUCKET)
        .with_region("us-east-1") // Public bucket is in us-east-1
        .with_skip_signature(true) // Anonymous access for public bucket
        .with_client_options(ClientOptions::default().with_allow_http(true));

    Ok(Arc::new(
        builder
            .build()
            .context("Building TPC-H S3 object store client")?,
    ))
}

/// Generate a unique benchmark prefix to avoid conflicts.
fn benchmark_prefix() -> String {
    format!("benchmarks/{}", Uuid::now_v7())
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn compute_sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(result)
}

/// Creates test data of specified size for benchmarking.
fn create_test_data(size_bytes: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size_bytes);
    for i in 0..size_bytes {
        data.push(((i * 7 + 13) % 256) as u8);
    }
    data
}

/// Writes a mock snapshot file to the temp directory.
fn write_mock_snapshot_file(temp_dir: &TempDir, size_bytes: usize) -> PathBuf {
    let local_path = temp_dir.path().join("snapshot.db");
    let data = create_test_data(size_bytes);
    std::fs::write(&local_path, &data).expect("write mock snapshot");
    local_path
}

/// Creates metadata JSON for a snapshot entry.
fn create_metadata_json(
    schema: &SchemaRef,
    prefix: &str,
    snapshot_location: &str,
    checksum: &str,
    size: u64,
) -> String {
    let schema_json = serde_json::to_value(schema.as_ref()).expect("serialize schema");
    let now_ms = Utc::now().timestamp_millis();

    serde_json::json!({
        "format-version": 1,
        "location": format!("s3://{SNAPSHOT_BUCKET}/{prefix}"),
        "last-updated-ms": now_ms,
        "benchmark_dataset": {
            "name": "benchmark_dataset",
            "schemas": [{
                "schema-id": 0,
                "schema": schema_json
            }],
            "current-schema-id": 0,
            "snapshots": [{
                "snapshot-id": 0,
                "timestamp-ms": now_ms,
                "snapshot": snapshot_location,
                "snapshot-checksum": checksum,
                "snapshot-checksum-algorithm": SNAPSHOT_CHECKSUM_ALGORITHM,
                "snapshot-size": size
            }],
            "current-snapshot-id": 0,
            "properties": {}
        }
    })
    .to_string()
}

/// Available acceleration engines for benchmarking.
fn available_engines() -> Vec<(&'static str, AccelerationEngine)> {
    let mut engines = vec![("cayenne", AccelerationEngine::Cayenne)];

    #[cfg(feature = "duckdb")]
    engines.push(("duckdb", AccelerationEngine::DuckDB));

    #[cfg(feature = "sqlite")]
    engines.push(("sqlite", AccelerationEngine::Sqlite));

    #[cfg(feature = "turso")]
    engines.push(("turso", AccelerationEngine::Turso));

    engines
}

/// Fetch TPC-H parquet data from the public demo bucket.
///
/// Downloads all parquet files for a given table and concatenates them.
/// Returns the raw bytes of the combined parquet data.
async fn fetch_tpch_data(table: &str) -> Result<Bytes> {
    use futures::TryStreamExt;

    let store = build_tpch_store()?;
    let prefix = ObjectPath::from(format!("tpch/{table}/"));

    // List all parquet files in the table directory
    let files: Vec<_> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .context("Listing TPC-H parquet files")?;

    // Find all .parquet files and download them
    let parquet_files: Vec<_> = files
        .iter()
        .filter(|meta| meta.location.as_ref().ends_with(".parquet"))
        .collect();

    if parquet_files.is_empty() {
        return Err(anyhow!("No parquet files found for table {table}"));
    }

    // For tables with multiple parquet files, just use the first one for simplicity
    // since we're testing I/O performance, not parquet merging
    let parquet_file = parquet_files.first().expect("has parquet file");

    let result = store
        .get(&parquet_file.location)
        .await
        .context("Fetching TPC-H parquet file")?;

    let bytes = result.bytes().await.context("Reading parquet bytes")?;
    Ok(bytes)
}

/// Cache TPC-H data locally to avoid repeated downloads during benchmarks.
fn cache_tpch_data(rt: &Runtime, table: &str, cache_dir: &TempDir) -> PathBuf {
    let cache_path = cache_dir.path().join(format!("{table}.parquet"));

    if !cache_path.exists() {
        let bytes = rt.block_on(async { fetch_tpch_data(table).await.expect("fetch tpch data") });

        std::fs::write(&cache_path, &bytes).expect("write cached tpch data");
        println!("Cached {table} ({} bytes)", bytes.len());
    }

    cache_path
}

/// Benchmark S3 upload operations for snapshots.
fn bench_s3_upload(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");

    // Build S3 store once
    let store = rt.block_on(async { build_s3_store().await.expect("Failed to build S3 store") });

    // Use smaller sizes for S3 to keep benchmark time reasonable
    let sizes: Vec<(usize, &str)> =
        vec![(1024, "1KB"), (100 * 1024, "100KB"), (1024 * 1024, "1MB")];

    let mut group = c.benchmark_group("s3_upload");
    group.sample_size(20); // Fewer samples for network I/O

    for (size, size_label) in &sizes {
        let data = create_test_data(*size);

        group.throughput(Throughput::Bytes(*size as u64));

        for (engine_name, _engine) in available_engines() {
            let bench_id = BenchmarkId::new(engine_name.to_string(), size_label);

            group.bench_with_input(bench_id, &data, |b, data| {
                b.iter(|| {
                    rt.block_on(async {
                        let prefix = benchmark_prefix();
                        let path =
                            ObjectPath::from(format!("{prefix}/dataset=benchmark/snapshot.db"));

                        store
                            .put(&path, Bytes::from(data.clone()).into())
                            .await
                            .expect("upload snapshot to S3");

                        // Cleanup
                        let _ = store.delete(&path).await;

                        black_box(());
                    });
                });
            });
        }
    }

    group.finish();
}

/// Benchmark S3 download operations for snapshots.
fn bench_s3_download(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");

    let store = rt.block_on(async { build_s3_store().await.expect("Failed to build S3 store") });

    let sizes: Vec<(usize, &str)> =
        vec![(1024, "1KB"), (100 * 1024, "100KB"), (1024 * 1024, "1MB")];

    let mut group = c.benchmark_group("s3_download");
    group.sample_size(20);

    for (size, size_label) in &sizes {
        let data = create_test_data(*size);

        group.throughput(Throughput::Bytes(*size as u64));

        for (engine_name, _engine) in available_engines() {
            let bench_id = BenchmarkId::new(engine_name.to_string(), size_label);

            // Pre-upload data for download benchmark
            let prefix = benchmark_prefix();
            let snapshot_path = ObjectPath::from(format!("{prefix}/dataset=benchmark/snapshot.db"));

            rt.block_on(async {
                store
                    .put(&snapshot_path, Bytes::from(data.clone()).into())
                    .await
                    .expect("setup snapshot in S3");
            });

            group.bench_with_input(bench_id, &snapshot_path, |b, path| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = store.get(path).await.expect("get snapshot from S3");
                        let bytes = result.bytes().await.expect("read bytes");
                        black_box(bytes.len());
                    });
                });
            });

            // Cleanup after benchmark
            rt.block_on(async {
                let _ = store.delete(&snapshot_path).await;
            });
        }
    }

    group.finish();
}

/// Benchmark S3 metadata read/write operations.
fn bench_s3_metadata_operations(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let schema = sample_schema();

    let store = rt.block_on(async { build_s3_store().await.expect("Failed to build S3 store") });

    let mut group = c.benchmark_group("s3_metadata_operations");
    group.sample_size(20);

    // Benchmark metadata write
    group.bench_function("write_metadata", |b| {
        b.iter(|| {
            rt.block_on(async {
                let prefix = benchmark_prefix();
                let path = ObjectPath::from(format!("{prefix}/{METADATA_FILE_NAME}"));
                let metadata_json =
                    create_metadata_json(&schema, &prefix, "snapshot.db", "abc123", 1024);

                store
                    .put(&path, Bytes::from(metadata_json).into())
                    .await
                    .expect("write metadata to S3");

                // Cleanup
                let _ = store.delete(&path).await;

                black_box(());
            });
        });
    });

    // Benchmark metadata read
    group.bench_function("read_metadata", |b| {
        let prefix = benchmark_prefix();
        let path = ObjectPath::from(format!("{prefix}/{METADATA_FILE_NAME}"));
        let metadata_json = create_metadata_json(&schema, &prefix, "snapshot.db", "abc123", 1024);

        rt.block_on(async {
            store
                .put(&path, Bytes::from(metadata_json).into())
                .await
                .expect("setup metadata in S3");
        });

        b.iter(|| {
            rt.block_on(async {
                let result = store.get(&path).await.expect("get metadata from S3");
                let bytes = result.bytes().await.expect("read bytes");
                let _parsed: serde_json::Value =
                    serde_json::from_slice(&bytes).expect("parse metadata");
                black_box(());
            });
        });

        // Cleanup
        rt.block_on(async {
            let _ = store.delete(&path).await;
        });
    });

    // Benchmark metadata parse (CPU-bound, no S3)
    group.bench_function("parse_metadata_cpu", |b| {
        let prefix = "test";
        let metadata_json = create_metadata_json(&schema, prefix, "snapshot.db", "abc123", 1024);
        let bytes = Bytes::from(metadata_json);

        b.iter(|| {
            let _parsed: serde_json::Value =
                serde_json::from_slice(&bytes).expect("parse metadata");
            black_box(());
        });
    });

    group.finish();
}

/// Benchmark checksum computation for different file sizes.
fn bench_checksum_computation(c: &mut Criterion) {
    let sizes: Vec<(usize, &str)> = vec![
        (1024, "1KB"),
        (100 * 1024, "100KB"),
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
    ];

    let mut group = c.benchmark_group("checksum_sha256");

    for (size, size_label) in &sizes {
        let data = create_test_data(*size);

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size_label), &data, |b, data| {
            b.iter(|| {
                let checksum = compute_sha256_hex(data);
                black_box(checksum);
            });
        });
    }

    group.finish();
}

/// Benchmark local file copy operations (lock-release pattern).
fn bench_local_file_copy(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");

    let sizes: Vec<(usize, &str)> = vec![
        (1024, "1KB"),
        (100 * 1024, "100KB"),
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
    ];

    let mut group = c.benchmark_group("local_file_copy");

    for (size, size_label) in &sizes {
        let temp_dir = TempDir::new().expect("create temp dir");
        let source_path = write_mock_snapshot_file(&temp_dir, *size);
        let dest_path = temp_dir.path().join("snapshot_copy.db");

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(size_label),
            &(source_path.clone(), dest_path.clone()),
            |b, (src, dst)| {
                b.iter(|| {
                    rt.block_on(async {
                        tokio::fs::copy(src, dst).await.expect("copy file");
                        tokio::fs::remove_file(dst).await.expect("cleanup");
                        black_box(());
                    });
                });
            },
        );
    }

    group.finish();
}

/// Benchmark end-to-end snapshot operations with S3.
///
/// This simulates a complete snapshot workflow: copy + checksum + upload to S3.
fn bench_end_to_end_s3_snapshot(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let schema = sample_schema();

    let store = rt.block_on(async { build_s3_store().await.expect("Failed to build S3 store") });

    let sizes: Vec<(usize, &str)> = vec![(1024, "1KB"), (100 * 1024, "100KB")];

    let mut group = c.benchmark_group("end_to_end_s3_snapshot");
    group.sample_size(10); // Fewer samples for full workflow

    for (size, size_label) in &sizes {
        group.throughput(Throughput::Bytes(*size as u64));

        for (engine_name, _engine) in available_engines() {
            let bench_id = BenchmarkId::new(engine_name.to_string(), size_label);

            let temp_dir = TempDir::new().expect("create temp dir");
            let source_path = write_mock_snapshot_file(&temp_dir, *size);

            group.bench_function(bench_id, |b| {
                b.iter(|| {
                    rt.block_on(async {
                        let prefix = benchmark_prefix();

                        // Step 1: Copy file locally
                        let copy_path = temp_dir.path().join("snapshot_copy.db");
                        tokio::fs::copy(&source_path, &copy_path)
                            .await
                            .expect("copy");

                        // Step 2: Read and compute checksum
                        let data = tokio::fs::read(&copy_path).await.expect("read");
                        let checksum = compute_sha256_hex(&data);

                        // Step 3: Upload to S3
                        let path =
                            ObjectPath::from(format!("{prefix}/dataset=benchmark/snapshot.db"));
                        store
                            .put(&path, Bytes::from(data).into())
                            .await
                            .expect("upload to S3");

                        // Step 4: Update metadata in S3
                        let metadata_path =
                            ObjectPath::from(format!("{prefix}/{METADATA_FILE_NAME}"));
                        let metadata_json = create_metadata_json(
                            &schema,
                            &prefix,
                            "snapshot.db",
                            &checksum,
                            *size as u64,
                        );
                        store
                            .put(&metadata_path, Bytes::from(metadata_json).into())
                            .await
                            .expect("write metadata to S3");

                        // Cleanup local
                        let _ = tokio::fs::remove_file(&copy_path).await;

                        // Cleanup S3
                        let _ = store.delete(&path).await;
                        let _ = store.delete(&metadata_path).await;

                        black_box(checksum);
                    });
                });
            });
        }
    }

    group.finish();
}

/// Benchmark upload/download with TPC-H data (realistic dataset sizes).
///
/// Uses small/medium TPC-H tables to provide realistic snapshot sizes.
fn bench_tpch_snapshot(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");

    // Build S3 store for uploads
    let store = rt.block_on(async { build_s3_store().await.expect("Failed to build S3 store") });

    // Cache directory for TPC-H data
    let cache_dir = TempDir::new().expect("create cache dir");

    // Use all TPC-H tables for realistic benchmark with large datasets
    // Approximate sizes: region (~400B), nation (~2KB), supplier (~80KB),
    // part (~2MB), partsupp (~12MB), customer (~13MB), orders (~17MB), lineitem (~75MB)
    let tables: Vec<&str> = vec![
        "region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem",
    ];

    let mut group = c.benchmark_group("tpch_snapshot");
    group.sample_size(5); // Fewer samples for large files

    for table in &tables {
        // Pre-download TPC-H data
        let cache_path = cache_tpch_data(&rt, table, &cache_dir);
        let data = std::fs::read(&cache_path).expect("read cached tpch data");
        let data_size = data.len() as u64;

        println!("Benchmarking {table}: {} bytes", data_size);

        group.throughput(Throughput::Bytes(data_size));

        for (engine_name, _engine) in available_engines() {
            // Benchmark upload
            let bench_id = BenchmarkId::new(format!("{engine_name}/upload"), table);
            group.bench_with_input(bench_id, &data, |b, data| {
                b.iter(|| {
                    rt.block_on(async {
                        let prefix = benchmark_prefix();
                        let path =
                            ObjectPath::from(format!("{prefix}/dataset={table}/snapshot.parquet"));

                        store
                            .put(&path, Bytes::from(data.clone()).into())
                            .await
                            .expect("upload tpch snapshot to S3");

                        // Cleanup
                        let _ = store.delete(&path).await;

                        black_box(());
                    });
                });
            });

            // Benchmark download (pre-upload data first)
            let prefix = benchmark_prefix();
            let snapshot_path =
                ObjectPath::from(format!("{prefix}/dataset={table}/snapshot.parquet"));

            rt.block_on(async {
                store
                    .put(&snapshot_path, Bytes::from(data.clone()).into())
                    .await
                    .expect("setup tpch snapshot in S3");
            });

            let bench_id = BenchmarkId::new(format!("{engine_name}/download"), table);
            group.bench_with_input(bench_id, &snapshot_path, |b, path| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = store.get(path).await.expect("get tpch snapshot from S3");
                        let bytes = result.bytes().await.expect("read bytes");
                        black_box(bytes.len());
                    });
                });
            });

            // Cleanup after benchmarks
            rt.block_on(async {
                let _ = store.delete(&snapshot_path).await;
            });
        }
    }

    group.finish();
}

/// Benchmark end-to-end snapshot with TPC-H data.
///
/// This tests the full workflow: read -> checksum -> upload -> metadata.
fn bench_tpch_end_to_end(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let schema = sample_schema();

    let store = rt.block_on(async { build_s3_store().await.expect("Failed to build S3 store") });

    let cache_dir = TempDir::new().expect("create cache dir");

    // Use lineitem table for realistic large file benchmarks
    let table = "lineitem";
    let cache_path = cache_tpch_data(&rt, table, &cache_dir);
    let source_data = std::fs::read(&cache_path).expect("read cached tpch data");
    let data_size = source_data.len() as u64;

    println!("End-to-end benchmark with {table}: {} bytes", data_size);

    let mut group = c.benchmark_group("tpch_end_to_end");
    group.sample_size(5); // Fewer samples for large files
    group.throughput(Throughput::Bytes(data_size));

    for (engine_name, _engine) in available_engines() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let source_path = temp_dir.path().join(format!("{table}.parquet"));
        std::fs::write(&source_path, &source_data).expect("write source file");

        let bench_id = BenchmarkId::from_parameter(engine_name);

        group.bench_function(bench_id, |b| {
            b.iter(|| {
                rt.block_on(async {
                    let prefix = benchmark_prefix();

                    // Step 1: Copy file locally
                    let copy_path = temp_dir.path().join("snapshot_copy.parquet");
                    tokio::fs::copy(&source_path, &copy_path)
                        .await
                        .expect("copy");

                    // Step 2: Read and compute checksum
                    let data = tokio::fs::read(&copy_path).await.expect("read");
                    let checksum = compute_sha256_hex(&data);

                    // Step 3: Upload to S3
                    let path =
                        ObjectPath::from(format!("{prefix}/dataset={table}/snapshot.parquet"));
                    store
                        .put(&path, Bytes::from(data).into())
                        .await
                        .expect("upload to S3");

                    // Step 4: Update metadata in S3
                    let metadata_path = ObjectPath::from(format!("{prefix}/{METADATA_FILE_NAME}"));
                    let metadata_json = create_metadata_json(
                        &schema,
                        &prefix,
                        "snapshot.parquet",
                        &checksum,
                        data_size,
                    );
                    store
                        .put(&metadata_path, Bytes::from(metadata_json).into())
                        .await
                        .expect("write metadata to S3");

                    // Cleanup local
                    let _ = tokio::fs::remove_file(&copy_path).await;

                    // Cleanup S3
                    let _ = store.delete(&path).await;
                    let _ = store.delete(&metadata_path).await;

                    black_box(checksum);
                });
            });
        });
    }

    group.finish();
}

/// Benchmark tar archive creation for directory-based snapshots (Cayenne).
///
/// This measures the time to create a tar archive from directories,
/// which is a key phase in the Cayenne snapshot lifecycle.
fn bench_tar_archive_creation(c: &mut Criterion) {
    use runtime_acceleration::snapshot::directory_archive::archive_directories;

    let rt = Runtime::new().expect("Failed to create runtime");
    let cache_dir = TempDir::new().expect("create cache dir");

    // Create test directories with TPC-H data to simulate Cayenne storage
    let tables = vec!["region", "nation", "supplier", "part"];

    // Download and cache TPC-H data
    for table in &tables {
        cache_tpch_data(&rt, table, &cache_dir);
    }

    // Create metadata and data directories simulating Cayenne structure
    let temp_dir = TempDir::new().expect("create temp dir");
    let metadata_dir = temp_dir.path().join("metadata");
    let data_dir = temp_dir.path().join("data");

    std::fs::create_dir_all(&metadata_dir).expect("create metadata dir");
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    // Populate directories with cached data
    for table in &tables {
        let src = cache_dir.path().join(format!("{table}.parquet"));
        let dst = data_dir.join(format!("{table}.parquet"));
        std::fs::copy(&src, &dst).expect("copy table data");
    }

    // Create a mock SQLite metadata file
    let metadata_db = metadata_dir.join("catalog.sqlite");
    std::fs::write(&metadata_db, create_test_data(10 * 1024)).expect("write metadata db");

    // Calculate total size
    let total_size: u64 = std::fs::read_dir(&data_dir)
        .expect("read data dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum::<u64>()
        + std::fs::metadata(&metadata_db)
            .expect("metadata file")
            .len();

    println!("Tar benchmark with {} bytes of data", total_size);

    let mut group = c.benchmark_group("tar_archive");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(total_size));

    // Benchmark tar creation only (no S3)
    let dirs = vec![
        (metadata_dir.clone(), "metadata/".to_string()),
        (data_dir.clone(), "data/".to_string()),
    ];

    group.bench_function("create_tar", |b| {
        b.iter(|| {
            rt.block_on(async {
                let archive_path = temp_dir.path().join("snapshot.tar");
                let file = tokio::fs::File::create(&archive_path)
                    .await
                    .expect("create archive file");

                let bytes_written = archive_directories(&dirs, file)
                    .await
                    .expect("create tar archive");

                // Cleanup
                let _ = tokio::fs::remove_file(&archive_path).await;

                black_box(bytes_written);
            });
        });
    });

    group.finish();
}

/// Benchmark tar extraction for directory-based snapshots (Cayenne).
///
/// This measures the time to extract a tar archive to directories,
/// which is a key phase in the Cayenne snapshot download lifecycle.
fn bench_tar_archive_extraction(c: &mut Criterion) {
    use runtime_acceleration::snapshot::directory_archive::{archive_directories, extract_archive};

    let rt = Runtime::new().expect("Failed to create runtime");
    let cache_dir = TempDir::new().expect("create cache dir");

    // Create test directories with TPC-H data
    let tables = vec!["region", "nation", "supplier", "part"];

    for table in &tables {
        cache_tpch_data(&rt, table, &cache_dir);
    }

    // Create source directories
    let source_temp = TempDir::new().expect("create source temp dir");
    let metadata_dir = source_temp.path().join("metadata");
    let data_dir = source_temp.path().join("data");

    std::fs::create_dir_all(&metadata_dir).expect("create metadata dir");
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    for table in &tables {
        let src = cache_dir.path().join(format!("{table}.parquet"));
        let dst = data_dir.join(format!("{table}.parquet"));
        std::fs::copy(&src, &dst).expect("copy table data");
    }

    let metadata_db = metadata_dir.join("catalog.sqlite");
    std::fs::write(&metadata_db, create_test_data(10 * 1024)).expect("write metadata db");

    // Create tar archive for extraction benchmark
    let dirs = vec![
        (metadata_dir.clone(), "metadata/".to_string()),
        (data_dir.clone(), "data/".to_string()),
    ];

    let archive_path = source_temp.path().join("snapshot.tar");
    rt.block_on(async {
        let file = tokio::fs::File::create(&archive_path)
            .await
            .expect("create archive file");
        archive_directories(&dirs, file)
            .await
            .expect("create tar archive");
    });

    let archive_size = std::fs::metadata(&archive_path)
        .expect("archive metadata")
        .len();
    println!(
        "Tar extraction benchmark with {} byte archive",
        archive_size
    );

    let mut group = c.benchmark_group("tar_extract");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(archive_size));

    group.bench_function("extract_tar", |b| {
        b.iter(|| {
            rt.block_on(async {
                let extract_temp = TempDir::new().expect("create extract temp dir");

                let file = tokio::fs::File::open(&archive_path)
                    .await
                    .expect("open archive file");

                extract_archive(file, extract_temp.path())
                    .await
                    .expect("extract tar archive");

                black_box(());
            });
        });
    });

    group.finish();
}

/// Benchmark end-to-end Cayenne snapshot with tar archival.
///
/// This tests the full Cayenne workflow: tar -> checksum -> upload -> metadata.
fn bench_cayenne_end_to_end(c: &mut Criterion) {
    use runtime_acceleration::snapshot::directory_archive::archive_directories;

    let rt = Runtime::new().expect("Failed to create runtime");
    let schema = sample_schema();

    let store = rt.block_on(async { build_s3_store().await.expect("Failed to build S3 store") });

    let cache_dir = TempDir::new().expect("create cache dir");

    // Create test directories with TPC-H data
    let tables = vec!["region", "nation", "supplier", "part", "customer"];

    for table in &tables {
        cache_tpch_data(&rt, table, &cache_dir);
    }

    // Create source directories simulating Cayenne
    let source_temp = TempDir::new().expect("create source temp dir");
    let metadata_dir = source_temp.path().join("metadata");
    let data_dir = source_temp.path().join("data");

    std::fs::create_dir_all(&metadata_dir).expect("create metadata dir");
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    for table in &tables {
        let src = cache_dir.path().join(format!("{table}.parquet"));
        let dst = data_dir.join(format!("{table}.parquet"));
        std::fs::copy(&src, &dst).expect("copy table data");
    }

    let metadata_db = metadata_dir.join("catalog.sqlite");
    std::fs::write(&metadata_db, create_test_data(10 * 1024)).expect("write metadata db");

    // Calculate total size
    let total_size: u64 = std::fs::read_dir(&data_dir)
        .expect("read data dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum::<u64>()
        + std::fs::metadata(&metadata_db)
            .expect("metadata file")
            .len();

    println!("Cayenne end-to-end benchmark with {} bytes", total_size);

    let mut group = c.benchmark_group("cayenne_end_to_end");
    group.sample_size(5); // Fewer samples for large files with S3
    group.throughput(Throughput::Bytes(total_size));

    let dirs = vec![
        (metadata_dir.clone(), "metadata/".to_string()),
        (data_dir.clone(), "data/".to_string()),
    ];

    group.bench_function("full_workflow", |b| {
        b.iter(|| {
            rt.block_on(async {
                let prefix = benchmark_prefix();

                // Phase 1: Create tar archive
                let archive_path = source_temp.path().join("snapshot.tar");
                let file = tokio::fs::File::create(&archive_path)
                    .await
                    .expect("create archive file");

                archive_directories(&dirs, file)
                    .await
                    .expect("create tar archive");

                // Phase 2: Read archive and compute checksum
                let data = tokio::fs::read(&archive_path).await.expect("read archive");
                let checksum = compute_sha256_hex(&data);

                // Phase 3: Upload to S3
                let path = ObjectPath::from(format!("{prefix}/dataset=cayenne/snapshot.tar"));
                store
                    .put(&path, Bytes::from(data).into())
                    .await
                    .expect("upload to S3");

                // Phase 4: Update metadata in S3
                let metadata_path = ObjectPath::from(format!("{prefix}/{METADATA_FILE_NAME}"));
                let metadata_json =
                    create_metadata_json(&schema, &prefix, "snapshot.tar", &checksum, total_size);
                store
                    .put(&metadata_path, Bytes::from(metadata_json).into())
                    .await
                    .expect("write metadata to S3");

                // Cleanup local
                let _ = tokio::fs::remove_file(&archive_path).await;

                // Cleanup S3
                let _ = store.delete(&path).await;
                let _ = store.delete(&metadata_path).await;

                black_box(checksum);
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_s3_upload,
    bench_s3_download,
    bench_s3_metadata_operations,
    bench_checksum_computation,
    bench_local_file_copy,
    bench_end_to_end_s3_snapshot,
    bench_tpch_snapshot,
    bench_tpch_end_to_end,
    bench_tar_archive_creation,
    bench_tar_archive_extraction,
    bench_cayenne_end_to_end,
);
criterion_main!(benches);
