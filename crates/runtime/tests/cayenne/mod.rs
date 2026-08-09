/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::configure_test_datafusion;
use crate::utils::runtime_ready_check_with_timeout;
use crate::{
    RecordBatch, init_tracing,
    utils::{register_test_connectors, test_request_context, wait_until_true},
};
use app::AppBuilder;
use arrow::array::{Int64Array, StringViewArray};
use arrow_flight::{FlightClient, FlightDescriptor, encode::FlightDataEncoderBuilder};
use arrow_schema::{DataType, Field, Schema};
use aws_sdk_credential_bridge::{S3CredentialProvider, get_or_init_sdk_config};
use data_components::RefreshableCatalogProvider;
use datafusion::assert_batches_eq;
use datafusion::sql::TableReference;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use object_store::{ClientOptions, ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath};
use rand::RngExt as _;
use runtime::auth::EndpointAuth;
use runtime::catalogconnector::cayenne::provider::CayenneCatalogProvider;
use runtime::config::Config;
use runtime::dataaccelerator::cayenne::s3::generate_bucket_name;
use runtime::dataupdate::{DataUpdate, UpdateType};
use runtime::{Runtime, accelerated_table::AcceleratedTable};
use runtime_auth::FlightBasicAuth;
use runtime_auth::api_key::ApiKeyAuth;
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::ApiKey;
use spicepod::param::Params;
use spicepod::partitioning::PartitionedBy;
use test_framework::queries::QuerySet;
use tokio::time::sleep;
use tonic::transport::Channel;

/// Gated serializable transaction (`BEGIN … COMMIT`) regression tests + bench.
/// Gated off Windows to match the other Cayenne provider-level tests.
#[cfg(not(target_os = "windows"))]
mod transaction;

/// Append a single row to a Cayenne-accelerated table through the Runtime write path.
///
/// This exercises the full Cayenne write pipeline:
///   `write_data` → `AcceleratedTable::insert_into` → `CayenneTableProvider::insert_into`
///   → `CayenneDataSink::write_all_append` → `write_to_snapshot`
///
/// The row schema is derived from the existing table schema so that only the
/// `VendorID` column is explicitly set; all other columns get NULL values.
async fn append_one_row_via_cayenne_accelerator(
    rt: &Runtime,
    table_name: &str,
    vendor_id: i64,
) -> Result<(), String> {
    use arrow::array::{ArrayRef, Int64Array, new_null_array};

    let table_ref = TableReference::bare(table_name);

    // Get the table's schema from the runtime catalog
    let table_provider = rt
        .datafusion()
        .get_table(&table_ref)
        .await
        .ok_or_else(|| format!("table {table_name} not found in catalog"))?;
    let schema = table_provider.schema();

    // Build column arrays and cast to the exact schema type when needed.
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            let col = if field.name() == "VendorID" {
                Arc::new(Int64Array::from(vec![vendor_id])) as ArrayRef
            } else {
                new_null_array(field.data_type(), 1)
            };

            if col.data_type() == field.data_type() {
                Ok(col)
            } else {
                arrow::compute::cast(&col, field.data_type()).map_err(|e| {
                    format!(
                        "failed to cast column '{}' from {:?} to {:?}: {e}",
                        field.name(),
                        col.data_type(),
                        field.data_type()
                    )
                })
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|e| format!("failed to build RecordBatch for append: {e}"))?;

    let data_update = DataUpdate {
        schema: Arc::clone(&schema),
        data: vec![batch],
        update_type: UpdateType::Append,
    };

    rt.datafusion()
        .write_data(&table_ref, data_update)
        .await
        .map_err(|e| format!("Cayenne append write_data failed: {e}"))?;

    Ok(())
}

async fn run_sql(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let query_result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("query failed: {e}"))?;

    query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("collect failed: {e}"))
}

fn first_i64_cell(batches: &[RecordBatch]) -> Result<i64, String> {
    let first_batch = batches
        .first()
        .ok_or_else(|| "query returned no batches".to_string())?;

    if first_batch.num_rows() != 1 {
        return Err(format!(
            "expected single-row result, got {} row(s)",
            first_batch.num_rows()
        ));
    }

    let col = first_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| "unexpected result column type, expected Int64".to_string())?;
    Ok(col.value(0))
}

async fn build_zone_store(
    bucket_name: &str,
    zone_id: &str,
    region: &str,
) -> Result<Arc<dyn ObjectStore>, String> {
    let endpoint = format!("https://{bucket_name}.s3express-{zone_id}.{region}.amazonaws.com");

    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
        .with_region(region)
        .with_s3_express(true)
        .with_virtual_hosted_style_request(true)
        .with_unsigned_payload(true)
        .with_endpoint(&endpoint)
        .with_client_options(ClientOptions::default());

    if let (Ok(key), Ok(secret)) = (
        std::env::var("AWS_ACCESS_KEY_ID"),
        std::env::var("AWS_SECRET_ACCESS_KEY"),
    ) {
        builder = builder
            .with_access_key_id(key)
            .with_secret_access_key(secret);
        if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
            builder = builder.with_token(token);
        }
    } else {
        let config = get_or_init_sdk_config()
            .await
            .map_err(|e| format!("failed to initialize AWS credentials: {e}"))?;
        let Some(config) = config else {
            return Err(
                "AWS credentials are required; configure AWS_PROFILE or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"
                    .to_string(),
            );
        };
        let provider = S3CredentialProvider::from_config(config.as_ref())
            .map_err(|e| format!("failed to create S3 credential provider: {e}"))?;
        builder = builder.with_credentials(Arc::new(provider));
    }

    let store = builder
        .build()
        .map_err(|e| format!("failed to build S3 object store for {bucket_name}: {e}"))?;
    Ok(Arc::new(store))
}

async fn list_objects_under_prefix(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<BTreeMap<String, u64>, String> {
    let mut objects = BTreeMap::new();
    let mut stream = store.list(Some(&ObjectPath::from(prefix.to_string())));

    while let Some(entry) = stream.next().await {
        let meta = entry.map_err(|e| format!("failed listing '{prefix}': {e}"))?;
        objects.insert(meta.location.as_ref().to_string(), meta.size);
    }

    Ok(objects)
}

fn normalize_zone_ids(raw_zone_ids: &str) -> Vec<String> {
    let mut zone_pool: Vec<String> = Vec::new();
    for raw_zone in raw_zone_ids.split(',') {
        let normalized = raw_zone.trim().to_ascii_lowercase();
        if !normalized.is_empty() && !zone_pool.iter().any(|z| z == &normalized) {
            zone_pool.push(normalized);
        }
    }
    zone_pool
}

async fn cleanup_s3_table_data(
    app_name: &str,
    table_name: &str,
    region: &str,
    zone_ids: &[String],
) {
    for zone_id in zone_ids {
        let Ok(bucket_name) = generate_bucket_name(app_name, table_name, zone_id) else {
            continue;
        };
        let Ok(store) = build_zone_store(&bucket_name, zone_id, region).await else {
            continue;
        };

        let prefix = ObjectPath::from(format!("{table_name}/"));
        let mut stream = store.list(Some(&prefix));
        while let Some(Ok(meta)) = stream.next().await {
            let _ = store.delete(&meta.location).await;
        }
    }
}

// Validation-only helper:
// This performs direct S3 reads to verify Cayenne's replication side effects.
// All acceleration operations in the test itself must go through Cayenne via Runtime/DataFusion APIs.
async fn validate_s3_replica_integrity_direct(
    app_name: &str,
    table_name: &str,
    region: &str,
    zone_ids: &[String],
) -> Result<(), String> {
    if zone_ids.len() < 2 {
        return Err("replica integrity validation requires at least 2 zone IDs".to_string());
    }

    let mut zone_stores = Vec::with_capacity(zone_ids.len());
    for zone_id in zone_ids {
        let bucket_name = generate_bucket_name(app_name, table_name, zone_id)
            .map_err(|e| format!("failed to generate bucket name: {e}"))?;
        let store = build_zone_store(&bucket_name, zone_id, region).await?;
        zone_stores.push((zone_id.clone(), bucket_name, store));
    }

    let prefix = format!("{table_name}/");
    let (primary_zone, _primary_bucket, primary_store) = &zone_stores[0];
    let primary_objects = list_objects_under_prefix(primary_store, &prefix).await?;
    if primary_objects.is_empty() {
        return Err(format!(
            "no objects found in primary zone {primary_zone} under prefix {prefix}"
        ));
    }

    for (zone_id, _bucket, store) in zone_stores.iter().skip(1) {
        let replica_objects = list_objects_under_prefix(store, &prefix).await?;
        if replica_objects != primary_objects {
            return Err(format!(
                "object set mismatch between primary zone {primary_zone} and replica zone {zone_id}"
            ));
        }

        for object_path in primary_objects.keys() {
            let object_path = ObjectPath::from(object_path.clone());
            let primary_bytes = primary_store
                .get(&object_path)
                .await
                .map_err(|e| format!("failed to fetch primary object {object_path}: {e}"))?
                .bytes()
                .await
                .map_err(|e| format!("failed to read primary object {object_path}: {e}"))?;
            let replica_bytes = store
                .get(&object_path)
                .await
                .map_err(|e| format!("failed to fetch replica object {object_path}: {e}"))?
                .bytes()
                .await
                .map_err(|e| format!("failed to read replica object {object_path}: {e}"))?;

            if primary_bytes != replica_bytes {
                return Err(format!(
                    "content mismatch for object {object_path} between primary zone {primary_zone} and replica zone {zone_id}"
                ));
            }
        }
    }

    Ok(())
}

fn make_s3_tpch_dataset(
    name: &str,
    partition_by: Option<String>,
    cayenne_data_dir: &std::path::Path,
    cayenne_metadata_dir: &std::path::Path,
) -> Dataset {
    let mut dataset = Dataset::new(
        format!("s3://spiceai-demo-datasets/tpch/{name}/"),
        name.to_string(),
    );
    dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "parquet".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(Params::from_string_map(HashMap::from([
            (
                "cayenne_file_path".to_string(),
                cayenne_data_dir.to_string_lossy().to_string(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                cayenne_metadata_dir.to_string_lossy().to_string(),
            ),
        ]))),
        refresh_sql: None,
        ..Acceleration::default()
    });

    if let Some(partition_by) = partition_by
        && let Some(accel) = dataset.acceleration.as_mut()
    {
        accel.partition_by = vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: partition_by,
        }];
    }

    dataset
}

#[tokio::test]
async fn test_cayenne_with_partitioned_tpch() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()
                .map_err(|e| format!("failed to create Cayenne temp directory: {e}"))?;
            let cayenne_data_dir = temp_dir.path().join("data");
            let cayenne_metadata_dir = temp_dir.path().join("metadata");

            // exclude lineitem, orders and customer to reduce egress
            let app = AppBuilder::new("test_cayenne_with_partitioned_tpch")
                .with_dataset(make_s3_tpch_dataset(
                    "nation",
                    Some("n_regionkey".to_string()),
                    &cayenne_data_dir,
                    &cayenne_metadata_dir,
                ))
                .with_dataset(make_s3_tpch_dataset(
                    "region",
                    None,
                    &cayenne_data_dir,
                    &cayenne_metadata_dir,
                ))
                .with_dataset(make_s3_tpch_dataset(
                    "supplier",
                    Some("bucket(10, s_suppkey)".to_string()),
                    &cayenne_data_dir,
                    &cayenne_metadata_dir,
                ))
                .with_dataset(make_s3_tpch_dataset(
                    "part",
                    Some("bucket(10, p_partkey)".to_string()),
                    &cayenne_data_dir,
                    &cayenne_metadata_dir,
                ))
                .with_dataset(make_s3_tpch_dataset(
                    "partsupp",
                    Some("bucket(10, ps_partkey)".to_string()),
                    &cayenne_data_dir,
                    &cayenne_metadata_dir,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(4)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check_with_timeout(&rt, Duration::from_mins(10)).await;

            let queries = QuerySet::Tpch
                .get_queries(None, None, None, None)
                .await
                .expect("to get queries");

            let queries = vec![
                queries.get(1).expect("TPCH q2 missing"),
                queries.get(10).expect("TPCH q11 missing"),
                queries.get(14).expect("TPCH q16 missing"),
            ];

            for query in queries {
                let query_result = rt
                    .datafusion()
                    .query_builder(&format!("EXPLAIN {}", query.sql))
                    .build()
                    .run()
                    .await
                    .expect("should run query");
                query_result
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .expect("should collect batches");
            }

            Ok(())
        })
        .await
}

#[tokio::test]
#[ignore = "requires AWS credentials for S3 Express One Zone live test"]
async fn test_cayenne_s3_express_multi_zone_live() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    // Keep test input minimal: hardcoded defaults with optional env overrides.
    // Defaults include two known-good zones. A third zone can be provided via env.
    let zone_ids =
        std::env::var("CAYENNE_S3_ZONE_IDS").unwrap_or_else(|_| "usw2-az1,usw2-az4".to_string());
    let region = std::env::var("CAYENNE_S3_REGION").unwrap_or_else(|_| "us-west-2".to_string());

    let mut zone_pool = normalize_zone_ids(&zone_ids);

    if let Ok(third_zone) = std::env::var("CAYENNE_S3_ZONE_ID_3") {
        let normalized = third_zone.trim().to_ascii_lowercase();
        if !normalized.is_empty() && !zone_pool.iter().any(|z| z == &normalized) {
            zone_pool.push(normalized);
        }
    }

    if zone_pool.is_empty() {
        return Err("At least 1 S3 zone ID is required for acceleration tests".to_string());
    }

    // Clean stale Cayenne catalog metadata from previous test runs.
    // The catalog is a local SQLite DB that persists across test invocations.
    // If a previous run created a table with a different configuration (e.g.
    // different PK/on_conflict), re-creation will fail with
    // "already exists with different configuration" unless the old entry is removed.
    let cayenne_db = std::path::PathBuf::from(".spice/data/metadata/cayenne.db");
    if cayenne_db.exists() {
        tokio::fs::remove_file(&cayenne_db)
            .await
            .map_err(|e| format!("failed to remove stale Cayenne catalog metadata: {e}"))?;
        tracing::info!("Removed stale Cayenne catalog metadata");
    }

    test_request_context().scope(async {
        for zone_count in [1usize, 2, 3] {
            if zone_pool.len() < zone_count {
                tracing::info!(
                    "Skipping {zone_count}-zone scenario: only {} zone(s) available",
                    zone_pool.len()
                );
                continue;
            }

            let scenario_zone_ids = zone_pool[..zone_count].join(",");
            let table_name = format!("taxi_multi_zone_live_{zone_count}z");
            let app_name = format!("test_cayenne_s3_express_multi_zone_live_{zone_count}z");

            let scenario_zones = zone_pool[..zone_count].to_vec();

            // Clean stale S3 data from previous test runs for this scenario.
            cleanup_s3_table_data(&app_name, &table_name, &region, &scenario_zones).await;

            let scenario_result: Result<(), String> = async {
            let mut dataset = Dataset::new(
                "s3://spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet",
                table_name.clone(),
            );
            dataset.access = AccessMode::ReadWrite;
            dataset.params = Some(Params::from_string_map(
                vec![
                    ("file_format".to_string(), "parquet".to_string()),
                    ("client_timeout".to_string(), "120s".to_string()),
                ]
                .into_iter()
                .collect(),
            ));

            let accel_params = Params::from_string_map(
                vec![
                    ("cayenne_s3_zone_ids".to_string(), scenario_zone_ids.clone()),
                    ("cayenne_s3_region".to_string(), region.clone()),
                ]
                .into_iter()
                .collect(),
            );

            let on_conflict = HashMap::from([(
                "VendorID".to_string(),
                OnConflictBehavior::Upsert,
            )]);

            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                primary_key: Some("VendorID".to_string()),
                on_conflict,
                params: Some(accel_params),
                ..Acceleration::default()
            });

            let app = AppBuilder::new(app_name.clone())
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(4)) => {
                    return Err(format!("Timed out waiting for datasets to load for {zone_count}-zone scenario"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check_with_timeout(&rt, Duration::from_mins(5)).await;

            let table_ref = TableReference::bare(table_name.as_str());
            if !rt.datafusion().is_accelerated(&table_ref).await {
                return Err(format!(
                    "Expected {table_name} to be registered as an accelerated table for {zone_count}-zone scenario"
                ));
            }

            let accelerated_provider = rt
                .datafusion()
                .get_accelerated_table_provider(&table_name)
                .await
                .map_err(|e| format!("failed to resolve accelerated provider: {e}"))?;
            if !accelerated_provider.is::<AcceleratedTable>() {
                return Err(format!(
                    "Expected provider for {table_name} to be AcceleratedTable in {zone_count}-zone scenario"
                ));
            }

            // Cayenne operation path (not direct S3): query through Runtime/DataFusion
            // against the accelerated table backed by Cayenne.
            let baseline_count = first_i64_cell(
                &run_sql(&rt, &format!("SELECT COUNT(*) AS c FROM {table_name}")).await?,
            )?;

            if baseline_count <= 0 {
                return Err(format!(
                    "Expected accelerated table {table_name} to contain rows before lifecycle ops, got count={baseline_count}"
                ));
            }

            // --- Cayenne write mutation: append one row through Cayenne accelerator ---
            // Derive a guaranteed-unique VendorID to avoid upsert deduplication.
            // The row is written through Runtime/DataFusion → AcceleratedTable → CayenneTableProvider.
            let new_vendor_id: i64 = first_i64_cell(
                &run_sql(
                    &rt,
                    &format!(r#"SELECT COALESCE(MAX("VendorID"), 0) + 1 AS c FROM {table_name}"#),
                )
                .await?,
            )?;
            append_one_row_via_cayenne_accelerator(&rt, &table_name, new_vendor_id).await?;

            // Wait for the SQL results cache to expire (TTL=1s) so the next query
            // reads fresh data from the Cayenne-accelerated table.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            // Verify the appended row is visible through Cayenne
            let post_append_count = first_i64_cell(
                &run_sql(&rt, &format!("SELECT COUNT(*) AS c FROM {table_name}")).await?,
            )?;
            if post_append_count != baseline_count + 1 {
                return Err(format!(
                    "Expected {table_name} row count to increase by 1 after append (baseline={baseline_count}), got {post_append_count}"
                ));
            }

            // Verify the specific row exists
            let new_row_count = first_i64_cell(
                &run_sql(
                    &rt,
                    &format!("SELECT COUNT(*) AS c FROM {table_name} WHERE \"VendorID\" = {new_vendor_id}"),
                )
                .await?,
            )?;
            if new_row_count != 1 {
                return Err(format!(
                    "Expected exactly 1 row with VendorID={new_vendor_id} after append, got {new_row_count}"
                ));
            }

            tracing::info!(
                "Cayenne write mutation validated: appended VendorID={new_vendor_id}, count {baseline_count} → {post_append_count}"
            );

            if zone_count >= 2 {
                validate_s3_replica_integrity_direct(&app_name, &table_name, &region, &scenario_zones)
                    .await?;

                tracing::info!(
                    "Cayenne S3 Express replica consistency validated for {zone_count}-zone scenario ({scenario_zone_ids})"
                );
            } else {
                tracing::info!(
                    "Cayenne S3 Express 1-zone scenario validated (no replica to compare)"
                );
            }

            Ok(())
            }
            .await;

            cleanup_s3_table_data(&app_name, &table_name, &region, &scenario_zones).await;
            scenario_result?;
        }

        // --- Post-test cleanup: remove S3 data and local metadata ---
        // Clean up all zone buckets for every scenario that ran so subsequent
        // test invocations start from a clean slate.
        for zone_count in 1..=zone_pool.len() {
            let table_name = format!("taxi_multi_zone_live_{zone_count}z");
            let app_name = format!("test_cayenne_s3_express_multi_zone_live_{zone_count}z");
            for zone_id in &zone_pool[..zone_count] {
                if let Ok(bucket_name) = generate_bucket_name(&app_name, &table_name, zone_id)
                    && let Ok(store) = build_zone_store(&bucket_name, zone_id, &region).await
                {
                    let prefix = ObjectPath::from(format!("{table_name}/"));
                    let mut stream = store.list(Some(&prefix));
                    while let Some(Ok(meta)) = stream.next().await {
                        let _ = store.delete(&meta.location).await;
                    }
                }
            }
        }
        let cayenne_db = std::path::PathBuf::from(".spice/data/metadata/cayenne.db");
        if cayenne_db.exists() {
            tokio::fs::remove_file(&cayenne_db)
                .await
                .map_err(|e| format!("failed to remove Cayenne catalog metadata: {e}"))?;
        }
        tracing::info!("Post-test cleanup complete: removed S3 data and local Cayenne catalog");

        Ok(())
    }).await
}

/// Creates a Cayenne `Catalog` component with `read_write_create` access.
fn make_cayenne_catalog(catalog_name: &str, data_dir: &str, metadata_dir: &str) -> Catalog {
    let mut catalog = Catalog::new("cayenne".to_string(), catalog_name.to_string())
        .with_access(AccessMode::ReadWriteCreate);
    catalog.params = Some(Params::from_string_map(
        vec![
            ("cayenne_data_dir".to_string(), data_dir.to_string()),
            ("cayenne_metadata_dir".to_string(), metadata_dir.to_string()),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>(),
    ));
    catalog
}

/// Run a SQL query and collect all result batches.
async fn run_query(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("query '{sql}' failed: {e}"))?;

    result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("collecting results for '{sql}' failed: {e}"))
}

/// Run a SQL statement (DDL/DML) and discard results.
async fn exec(rt: &Runtime, sql: &str) -> Result<(), String> {
    run_query(rt, sql).await?;
    Ok(())
}

/// Send record batches to a Cayenne table via Flight `DoPut`.
async fn doput_to_table(
    client: &mut FlightClient,
    table_path: &[&str],
    batch: RecordBatch,
) -> Result<(), String> {
    let flight_descriptor = FlightDescriptor::new_path(
        table_path
            .iter()
            .map(std::string::ToString::to_string)
            .collect(),
    );

    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(futures::stream::iter(
            vec![Ok(batch)].into_iter().collect::<Vec<_>>(),
        ));

    let _response: Vec<_> = client
        .do_put(flight_data_stream)
        .await
        .map_err(|e| format!("do_put failed: {e}"))?
        .try_collect()
        .await
        .map_err(|e| format!("do_put response stream failed: {e}"))?;

    Ok(())
}

/// Build a `RecordBatch` with (id: Int64, name: `Utf8View`) columns.
fn make_batch(ids: &[i64], names: &[&str]) -> Result<RecordBatch, String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8View, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringViewArray::from(names.to_vec())),
        ],
    )
    .map_err(|e| format!("failed to create RecordBatch: {e}"))
}

/// Helper: `DoPut` data, refresh catalog, query, and assert expected output.
async fn doput_refresh_and_assert(
    client: &mut FlightClient,
    table_path: &[&str],
    rt: &Runtime,
    cayenne_provider: &CayenneCatalogProvider,
    ids: &[i64],
    names: &[&str],
    expected: &[&str],
) -> Result<(), String> {
    let batch = make_batch(ids, names)?;
    doput_to_table(client, table_path, batch).await?;

    cayenne_provider
        .refresh()
        .await
        .map_err(|e| format!("catalog refresh failed: {e}"))?;

    let batches = run_query(rt, "SELECT id, name FROM stlcyc.ns.items ORDER BY id").await?;

    let expected_vec: Vec<String> = expected
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    let expected_refs: Vec<&str> = expected_vec
        .iter()
        .map(std::string::String::as_str)
        .collect();
    assert_batches_eq!(&expected_refs, &batches);

    Ok(())
}

#[tokio::test]
#[ignore = "Requires non-distributed Cayenne catalog support: https://github.com/spiceai/spiceai/issues/9942"]
async fn test_cayenne_doput_upsert_cycle_stale() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async {
            let catalog = make_cayenne_catalog(
                "stlcyc",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;
            let metrics_port: u16 = http_port + 2;

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), http_port))
                .with_flight_bind_address(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    flight_port,
                ));

            let app = AppBuilder::new("cayenne_doput_upsert_cycle_stale_test")
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let registry = prometheus::Registry::new();
            let rt = Arc::new(
                Runtime::builder()
                    .with_metrics_server(
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), metrics_port),
                        registry,
                    )
                    .with_app(app)
                    .with_runtime_config(Config::default().with_caching_disabled())
                    .build()
                    .await,
            );

            let cloned_rt = Arc::clone(&rt);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    return Err("Timeout waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

            let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("testkey:rw")]))
                as Arc<dyn FlightBasicAuth + Send + Sync>;
            let endpoint_auth = EndpointAuth::default().with_flight_basic_auth(auth);

            let server_rt = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(server_rt.start_servers(api_config, None, endpoint_auth)).await
            });

            wait_until_true(Duration::from_secs(10), || async {
                reqwest::get(format!("http://localhost:{http_port}/health"))
                    .await
                    .is_ok()
            })
            .await;

            let channel = {
                let start = std::time::Instant::now();
                loop {
                    if start.elapsed() > Duration::from_secs(30) {
                        return Err("Flight server not ready within 30s".to_string());
                    }
                    match Channel::from_shared(format!("http://localhost:{flight_port}"))
                        .map_err(|e| format!("invalid URI: {e}"))?
                        .connect()
                        .await
                    {
                        Ok(ch) => break ch,
                        Err(_) => sleep(Duration::from_millis(100)).await,
                    }
                }
            };

            let mut client = FlightClient::new(channel);
            client
                .add_header("authorization", "Bearer testkey")
                .map_err(|e| format!("failed to add auth header: {e}"))?;

            exec(&rt, "CREATE SCHEMA stlcyc.ns").await?;
            exec(
                &rt,
                "CREATE TABLE stlcyc.ns.items (
                    id BIGINT NOT NULL,
                    name VARCHAR,
                    PRIMARY KEY (id)
                )",
            )
            .await?;

            let table_path = &["stlcyc", "ns", "items"];

            let df = rt.datafusion();
            let catalog_provider = df
                .ctx
                .catalog("stlcyc")
                .ok_or("catalog 'stlcyc' not found")?;
            let cayenne_provider = catalog_provider
                .downcast_ref::<CayenneCatalogProvider>()
                .ok_or("failed to downcast to CayenneCatalogProvider")?;

            doput_refresh_and_assert(
                &mut client,
                table_path,
                &rt,
                cayenne_provider,
                &[1, 2],
                &["Alice", "Bob"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob   |",
                    "+----+-------+",
                ],
            )
            .await?;

            tokio::time::sleep(Duration::from_secs(1)).await;

            doput_refresh_and_assert(
                &mut client,
                table_path,
                &rt,
                cayenne_provider,
                &[1, 2],
                &["Alice2", "Bob2"],
                &[
                    "+----+--------+",
                    "| id | name   |",
                    "+----+--------+",
                    "| 1  | Alice2 |",
                    "| 2  | Bob2   |",
                    "+----+--------+",
                ],
            )
            .await?;

            tokio::time::sleep(Duration::from_secs(1)).await;

            doput_refresh_and_assert(
                &mut client,
                table_path,
                &rt,
                cayenne_provider,
                &[1, 2],
                &["Alice", "Bob"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob   |",
                    "+----+-------+",
                ],
            )
            .await?;

            Ok(())
        })
        .await
}

/// Helper struct that bundles everything needed to run Cayenne `DoPut` + SQL
/// operations against a local catalog backed runtime.
struct CayenneTestHarness {
    rt: Arc<Runtime>,
    cayenne_provider: *const CayenneCatalogProvider,
    client: FlightClient,
}

// Safety: The CayenneCatalogProvider pointer is derived from an Arc held by the
// runtime which outlives the harness. We only use it behind &self borrows and
// never send it across threads independently.
unsafe impl Send for CayenneTestHarness {}

impl CayenneTestHarness {
    /// Spin up a Cayenne-backed runtime with HTTP + Flight servers.
    async fn new(
        data_dir: &std::path::Path,
        metadata_dir: &std::path::Path,
    ) -> Result<Self, String> {
        let catalog = make_cayenne_catalog(
            "cyc",
            &data_dir.to_string_lossy(),
            &metadata_dir.to_string_lossy(),
        );

        let mut rng = rand::rng();
        let http_port: u16 = rng.random_range(50000..60000);
        let flight_port: u16 = http_port + 1;
        let metrics_port: u16 = http_port + 2;

        let api_config = Config::new()
            .with_http_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), http_port))
            .with_flight_bind_address(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                flight_port,
            ));

        let app = AppBuilder::new("cayenne_comprehensive_test")
            .with_catalog(catalog)
            .build();

        configure_test_datafusion();
        let registry = prometheus::Registry::new();
        let rt = Arc::new(
            Runtime::builder()
                .with_metrics_server(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), metrics_port),
                    registry,
                )
                .with_app(app)
                .with_runtime_config(Config::default().with_caching_disabled())
                .build()
                .await,
        );

        let cloned_rt = Arc::clone(&rt);
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(30)) => {
                return Err("Timeout waiting for components to load".to_string());
            }
            () = cloned_rt.load_components() => {}
        }
        runtime_ready_check_with_timeout(&rt, Duration::from_secs(30)).await;

        let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("testkey:rw")]))
            as Arc<dyn FlightBasicAuth + Send + Sync>;
        let endpoint_auth = EndpointAuth::default().with_flight_basic_auth(auth);

        let server_rt = Arc::clone(&rt);
        tokio::spawn(async move {
            Box::pin(server_rt.start_servers(api_config, None, endpoint_auth)).await
        });

        wait_until_true(Duration::from_secs(10), || async {
            reqwest::get(format!("http://localhost:{http_port}/health"))
                .await
                .is_ok()
        })
        .await;

        let channel = {
            let start = std::time::Instant::now();
            loop {
                if start.elapsed() > Duration::from_secs(30) {
                    return Err("Flight server not ready within 30s".to_string());
                }
                match Channel::from_shared(format!("http://localhost:{flight_port}"))
                    .map_err(|e| format!("invalid URI: {e}"))?
                    .connect()
                    .await
                {
                    Ok(ch) => break ch,
                    Err(_) => sleep(Duration::from_millis(100)).await,
                }
            }
        };

        let mut client = FlightClient::new(channel);
        client
            .add_header("authorization", "Bearer testkey")
            .map_err(|e| format!("failed to add auth header: {e}"))?;

        // Create the schema once
        exec(&rt, "CREATE SCHEMA cyc.ns").await?;

        let df = rt.datafusion();
        let catalog_provider = df.ctx.catalog("cyc").ok_or("catalog 'cyc' not found")?;
        let cayenne_provider = catalog_provider
            .downcast_ref::<CayenneCatalogProvider>()
            .ok_or("failed to downcast to CayenneCatalogProvider")?;

        Ok(Self {
            rt,
            cayenne_provider: std::ptr::from_ref(cayenne_provider),
            client,
        })
    }

    fn cayenne_provider(&self) -> &CayenneCatalogProvider {
        // Safety: pointer is valid for the lifetime of `self.rt`
        unsafe { &*self.cayenne_provider }
    }

    /// Create a fresh table with `(id BIGINT PK, name VARCHAR)`.
    async fn create_table(&self, table_name: &str) -> Result<(), String> {
        exec(
            &self.rt,
            &format!(
                "CREATE TABLE cyc.ns.{table_name} (
                    id BIGINT NOT NULL,
                    name VARCHAR,
                    PRIMARY KEY (id)
                )"
            ),
        )
        .await
    }

    /// `DoPut` a batch, refresh the catalog, then SELECT and assert.
    async fn doput_and_assert(
        &mut self,
        table_name: &str,
        ids: &[i64],
        names: &[&str],
        expected: &[&str],
    ) -> Result<(), String> {
        let batch = make_batch(ids, names)?;
        let table_path: Vec<&str> = vec!["cyc", "ns", table_name];
        doput_to_table(&mut self.client, &table_path, batch).await?;

        self.cayenne_provider()
            .refresh()
            .await
            .map_err(|e| format!("catalog refresh failed: {e}"))?;

        self.assert_query(table_name, expected).await
    }

    /// Run a SQL mutation (DELETE / UPDATE / INSERT INTO), refresh, and assert.
    async fn sql_mutate_and_assert(
        &self,
        sql: &str,
        table_name: &str,
        expected: &[&str],
    ) -> Result<(), String> {
        exec(&self.rt, sql).await?;

        self.cayenne_provider()
            .refresh()
            .await
            .map_err(|e| format!("catalog refresh failed: {e}"))?;

        self.assert_query(table_name, expected).await
    }

    /// SELECT all rows ordered by id and assert.
    async fn assert_query(&self, table_name: &str, expected: &[&str]) -> Result<(), String> {
        let batches = run_query(
            &self.rt,
            &format!("SELECT id, name FROM cyc.ns.{table_name} ORDER BY id"),
        )
        .await?;

        let expected_vec: Vec<String> = expected
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        let expected_refs: Vec<&str> = expected_vec
            .iter()
            .map(std::string::String::as_str)
            .collect();
        assert_batches_eq!(&expected_refs, &batches);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Comprehensive DoPut / DELETE / UPDATE edge-case test
// ---------------------------------------------------------------------------

/// Exercises every combination of INSERT (`DoPut`), SQL DELETE, and SQL UPDATE
/// against Cayenne tables with a PRIMARY KEY.
///
/// Each scenario uses its own table so failures are independent.
#[tokio::test]
#[ignore = "Requires non-distributed Cayenne catalog support: https://github.com/spiceai/spiceai/issues/9942"]
async fn test_cayenne_dml_comprehensive() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let temp_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    // Box::pin the inner future to heap-allocate it. The 17 test scenarios
    // produce a state machine struct too large for the default thread stack.
    test_request_context()
        .scope(Box::pin(async {
            let mut h = CayenneTestHarness::new(&data_dir, &metadata_dir).await?;

            // ---------------------------------------------------------------
            // S1  Basic insert – two rows
            // ---------------------------------------------------------------
            h.create_table("s1").await?;
            h.doput_and_assert(
                "s1",
                &[1, 2],
                &["Alice", "Bob"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob   |",
                    "+----+-------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S2  Upsert same PKs five times (extended cycle)
            // ---------------------------------------------------------------
            h.create_table("s2").await?;
            for round in 1..=5u32 {
                let name_a = format!("A{round}");
                let name_b = format!("B{round}");
                h.doput_and_assert(
                    "s2",
                    &[1, 2],
                    &[&name_a, &name_b],
                    &[
                        "+----+------+",
                        "| id | name |",
                        "+----+------+",
                        &format!("| 1  | {name_a:<4} |"),
                        &format!("| 2  | {name_b:<4} |"),
                        "+----+------+",
                    ],
                )
                .await
                .map_err(|e| format!("S2 round {round}: {e}"))?;
            }

            // ---------------------------------------------------------------
            // S3  Upsert with partial PK overlap (some new, some existing)
            // ---------------------------------------------------------------
            h.create_table("s3").await?;
            h.doput_and_assert(
                "s3",
                &[1, 2],
                &["Alice", "Bob"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob   |",
                    "+----+-------+",
                ],
            )
            .await?;
            // Upsert ids 2,3 - id 2 updated, id 3 inserted
            h.doput_and_assert(
                "s3",
                &[2, 3],
                &["Bob2", "Carol"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob2  |",
                    "| 3  | Carol |",
                    "+----+-------+",
                ],
            )
            .await?;
            // Upsert ids 1,3,4 - ids 1,3 updated, id 4 inserted
            h.doput_and_assert(
                "s3",
                &[1, 3, 4],
                &["Ax", "Cx", "Dave"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | Ax   |",
                    "| 2  | Bob2 |",
                    "| 3  | Cx   |",
                    "| 4  | Dave |",
                    "+----+------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S4  Insert then DELETE one specific row
            // ---------------------------------------------------------------
            h.create_table("s4").await?;
            h.doput_and_assert(
                "s4",
                &[1, 2, 3],
                &["Alice", "Bob", "Carol"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob   |",
                    "| 3  | Carol |",
                    "+----+-------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "DELETE FROM cyc.ns.s4 WHERE id = 2",
                "s4",
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 3  | Carol |",
                    "+----+-------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S5  Insert then DELETE all rows
            // ---------------------------------------------------------------
            h.create_table("s5").await?;
            h.doput_and_assert(
                "s5",
                &[1, 2],
                &["Alice", "Bob"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob   |",
                    "+----+-------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert("DELETE FROM cyc.ns.s5", "s5", &["++", "++"])
                .await?;

            // ---------------------------------------------------------------
            // S6  Insert -> DELETE one row -> re-insert same PK with new value
            // ---------------------------------------------------------------
            h.create_table("s6").await?;
            h.doput_and_assert(
                "s6",
                &[1, 2],
                &["Alice", "Bob"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob   |",
                    "+----+-------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "DELETE FROM cyc.ns.s6 WHERE id = 1",
                "s6",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 2  | Bob  |",
                    "+----+------+",
                ],
            )
            .await?;
            // Re-insert id=1 with different value
            h.doput_and_assert(
                "s6",
                &[1],
                &["Alice2"],
                &[
                    "+----+--------+",
                    "| id | name   |",
                    "+----+--------+",
                    "| 1  | Alice2 |",
                    "| 2  | Bob    |",
                    "+----+--------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S7  Insert -> SQL UPDATE one row
            // ---------------------------------------------------------------
            h.create_table("s7").await?;
            h.doput_and_assert(
                "s7",
                &[1, 2],
                &["Alice", "Bob"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "| 2  | Bob   |",
                    "+----+-------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "UPDATE cyc.ns.s7 SET name = 'Alice2' WHERE id = 1",
                "s7",
                &[
                    "+----+--------+",
                    "| id | name   |",
                    "+----+--------+",
                    "| 1  | Alice2 |",
                    "| 2  | Bob    |",
                    "+----+--------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S8  Insert -> UPDATE -> UPDATE again (chained updates)
            // ---------------------------------------------------------------
            h.create_table("s8").await?;
            h.doput_and_assert(
                "s8",
                &[1, 2],
                &["A", "B"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "UPDATE cyc.ns.s8 SET name = 'X' WHERE id = 1",
                "s8",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | X    |",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "UPDATE cyc.ns.s8 SET name = 'Y' WHERE id = 1",
                "s8",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | Y    |",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "UPDATE cyc.ns.s8 SET name = 'Z'",
                "s8",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | Z    |",
                    "| 2  | Z    |",
                    "+----+------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S9  Insert -> Upsert -> DELETE -> verify deletion post-upsert
            // ---------------------------------------------------------------
            h.create_table("s9").await?;
            h.doput_and_assert(
                "s9",
                &[1, 2],
                &["A", "B"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.doput_and_assert(
                "s9",
                &[1, 2],
                &["A2", "B2"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A2   |",
                    "| 2  | B2   |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "DELETE FROM cyc.ns.s9 WHERE id = 1",
                "s9",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 2  | B2   |",
                    "+----+------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S10  Interleaved: Insert 1,2 -> Upsert 1 -> Delete 2 -> Insert 3
            // ---------------------------------------------------------------
            h.create_table("s10").await?;
            h.doput_and_assert(
                "s10",
                &[1, 2],
                &["A", "B"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.doput_and_assert(
                "s10",
                &[1],
                &["A2"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A2   |",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "DELETE FROM cyc.ns.s10 WHERE id = 2",
                "s10",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A2   |",
                    "+----+------+",
                ],
            )
            .await?;
            h.doput_and_assert(
                "s10",
                &[3],
                &["C"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A2   |",
                    "| 3  | C    |",
                    "+----+------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S11  Upsert with NULL -> non-NULL -> back to NULL
            // ---------------------------------------------------------------
            h.create_table("s11").await?;
            {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8View, true),
                ]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(vec![1])),
                        Arc::new(StringViewArray::from(vec![None::<&str>])),
                    ],
                )
                .map_err(|e| format!("batch: {e}"))?;
                doput_to_table(&mut h.client, &["cyc", "ns", "s11"], batch).await?;
                h.cayenne_provider()
                    .refresh()
                    .await
                    .map_err(|e| e.to_string())?;
            }
            h.assert_query(
                "s11",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  |      |",
                    "+----+------+",
                ],
            )
            .await?;
            h.doput_and_assert(
                "s11",
                &[1],
                &["Alice"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | Alice |",
                    "+----+-------+",
                ],
            )
            .await?;
            {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8View, true),
                ]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(vec![1])),
                        Arc::new(StringViewArray::from(vec![None::<&str>])),
                    ],
                )
                .map_err(|e| format!("batch: {e}"))?;
                doput_to_table(&mut h.client, &["cyc", "ns", "s11"], batch).await?;
                h.cayenne_provider()
                    .refresh()
                    .await
                    .map_err(|e| e.to_string())?;
            }
            h.assert_query(
                "s11",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  |      |",
                    "+----+------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S12  DELETE -> Upsert deleted PK -> verify re-appears
            // ---------------------------------------------------------------
            h.create_table("s12").await?;
            h.doput_and_assert(
                "s12",
                &[1, 2],
                &["A", "B"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "DELETE FROM cyc.ns.s12 WHERE id = 1",
                "s12",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.doput_and_assert(
                "s12",
                &[1],
                &["A_new"],
                &[
                    "+----+-------+",
                    "| id | name  |",
                    "+----+-------+",
                    "| 1  | A_new |",
                    "| 2  | B     |",
                    "+----+-------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S13  DELETE all -> Insert -> verify
            // ---------------------------------------------------------------
            h.create_table("s13").await?;
            h.doput_and_assert(
                "s13",
                &[1, 2],
                &["A", "B"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert("DELETE FROM cyc.ns.s13", "s13", &["++", "++"])
                .await?;
            h.doput_and_assert(
                "s13",
                &[10, 20],
                &["X", "Y"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 10 | X    |",
                    "| 20 | Y    |",
                    "+----+------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S14  Single-row table - multiple upserts (8 rounds)
            // ---------------------------------------------------------------
            h.create_table("s14").await?;
            for round in 1..=8u32 {
                let name = format!("v{round}");
                h.doput_and_assert(
                    "s14",
                    &[1],
                    &[&name],
                    &[
                        "+----+------+",
                        "| id | name |",
                        "+----+------+",
                        &format!("| 1  | {name:<4} |"),
                        "+----+------+",
                    ],
                )
                .await
                .map_err(|e| format!("S14 round {round}: {e}"))?;
            }

            // ---------------------------------------------------------------
            // S15  Insert -> UPDATE -> DELETE updated row -> verify others
            // ---------------------------------------------------------------
            h.create_table("s15").await?;
            h.doput_and_assert(
                "s15",
                &[1, 2, 3],
                &["A", "B", "C"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B    |",
                    "| 3  | C    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "UPDATE cyc.ns.s15 SET name = 'B2' WHERE id = 2",
                "s15",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B2   |",
                    "| 3  | C    |",
                    "+----+------+",
                ],
            )
            .await?;
            h.sql_mutate_and_assert(
                "DELETE FROM cyc.ns.s15 WHERE id = 2",
                "s15",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 3  | C    |",
                    "+----+------+",
                ],
            )
            .await?;

            // ---------------------------------------------------------------
            // S16  Rapid back-to-back upserts (10 rounds, no sleep)
            // ---------------------------------------------------------------
            h.create_table("s16").await?;
            h.doput_and_assert(
                "s16",
                &[1],
                &["v0"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | v0   |",
                    "+----+------+",
                ],
            )
            .await?;
            for round in 1..=10u32 {
                let name = format!("v{round}");
                h.doput_and_assert(
                    "s16",
                    &[1],
                    &[&name],
                    &[
                        "+----+------+",
                        "| id | name |",
                        "+----+------+",
                        &format!("| 1  | {name:<4} |"),
                        "+----+------+",
                    ],
                )
                .await
                .map_err(|e| format!("S16 round {round}: {e}"))?;
            }

            // ---------------------------------------------------------------
            // S17  Mix: DoPut inserts + DoPut upsert + SQL DELETE + SQL UPDATE
            // ---------------------------------------------------------------
            h.create_table("s17").await?;
            h.doput_and_assert(
                "s17",
                &[1, 2, 3, 4, 5],
                &["A", "B", "C", "D", "E"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B    |",
                    "| 3  | C    |",
                    "| 4  | D    |",
                    "| 5  | E    |",
                    "+----+------+",
                ],
            )
            .await?;
            // Upsert ids 2,4 (overlap) + insert id 6 (new)
            h.doput_and_assert(
                "s17",
                &[2, 4, 6],
                &["B2", "D2", "F"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B2   |",
                    "| 3  | C    |",
                    "| 4  | D2   |",
                    "| 5  | E    |",
                    "| 6  | F    |",
                    "+----+------+",
                ],
            )
            .await?;
            // Delete ids 3,5
            h.sql_mutate_and_assert(
                "DELETE FROM cyc.ns.s17 WHERE id IN (3, 5)",
                "s17",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A    |",
                    "| 2  | B2   |",
                    "| 4  | D2   |",
                    "| 6  | F    |",
                    "+----+------+",
                ],
            )
            .await?;
            // Update remaining: set name = name || '!'
            h.sql_mutate_and_assert(
                "UPDATE cyc.ns.s17 SET name = name || '!'",
                "s17",
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | A!   |",
                    "| 2  | B2!  |",
                    "| 4  | D2!  |",
                    "| 6  | F!   |",
                    "+----+------+",
                ],
            )
            .await?;
            // DoPut upsert + new: ids 1 (existing) and 7 (new)
            h.doput_and_assert(
                "s17",
                &[1, 7],
                &["AX", "G"],
                &[
                    "+----+------+",
                    "| id | name |",
                    "+----+------+",
                    "| 1  | AX   |",
                    "| 2  | B2!  |",
                    "| 4  | D2!  |",
                    "| 6  | F!   |",
                    "| 7  | G    |",
                    "+----+------+",
                ],
            )
            .await?;

            Ok(())
        }))
        .await
}

// ---------------------------------------------------------------------------
// Datalake (cold) tier — end-to-end integration test.
// ---------------------------------------------------------------------------

/// S3 bucket for the datalake e2e test — shared with the snapshot-refresh
/// integration tests. Per-run unique prefixes keep concurrent runs disjoint,
/// and the tiny `nation` dataset keeps transfer to a few KB per run.
const DATALAKE_TEST_BUCKET: &str = "spiceai-snapshot-integration-tests";
const DATALAKE_TEST_REGION: &str = "us-west-2";

/// Build the `nation` dataset (25 rows, public demo bucket) accelerated by
/// Cayenne with the datalake tier enabled at `datalake_location`.
///
/// `warm_max_age_ms` sets the residency trigger when present; left unset the
/// tier promotes on `warm_max_bytes` alone.
fn make_datalake_nation_dataset(
    cayenne_data_dir: &std::path::Path,
    cayenne_metadata_dir: &std::path::Path,
    datalake_location: &str,
    s3_creds: (&str, &str),
    warm_max_bytes: &str,
    warm_max_age_ms: Option<&str>,
    refresh_mode: RefreshMode,
) -> Dataset {
    let mut dataset = Dataset::new(
        "s3://spiceai-demo-datasets/tpch/nation/".to_string(),
        "nation".to_string(),
    );
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("s3_auth".to_string(), "public".to_string()),
        ]
        .into_iter()
        .collect(),
    ));

    let mut params = HashMap::from([
        (
            "cayenne_file_path".to_string(),
            cayenne_data_dir.to_string_lossy().to_string(),
        ),
        (
            "cayenne_metadata_dir".to_string(),
            cayenne_metadata_dir.to_string_lossy().to_string(),
        ),
        (
            "cayenne_datalake_location".to_string(),
            datalake_location.to_string(),
        ),
        (
            "cayenne_datalake_warm_max_bytes".to_string(),
            warm_max_bytes.to_string(),
        ),
        (
            "cayenne_datalake_tiering_check_interval_ms".to_string(),
            "500".to_string(),
        ),
    ]);
    if let Some(age_ms) = warm_max_age_ms {
        params.insert(
            "cayenne_datalake_warm_max_age_ms".to_string(),
            age_ms.to_string(),
        );
    }
    let (key, secret) = s3_creds;
    params.insert("cayenne_datalake_s3_auth".to_string(), "key".to_string());
    params.insert("cayenne_datalake_s3_key".to_string(), key.to_string());
    params.insert("cayenne_datalake_s3_secret".to_string(), secret.to_string());
    params.insert(
        "cayenne_datalake_s3_region".to_string(),
        DATALAKE_TEST_REGION.to_string(),
    );

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(refresh_mode),
        primary_key: Some("n_nationkey".to_string()),
        on_conflict: HashMap::from([("n_nationkey".to_string(), OnConflictBehavior::Upsert)]),
        params: Some(Params::from_string_map(params)),
        refresh_sql: None,
        ..Acceleration::default()
    });
    dataset
}

/// Number of files in the datalake manifest, read straight from the Cayenne
/// metastore.
///
/// This is the only signal that a promotion actually *committed*. Cold files are
/// uploaded before the metastore commit, so the presence of a `.vortex` object is
/// also consistent with an orphan from a failed commit — after which the warm
/// tier is untouched and cross-tier queries still answer from it.
///
/// `Ok(0)` covers "the table isn't there yet", which is the normal state before
/// the first promotion.
fn committed_datalake_file_count(metadata_dir: &std::path::Path) -> Result<i64, String> {
    let db_path = metadata_dir.join("cayenne.db");
    if !db_path.exists() {
        return Ok(0);
    }
    let conn = rusqlite::Connection::open(&db_path)
        .map_err(|e| format!("failed to open the Cayenne metastore {}: {e}", db_path.display()))?;
    // The runtime holds this database open; wait rather than fail on its writes.
    conn.busy_timeout(Duration::from_secs(10))
        .map_err(|e| format!("failed to set the metastore busy timeout: {e}"))?;
    let exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'cayenne_cold_tier_file'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("failed to probe for the datalake manifest table: {e}"))?;
    if exists == 0 {
        return Ok(0);
    }
    conn.query_row("SELECT COUNT(*) FROM cayenne_cold_tier_file", [], |row| {
        row.get(0)
    })
    .map_err(|e| format!("failed to count datalake manifest rows: {e}"))
}

/// Run `sql` and return the single `i64` value of the first column of the
/// first row (for `SELECT COUNT(*) …`-style assertions).
async fn query_single_i64(rt: &Runtime, sql: &str) -> Result<i64, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("query '{sql}' failed to plan: {e}"))?;
    let batches = result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("query '{sql}' failed to execute: {e}"))?;
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| format!("query '{sql}' returned no rows"))?;
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("query '{sql}' first column is not Int64"))?;
    Ok(col.value(0))
}

/// Full datalake-tier cycle against real S3:
/// registration probe → warm load (append mode; the datalake tier supports
/// `refresh_mode` 'changes' and 'append' only) → background promotion to the
/// datalake → cross-tier query correctness → restart with a tuned param
/// (reconciled, re-appended rows upsert-dedupe against the promoted data).
///
/// Runs against real S3 (bucket [`DATALAKE_TEST_BUCKET`]) with a few KB of
/// transfer per run. Credentials: `AWS_SNAPSHOT_KEY`/`AWS_SNAPSHOT_SECRET`
/// (already provisioned in the integration-test CI environment; the same pair
/// the snapshot-refresh tests use). FAILS when the credentials are missing —
/// a silent skip would read as coverage that doesn't exist.
#[tokio::test]
async fn test_cayenne_datalake_tier_e2e() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let (Ok(key), Ok(secret)) = (
        std::env::var("AWS_SNAPSHOT_KEY"),
        std::env::var("AWS_SNAPSHOT_SECRET"),
    ) else {
        return Err(
            "AWS_SNAPSHOT_KEY/AWS_SNAPSHOT_SECRET must be set (read/write on the integration-test bucket)"
                .to_string(),
        );
    };
    if key.is_empty() || secret.is_empty() {
        return Err(
            "AWS_SNAPSHOT_KEY/AWS_SNAPSHOT_SECRET must be non-empty (read/write on the integration-test bucket)"
                .to_string(),
        );
    }

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()
                .map_err(|e| format!("failed to create Cayenne temp directory: {e}"))?;
            let data_dir = temp_dir.path().join("data");
            let metadata_dir = temp_dir.path().join("metadata");

            let run_suffix: u64 = rand::rng().random();
            let prefix = format!("cayenne-datalake-e2e/{run_suffix:016x}");
            let location = format!("s3://{DATALAKE_TEST_BUCKET}/{prefix}/");

            // Direct S3 client for listing/cleanup (validation only — all
            // acceleration operations go through the Runtime).
            let store: Arc<dyn ObjectStore> = Arc::new(
                AmazonS3Builder::new()
                    .with_bucket_name(DATALAKE_TEST_BUCKET)
                    .with_region(DATALAKE_TEST_REGION)
                    .with_access_key_id(&key)
                    .with_secret_access_key(&secret)
                    .build()
                    .map_err(|e| format!("failed to build S3 client: {e}"))?,
            );

            // Precondition: the run's prefix must be empty, so the later
            // "found .vortex objects" signal can only come from THIS run's
            // promotion — never from leftovers of a prior run.
            let preexisting = list_objects_under_prefix(&store, &format!("{prefix}/")).await?;
            if !preexisting.is_empty() {
                return Err(format!(
                    "datalake test prefix s3://{DATALAKE_TEST_BUCKET}/{prefix}/ is not empty at test start ({} objects, e.g. {:?}); promotion detection would be unsound",
                    preexisting.len(),
                    preexisting.keys().next()
                ));
            }

            let result = datalake_e2e_inner(
                &data_dir,
                &metadata_dir,
                &location,
                (&key, &secret),
                &store,
                &prefix,
            )
            .await;

            // Best-effort cleanup of this run's unique prefix.
            let list_prefix = ObjectPath::from(format!("{prefix}/"));
            let mut stream = store.list(Some(&list_prefix));
            while let Some(Ok(meta)) = stream.next().await {
                let _ = store.delete(&meta.location).await;
            }

            result
        })
        .await
}

async fn datalake_e2e_inner(
    data_dir: &std::path::Path,
    metadata_dir: &std::path::Path,
    location: &str,
    s3_creds: (&str, &str),
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<(), String> {
    // ---- Phase 1: load, promote, and query across tiers. -------------------
    {
        let app = AppBuilder::new("test_cayenne_datalake_e2e")
            .with_dataset(make_datalake_nation_dataset(
                data_dir,
                metadata_dir,
                location,
                s3_creds,
                "1", // tiny trigger: promote as soon as any warm data exists
                None,
                RefreshMode::Append,
            ))
            .build();
        configure_test_datafusion();
        let rt = Runtime::builder().with_app(app).build().await;
        let cloned_rt = Arc::new(rt.clone());
        tokio::select! {
            () = tokio::time::sleep(Duration::from_mins(4)) => {
                return Err("Timed out waiting for dataset to load".to_string());
            }
            () = cloned_rt.load_components() => {}
        }
        runtime_ready_check_with_timeout(&rt, Duration::from_mins(5)).await;

        let count = query_single_i64(&rt, "SELECT COUNT(*) FROM nation").await?;
        if count != 25 {
            return Err(format!(
                "expected 25 nation rows before promotion, got {count}"
            ));
        }

        // Wait for the background promoter (500ms cadence, trigger = 1 byte)
        // to graduate the warm tier: cold .vortex objects appear under the
        // location prefix.
        let promoted = wait_until_true(Duration::from_mins(3), || async {
            let list_prefix = ObjectPath::from(format!("{prefix}/"));
            let mut stream = store.list(Some(&list_prefix));
            while let Some(Ok(meta)) = stream.next().await {
                if meta.location.as_ref().ends_with(".vortex") && meta.size > 0 {
                    return true;
                }
            }
            false
        })
        .await;
        if !promoted {
            return Err(
                "timed out waiting for warm→datalake promotion (no .vortex objects on the store)"
                    .to_string(),
            );
        }

        // Cross-tier correctness after promotion: full count and a point
        // lookup both served with the warm tier cleared.
        let count = query_single_i64(&rt, "SELECT COUNT(*) FROM nation").await?;
        if count != 25 {
            return Err(format!(
                "expected 25 nation rows after promotion, got {count}"
            ));
        }
        let point =
            query_single_i64(&rt, "SELECT COUNT(*) FROM nation WHERE n_nationkey = 7").await?;
        if point != 1 {
            return Err(format!(
                "expected 1 row for n_nationkey=7 after promotion, got {point}"
            ));
        }
    }

    // ---- Phase 2: reopen with a tuned trigger param — reconciled, and the
    // promoted data is still readable from the datalake tier. ----------------
    {
        let app = AppBuilder::new("test_cayenne_datalake_e2e")
            .with_dataset(make_datalake_nation_dataset(
                data_dir,
                metadata_dir,
                location,
                s3_creds,
                "999999999999", // changed trigger: reconcile persists it silently
                None,
                RefreshMode::Append,
            ))
            .build();
        let rt = Runtime::builder().with_app(app).build().await;
        let cloned_rt = Arc::new(rt.clone());
        tokio::select! {
            () = tokio::time::sleep(Duration::from_mins(4)) => {
                return Err("Timed out waiting for dataset to reload".to_string());
            }
            () = cloned_rt.load_components() => {}
        }
        runtime_ready_check_with_timeout(&rt, Duration::from_mins(5)).await;

        let count = query_single_i64(&rt, "SELECT COUNT(*) FROM nation").await?;
        if count != 25 {
            return Err(format!(
                "expected 25 nation rows after restart, got {count}"
            ));
        }
    }
    Ok(())
}

/// The residency-age trigger promotes end-to-end, through the real runtime, on a
/// table whose size can never reach the byte threshold — the low-volume case the
/// size triggers alone leave in the warm tier indefinitely.
///
/// Uses its own S3 prefix and data directory, asserted empty at start, so the
/// appearance of any `.vortex` object can only be an age-triggered promotion: the
/// byte trigger is set beyond what 25 rows can reach and the file trigger is left
/// off. Credentials and cost are as for [`test_cayenne_datalake_tier_e2e`] — it
/// FAILS rather than skips when they are missing.
#[tokio::test]
async fn test_cayenne_datalake_warm_max_age_e2e() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    let (Ok(key), Ok(secret)) = (
        std::env::var("AWS_SNAPSHOT_KEY"),
        std::env::var("AWS_SNAPSHOT_SECRET"),
    ) else {
        return Err(
            "AWS_SNAPSHOT_KEY/AWS_SNAPSHOT_SECRET must be set (read/write on the integration-test bucket)"
                .to_string(),
        );
    };
    if key.is_empty() || secret.is_empty() {
        return Err(
            "AWS_SNAPSHOT_KEY/AWS_SNAPSHOT_SECRET must be non-empty (read/write on the integration-test bucket)"
                .to_string(),
        );
    }

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()
                .map_err(|e| format!("failed to create Cayenne temp directory: {e}"))?;
            let data_dir = temp_dir.path().join("data");
            let metadata_dir = temp_dir.path().join("metadata");

            let run_suffix: u64 = rand::rng().random();
            let prefix = format!("cayenne-datalake-age-e2e/{run_suffix:016x}");
            let location = format!("s3://{DATALAKE_TEST_BUCKET}/{prefix}/");

            let store: Arc<dyn ObjectStore> = Arc::new(
                AmazonS3Builder::new()
                    .with_bucket_name(DATALAKE_TEST_BUCKET)
                    .with_region(DATALAKE_TEST_REGION)
                    .with_access_key_id(&key)
                    .with_secret_access_key(&secret)
                    .build()
                    .map_err(|e| format!("failed to build S3 client: {e}"))?,
            );

            // Precondition for soundness: an empty prefix means a later .vortex
            // object can only have come from this run's age-triggered promotion.
            let preexisting = list_objects_under_prefix(&store, &format!("{prefix}/")).await?;
            if !preexisting.is_empty() {
                return Err(format!(
                    "datalake age test prefix s3://{DATALAKE_TEST_BUCKET}/{prefix}/ is not empty at test start ({} objects, e.g. {:?}); promotion detection would be unsound",
                    preexisting.len(),
                    preexisting.keys().next()
                ));
            }

            let result = datalake_age_trigger_e2e_inner(
                &data_dir,
                &metadata_dir,
                &location,
                (&key, &secret),
                &store,
                &prefix,
            )
            .await;

            let list_prefix = ObjectPath::from(format!("{prefix}/"));
            let mut stream = store.list(Some(&list_prefix));
            while let Some(Ok(meta)) = stream.next().await {
                let _ = store.delete(&meta.location).await;
            }

            result
        })
        .await
}

async fn datalake_age_trigger_e2e_inner(
    data_dir: &std::path::Path,
    metadata_dir: &std::path::Path,
    location: &str,
    s3_creds: (&str, &str),
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<(), String> {
    let app = AppBuilder::new("test_cayenne_datalake_age_e2e")
        .with_dataset(make_datalake_nation_dataset(
            data_dir,
            metadata_dir,
            location,
            s3_creds,
            // Beyond anything 25 rows can reach, so the byte trigger can never
            // fire and only residency age can promote.
            "999999999999",
            Some("3000"),
            RefreshMode::Append,
        ))
        .build();
    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;
    let cloned_rt = Arc::new(rt.clone());
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(4)) => {
            return Err("Timed out waiting for dataset to load".to_string());
        }
        () = cloned_rt.load_components() => {}
    }
    runtime_ready_check_with_timeout(&rt, Duration::from_mins(5)).await;

    let count = query_single_i64(&rt, "SELECT COUNT(*) FROM nation").await?;
    if count != 25 {
        return Err(format!(
            "expected 25 nation rows before promotion, got {count}"
        ));
    }

    // The 500ms tiering tick marks the warm generation, then promotes once it has
    // been resident 3s. The ceiling is generous because the assertion under test
    // is that promotion happens at all without a reachable size trigger.
    //
    // Poll the COMMITTED manifest, not the store: the cold files are uploaded
    // before the metastore commit, so an object alone would also be satisfied by
    // an orphan left behind by a failed commit — under which the warm tier is
    // still intact and the queries below would pass without a promotion.
    let promoted = wait_until_true(Duration::from_mins(3), || async {
        committed_datalake_file_count(metadata_dir).unwrap_or(0) > 0
    })
    .await;
    if !promoted {
        let detail = committed_datalake_file_count(metadata_dir)
            .map_or_else(|e| format!(" (last manifest read: {e})"), |_| String::new());
        return Err(format!(
            "timed out waiting for an age-triggered warm→datalake promotion (no committed datalake manifest rows); the byte trigger was unreachable, so only cayenne_datalake_warm_max_age_ms could promote{detail}"
        ));
    }

    // The commit registers files it has already uploaded, so the manifest above
    // implies these objects — assert it to catch a manifest referencing nothing.
    let objects = list_objects_under_prefix(store, &format!("{prefix}/")).await?;
    if !objects
        .iter()
        .any(|(path, size)| path.ends_with(".vortex") && *size > 0)
    {
        return Err(format!(
            "datalake manifest is committed but the store holds no non-empty .vortex object under {prefix}/ ({} objects)",
            objects.len()
        ));
    }

    // Cross-tier correctness after an age-triggered move: the promoted rows are
    // served from the datalake with the warm tier cleared.
    let count = query_single_i64(&rt, "SELECT COUNT(*) FROM nation").await?;
    if count != 25 {
        return Err(format!(
            "expected 25 nation rows after the age-triggered promotion, got {count}"
        ));
    }
    let point = query_single_i64(&rt, "SELECT COUNT(*) FROM nation WHERE n_nationkey = 7").await?;
    if point != 1 {
        return Err(format!(
            "expected 1 row for n_nationkey=7 after the age-triggered promotion, got {point}"
        ));
    }

    Ok(())
}
