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
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_credential_bridge::{S3CredentialProvider, get_or_init_sdk_config};
use crate::configure_test_datafusion;
use crate::utils::runtime_ready_check_with_timeout;
use crate::{
    RecordBatch, init_tracing,
    utils::{register_test_connectors, test_request_context},
};
use app::AppBuilder;
use datafusion::sql::TableReference;
use futures::{StreamExt, TryStreamExt};
use object_store::{ClientOptions, ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath};
use runtime::dataupdate::{DataUpdate, UpdateType};
use runtime::{Runtime, accelerated_table::AcceleratedTable};
use runtime::dataaccelerator::cayenne::s3::generate_bucket_name;
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use spicepod::partitioning::PartitionedBy;
use test_framework::queries::QuerySet;

/// Append a single row to a Cayenne-accelerated table through the Runtime write path.
///
/// This exercises the full Cayenne write pipeline:
///   `write_data` → `AcceleratedTable::insert_into` → `CayenneTableProvider::insert_into`
///   → `CayenneDataSink::write_all_append` → `chunk_and_write_parallel_to_snapshot`
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

    // Build column arrays: VendorID gets the provided value, everything else is NULL.
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "VendorID" {
                Arc::new(Int64Array::from(vec![vendor_id])) as ArrayRef
            } else {
                new_null_array(field.data_type(), 1)
            }
        })
        .collect();

    // Cast VendorID to match exact schema type (source may use non-Int64)
    let columns: Vec<ArrayRef> = columns
        .into_iter()
        .zip(schema.fields())
        .map(|(col, field)| {
            if col.data_type() != field.data_type() {
                arrow::compute::cast(&col, field.data_type()).map_err(|e| {
                    format!(
                        "failed to cast column '{}' from {:?} to {:?}: {e}",
                        field.name(),
                        col.data_type(),
                        field.data_type()
                    )
                })
            } else {
                Ok(col)
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
    let endpoint = format!(
        "https://{bucket_name}.s3express-{zone_id}.{region}.amazonaws.com"
    );

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
        builder = builder.with_access_key_id(key).with_secret_access_key(secret);
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

fn make_s3_tpch_dataset(name: &str, partition_by: Option<String>) -> Dataset {
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
            // exclude lineitem, orders and customer to reduce egress
            let app = AppBuilder::new("test_cayenne_with_partitioned_tpch")
                .with_dataset(make_s3_tpch_dataset(
                    "nation",
                    Some("n_regionkey".to_string()),
                ))
                .with_dataset(make_s3_tpch_dataset("region", None))
                .with_dataset(make_s3_tpch_dataset(
                    "supplier",
                    Some("bucket(10, s_suppkey)".to_string()),
                ))
                .with_dataset(make_s3_tpch_dataset(
                    "part",
                    Some("bucket(10, p_partkey)".to_string()),
                ))
                .with_dataset(make_s3_tpch_dataset(
                    "partsupp",
                    Some("bucket(10, ps_partkey)".to_string()),
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check_with_timeout(&rt, Duration::from_secs(600)).await;

            let queries = QuerySet::Tpch
                .get_queries(None, None, None)
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
    let zone_ids = std::env::var("CAYENNE_S3_ZONE_IDS")
        .unwrap_or_else(|_| "usw2-az1,usw2-az4".to_string());
    let region = std::env::var("CAYENNE_S3_REGION").unwrap_or_else(|_| "us-west-2".to_string());

    let mut zone_pool: Vec<String> = zone_ids
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(std::string::ToString::to_string)
        .collect();

    if let Ok(third_zone) = std::env::var("CAYENNE_S3_ZONE_ID_3") {
        let third_zone = third_zone.trim();
        if !third_zone.is_empty() && !zone_pool.iter().any(|z| z == third_zone) {
            zone_pool.push(third_zone.to_string());
        }
    }

    if zone_pool.is_empty() {
        return Err(
            "At least 1 S3 zone ID is required for acceleration tests".to_string(),
        );
    }

    // Clean stale Cayenne catalog metadata from previous test runs.
    // The catalog is a local SQLite DB that persists across test invocations.
    // If a previous run created a table with a different configuration (e.g.
    // different PK/on_conflict), re-creation will fail with
    // "already exists with different configuration" unless the old entry is removed.
    let cayenne_db = std::path::PathBuf::from(".spice/data/metadata/cayenne.db");
    if cayenne_db.exists() {
        let _ = std::fs::remove_file(&cayenne_db);
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

            // Clean stale S3 data from previous test runs for this scenario.
            // Each zone bucket may contain Vortex files from a prior run whose
            // metadata no longer matches the current table configuration.
            for zone_id in &zone_pool[..zone_count] {
                let bucket_name = generate_bucket_name(&app_name, &table_name, zone_id)
                    .map_err(|e| format!("failed to generate bucket name: {e}"))?;
                if let Ok(store) = build_zone_store(&bucket_name, zone_id, &region).await {
                    let prefix = ObjectPath::from(format!("{table_name}/"));
                    let mut stream = store.list(Some(&prefix));
                    while let Some(Ok(meta)) = stream.next().await {
                        let _ = store.delete(&meta.location).await;
                    }
                    tracing::info!("Cleaned S3 data for {table_name} in zone {zone_id}");
                }
            }

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
                () = tokio::time::sleep(std::time::Duration::from_secs(240)) => {
                    return Err(format!("Timed out waiting for datasets to load for {zone_count}-zone scenario"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check_with_timeout(&rt, Duration::from_secs(300)).await;

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
            if accelerated_provider
                .as_any()
                .downcast_ref::<AcceleratedTable>()
                .is_none()
            {
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
            // Uses a VendorID that does NOT exist to avoid upsert deduplication.
            // The row is written through Runtime/DataFusion → AcceleratedTable → CayenneTableProvider.
            let new_vendor_id: i64 = 999_999;
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
                let scenario_zones = zone_pool[..zone_count].to_vec();
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
        }

        // --- Post-test cleanup: remove S3 data and local metadata ---
        // Clean up all zone buckets for every scenario that ran so subsequent
        // test invocations start from a clean slate.
        for zone_count in 1..=zone_pool.len() {
            let table_name = format!("taxi_multi_zone_live_{zone_count}z");
            let app_name = format!("test_cayenne_s3_express_multi_zone_live_{zone_count}z");
            for zone_id in &zone_pool[..zone_count] {
                if let Ok(bucket_name) = generate_bucket_name(&app_name, &table_name, zone_id) {
                    if let Ok(store) = build_zone_store(&bucket_name, zone_id, &region).await {
                        let prefix = ObjectPath::from(format!("{table_name}/"));
                        let mut stream = store.list(Some(&prefix));
                        while let Some(Ok(meta)) = stream.next().await {
                            let _ = store.delete(&meta.location).await;
                        }
                    }
                }
            }
        }
        let cayenne_db = std::path::PathBuf::from(".spice/data/metadata/cayenne.db");
        if cayenne_db.exists() {
            let _ = std::fs::remove_file(&cayenne_db);
        }
        tracing::info!("Post-test cleanup complete: removed S3 data and local Cayenne catalog");

        Ok(())
    }).await
}
