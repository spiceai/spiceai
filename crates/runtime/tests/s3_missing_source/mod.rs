/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Deleted S3 objects vs a full refresh into a Cayenne file acceleration.
//!
//! A deleted object, or a prefix left with no data object, used to list as an
//! empty table, so the next full refresh replaced the acceleration with zero
//! rows and the dataset stayed ready. The refresh must fail and keep the rows.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::ensure;
use app::AppBuilder;
use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use bollard::secret::HealthConfig;
use datafusion::common::TableReference;
use datafusion::parquet::arrow::ArrowWriter;
use runtime::Runtime;
use runtime::accelerated::refresh_completion::RefreshCompletionOutcome;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::{caching::SQLResultsCacheConfig, dataset::Dataset},
    param::Params,
};

use crate::{
    configure_test_datafusion,
    docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port},
    init_tracing,
    utils::{run_query, runtime_ready_check, test_request_context},
};

const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "missing-source";
const CONTAINER_NAME: &str = "spice_test_rustfs_missing_source";

async fn start_object_store(name: &str) -> Result<RunningContainer, anyhow::Error> {
    let container = ContainerRunnerBuilder::new(name)
        .image("rustfs/rustfs:latest".to_string())
        .publish_port(9000)
        .add_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .add_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
        .command(["/data"])
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "netstat -tulpn | grep 9000 || exit 1".to_string(),
            ]),
            interval: Some(500_000_000),
            timeout: Some(1_000_000_000),
            retries: Some(20),
            start_period: Some(2_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(Duration::from_mins(2)))
        .await?;
    wait_for_tcp_port(
        "127.0.0.1",
        container.host_port(9000)?,
        Duration::from_mins(1),
    )
    .await?;
    Ok(container)
}

fn s3_client(endpoint: &str) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

async fn create_bucket_for_test(
    _endpoint: &str,
    client: &aws_sdk_s3::Client,
) -> Result<(), anyhow::Error> {
    // The store may still be finishing startup after its port opens.
    let mut last_error = String::new();
    for _ in 0..50 {
        match client.create_bucket().bucket(BUCKET).send().await {
            Ok(_) => return Ok(()),
            Err(e) => last_error = format!("{e:?}"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow::anyhow!(
        "failed to create bucket {BUCKET}: {last_error}"
    ))
}

const BUCKET_PREFIX: &str = "pfx";
const BUCKET_MARKER: &str = "marker";
const OBJECT_KEY: &str = "one/orders.parquet";
const ROWS_PER_OBJECT: i64 = 50;
const REFRESH_TIMEOUT: Duration = Duration::from_mins(1);

fn parquet_bytes(first_id: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Vec<i64> = (first_id..first_id + ROWS_PER_OBJECT).collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(ids))])
        .expect("id batch");
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    buffer
}

async fn put_object(
    client: &aws_sdk_s3::Client,
    key: &str,
    body: Vec<u8>,
) -> Result<(), anyhow::Error> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await?;
    Ok(())
}

async fn delete_object(client: &aws_sdk_s3::Client, key: &str) -> Result<(), anyhow::Error> {
    client
        .delete_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await?;
    Ok(())
}

/// The keys under `prefix`, so a test can show the listing it relies on.
async fn list_keys(
    client: &aws_sdk_s3::Client,
    prefix: &str,
) -> Result<Vec<String>, anyhow::Error> {
    let listed = client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(prefix)
        .send()
        .await?;
    Ok(listed
        .contents()
        .iter()
        .filter_map(|object| object.key().map(str::to_string))
        .collect())
}

fn prefix_part(i: i64) -> String {
    format!("{BUCKET_PREFIX}/part-{i}.parquet")
}

fn marker_part() -> String {
    format!("{BUCKET_MARKER}/part-0.parquet")
}

/// A Cayenne `mode: file` acceleration with `refresh_mode: full`, refreshed only
/// on request and never retried, so each refresh's outcome is the one asserted.
fn cayenne_dataset(endpoint: &str, from: &str, name: &str, dir: &std::path::Path) -> Dataset {
    let mut dataset = Dataset::new(format!("s3://{BUCKET}/{from}"), name);
    dataset.params = Some(Params::from_string_map(HashMap::from([
        ("file_format".to_string(), "parquet".to_string()),
        ("s3_endpoint".to_string(), endpoint.to_string()),
        ("s3_region".to_string(), "us-east-1".to_string()),
        ("s3_auth".to_string(), "key".to_string()),
        ("s3_key".to_string(), ACCESS_KEY.to_string()),
        ("s3_secret".to_string(), SECRET_KEY.to_string()),
        ("s3_url_style".to_string(), "path".to_string()),
        ("allow_http".to_string(), "true".to_string()),
    ])));
    let data_dir = dir.join(name).join("data");
    let metadata_dir = dir.join(name).join("metadata");
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        refresh_retry_enabled: false,
        params: Some(Params::from_string_map(HashMap::from([
            (
                "cayenne_file_path".to_string(),
                data_dir.display().to_string(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                metadata_dir.display().to_string(),
            ),
        ]))),
        ..Acceleration::default()
    });
    dataset
}

async fn start_runtime(
    endpoint: &str,
    dir: &std::path::Path,
) -> Result<Arc<Runtime>, anyhow::Error> {
    configure_test_datafusion();
    let rt = Arc::new(
        Runtime::builder()
            .with_app_opt(Some(Arc::new(
                AppBuilder::new("s3_missing_source")
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..SQLResultsCacheConfig::default()
                    })
                    .with_dataset(cayenne_dataset(endpoint, "pfx/", "prefix_cay", dir))
                    .with_dataset(cayenne_dataset(endpoint, "marker/", "marker_cay", dir))
                    .with_dataset(cayenne_dataset(endpoint, OBJECT_KEY, "object_cay", dir))
                    .build(),
            )))
            .build()
            .await,
    );
    let cloned = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(2)) => {
            return Err(anyhow::anyhow!("timed out loading the S3 datasets"));
        }
        () = cloned.load_components() => {}
    }
    runtime_ready_check(rt.as_ref()).await;
    Ok(rt)
}

/// Every `id` in `table`, in order, so a check compares the exact rows.
async fn ids(rt: &Arc<Runtime>, table: &str) -> Result<Vec<i64>, anyhow::Error> {
    let sql = format!("SELECT id FROM {table} ORDER BY id");
    let mut ids = Vec::new();
    for batch in run_query(rt, &sql).await? {
        let column = arrow::compute::cast(batch.column(0), &DataType::Int64)?;
        let column = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cast to Int64");
        ids.extend((0..column.len()).map(|i| column.value(i)));
    }
    Ok(ids)
}

/// The ids `first..first + ROWS_PER_OBJECT` for each `first`, in order.
fn expected(firsts: &[i64]) -> Vec<i64> {
    let mut ids: Vec<i64> = firsts
        .iter()
        .flat_map(|first| *first..*first + ROWS_PER_OBJECT)
        .collect();
    ids.sort_unstable();
    ids
}

/// Requests a refresh of `table` and returns how it ended.
async fn refresh(
    rt: &Arc<Runtime>,
    table: &str,
) -> Result<RefreshCompletionOutcome, anyhow::Error> {
    let waiter = rt
        .datafusion()
        .refresh_table(&TableReference::from(table), None)
        .await
        .map_err(|e| anyhow::anyhow!("refresh_table failed for {table}: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("no refresh notifier for {table}"))?;
    tokio::time::timeout(REFRESH_TIMEOUT, waiter.wait())
        .await
        .map_err(|_| anyhow::anyhow!("the refresh of {table} did not end in {REFRESH_TIMEOUT:?}"))
}

/// A full refresh whose source lists no data file fails and keeps the rows the
/// last good refresh loaded.
async fn assert_refresh_fails_and_keeps(
    rt: &Arc<Runtime>,
    table: &str,
    kept: &[i64],
) -> Result<(), anyhow::Error> {
    let outcome = refresh(rt, table).await?;
    ensure!(
        outcome.is_terminal_failure(),
        "{table}: a refresh of a source with no data files must fail, got {outcome:?}"
    );
    let now = ids(rt, table).await?;
    ensure!(
        now == kept,
        "{table}: the failed refresh must keep the last loaded ids, got {} rows",
        now.len()
    );
    Ok(())
}

async fn assert_refresh_loads(
    rt: &Arc<Runtime>,
    table: &str,
    loaded: &[i64],
) -> Result<(), anyhow::Error> {
    let outcome = refresh(rt, table).await?;
    ensure!(
        outcome.is_answered(),
        "{table}: a refresh of a restored source must succeed, got {outcome:?}"
    );
    let now = ids(rt, table).await?;
    ensure!(
        now == loaded,
        "{table}: the refresh must load exactly the restored ids, got {} rows",
        now.len()
    );
    Ok(())
}

/// Deleted S3 objects must not refresh a Cayenne file acceleration to zero rows:
/// an emptied prefix, a prefix whose only object left is its zero-byte folder
/// marker, and a single-object dataset whose key now answers `404 NoSuchKey`.
/// Each refresh fails and keeps the last good rows; restored objects load again.
#[tokio::test]
async fn deleted_s3_objects_fail_the_refresh_and_keep_the_acceleration() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let container = start_object_store(CONTAINER_NAME).await?;
            let endpoint = format!("http://127.0.0.1:{}", container.host_port(9000)?);
            let result = async {
                let client = s3_client(&endpoint);
                create_bucket_for_test(&endpoint, &client).await?;
                for i in 0..3 {
                    put_object(&client, &prefix_part(i), parquet_bytes(i * 1000)).await?;
                }
                put_object(&client, &format!("{BUCKET_MARKER}/"), Vec::new()).await?;
                put_object(&client, &marker_part(), parquet_bytes(0)).await?;
                put_object(&client, OBJECT_KEY, parquet_bytes(0)).await?;

                let dir = tempfile::tempdir()?;
                let rt = start_runtime(&endpoint, dir.path()).await?;
                let prefix_rows = expected(&[0, 1000, 2000]);
                let one_object = expected(&[0]);
                ensure!(ids(&rt, "prefix_cay").await? == prefix_rows);
                ensure!(ids(&rt, "marker_cay").await? == one_object);
                ensure!(ids(&rt, "object_cay").await? == one_object);

                for i in 0..3 {
                    delete_object(&client, &prefix_part(i)).await?;
                }
                delete_object(&client, &marker_part()).await?;
                delete_object(&client, OBJECT_KEY).await?;
                // Non-vacuous: the prefix lists nothing, and the folder marker is the only key left.
                ensure!(list_keys(&client, "pfx/").await?.is_empty());
                ensure!(list_keys(&client, "marker/").await? == vec!["marker/".to_string()]);
                ensure!(list_keys(&client, OBJECT_KEY).await?.is_empty());

                assert_refresh_fails_and_keeps(&rt, "prefix_cay", &prefix_rows).await?;
                assert_refresh_fails_and_keeps(&rt, "marker_cay", &one_object).await?;
                assert_refresh_fails_and_keeps(&rt, "object_cay", &one_object).await?;

                // Restored objects (different rows) load on the next refresh.
                put_object(&client, &prefix_part(0), parquet_bytes(5000)).await?;
                put_object(&client, &marker_part(), parquet_bytes(6000)).await?;
                put_object(&client, OBJECT_KEY, parquet_bytes(7000)).await?;
                assert_refresh_loads(&rt, "prefix_cay", &expected(&[5000])).await?;
                assert_refresh_loads(&rt, "marker_cay", &expected(&[6000])).await?;
                assert_refresh_loads(&rt, "object_cay", &expected(&[7000])).await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;
            container.remove_after(result).await
        })
        .await
}
