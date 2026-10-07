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

//! S3 folder markers under a format-selected listing.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/14816>. Glue
//! registers Parquet and ORC tables with `file_extension: '*'` so extensionless
//! Hive objects (`000000_0`) are read. An S3 folder marker (the zero-byte key
//! `table/`) lists as `table`, which that listing also accepted as a data
//! object, so every scan failed with "Parquet files are at least 8 bytes long,
//! but file length is 0".

use std::sync::Arc;
use std::time::Duration;

use anyhow::ensure;
use app::AppBuilder;
use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use bollard::secret::HealthConfig;
use datafusion::parquet::arrow::ArrowWriter;
use futures::StreamExt;
use runtime::Runtime;
use spicepod::{
    component::{caching::SQLResultsCacheConfig, dataset::Dataset},
    param::Params,
};

use crate::{
    configure_test_datafusion,
    docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port},
    init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "glue-tables";
/// The table location, as Glue stores it (no trailing slash).
const TABLE_PREFIX: &str = "tpch/supplier";
/// Extensionless Hive data objects, ids `1..=50` and `51..=100`.
const DATA_OBJECTS: [(&str, i64); 2] = [("000000_0", 1), ("000001_0", 51)];
const ROWS_PER_OBJECT: i64 = 50;

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

async fn create_bucket(client: &aws_sdk_s3::Client) -> Result<(), anyhow::Error> {
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

/// The zero-byte `table/` object the S3 console's "Create folder" (and many
/// writers) leave at a table location.
async fn put_folder_marker(client: &aws_sdk_s3::Client) -> Result<(), anyhow::Error> {
    put_object(client, &format!("{TABLE_PREFIX}/"), Vec::new()).await
}

async fn put_data_objects(client: &aws_sdk_s3::Client) -> Result<(), anyhow::Error> {
    for (name, first_id) in DATA_OBJECTS {
        put_object(
            client,
            &format!("{TABLE_PREFIX}/{name}"),
            parquet_bytes(first_id),
        )
        .await?;
    }
    Ok(())
}

fn parquet_bytes(first_id: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (first_id..first_id + ROWS_PER_OBJECT).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("Supplier#{id:09}")).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("supplier batch");
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    buffer
}

/// The dataset the Glue connector builds for a Parquet table: the location with
/// a trailing slash, `file_extension: '*'` and Hive partitioning.
fn glue_parquet_dataset(endpoint: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("s3://{BUCKET}/{TABLE_PREFIX}/"), "supplier");
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("file_extension".to_string(), "*".to_string()),
            ("hive_partitioning_enabled".to_string(), "true".to_string()),
            ("s3_endpoint".to_string(), endpoint.to_string()),
            ("s3_region".to_string(), "us-east-1".to_string()),
            ("s3_auth".to_string(), "key".to_string()),
            ("s3_key".to_string(), ACCESS_KEY.to_string()),
            ("s3_secret".to_string(), SECRET_KEY.to_string()),
            ("s3_url_style".to_string(), "path".to_string()),
            ("allow_http".to_string(), "true".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset.metadata.insert(
        "_last_modified".to_string(),
        serde_json::Value::String("enabled".to_string()),
    );
    dataset
}

async fn start_runtime(endpoint: &str) -> Result<Arc<Runtime>, anyhow::Error> {
    configure_test_datafusion();
    let rt = Arc::new(
        Runtime::builder()
            .with_app_opt(Some(Arc::new(
                AppBuilder::new("s3_folder_marker")
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..SQLResultsCacheConfig::default()
                    })
                    .with_dataset(glue_parquet_dataset(endpoint))
                    .build(),
            )))
            .build()
            .await,
    );
    let cloned = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            return Err(anyhow::anyhow!("timed out loading the supplier dataset"));
        }
        () = cloned.load_components() => {}
    }
    runtime_ready_check(rt.as_ref()).await;
    Ok(rt)
}

async fn query_batches(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("{sql}: {e}"))?;
    let mut batches = Vec::new();
    let mut data = result.data;
    while let Some(batch) = data.next().await {
        batches.push(batch.map_err(|e| anyhow::anyhow!("{sql}: {e}"))?);
    }
    Ok(batches)
}

/// `(count(*), sum(s_suppkey), min(s_name), max(s_name))` of the query.
async fn supplier_summary(
    rt: &Runtime,
    sql: &str,
) -> Result<(i64, i64, String, String), anyhow::Error> {
    let batches = query_batches(rt, sql).await?;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    ensure!(rows == 1, "{sql}: expected one summary row, got {rows}");
    let batch = batches
        .iter()
        .find(|b| b.num_rows() == 1)
        .expect("one summary row");
    let int = |i: usize| -> Result<i64, anyhow::Error> {
        let column = batch.column(i);
        column
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(0))
            .ok_or_else(|| anyhow::anyhow!("column {i} is {}", column.data_type()))
    };
    let string = |i: usize| -> Result<String, anyhow::Error> {
        let column = arrow::compute::cast(batch.column(i), &DataType::Utf8)?;
        Ok(column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8")
            .value(0)
            .to_string())
    };
    Ok((int(0)?, int(1)?, string(2)?, string(3)?))
}

/// The `DataSourceExec` line of the query's physical plan.
async fn scan_line(rt: &Runtime, sql: &str) -> Result<String, anyhow::Error> {
    let batches = query_batches(rt, &format!("EXPLAIN {sql}")).await?;
    let pretty = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    pretty
        .lines()
        .find(|line| line.contains("DataSourceExec"))
        .map(|line| line.trim().to_string())
        .ok_or_else(|| anyhow::anyhow!("no DataSourceExec in the plan of {sql}:\n{pretty}"))
}

const FULL_TABLE: (i64, i64, &str, &str) = (100, 5050, "Supplier#000000001", "Supplier#000000100");

async fn assert_reads_only_the_data_objects(rt: &Runtime) -> Result<(), anyhow::Error> {
    // The plain scan and the `_last_modified`-pruned scan each build their own
    // file list from the object listing.
    for sql in [
        "SELECT count(*), sum(s_suppkey), min(s_name), max(s_name) FROM supplier",
        "SELECT count(*), sum(s_suppkey), min(s_name), max(s_name) FROM supplier \
         WHERE _last_modified > TIMESTAMP '1970-01-01T00:00:00Z'",
    ] {
        let (count, sum, min, max) = supplier_summary(rt, sql).await?;
        assert_eq!(
            (count, sum, min.as_str(), max.as_str()),
            FULL_TABLE,
            "{sql}"
        );

        // Non-vacuous: the scan lists exactly the two data objects, and the
        // folder marker (`tpch/supplier`) is not among them.
        let scan = scan_line(rt, sql).await?;
        for (name, _) in DATA_OBJECTS {
            ensure!(
                scan.contains(&format!("{TABLE_PREFIX}/{name}")),
                "{sql}: the scan does not read {name}: {scan}"
            );
        }
        ensure!(
            !scan.contains(&format!("{TABLE_PREFIX},"))
                && !scan.contains(&format!("{TABLE_PREFIX}]")),
            "{sql}: the scan reads the folder marker: {scan}"
        );
    }
    Ok(())
}

/// The layout the Glue TPC-H tables have: a folder marker at the table
/// location, created before the data was uploaded into it.
#[tokio::test]
async fn a_folder_marker_at_the_table_location_is_not_scanned() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let container = start_object_store("spice_test_rustfs_folder_marker_scan").await?;
            let endpoint = format!("http://127.0.0.1:{}", container.host_port(9000)?);
            let result = async {
                let client = s3_client(&endpoint);
                create_bucket(&client).await?;
                put_folder_marker(&client).await?;
                put_data_objects(&client).await?;

                let rt = start_runtime(&endpoint).await?;
                assert_reads_only_the_data_objects(&rt).await
            }
            .await;
            container.remove().await?;
            result
        })
        .await
}
