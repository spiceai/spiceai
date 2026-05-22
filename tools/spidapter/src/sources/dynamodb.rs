// Copyright 2026 Spice AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime, TelemetryConfig};
use spicepod::param::Params;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::{AdbcDriver, DatasetConfig, SetupResponse};
use uuid::Uuid;

use super::super::{RunState, SetupConfig, resolve_aws_region};

pub(crate) const DYNAMODB_ADBC_DEFAULT_PARALLELISM: usize = 10;

/// Tracks `DynamoDB` tables created during setup so they can be cleaned up on teardown.
#[derive(Debug, Clone)]
pub(crate) struct DynamoDbTeardownInfo {
    pub(crate) table_names: Vec<String>,
    pub(crate) region: String,
}

pub(crate) fn build_dynamodb_setup_response(
    setup_config: &SetupConfig,
    state: &RunState,
    datasets: &HashMap<String, DatasetConfig>,
    table_name_map: &HashMap<String, String>,
) -> SetupResponse {
    let region = resolve_aws_region(setup_config);

    let mut dynamodb_kwargs: HashMap<String, serde_json::Value> = HashMap::from([(
        "adbc.driver.dynamodb.region".into(),
        serde_json::Value::String(region),
    )]);

    // Forward AWS credentials from environment to the DynamoDB ADBC driver.
    for (env_var, adbc_key) in [
        ("AWS_ACCESS_KEY_ID", "adbc.driver.dynamodb.access_key_id"),
        (
            "AWS_SECRET_ACCESS_KEY",
            "adbc.driver.dynamodb.secret_access_key",
        ),
        ("AWS_SESSION_TOKEN", "adbc.driver.dynamodb.session_token"),
    ] {
        if let Ok(val) = std::env::var(env_var) {
            dynamodb_kwargs.insert(adbc_key.into(), serde_json::Value::String(val));
        }
    }

    for dataset_name in datasets.keys() {
        let parallelism = dynamodb_table_throughput(dataset_name)
            .map_or(DYNAMODB_ADBC_DEFAULT_PARALLELISM, |(_, p)| p);
        dynamodb_kwargs.insert(
            format!("adbc.driver.dynamodb.parallelism.{dataset_name}"),
            serde_json::Value::String(parallelism.to_string()),
        );
    }

    // Flight SQL read driver pointing to the provisioned Spice Cloud app.
    let read_db_kwargs = HashMap::from([
        (
            "uri".into(),
            serde_json::Value::String(state.flight_url().to_string()),
        ),
        ("username".into(), serde_json::Value::String(String::new())),
        (
            "password".into(),
            serde_json::Value::String(state.password().to_string()),
        ),
    ]);

    SetupResponse {
        driver: AdbcDriver::Dynamodb,
        db_kwargs: dynamodb_kwargs,
        catalog_namespace: None,
        read_driver: Some((AdbcDriver::Flightsql, read_db_kwargs)),
        endpoints: HashMap::new(),
        table_name_map: table_name_map.clone(),
    }
}

/// Pre-create `DynamoDB` tables so that spicebench can ingest via `CreateAppend` mode
/// without needing to set the `partition_key` statement option.
///
/// Returns a mapping from logical dataset name → physical `DynamoDB` table name.
// Validated, self-contained spec for one DynamoDB table — all data extracted before spawning.
struct TableSpec {
    dataset_name: String,
    physical_name: String,
    partition_key: String,
    pk_type: aws_sdk_dynamodb::types::ScalarAttributeType,
    sort_key: Option<(String, aws_sdk_dynamodb::types::ScalarAttributeType)>,
}

pub(crate) async fn create_dynamodb_tables(
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<HashMap<String, String>> {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ProvisionedThroughput,
        StreamSpecification, StreamViewType, Tag,
    };

    let region = resolve_aws_region(setup_config);
    let client = build_dynamodb_client(&region).await;
    let prefix = dynamodb_table_prefix();

    eprintln!(
        "[stdio] DynamoDB: pre-creating {} table(s) in region {region} (prefix: {prefix})",
        datasets.len(),
    );

    // Validate schemas synchronously before spawning any tasks.
    let specs: Vec<TableSpec> = datasets
        .iter()
        .map(|(dataset_name, config)| {
            let physical_name = format!("{prefix}{dataset_name}");
            let partition_key = config.primary_key_columns.first().ok_or_else(|| {
                anyhow::anyhow!(
                    "Dataset '{dataset_name}' has no primary key columns; cannot create DynamoDB table"
                )
            })?;
            let pk_type = infer_dynamodb_key_type(
                config.schema.field_with_name(partition_key).map_err(|_| {
                    anyhow::anyhow!(
                        "Dataset '{dataset_name}': partition key '{partition_key}' not found in schema"
                    )
                })?.data_type(),
            );
            let sort_key = config.primary_key_columns.get(1).map(|sk| {
                let sk_type = infer_dynamodb_key_type(
                    config.schema.field_with_name(sk).map_err(|_| {
                        anyhow::anyhow!("Dataset '{dataset_name}': sort key '{sk}' not found in schema")
                    })?.data_type(),
                );
                Ok::<_, anyhow::Error>((sk.clone(), sk_type))
            }).transpose()?;
            Ok(TableSpec {
                dataset_name: dataset_name.clone(),
                physical_name,
                partition_key: partition_key.clone(),
                pk_type,
                sort_key,
            })
        })
        .collect::<anyhow::Result<_>>()?;

    // Phase 1: send all CreateTable requests concurrently.
    let mut create_tasks: tokio::task::JoinSet<anyhow::Result<(String, String, bool)>> =
        tokio::task::JoinSet::new();

    for spec in specs {
        let client = client.clone();
        create_tasks.spawn(async move {
            let TableSpec { dataset_name, physical_name, partition_key, pk_type, sort_key } = spec;

            eprintln!(
                "[stdio] DynamoDB: creating table '{physical_name}' for dataset '{dataset_name}' (partition_key='{partition_key}'{sk_log})",
                sk_log = sort_key.as_ref().map_or(String::new(), |(sk, _)| format!(", sort_key='{sk}'"))
            );

            let mut create_req = client
                .create_table()
                .table_name(&physical_name)
                .tags(Tag::builder().key("type").value("spicebench").build()?)
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(&partition_key)
                        .key_type(KeyType::Hash)
                        .build()?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(&partition_key)
                        .attribute_type(pk_type)
                        .build()?,
                );

            if let Some((wcu, _)) = dynamodb_table_throughput(&dataset_name) {
                create_req = create_req
                    .billing_mode(BillingMode::Provisioned)
                    .provisioned_throughput(
                        ProvisionedThroughput::builder()
                            .write_capacity_units(wcu)
                            .read_capacity_units(10000)
                            .build()?,
                    );
            } else {
                create_req = create_req.billing_mode(BillingMode::PayPerRequest);
            }

            create_req = create_req.stream_specification(
                StreamSpecification::builder()
                    .stream_enabled(true)
                    .stream_view_type(StreamViewType::NewAndOldImages)
                    .build()?,
            );

            if let Some((sk, sk_type)) = sort_key {
                create_req = create_req
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name(&sk)
                            .key_type(KeyType::Range)
                            .build()?,
                    )
                    .attribute_definitions(
                        AttributeDefinition::builder()
                            .attribute_name(&sk)
                            .attribute_type(sk_type)
                            .build()?,
                    );
            }

            let needs_wait = match create_req.send().await {
                Ok(_) => {
                    eprintln!(
                        "[stdio] DynamoDB: table '{physical_name}' created, waiting for ACTIVE status..."
                    );
                    true
                }
                Err(err) => {
                    let is_in_use = err.as_service_error().is_some_and(
                        aws_sdk_dynamodb::operation::create_table::CreateTableError::is_resource_in_use_exception,
                    );
                    if is_in_use {
                        eprintln!(
                            "[stdio] DynamoDB: table '{physical_name}' already exists, skipping creation"
                        );
                        false
                    } else {
                        eprintln!("[stdio] DynamoDB: error creating table '{physical_name}': {err:?}");
                        return Err(anyhow::anyhow!(
                            "Failed to create DynamoDB table '{physical_name}': {err}"
                        ));
                    }
                }
            };

            Ok((dataset_name, physical_name, needs_wait))
        });
    }

    let mut table_name_map: HashMap<String, String> = HashMap::new();
    let mut wait_tables: Vec<String> = Vec::new();

    while let Some(result) = create_tasks.join_next().await {
        let (dataset_name, physical_name, needs_wait) =
            result.map_err(|e| anyhow::anyhow!("DynamoDB create task panicked: {e}"))??;
        table_name_map.insert(dataset_name, physical_name.clone());
        if needs_wait {
            wait_tables.push(physical_name);
        }
    }

    // Phase 2: wait for all newly-created tables to become ACTIVE concurrently.
    let mut wait_tasks: tokio::task::JoinSet<anyhow::Result<()>> = tokio::task::JoinSet::new();
    for physical_name in wait_tables {
        let client = client.clone();
        wait_tasks.spawn(async move { wait_for_table_active(&client, &physical_name).await });
    }
    while let Some(result) = wait_tasks.join_next().await {
        result.map_err(|e| anyhow::anyhow!("DynamoDB wait task panicked: {e}"))??;
    }

    eprintln!("[stdio] DynamoDB: table pre-creation complete");
    Ok(table_name_map)
}

pub(crate) async fn delete_dynamodb_tables(info: &DynamoDbTeardownInfo) -> anyhow::Result<()> {
    eprintln!(
        "[stdio] teardown: deleting {} DynamoDB table(s) in region {}",
        info.table_names.len(),
        info.region,
    );

    let client = build_dynamodb_client(&info.region).await;

    for table_name in &info.table_names {
        eprintln!("[stdio] teardown: deleting DynamoDB table '{table_name}'");
        match client.delete_table().table_name(table_name).send().await {
            Ok(_) => {
                eprintln!("[stdio] teardown: DynamoDB table '{table_name}' deleted");
            }
            Err(err) => {
                let is_not_found = err
                    .as_service_error()
                    .is_some_and(aws_sdk_dynamodb::operation::delete_table::DeleteTableError::is_resource_not_found_exception);
                if is_not_found {
                    eprintln!(
                        "[stdio] teardown: DynamoDB table '{table_name}' not found (already deleted?)"
                    );
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to delete DynamoDB table '{table_name}': {err}"
                    ));
                }
            }
        }
    }

    eprintln!("[stdio] teardown: DynamoDB table cleanup complete");
    Ok(())
}

pub(crate) async fn build_dynamodb_client(region: &str) -> aws_sdk_dynamodb::Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_dynamodb::config::Region::new(region.to_string()))
        .load()
        .await;
    aws_sdk_dynamodb::Client::new(&sdk_config)
}

/// Infer the `DynamoDB` key attribute type from the Arrow data type of the partition key column.
pub(crate) fn infer_dynamodb_key_type(
    data_type: &DataType,
) -> aws_sdk_dynamodb::types::ScalarAttributeType {
    use aws_sdk_dynamodb::types::ScalarAttributeType;
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => ScalarAttributeType::B,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => ScalarAttributeType::N,
        _ => ScalarAttributeType::S,
    }
}

/// Returns provisioned WCU and write parallelism for well-known TPC-H tables.
/// Other tables use on-demand billing (no provisioned capacity).
pub(crate) fn dynamodb_table_throughput(table_name: &str) -> Option<(i64, usize)> {
    match table_name {
        "lineitem" => Some((15_000, 30)),
        "orders" => Some((8_000, 20)),
        _ => None,
    }
}

/// Returns the number of scan segments for well-known TPC-H tables.
pub(crate) fn dynamodb_scan_segments(table_name: &str) -> usize {
    match table_name {
        "lineitem" => 48,
        "orders" | "partsupp" => 16,
        "supplier" => 4,
        "nation" | "region" => 1,
        _ => 8,
    }
}

/// Generate a short timestamp-based prefix for `DynamoDB` table names to avoid collisions
/// between concurrent benchmark runs.
pub(crate) fn dynamodb_table_prefix() -> String {
    use std::time::SystemTime;
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let short = format!("{:x}", ts % 0x00FF_FFFF);
    format!("sb_{short}_")
}

pub(crate) async fn wait_for_table_active(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(120);
    let started = tokio::time::Instant::now();

    loop {
        let resp = client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to describe table '{table_name}': {e:?}"))?;

        if let Some(table) = resp.table()
            && table.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active)
        {
            eprintln!("[stdio] DynamoDB: table '{table_name}' is ACTIVE");
            return Ok(());
        }

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for DynamoDB table '{table_name}' to become ACTIVE",
                timeout.as_secs()
            ));
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub(crate) fn generate_dynamodb_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    dynamodb_table_names: &HashMap<String, String>,
    acceleration_engine: &str,
) -> SpicepodDefinition {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let region = resolve_aws_region(setup_config);

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        ..Runtime::default()
    };

    for dataset_name in datasets.keys() {
        let mut param_map = HashMap::from([
            ("ready_lag".to_string(), "1h".to_string()),
            ("dynamodb_aws_region".to_string(), region.clone()),
            ("dynamodb_aws_auth".to_string(), "key".to_string()),
            (
                "dynamodb_aws_access_key_id".to_string(),
                "${env:AWS_ACCESS_KEY_ID}".to_string(),
            ),
            (
                "dynamodb_aws_secret_access_key".to_string(),
                "${env:AWS_SECRET_ACCESS_KEY}".to_string(),
            ),
        ]);

        if std::env::var("AWS_SESSION_TOKEN").is_ok() {
            param_map.insert(
                "dynamodb_aws_session_token".to_string(),
                "${env:AWS_SESSION_TOKEN}".to_string(),
            );
        }

        param_map.insert(
            "scan_segments".to_string(),
            dynamodb_scan_segments(dataset_name).to_string(),
        );
        param_map.insert("schema_infer_max_records".to_string(), "200".to_string());

        let physical_name = dynamodb_table_names
            .get(dataset_name)
            .cloned()
            .unwrap_or_else(|| dataset_name.clone());
        let mut dataset = Dataset::new(format!("dynamodb:{physical_name}"), dataset_name.as_str());
        dataset.params = Some(Params::from_string_map(param_map));
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some(acceleration_engine.to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            ..Acceleration::default()
        });

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    spicepod
}

/// Write seed rows (base64-encoded Arrow IPC streams) into `DynamoDB` tables before
/// the spicepod starts. This allows schema inference to succeed and primes the
/// change-stream position so Spice receives subsequent ingested rows as CDC events.
pub(crate) async fn seed_dynamodb_rows(
    setup_config: &SetupConfig,
    table_name_map: &HashMap<String, String>,
    seed_data: &HashMap<String, String>,
) -> anyhow::Result<()> {
    use arrow::ipc::reader::StreamReader;
    use aws_sdk_dynamodb::types::AttributeValue;
    use base64::Engine as _;

    if seed_data.is_empty() {
        return Ok(());
    }

    let region = resolve_aws_region(setup_config);
    let client = build_dynamodb_client(&region).await;

    for (dataset_name, encoded) in seed_data {
        let physical_name = table_name_map
            .get(dataset_name)
            .cloned()
            .unwrap_or_else(|| dataset_name.clone());

        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| anyhow::anyhow!("seed_data decode error for '{dataset_name}': {e}"))?;

        let cursor = std::io::Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| anyhow::anyhow!("Arrow IPC read error for '{dataset_name}': {e}"))?;

        let mut row_count = 0usize;
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| anyhow::anyhow!("Arrow batch error '{dataset_name}': {e}"))?;
            let schema = batch.schema();

            for row in 0..batch.num_rows() {
                let mut item: HashMap<String, AttributeValue> = HashMap::new();

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col = batch.column(col_idx);
                    if col.is_null(row) {
                        continue;
                    }
                    let av = arrow_scalar_to_attribute_value(col.as_ref(), row);
                    item.insert(field.name().clone(), av);
                }

                client
                    .put_item()
                    .table_name(&physical_name)
                    .set_item(Some(item))
                    .send()
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("DynamoDB PutItem failed for table '{physical_name}': {e}")
                    })?;

                row_count += 1;
            }
        }

        eprintln!("[stdio] DynamoDB: seeded {row_count} row(s) into '{physical_name}'");
    }

    Ok(())
}

fn arrow_scalar_to_attribute_value(
    col: &dyn Array,
    row: usize,
) -> aws_sdk_dynamodb::types::AttributeValue {
    use aws_sdk_dynamodb::types::AttributeValue;

    match col.data_type() {
        DataType::Boolean => {
            let v = col.as_boolean().value(row);
            AttributeValue::Bool(v)
        }
        DataType::Int8 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::Int8Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int16 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::Int16Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int32 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::Int32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int64 => {
            AttributeValue::N(col.as_primitive::<Int64Type>().value(row).to_string())
        }
        DataType::UInt8 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::UInt8Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt16 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt32 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt64 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Float32 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::Float32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Float64 => {
            AttributeValue::N(col.as_primitive::<Float64Type>().value(row).to_string())
        }
        DataType::Decimal128(_, scale) => {
            let raw = col
                .as_primitive::<arrow::datatypes::Decimal128Type>()
                .value(row);
            #[expect(clippy::cast_sign_loss)]
            let scale = *scale as u32;
            let divisor = 10i128.pow(scale);
            AttributeValue::N(format!(
                "{}.{:0>width$}",
                raw / divisor,
                (raw % divisor).abs(),
                width = scale as usize
            ))
        }
        DataType::Utf8 => AttributeValue::S(col.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => AttributeValue::S(col.as_string::<i64>().value(row).to_string()),
        DataType::Binary => AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(
            col.as_binary::<i32>().value(row).to_vec(),
        )),
        DataType::LargeBinary => AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(
            col.as_binary::<i64>().value(row).to_vec(),
        )),
        _ => AttributeValue::S(
            arrow::util::display::array_value_to_string(col, row).unwrap_or_default(),
        ),
    }
}
