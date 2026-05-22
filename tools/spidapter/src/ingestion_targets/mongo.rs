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

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime, TelemetryConfig};
use spicepod::param::Params;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::{AdbcDriver, DatasetConfig, SetupResponse};
use uuid::Uuid;

use super::super::{RunState, SetupConfig};

/// Appends query parameters to a MongoDB URI, preserving any existing query string.
fn append_uri_params(uri: &str, params: &[(&str, &str)]) -> String {
    let sep = if uri.contains('?') { "&" } else { "?" };
    let qs: String = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{uri}{sep}{qs}")
}

pub(crate) fn build_mongodb_setup_response(
    connection_string: &str,
    state: &RunState,
) -> SetupResponse {
    let mut kwargs: HashMap<String, serde_json::Value> = HashMap::new();
    // Embed tls=false so the ADBC driver doesn't attempt a TLS handshake
    // against a plain-TCP server (same as the spicepod connection string).
    let uri = append_uri_params(connection_string, &[("tls", "false")]);
    kwargs.insert("uri".into(), serde_json::Value::String(uri));

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
        driver: AdbcDriver::MongoDB,
        db_kwargs: kwargs,
        catalog_namespace: None,
        read_driver: Some((AdbcDriver::Flightsql, read_db_kwargs)),
        endpoints: HashMap::new(),
        table_name_map: HashMap::new(),
    }
}

pub(crate) fn generate_mongodb_spicepod(
    run_id: &Uuid,
    connection_string: &str,
    _database: &str,
    acceleration_engine: &str,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<SpicepodDefinition> {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        ..Runtime::default()
    };

    // Build the connection string: embed db path and disable TLS for plain servers.
    // The library default (sslmode=required) causes unexpected EOF on non-TLS containers,
    // so we embed tls=false in the URI directly (respected over sslmode param).
    let conn_str = append_uri_params(connection_string, &[("tls", "false")]);

    for dataset_name in datasets.keys() {
        // Connection string is written literally into the spicepod — ${env:...} substitution
        // is unreliable for this parameter in the current runtime.
        let mut param_map = HashMap::from([(
            "mongodb_connection_string".to_string(),
            conn_str.clone(),
        )]);

        if let Some(endpoint) = &setup_config.endpoint {
            param_map.insert("mongodb_host".to_string(), endpoint.clone());
        }

        // MongoDB delete events only include the document `_id`, so CDC requires
        // `acceleration.primary_key: _id` regardless of the logical dataset schema.
        let primary_key = Some("_id".to_string());

        let mut dataset = Dataset::new(
            format!("mongodb:{dataset_name}"),
            dataset_name.as_str(),
        );
        dataset.params = Some(Params::from_string_map(param_map));
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some(acceleration_engine.to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key,
            on_conflict: HashMap::from([("_id".to_string(), OnConflictBehavior::Upsert)]),
            ..Acceleration::default()
        });

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    Ok(spicepod)
}

/// Write seed rows (base64-encoded Arrow IPC streams) into MongoDB collections before
/// the spicepod starts. This allows schema inference to succeed and primes the
/// change-stream position so Spice receives subsequent ingested rows as CDC events.
pub(crate) async fn seed_mongodb_rows(
    connection_string: &str,
    database: &str,
    seed_data: &HashMap<String, String>,
) -> anyhow::Result<()> {
    use arrow::ipc::reader::StreamReader;
    use base64::Engine as _;
    use mongodb::bson::Document;

    if seed_data.is_empty() {
        return Ok(());
    }

    let conn_str = connection_string.to_string();
    let db_name = database.to_string();

    let client = mongodb::Client::with_uri_str(&conn_str)
        .await
        .map_err(|e| anyhow::anyhow!("MongoDB connect error: {e}"))?;
    let db = client.database(&db_name);

    for (dataset_name, encoded) in seed_data {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| anyhow::anyhow!("seed_data decode error for '{dataset_name}': {e}"))?;

        let cursor = std::io::Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| anyhow::anyhow!("Arrow IPC read error for '{dataset_name}': {e}"))?;

        let collection = db.collection::<Document>(dataset_name);

        // Drop the collection before seeding to remove stale data from previous runs.
        // Without this, old seed rows with incorrect types (e.g. Date32 stored as
        // Bson::String) persist and contaminate schema inference in subsequent runs.
        collection
            .drop()
            .await
            .map_err(|e| anyhow::anyhow!("MongoDB drop failed for '{dataset_name}': {e}"))?;

        let mut row_count = 0usize;

        for batch_result in reader {
            let batch =
                batch_result.map_err(|e| anyhow::anyhow!("Arrow batch error '{dataset_name}': {e}"))?;
            let schema = batch.schema();
            let mut docs: Vec<Document> = Vec::with_capacity(batch.num_rows());

            for row in 0..batch.num_rows() {
                let mut doc = Document::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col = batch.column(col_idx);
                    if col.is_null(row) {
                        continue;
                    }
                    let bval = arrow_scalar_to_bson(col.as_ref(), row);
                    doc.insert(field.name().clone(), bval);
                }
                docs.push(doc);
            }

            row_count += docs.len();
            if !docs.is_empty() {
                collection
                    .insert_many(docs)
                    .await
                    .map_err(|e| anyhow::anyhow!("MongoDB insert_many failed for '{dataset_name}': {e}"))?;
            }
        }

        eprintln!("[stdio] MongoDB: seeded {row_count} row(s) into '{dataset_name}'");
    }

    Ok(())
}

fn arrow_scalar_to_bson(col: &dyn Array, row: usize) -> mongodb::bson::Bson {
    use arrow::datatypes::TimeUnit;
    use mongodb::bson::Bson;

    match col.data_type() {
        DataType::Boolean => Bson::Boolean(col.as_boolean().value(row)),
        DataType::Int8 => Bson::Int32(col.as_primitive::<arrow::datatypes::Int8Type>().value(row).into()),
        DataType::Int16 => Bson::Int32(col.as_primitive::<arrow::datatypes::Int16Type>().value(row).into()),
        DataType::Int32 => Bson::Int32(col.as_primitive::<arrow::datatypes::Int32Type>().value(row)),
        DataType::Int64 => Bson::Int64(col.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Bson::Int32(col.as_primitive::<arrow::datatypes::UInt8Type>().value(row).into()),
        DataType::UInt16 => Bson::Int32(col.as_primitive::<arrow::datatypes::UInt16Type>().value(row).into()),
        DataType::UInt32 => Bson::Int64(col.as_primitive::<arrow::datatypes::UInt32Type>().value(row).into()),
        DataType::UInt64 => Bson::Int64(col.as_primitive::<arrow::datatypes::UInt64Type>().value(row) as i64),
        DataType::Float32 => Bson::Double(col.as_primitive::<arrow::datatypes::Float32Type>().value(row).into()),
        DataType::Float64 => Bson::Double(col.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(_, scale) => {
            let raw = col.as_primitive::<arrow::datatypes::Decimal128Type>().value(row);
            let scale = *scale as u32;
            let divisor = 10i128.pow(scale);
            let f = (raw as f64) / (divisor as f64);
            Bson::Double(f)
        }
        DataType::Utf8 => Bson::String(col.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Bson::String(col.as_string::<i64>().value(row).to_string()),
        DataType::Binary => Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: col.as_binary::<i32>().value(row).to_vec(),
        }),
        DataType::LargeBinary => Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: col.as_binary::<i64>().value(row).to_vec(),
        }),
        // Temporal types must be stored as Bson::DateTime so that datafusion-table-providers
        // infers the correct Arrow type (Date32 or Timestamp) rather than Utf8/VARCHAR.
        DataType::Date32 => {
            let days = col.as_primitive::<arrow::datatypes::Date32Type>().value(row) as i64;
            Bson::DateTime(mongodb::bson::DateTime::from_millis(days * 86_400 * 1_000))
        }
        DataType::Date64 => {
            let millis = col.as_primitive::<arrow::datatypes::Date64Type>().value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let secs = col.as_primitive::<arrow::datatypes::TimestampSecondType>().value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(secs * 1_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let millis = col.as_primitive::<arrow::datatypes::TimestampMillisecondType>().value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = col.as_primitive::<arrow::datatypes::TimestampMicrosecondType>().value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(micros / 1_000))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = col.as_primitive::<arrow::datatypes::TimestampNanosecondType>().value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(nanos / 1_000_000))
        }
        _ => Bson::String(arrow::util::display::array_value_to_string(col, row).unwrap_or_default()),
    }
}
