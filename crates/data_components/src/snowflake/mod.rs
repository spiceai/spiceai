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

pub mod provider;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider,
    sql::{TableReference, unparser::dialect},
};
use datafusion_table_providers::sql::{
    db_connection_pool::DbConnectionPool,
    sql_provider_datafusion::SqlTable,
};
use snowflake_api::SnowflakeApi;
use std::sync::Arc;

use crate::Read;
use crate::schema_discovery::{
    NoPermissionsCheck, SchemaDiscoveryWarning, SchemaProbeResult, discover_schema,
};

pub type SnowflakeConnectionPool =
    dyn DbConnectionPool<Arc<SnowflakeApi>, &'static dyn Sync> + Send + Sync;

pub struct SnowflakeTableFactory {
    pool: Arc<SnowflakeConnectionPool>,
}

impl std::fmt::Debug for SnowflakeTableFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeTableFactory")
            .finish_non_exhaustive()
    }
}

impl SnowflakeTableFactory {
    #[must_use]
    pub fn new(pool: Arc<SnowflakeConnectionPool>) -> Self {
        Self { pool }
    }
}

fn snowflake_dialect() -> dialect::CustomDialect {
    dialect::CustomDialectBuilder::new()
        .with_identifier_quote_style('"')
        .build()
}

/// Probes `information_schema.columns` for a Snowflake table.
///
/// Returns richer metadata than `SHOW COLUMNS`: explicit `IS_NULLABLE`,
/// `NUMERIC_PRECISION`, `NUMERIC_SCALE`, and standard SQL `DATA_TYPE`.
async fn probe_snowflake_information_schema(
    api: &SnowflakeApi,
    table_reference: &TableReference,
) -> SchemaProbeResult {
    let table = table_reference.table().replace('\'', "''");
    let schema_filter = table_reference
        .schema()
        .map(|s| format!(" AND table_schema = '{}'", s.replace('\'', "''")))
        .unwrap_or_default();
    let catalog_filter = table_reference
        .catalog()
        .map(|c| format!(" AND table_catalog = '{}'", c.replace('\'', "''")))
        .unwrap_or_default();

    let sql = format!(
        "SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale, character_maximum_length \
         FROM information_schema.columns \
         WHERE table_name = '{table}'{schema_filter}{catalog_filter} \
         ORDER BY ordinal_position"
    );

    match api.exec(&sql).await {
        Ok(snowflake_api::QueryResult::Json(resp)) => {
            match parse_information_schema_json(&resp.value, &table_reference.to_string()) {
                Ok(schema) => SchemaProbeResult::Ok(schema),
                Err(e) if is_snowflake_access_denied(&e) => {
                    SchemaProbeResult::AccessDenied(e.to_string())
                }
                Err(e) => SchemaProbeResult::Failed(e.into()),
            }
        }
        Ok(snowflake_api::QueryResult::Arrow(batches)) => {
            match parse_information_schema_arrow(&batches, &table_reference.to_string()) {
                Ok(schema) => SchemaProbeResult::Ok(schema),
                Err(e) => SchemaProbeResult::Failed(e.into()),
            }
        }
        Ok(snowflake_api::QueryResult::Empty) => {
            SchemaProbeResult::Failed("information_schema returned empty result".to_string().into())
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Insufficient privileges")
                || msg.contains("access denied")
                || msg.contains("does not exist or not authorized")
            {
                SchemaProbeResult::AccessDenied(msg)
            } else {
                SchemaProbeResult::Failed(Box::new(e))
            }
        }
    }
}

/// Probes `SHOW COLUMNS IN <table>` for a Snowflake table.
///
/// This is the existing schema discovery path. It returns JSON-encoded
/// type descriptors with embedded nullability.
async fn probe_snowflake_show_columns(
    api: &SnowflakeApi,
    table_reference: &TableReference,
) -> SchemaProbeResult {
    let table = table_reference.to_quoted_string();
    let sql = format!("SHOW COLUMNS IN {table}");

    match api.exec(&sql).await {
        Ok(snowflake_api::QueryResult::Json(resp)) => {
            // Delegate to the existing connection-pool parser via re-executing
            // get_schema on a temporary connection. Instead, parse inline.
            match parse_show_columns_json(&resp.value) {
                Ok(schema) => SchemaProbeResult::Ok(schema),
                Err(e) if is_snowflake_access_denied(&e) => {
                    SchemaProbeResult::AccessDenied(e.to_string())
                }
                Err(e) => SchemaProbeResult::Failed(e.into()),
            }
        }
        Ok(snowflake_api::QueryResult::Arrow(_)) => SchemaProbeResult::Failed(
            "Unexpected Arrow response from SHOW COLUMNS".to_string().into(),
        ),
        Ok(snowflake_api::QueryResult::Empty) => {
            SchemaProbeResult::Failed("SHOW COLUMNS returned empty result".to_string().into())
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Insufficient privileges")
                || msg.contains("access denied")
                || msg.contains("does not exist or not authorized")
            {
                SchemaProbeResult::AccessDenied(msg)
            } else {
                SchemaProbeResult::Failed(Box::new(e))
            }
        }
    }
}

fn is_snowflake_access_denied(msg: &str) -> bool {
    msg.contains("Insufficient privileges")
        || msg.contains("access denied")
        || msg.contains("does not exist or not authorized")
}

/// Parses the JSON response from `SHOW COLUMNS IN <table>`.
///
/// This mirrors the `parse_schema_from_json` logic in `snowflakeconn.rs`.
fn parse_show_columns_json(resp: &serde_json::Value) -> std::result::Result<SchemaRef, String> {
    let columns = resp
        .as_array()
        .ok_or_else(|| "SHOW COLUMNS response is not an array".to_string())?;

    let mut fields = Vec::new();
    for (i, column) in columns.iter().enumerate() {
        let row = column
            .as_array()
            .ok_or_else(|| format!("SHOW COLUMNS row {i} is not an array"))?;
        if row.len() < 5 {
            return Err(format!("SHOW COLUMNS row {i} has fewer than 5 fields"));
        }

        let col_name = row[2]
            .as_str()
            .ok_or_else(|| format!("SHOW COLUMNS row {i}: invalid column name"))?;
        let data_type_json = row[3]
            .as_str()
            .ok_or_else(|| format!("SHOW COLUMNS row {i}: invalid data type"))?;
        let data_type =
            parse_snowflake_type_json(data_type_json).map_err(|e| format!("row {i}: {e}"))?;
        let is_nullable = row[4]
            .as_str()
            .is_none_or(|s| s.to_uppercase() == "TRUE");

        fields.push(Field::new(col_name, data_type, is_nullable));
    }

    if fields.is_empty() {
        return Err("SHOW COLUMNS returned no columns".to_string());
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Parses the JSON response from `information_schema.columns`.
///
/// Expected columns: `column_name`, `data_type`, `is_nullable`,
/// `numeric_precision`, `numeric_scale`, `character_maximum_length`.
fn parse_information_schema_json(
    resp: &serde_json::Value,
    table_name: &str,
) -> std::result::Result<SchemaRef, String> {
    let rows = resp
        .as_array()
        .ok_or_else(|| "information_schema response is not an array".to_string())?;

    if rows.is_empty() {
        return Err(format!(
            "information_schema.columns returned no rows for '{table_name}'"
        ));
    }

    let mut fields = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let row = row
            .as_array()
            .ok_or_else(|| format!("information_schema row {i} is not an array"))?;
        if row.len() < 3 {
            return Err(format!("information_schema row {i} has fewer than 3 fields"));
        }

        let col_name = row[0]
            .as_str()
            .ok_or_else(|| format!("information_schema row {i}: invalid column name"))?;
        let data_type_str = row[1]
            .as_str()
            .ok_or_else(|| format!("information_schema row {i}: invalid data type"))?;
        let is_nullable = row[2]
            .as_str()
            .map(|s| s.to_uppercase() == "YES")
            .unwrap_or(true);

        let precision = row.get(3).and_then(|v| v.as_str()).and_then(|s| s.parse::<u8>().ok());
        let scale = row.get(4).and_then(|v| v.as_str()).and_then(|s| s.parse::<i8>().ok());

        let data_type = map_snowflake_sql_type(data_type_str, precision, scale);

        fields.push(Field::new(col_name, data_type, is_nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Parses an Arrow response from `information_schema.columns`.
fn parse_information_schema_arrow(
    batches: &[arrow::array::RecordBatch],
    table_name: &str,
) -> std::result::Result<SchemaRef, String> {
    use arrow::array::AsArray;

    let mut fields = Vec::new();
    for batch in batches {
        if batch.num_columns() < 3 {
            return Err("information_schema Arrow response has fewer than 3 columns".to_string());
        }
        let col_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("column_name column is not StringArray")?;
        let data_types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("data_type column is not StringArray")?;
        let nullables = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("is_nullable column is not StringArray")?;

        let precisions: Option<&arrow::array::StringArray> = if batch.num_columns() > 3 {
            batch.column(3).as_string_opt()
        } else {
            None
        };
        let scales: Option<&arrow::array::StringArray> = if batch.num_columns() > 4 {
            batch.column(4).as_string_opt()
        } else {
            None
        };

        for i in 0..batch.num_rows() {
            let col_name = col_names.value(i);
            let data_type_str = data_types.value(i);
            let is_nullable = nullables.value(i).to_uppercase() == "YES";
            let precision = precisions
                .and_then(|p| if p.is_null(i) { None } else { Some(p.value(i)) })
                .and_then(|s| s.parse::<u8>().ok());
            let scale = scales
                .and_then(|s| if s.is_null(i) { None } else { Some(s.value(i)) })
                .and_then(|s| s.parse::<i8>().ok());

            let data_type = map_snowflake_sql_type(data_type_str, precision, scale);
            fields.push(Field::new(col_name, data_type, is_nullable));
        }
    }

    if fields.is_empty() {
        return Err(format!(
            "information_schema.columns returned no rows for '{table_name}'"
        ));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Maps a Snowflake SQL `DATA_TYPE` name to an Arrow `DataType`.
///
/// Uses the standard SQL type names returned by `information_schema.columns`.
fn map_snowflake_sql_type(
    sql_type: &str,
    precision: Option<u8>,
    scale: Option<i8>,
) -> DataType {
    match sql_type.to_uppercase().as_str() {
        "NUMBER" | "DECIMAL" | "NUMERIC" | "INT" | "INTEGER" | "BIGINT" | "SMALLINT"
        | "TINYINT" | "BYTEINT" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE"
        | "DOUBLE PRECISION" | "REAL" => {
            let p = precision.unwrap_or(38);
            let s = scale.unwrap_or(0);
            if s == 0 && sql_type.to_uppercase().as_str() != "NUMBER" {
                // Integer-like types
                match sql_type.to_uppercase().as_str() {
                    "FLOAT" | "FLOAT4" | "REAL" => DataType::Float32,
                    "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => DataType::Float64,
                    _ => DataType::Decimal128(p, s),
                }
            } else {
                DataType::Decimal128(p, s)
            }
        }
        "VARCHAR" | "CHAR" | "CHARACTER" | "STRING" | "TEXT" | "VARIANT" | "OBJECT" | "ARRAY" => {
            DataType::Utf8
        }
        "BINARY" | "VARBINARY" => DataType::Binary,
        "BOOLEAN" => DataType::Boolean,
        "DATE" => DataType::Date32,
        "DATETIME" | "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        "TIMESTAMP_TZ" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        "TIME" => DataType::Time64(TimeUnit::Nanosecond),
        _ => DataType::Utf8, // fallback for unrecognized types
    }
}

/// Parses a JSON type descriptor from `SHOW COLUMNS` (e.g. `{"type":"FIXED","precision":38,...}`).
fn parse_snowflake_type_json(json_str: &str) -> std::result::Result<DataType, String> {
    let v: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| format!("invalid type JSON: {e}"))?;

    match v["type"].as_str() {
        Some("FIXED") => {
            let precision = v["precision"].as_u64().unwrap_or(38) as u8;
            let scale = v["scale"].as_i64().unwrap_or(0) as i8;
            Ok(DataType::Decimal128(precision, scale))
        }
        Some("TEXT" | "VARIANT" | "ARRAY") => Ok(DataType::Utf8),
        Some("REAL") => Ok(DataType::Float64),
        Some("BINARY") => Ok(DataType::Binary),
        Some("BOOLEAN") => Ok(DataType::Boolean),
        Some("DATE") => Ok(DataType::Date32),
        Some("TIMESTAMP_NTZ") => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        Some("TIME") => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        Some("TIMESTAMP_TZ") => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        )),
        Some(t) => Err(format!("unsupported Snowflake type: {t}")),
        None => Err("missing type field in JSON descriptor".to_string()),
    }
}

#[async_trait]
impl Read for SnowflakeTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> std::result::Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let dialect = Arc::new(snowflake_dialect());
        let pool = Arc::clone(&self.pool);

        // Get a connection to access the Snowflake API for parallel probes
        let conn = pool.connect().await?;
        let sf_conn = conn.as_any().downcast_ref::<db_connection_pool::dbconnection::snowflakeconn::SnowflakeConnection>()
            .ok_or_else(|| "Failed to downcast Snowflake connection".to_string())?;
        let api = Arc::clone(&sf_conn.api);

        let result = discover_schema(
            &table_reference.to_string(),
            probe_snowflake_information_schema(&api, &table_reference),
            probe_snowflake_show_columns(&api, &table_reference),
            &NoPermissionsCheck,
        )
        .await?;

        for warning in &result.warnings {
            match warning {
                SchemaDiscoveryWarning::MetadataFallback { reason, nuance } => {
                    tracing::warn!(table = %table_reference, "{reason}. {nuance}");
                }
                SchemaDiscoveryWarning::PermissionsUnavailable { reason } => {
                    tracing::warn!(table = %table_reference, "Permissions check unavailable: {reason}");
                }
            }
        }

        let table_provider = Arc::new(
            SqlTable::new_with_schema(
                "snowflake",
                &pool,
                result.schema,
                table_reference,
                None,
            )
            .with_dialect(dialect),
        );

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}
