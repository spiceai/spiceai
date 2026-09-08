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

pub mod federation;
pub mod provider;
mod write;

use crate::function_support::FunctionSupport;
use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, i256};
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider,
    sql::{
        TableReference,
        sqlparser::{
            dialect::GenericDialect,
            parser::{Parser, ParserError},
        },
        unparser::dialect::{self, Dialect},
    },
};
use datafusion_table_providers::sql::{
    db_connection_pool::DbConnectionPool, sql_provider_datafusion::SqlTable,
};
use snowflake_api::SnowflakeApi;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use crate::schema_discovery::{NoPermissionsCheck, SchemaProbeResult, discover_schema};
use crate::{
    CLUSTERING_KEY_METADATA_KEY, CLUSTERING_METADATA_KEY, DESCRIPTION_METADATA_KEY, Read,
    ReadWrite, SOURCE_TYPE_METADATA_KEY,
};

pub type SnowflakeConnectionPool =
    dyn DbConnectionPool<Arc<SnowflakeApi>, &'static dyn Sync> + Send + Sync;

pub struct SnowflakeTableFactory {
    pool: Arc<SnowflakeConnectionPool>,
    write_lock: Arc<Mutex<()>>,
    function_support: Option<FunctionSupport>,
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
        Self {
            pool,
            write_lock: Arc::new(Mutex::new(())),
            function_support: None,
        }
    }

    /// Install a function deny-list so federation pushdown skips remote
    /// execution for any plan touching one of the denied functions. Plans that
    /// would otherwise be unparsed into Snowflake SQL with Spice-only UDFs
    /// (e.g. `json_get_str`) fall back to local `DataFusion` evaluation instead.
    #[must_use]
    pub fn with_function_support(mut self, function_support: FunctionSupport) -> Self {
        self.function_support = Some(function_support);
        self
    }

    /// Returns the currently configured [`FunctionSupport`], if any. Used by
    /// connector tests to confirm the Spice deny-list is wired through to the
    /// federation layer.
    #[must_use]
    pub fn function_support(&self) -> Option<&FunctionSupport> {
        self.function_support.as_ref()
    }
}

/// Parses a Snowflake table path string (e.g. `db.schema.table` or `"my.schema".table`)
/// using SQL parser rules and returns a fully-quoted identifier string safe to embed in
/// Snowflake SQL statements.
///
/// Each identifier part is unquoted by the parser (handling embedded `""` escapes and
/// dot-in-quoted-identifier cases), then re-quoted with proper `""` escaping.
///
/// # Errors
/// Returns a `ParserError` if `path` is not a valid 1-, 2-, or 3-part Snowflake identifier
/// or contains NUL bytes.
pub fn quote_snowflake_table_path(path: &str) -> std::result::Result<String, ParserError> {
    let table_reference = parse_snowflake_table_reference(path)?;
    let identifier_parts = table_reference.to_vec();

    if identifier_parts.iter().any(|part| part.contains('\0')) {
        return Err(ParserError::ParserError(
            "Snowflake identifiers cannot contain NUL bytes".to_string(),
        ));
    }

    Ok(identifier_parts
        .iter()
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join("."))
}

fn parse_snowflake_table_reference(path: &str) -> std::result::Result<TableReference, ParserError> {
    let dialect = GenericDialect {};
    let identifier_parts = Parser::new(&dialect)
        .try_with_sql(path)?
        .parse_multipart_identifier()?
        .into_iter()
        .map(|identifier| identifier.value)
        .collect::<Vec<_>>();

    match identifier_parts.as_slice() {
        [table] => Ok(TableReference::bare(table.clone())),
        [schema, table] => Ok(TableReference::partial(schema.clone(), table.clone())),
        [catalog, schema, table] => Ok(TableReference::full(
            catalog.clone(),
            schema.clone(),
            table.clone(),
        )),
        _ => Err(ParserError::ParserError(format!(
            "Invalid Snowflake table path: expected 1-3 identifier parts, got: {path}"
        ))),
    }
}

fn snowflake_dialect() -> dialect::CustomDialect {
    dialect::CustomDialectBuilder::new()
        .with_identifier_quote_style('"')
        // Render `expr AT TIME ZONE 'tz'` as `CAST(CONVERT_TIMEZONE('tz', expr) AS
        // TIMESTAMP_NTZ)` rather than dropping the zone name (which silently changed
        // the meaning of federated predicates). Correct composition of chained
        // conversions relies on the session TIMEZONE being pinned to UTC, which the
        // Snowflake connection pool enforces on connect (see the `ALTER SESSION SET
        // TIMEZONE` call in `db_connection_pool::snowflakepool`). Background:
        // https://github.com/spiceai/datafusion/pull/160.
        .with_timezone_cast_style(dialect::TimezoneCastStyle::ConvertTimezone)
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
        .map(|s| format!(" AND c.table_schema = '{}'", s.replace('\'', "''")))
        .unwrap_or_default();
    let catalog_filter = table_reference
        .catalog()
        .map(|c| format!(" AND c.table_catalog = '{}'", c.replace('\'', "''")))
        .unwrap_or_default();

    // When a catalog (database) is specified, qualify `information_schema` with it so
    // Snowflake can resolve the view even when the session has no current database set
    // (error 090105: "Cannot perform SELECT. This session does not have a current database.").
    let from_clause = if let Some(c) = table_reference.catalog() {
        // Escape double-quotes by doubling them (SQL identifier quoting rules).
        let escaped = c.replace('"', "\"\"");
        format!("\"{escaped}\".information_schema.columns")
    } else {
        "information_schema.columns".to_string()
    };

    let tables_from_clause = if let Some(c) = table_reference.catalog() {
        let escaped = c.replace('"', "\"\"");
        format!("\"{escaped}\".information_schema.tables")
    } else {
        "information_schema.tables".to_string()
    };

    let sql = format!(
        "SELECT c.column_name, c.data_type, c.is_nullable, c.numeric_precision, c.numeric_scale, c.comment, t.comment, t.clustering_key \
         FROM {from_clause} c \
         LEFT JOIN {tables_from_clause} t \
             ON c.table_catalog = t.table_catalog \
             AND c.table_schema = t.table_schema \
             AND c.table_name = t.table_name \
         WHERE c.table_name = '{table}'{schema_filter}{catalog_filter} \
         ORDER BY c.ordinal_position"
    );

    match api.exec(&sql).await {
        Ok(snowflake_api::QueryResult::Json(resp)) => {
            match parse_information_schema_json(&resp.value, &table_reference.to_string()) {
                Ok(schema) => SchemaProbeResult::Ok(schema),
                Err(e) if is_snowflake_access_denied(&e) => SchemaProbeResult::AccessDenied(e),
                Err(e) => SchemaProbeResult::Failed(e.into()),
            }
        }
        Ok(snowflake_api::QueryResult::Arrow(batches)) => {
            match parse_information_schema_arrow(&batches, &table_reference.to_string()) {
                Ok(schema) => SchemaProbeResult::Ok(schema),
                Err(e) => SchemaProbeResult::Failed(e.into()),
            }
        }
        Ok(snowflake_api::QueryResult::Empty) => SchemaProbeResult::Failed(
            "information_schema returned empty result"
                .to_string()
                .into(),
        ),
        Err(e) => classify_snowflake_error(e),
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
            match db_connection_pool::dbconnection::snowflakeconn::parse_schema_from_json(
                &resp.value,
            ) {
                Ok(schema) => SchemaProbeResult::Ok(schema),
                Err(e) => {
                    let msg = e.to_string();
                    if is_snowflake_access_denied(&msg) {
                        SchemaProbeResult::AccessDenied(msg)
                    } else {
                        SchemaProbeResult::Failed(msg.into())
                    }
                }
            }
        }
        Ok(snowflake_api::QueryResult::Arrow(_)) => SchemaProbeResult::Failed(
            "Unexpected Arrow response from SHOW COLUMNS"
                .to_string()
                .into(),
        ),
        Ok(snowflake_api::QueryResult::Empty) => {
            SchemaProbeResult::Failed("SHOW COLUMNS returned empty result".to_string().into())
        }
        Err(e) => classify_snowflake_error(e),
    }
}

/// Classifies a Snowflake API error as access-denied or generic failure.
fn classify_snowflake_error(e: snowflake_api::SnowflakeApiError) -> SchemaProbeResult {
    let msg = e.to_string();
    if is_snowflake_access_denied(&msg) {
        SchemaProbeResult::AccessDenied(msg)
    } else {
        SchemaProbeResult::Failed(Box::new(e))
    }
}

fn is_snowflake_access_denied(msg: &str) -> bool {
    msg.contains("Insufficient privileges")
        || msg.contains("access denied")
        || msg.contains("does not exist or not authorized")
}

/// Parses the JSON response from `information_schema.columns`.
///
/// Expected columns: `column_name`, `data_type`, `is_nullable`,
/// `numeric_precision`, `numeric_scale`, `comment`, `table_comment`, `clustering_key`.
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
    let mut schema_metadata = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let row = row
            .as_array()
            .ok_or_else(|| format!("information_schema row {i} is not an array"))?;
        if row.len() < 3 {
            return Err(format!(
                "information_schema row {i} has fewer than 3 fields"
            ));
        }

        let col_name = row[0]
            .as_str()
            .ok_or_else(|| format!("information_schema row {i}: invalid column name"))?;
        let data_type_str = row[1]
            .as_str()
            .ok_or_else(|| format!("information_schema row {i}: invalid data type"))?;
        let is_nullable = row[2].as_str().is_none_or(|s| s.to_uppercase() == "YES");

        let (precision, scale) =
            precision_and_scale(data_type_str, col_name, table_name, |column, metadata| {
                json_information_schema_integer(row.get(column), metadata, col_name, table_name)
            })?;

        let data_type = map_snowflake_sql_type(data_type_str, precision, scale);
        let source_type = snowflake_source_type(data_type_str, precision, scale);

        if let Some(table_comment) = optional_json_string(row.get(6)) {
            schema_metadata
                .entry(DESCRIPTION_METADATA_KEY.to_string())
                .or_insert_with(|| table_comment.to_string());
        }
        if let Some(clustering_key) = optional_json_string(row.get(7)) {
            schema_metadata
                .entry(CLUSTERING_KEY_METADATA_KEY.to_string())
                .or_insert_with(|| clustering_key.to_string());
        }

        fields.push(field_with_optional_metadata(
            Field::new(col_name, data_type, is_nullable),
            optional_json_string(row.get(5)),
            Some(&source_type),
        ));
    }

    let fields = fields_with_clustering_metadata(
        fields,
        schema_metadata
            .get(CLUSTERING_KEY_METADATA_KEY)
            .map(String::as_str),
    );

    Ok(Arc::new(Schema::new_with_metadata(fields, schema_metadata)))
}

/// Parses an Arrow response from `information_schema.columns`.
fn parse_information_schema_arrow(
    batches: &[arrow::array::RecordBatch],
    table_name: &str,
) -> std::result::Result<SchemaRef, String> {
    use arrow::array::AsArray;

    let mut fields = Vec::new();
    let mut schema_metadata = HashMap::new();
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

        let comments: Option<&arrow::array::StringArray> = if batch.num_columns() > 5 {
            batch.column(5).as_string_opt()
        } else {
            None
        };
        let table_comments: Option<&arrow::array::StringArray> = if batch.num_columns() > 6 {
            batch.column(6).as_string_opt()
        } else {
            None
        };
        let clustering_keys: Option<&arrow::array::StringArray> = if batch.num_columns() > 7 {
            batch.column(7).as_string_opt()
        } else {
            None
        };

        for i in 0..batch.num_rows() {
            let col_name = col_names.value(i);
            let data_type_str = data_types.value(i);
            let is_nullable = nullables.value(i).to_uppercase() == "YES";
            let (precision, scale) =
                precision_and_scale(data_type_str, col_name, table_name, |column, metadata| {
                    // `numeric_precision` and `numeric_scale` arrive in whatever
                    // numeric or text Arrow type Snowflake encoded them as, so
                    // hand the reader the erased array rather than committing to
                    // one representation.
                    let array =
                        (batch.num_columns() > column).then(|| batch.column(column).as_ref());
                    arrow_information_schema_integer(array, i, metadata, col_name, table_name)
                })?;

            let data_type = map_snowflake_sql_type(data_type_str, precision, scale);
            let source_type = snowflake_source_type(data_type_str, precision, scale);
            if let Some(table_comment) = optional_arrow_string(table_comments, i) {
                schema_metadata
                    .entry(DESCRIPTION_METADATA_KEY.to_string())
                    .or_insert_with(|| table_comment.to_string());
            }
            if let Some(clustering_key) = optional_arrow_string(clustering_keys, i) {
                schema_metadata
                    .entry(CLUSTERING_KEY_METADATA_KEY.to_string())
                    .or_insert_with(|| clustering_key.to_string());
            }

            fields.push(field_with_optional_metadata(
                Field::new(col_name, data_type, is_nullable),
                optional_arrow_string(comments, i),
                Some(&source_type),
            ));
        }
    }

    if fields.is_empty() {
        return Err(format!(
            "information_schema.columns returned no rows for '{table_name}'"
        ));
    }

    let fields = fields_with_clustering_metadata(
        fields,
        schema_metadata
            .get(CLUSTERING_KEY_METADATA_KEY)
            .map(String::as_str),
    );

    Ok(Arc::new(Schema::new_with_metadata(fields, schema_metadata)))
}

/// The widest precision an Arrow `Decimal128` — and a Snowflake `NUMBER` — can
/// represent.
const MAX_DECIMAL_PRECISION: u8 = 38;

/// Reads one `information_schema.columns` integer cell out of a Snowflake Arrow
/// response, whatever type Snowflake encoded it as.
///
/// `NUMERIC_PRECISION` and `NUMERIC_SCALE` are declared `NUMBER(38,0)`, and
/// Snowflake picks the Arrow encoding per result chunk: an integer of whichever
/// width fits, a decimal, or text. Reading a single representation drops the
/// precision and scale of every column, registering a `NUMBER(12,2)` measure as
/// `Decimal128(38, 0)` so its fraction is rounded away — permanently, once the
/// table is accelerated. A cell that cannot be read exactly is an error rather
/// than an absent value, so discovery falls back to `SHOW COLUMNS` instead of
/// registering a scale the source never declared.
fn arrow_information_schema_integer(
    array: Option<&dyn Array>,
    row: usize,
    metadata_column: &str,
    column_name: &str,
    table_name: &str,
) -> std::result::Result<Option<i128>, String> {
    use arrow::array::AsArray;
    use arrow::datatypes::{
        Decimal128Type, Decimal256Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
        UInt16Type, UInt32Type, UInt64Type,
    };

    let Some(array) = array else {
        return Ok(None);
    };
    if row >= array.len() || array.is_null(row) {
        return Ok(None);
    }

    let unusable = |rendered: &str| {
        format!(
            "column '{column_name}' of table '{table_name}' reported `{metadata_column}` as {rendered}, which is not a whole number"
        )
    };

    let value = match array.data_type() {
        DataType::Int8 => i128::from(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => i128::from(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => i128::from(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => i128::from(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => i128::from(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => i128::from(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => i128::from(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => i128::from(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Decimal128(_, scale) => {
            let unscaled = array.as_primitive::<Decimal128Type>().value(row);
            integer_from_decimal(unscaled, *scale)
                .ok_or_else(|| unusable(&decimal_rendering(unscaled, *scale)))?
        }
        DataType::Decimal256(_, scale) => {
            let unscaled = array.as_primitive::<Decimal256Type>().value(row);
            integer_from_wide_decimal(unscaled, *scale)
                .ok_or_else(|| unusable(&decimal_rendering(unscaled, *scale)))?
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let text = match array.data_type() {
                DataType::Utf8 => array.as_string::<i32>().value(row),
                DataType::LargeUtf8 => array.as_string::<i64>().value(row),
                _ => array.as_string_view().value(row),
            }
            .trim();
            if text.is_empty() {
                return Ok(None);
            }
            integer_from_text(text).ok_or_else(|| unusable(&format!("'{text}'")))?
        }
        // A metadata column that is null in every row can arrive as an Arrow
        // `Null` array, which carries no null buffer for `is_null` to read.
        DataType::Null => return Ok(None),
        other => return Err(unusable(&format!("Arrow type {other}"))),
    };

    Ok(Some(value))
}

/// Reads one `information_schema.columns` integer cell out of a Snowflake JSON
/// response, which renders the value as a JSON number or as text.
fn json_information_schema_integer(
    value: Option<&serde_json::Value>,
    metadata_column: &str,
    column_name: &str,
    table_name: &str,
) -> std::result::Result<Option<i128>, String> {
    let rendered = match value {
        None | Some(serde_json::Value::Null) => return Ok(None),
        Some(serde_json::Value::String(text)) => text.trim().to_string(),
        Some(serde_json::Value::Number(number)) => number.to_string(),
        Some(other) => {
            return Err(format!(
                "column '{column_name}' of table '{table_name}' reported `{metadata_column}` as {other}, which is not a whole number"
            ));
        }
    };
    if rendered.is_empty() {
        return Ok(None);
    }

    integer_from_text(&rendered).map(Some).ok_or_else(|| {
        format!(
            "column '{column_name}' of table '{table_name}' reported `{metadata_column}` as '{rendered}', which is not a whole number"
        )
    })
}

/// Describes a decimal metadata cell for an error message, keeping the unscaled
/// value and the scale separate so a negative scale still reads clearly.
fn decimal_rendering(unscaled: impl std::fmt::Display, scale: i8) -> String {
    format!("the decimal value {unscaled} with scale {scale}")
}

/// Converts a decimal-encoded metadata value into the whole number it stands
/// for, or `None` when it carries a fraction and so is not the integer
/// `information_schema` declares.
fn integer_from_decimal(unscaled: i128, scale: i8) -> Option<i128> {
    let power = 10_i128.checked_pow(u32::from(scale.unsigned_abs()))?;
    match scale.cmp(&0) {
        std::cmp::Ordering::Equal => Some(unscaled),
        std::cmp::Ordering::Greater => (unscaled % power == 0).then_some(unscaled / power),
        std::cmp::Ordering::Less => unscaled.checked_mul(power),
    }
}

/// Converts a 256-bit decimal metadata value into the whole number it stands
/// for.
///
/// The rescaling happens at full width and only the result is narrowed: a
/// `Decimal256(76, 38)` cell holding `12` carries a coefficient of 12 followed
/// by 38 zeros, which no [`i128`] can hold even though the number it represents
/// is small.
fn integer_from_wide_decimal(unscaled: i256, scale: i8) -> Option<i128> {
    let power = i256::from(10_i64).checked_pow(u32::from(scale.unsigned_abs()))?;
    match scale.cmp(&0) {
        std::cmp::Ordering::Equal => unscaled.to_i128(),
        std::cmp::Ordering::Greater => {
            if unscaled.checked_rem(power)? != i256::ZERO {
                return None;
            }
            unscaled.checked_div(power)?.to_i128()
        }
        std::cmp::Ordering::Less => unscaled.checked_mul(power)?.to_i128(),
    }
}

/// Parses a whole number Snowflake rendered as text, accepting the zero
/// fraction a decimal rendering carries (`12`, `12.`, `12.00`).
fn integer_from_text(text: &str) -> Option<i128> {
    if let Ok(value) = text.parse::<i128>() {
        return Some(value);
    }

    let (integer, fraction) = text.split_once('.')?;
    if fraction.bytes().any(|digit| digit != b'0') {
        return None;
    }
    integer.parse::<i128>().ok()
}

/// Whether a Snowflake `information_schema` `DATA_TYPE` is fixed-point, i.e. a
/// type whose Arrow mapping is a decimal and therefore carries `NUMBER`'s
/// precision and scale.
fn is_fixed_point_snowflake_type(sql_type: &str) -> bool {
    matches!(
        sql_type.to_uppercase().as_str(),
        "NUMBER"
            | "DECIMAL"
            | "NUMERIC"
            | "INT"
            | "INTEGER"
            | "BIGINT"
            | "SMALLINT"
            | "TINYINT"
            | "BYTEINT"
    )
}

/// Position of `NUMERIC_PRECISION` in the `information_schema.columns` row
/// [`probe_snowflake_information_schema`] selects.
const PRECISION_COLUMN: usize = 3;
/// Position of `NUMERIC_SCALE` in that same row.
const SCALE_COLUMN: usize = 4;

/// Reads the precision and scale a column's Arrow mapping needs out of one
/// `information_schema.columns` row, where `read` fetches a raw metadata cell by
/// its position in the row.
///
/// Only a fixed-point type consults these two columns, so they are left unread
/// for every other type. Snowflake does not report a decimal's digits there for
/// a float, a string, or a timestamp, and rejecting whatever it does report
/// would fail the preferred probe over metadata that nothing goes on to use.
///
/// A fixed-point column must report both. Without them [`map_snowflake_sql_type`]
/// falls back to `Decimal128(38, 0)`, the scale-zero mapping that rounds every
/// fraction away; failing the probe instead hands discovery to `SHOW COLUMNS`,
/// whose type descriptor carries the real precision and scale.
fn precision_and_scale(
    sql_type: &str,
    column_name: &str,
    table_name: &str,
    read: impl Fn(usize, &str) -> std::result::Result<Option<i128>, String>,
) -> std::result::Result<(Option<u8>, Option<i8>), String> {
    if !is_fixed_point_snowflake_type(sql_type) {
        return Ok((None, None));
    }

    let precision = read(PRECISION_COLUMN, "numeric_precision")?
        .map(|raw| decimal_precision(raw, column_name, table_name))
        .transpose()?;
    let scale = read(SCALE_COLUMN, "numeric_scale")?
        .map(|raw| decimal_scale(raw, precision, column_name, table_name))
        .transpose()?;

    let missing = match (precision, scale) {
        (Some(_), Some(_)) => return Ok((precision, scale)),
        (None, Some(_)) => "`numeric_precision`",
        (Some(_), None) => "`numeric_scale`",
        (None, None) => "`numeric_precision` and `numeric_scale`",
    };

    Err(format!(
        "column '{column_name}' of table '{table_name}' is a {sql_type} column, but Snowflake reported no {missing} for it, so the digits it stores after the decimal point are unknown"
    ))
}

/// Validates a Snowflake `NUMERIC_PRECISION` as an Arrow decimal precision.
fn decimal_precision(
    raw: i128,
    column_name: &str,
    table_name: &str,
) -> std::result::Result<u8, String> {
    u8::try_from(raw)
        .ok()
        .filter(|precision| (1..=MAX_DECIMAL_PRECISION).contains(precision))
        .ok_or_else(|| {
            format!(
                "column '{column_name}' of table '{table_name}' reported `numeric_precision` {raw}, outside the 1 to {MAX_DECIMAL_PRECISION} digits a decimal column can hold"
            )
        })
}

/// Validates a Snowflake `NUMERIC_SCALE` as an Arrow decimal scale, which the
/// column's own precision bounds.
fn decimal_scale(
    raw: i128,
    precision: Option<u8>,
    column_name: &str,
    table_name: &str,
) -> std::result::Result<i8, String> {
    let limit = i128::from(precision.unwrap_or(MAX_DECIMAL_PRECISION));
    i8::try_from(raw)
        .ok()
        .filter(|_| (0..=limit).contains(&raw))
        .ok_or_else(|| {
            format!(
                "column '{column_name}' of table '{table_name}' reported `numeric_scale` {raw}, outside the 0 to {limit} digits its `numeric_precision` allows"
            )
        })
}

fn optional_json_string(value: Option<&serde_json::Value>) -> Option<&str> {
    value
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn optional_arrow_string(array: Option<&arrow::array::StringArray>, index: usize) -> Option<&str> {
    array
        .filter(|array| !array.is_null(index))
        .map(|array| array.value(index).trim())
        .filter(|value| !value.is_empty())
}

fn field_with_optional_metadata(
    field: Field,
    comment: Option<&str>,
    source_type: Option<&str>,
) -> Field {
    let mut metadata = HashMap::new();
    if let Some(comment) = comment {
        metadata.insert(DESCRIPTION_METADATA_KEY.to_string(), comment.to_string());
    }
    if let Some(source_type) = source_type.map(str::trim).filter(|value| !value.is_empty()) {
        metadata.insert(
            SOURCE_TYPE_METADATA_KEY.to_string(),
            source_type.to_string(),
        );
    }

    if metadata.is_empty() {
        return field;
    }

    field.with_metadata(metadata)
}

fn fields_with_clustering_metadata(fields: Vec<Field>, clustering_key: Option<&str>) -> Vec<Field> {
    let Some(clustering_key) = clustering_key else {
        return fields;
    };
    let clustering_columns = snowflake_clustering_columns(clustering_key);
    if clustering_columns.is_empty() {
        return fields;
    }

    fields
        .into_iter()
        .map(|field| {
            let position = clustering_columns
                .iter()
                .position(|column| column == field.name())
                .or_else(|| {
                    clustering_columns
                        .iter()
                        .position(|column| column.eq_ignore_ascii_case(field.name()))
                });

            if let Some(position) = position {
                let mut metadata = field.metadata().clone();
                metadata.insert(
                    CLUSTERING_METADATA_KEY.to_string(),
                    (position + 1).to_string(),
                );
                field.with_metadata(metadata)
            } else {
                field
            }
        })
        .collect()
}

fn snowflake_source_type(sql_type: &str, precision: Option<u8>, scale: Option<i8>) -> String {
    let upper = sql_type.to_uppercase();
    if matches!(upper.as_str(), "NUMBER" | "DECIMAL" | "NUMERIC") {
        match (precision, scale) {
            (Some(precision), Some(scale)) => format!("{upper}({precision},{scale})"),
            (Some(precision), None) => format!("{upper}({precision})"),
            _ => upper,
        }
    } else {
        sql_type.to_string()
    }
}

fn snowflake_clustering_columns(clustering_key: &str) -> Vec<String> {
    let mut expression = clustering_key.trim();
    if let Some(inner) = expression
        .strip_prefix("LINEAR(")
        .and_then(|value| value.strip_suffix(')'))
    {
        expression = inner;
    } else if let Some(inner) = expression
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    {
        expression = inner;
    }

    split_snowflake_clustering_expression(expression)
        .into_iter()
        .filter_map(simple_snowflake_identifier)
        .collect()
}

fn split_snowflake_clustering_expression(expression: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    let mut in_quotes = false;
    let mut chars = expression.char_indices().peekable();

    while let Some((index, ch)) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && chars.peek().is_some_and(|(_, next)| *next == '"') {
                    let _ = chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            '(' if !in_quotes => depth += 1,
            ')' if !in_quotes && depth > 0 => depth -= 1,
            ',' if !in_quotes && depth == 0 => {
                parts.push(expression[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }

    parts.push(expression[start..].trim());
    parts
}

fn simple_snowflake_identifier(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.contains('(') || value.contains(')') {
        return None;
    }

    let value = value
        .rsplit_once('.')
        .map_or(value, |(_, column)| column.trim());
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        Some(value[1..value.len() - 1].replace("\"\"", "\""))
    } else {
        Some(value.to_string())
    }
}

/// Maps a Snowflake SQL `DATA_TYPE` name to an Arrow `DataType`.
///
/// Uses the standard SQL type names returned by `information_schema.columns`.
fn map_snowflake_sql_type(sql_type: &str, precision: Option<u8>, scale: Option<i8>) -> DataType {
    let upper = sql_type.to_uppercase();
    match upper.as_str() {
        fixed_point if is_fixed_point_snowflake_type(fixed_point) => DataType::Decimal128(
            precision.unwrap_or(MAX_DECIMAL_PRECISION),
            scale.unwrap_or(0),
        ),
        // Snowflake documents that FLOAT, FLOAT4, FLOAT8, REAL, DOUBLE, and
        // DOUBLE PRECISION are all stored internally as 64-bit
        // double-precision floats (see "Summary of data types" footnote [1]).
        // Mapping any of these to Float32 would be lossy and would also
        // disagree with the `SHOW COLUMNS` path that maps "REAL" to Float64.
        // Use Float64 for the entire float family to keep both discovery paths
        // consistent.
        "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "REAL" => {
            DataType::Float64
        }
        "VARCHAR" | "CHAR" | "CHARACTER" | "STRING" | "TEXT" | "VARIANT" | "OBJECT" | "ARRAY"
        // Structured MAP collapses to JSON text here because
        // `information_schema.columns` does not expose key/value types.
        | "MAP"
        // GEOGRAPHY/GEOMETRY are serialized by Snowflake as WKT/GeoJSON/WKB
        // text over the wire; Utf8 is the correct lossless mapping.
        | "GEOGRAPHY" | "GEOMETRY"
        // UUID and FILE are textual representations in Snowflake's wire format.
        | "UUID" | "FILE" => {
            DataType::Utf8
        }
        // DECFLOAT uses a dynamic base-10 exponent with up to 38 significant
        // digits, which cannot be losslessly represented by a fixed-scale
        // Arrow Decimal. Fall back to Utf8 to preserve exact values.
        "DECFLOAT" => DataType::Utf8,
        "BINARY" | "VARBINARY" => DataType::Binary,
        "BOOLEAN" => DataType::Boolean,
        "DATE" => DataType::Date32,
        "DATETIME" | "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        "TIMESTAMP_TZ" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        "TIME" => DataType::Time64(TimeUnit::Nanosecond),
        other => {
            tracing::warn!("Unrecognized Snowflake data type '{other}', defaulting to Utf8");
            DataType::Utf8
        }
    }
}

#[async_trait]
impl Read for SnowflakeTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> std::result::Result<
        Arc<dyn TableProvider + 'static>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        self.table_provider(table_reference, false).await
    }
}

#[async_trait]
impl ReadWrite for SnowflakeTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> std::result::Result<
        Arc<dyn TableProvider + 'static>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        self.table_provider(table_reference, true).await
    }
}

impl SnowflakeTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        writable: bool,
    ) -> std::result::Result<
        Arc<dyn TableProvider + 'static>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let dialect: Arc<dyn Dialect + Send + Sync> = Arc::new(snowflake_dialect());
        let pool = Arc::clone(&self.pool);

        // Get a connection only long enough to extract the API handle.
        let api = {
            let conn = pool.connect().await?;
            let sf_conn = conn
                .as_any()
                .downcast_ref::<db_connection_pool::dbconnection::snowflakeconn::SnowflakeConnection>()
                .ok_or_else(|| "Failed to downcast Snowflake connection".to_string())?;
            Arc::clone(&sf_conn.api)
        };

        let result = discover_schema(
            &table_reference.to_string(),
            probe_snowflake_information_schema(&api, &table_reference),
            probe_snowflake_show_columns(&api, &table_reference),
            &NoPermissionsCheck,
        )
        .await?;

        result.log_warnings(table_reference.to_string());
        let schema = Arc::clone(&result.schema);
        let table_reference_for_provider = table_reference.clone();

        let sql_table = Arc::new(
            SqlTable::new_with_schema(
                "snowflake",
                &pool,
                Arc::clone(&schema),
                table_reference_for_provider.clone(),
                None,
            )
            .with_dialect(Arc::clone(&dialect)),
        );

        let table_provider = Arc::new(federation::create_spice_federated_table_provider(
            sql_table,
            Arc::clone(&schema),
            table_reference_for_provider,
            self.function_support.clone(),
        ));

        if writable {
            let table_provider: Arc<dyn TableProvider> = table_provider;
            return Ok(write::SnowflakeTableProvider::create(
                &table_provider,
                pool,
                table_reference,
                schema,
                dialect,
                Arc::clone(&self.write_lock),
            ));
        }

        Ok(table_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Int8Array, Int16Array,
        Int32Array, Int64Array, NullArray, RecordBatch, StringArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    };

    #[test]
    fn snowflake_dialect_unparses_at_time_zone_as_convert_timezone() {
        use datafusion::logical_expr::expr::Cast;
        use datafusion::logical_expr::{Expr, col};
        use datafusion::sql::unparser::Unparser;

        let dialect = snowflake_dialect();
        let unparser = Unparser::new(&dialect);

        // MQL_AT AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles'
        let inner = Expr::Cast(Cast::new(
            Box::new(col("MQL_AT")),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ));
        let outer = Expr::Cast(Cast::new(
            Box::new(inner),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Los_Angeles".into())),
        ));

        let sql = unparser
            .expr_to_sql(&outer)
            .expect("unparse AT TIME ZONE chain")
            .to_string();

        // Both zones must survive as CONVERT_TIMEZONE calls with a naive NTZ result,
        // never the zone-dropping `CAST(... AS TIMESTAMP WITH TIME ZONE)`.
        assert!(
            sql.contains("CONVERT_TIMEZONE('UTC'"),
            "inner zone dropped: {sql}"
        );
        assert!(
            sql.contains("CONVERT_TIMEZONE('America/Los_Angeles'"),
            "outer zone dropped: {sql}"
        );
        assert!(sql.contains("AS TIMESTAMP_NTZ"), "missing NTZ cast: {sql}");
        assert!(
            !sql.to_uppercase().contains("TIMESTAMP WITH TIME ZONE"),
            "timezone was silently stripped: {sql}"
        );
    }

    #[test]
    fn test_parse_information_schema_json_preserves_comment_metadata() {
        let rows = serde_json::json!([
            [
                "ID",
                "NUMBER",
                "NO",
                38,
                0,
                "stable identifier",
                "customer dimension",
                "LINEAR(ID, NAME)"
            ],
            [
                "NAME",
                "VARCHAR",
                "YES",
                null,
                null,
                "display name",
                "customer dimension",
                "LINEAR(ID, NAME)"
            ],
            [
                "AMOUNT",
                "NUMBER",
                "YES",
                12,
                2,
                "",
                "customer dimension",
                "LINEAR(ID, NAME)"
            ]
        ]);

        let schema = parse_information_schema_json(&rows, "CUSTOMERS")
            .expect("information_schema rows should parse");

        assert_eq!(
            schema
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("customer dimension")
        );
        assert_eq!(
            schema
                .metadata()
                .get(CLUSTERING_KEY_METADATA_KEY)
                .map(String::as_str),
            Some("LINEAR(ID, NAME)")
        );
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("stable identifier")
        );
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("NUMBER(38,0)")
        );
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(CLUSTERING_METADATA_KEY)
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("display name")
        );
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("VARCHAR")
        );
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(CLUSTERING_METADATA_KEY)
                .map(String::as_str),
            Some("2")
        );
        assert!(
            schema
                .field(2)
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .is_none()
        );
        assert_eq!(
            schema
                .field(2)
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("NUMBER(12,2)")
        );
    }

    /// Every Snowflake `information_schema.columns` `DATA_TYPE` we support, paired
    /// with its expected Arrow mapping. Covers integer/decimal/float variants,
    /// string/semi-structured types (including `OBJECT`), binary, boolean,
    /// date/time, and all timestamp variants.
    #[test]
    fn test_map_snowflake_sql_type_all_types() {
        let cases: &[(&str, Option<u8>, Option<i8>, DataType)] = &[
            // Numeric: NUMBER always decimal; precision/scale honored
            ("NUMBER", Some(38), Some(0), DataType::Decimal128(38, 0)),
            ("NUMBER", Some(12), Some(4), DataType::Decimal128(12, 4)),
            ("NUMBER", None, None, DataType::Decimal128(38, 0)),
            ("DECIMAL", Some(10), Some(2), DataType::Decimal128(10, 2)),
            ("NUMERIC", Some(10), Some(2), DataType::Decimal128(10, 2)),
            // Integer aliases collapse to Decimal128 with provided precision/scale
            ("INT", Some(38), Some(0), DataType::Decimal128(38, 0)),
            ("INTEGER", Some(38), Some(0), DataType::Decimal128(38, 0)),
            ("BIGINT", Some(38), Some(0), DataType::Decimal128(38, 0)),
            ("SMALLINT", Some(38), Some(0), DataType::Decimal128(38, 0)),
            ("TINYINT", Some(38), Some(0), DataType::Decimal128(38, 0)),
            ("BYTEINT", Some(38), Some(0), DataType::Decimal128(38, 0)),
            // Float family
            // Float family: Snowflake stores all float variants as 64-bit
            // DOUBLE internally, so every variant maps to Float64 to avoid
            // lossy narrowing and to agree with the SHOW COLUMNS path.
            ("FLOAT", None, None, DataType::Float64),
            ("FLOAT4", None, None, DataType::Float64),
            ("REAL", None, None, DataType::Float64),
            ("FLOAT8", None, None, DataType::Float64),
            ("DOUBLE", None, None, DataType::Float64),
            ("DOUBLE PRECISION", None, None, DataType::Float64),
            // String-like
            ("VARCHAR", None, None, DataType::Utf8),
            ("CHAR", None, None, DataType::Utf8),
            ("CHARACTER", None, None, DataType::Utf8),
            ("STRING", None, None, DataType::Utf8),
            ("TEXT", None, None, DataType::Utf8),
            // Semi-structured: JSON-serialized strings from Snowflake
            ("VARIANT", None, None, DataType::Utf8),
            ("OBJECT", None, None, DataType::Utf8),
            ("ARRAY", None, None, DataType::Utf8),
            // Binary
            ("BINARY", None, None, DataType::Binary),
            ("VARBINARY", None, None, DataType::Binary),
            // Boolean / Date / Time
            ("BOOLEAN", None, None, DataType::Boolean),
            ("DATE", None, None, DataType::Date32),
            ("TIME", None, None, DataType::Time64(TimeUnit::Nanosecond)),
            // Timestamps
            (
                "DATETIME",
                None,
                None,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "TIMESTAMP",
                None,
                None,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "TIMESTAMP_NTZ",
                None,
                None,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "TIMESTAMP_LTZ",
                None,
                None,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "TIMESTAMP_TZ",
                None,
                None,
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ),
        ];

        for (sql_type, precision, scale, expected) in cases {
            let got = map_snowflake_sql_type(sql_type, *precision, *scale);
            assert_eq!(
                got, *expected,
                "Mismatch for SQL type '{sql_type}' (precision={precision:?}, scale={scale:?})"
            );
        }
    }

    #[test]
    fn test_map_snowflake_sql_type_is_case_insensitive() {
        assert_eq!(map_snowflake_sql_type("object", None, None), DataType::Utf8);
        assert_eq!(map_snowflake_sql_type("Object", None, None), DataType::Utf8);
        assert_eq!(
            map_snowflake_sql_type("varchar", None, None),
            DataType::Utf8
        );
    }

    #[test]
    fn test_map_snowflake_sql_type_unknown_falls_back_to_utf8() {
        // Unknown/future types must not crash the connector; fall back to Utf8
        // (lossless text representation) while logging a warning.
        assert_eq!(
            map_snowflake_sql_type("SOMETHING_NEW", None, None),
            DataType::Utf8
        );
    }

    #[test]
    fn test_map_snowflake_sql_type_extended_types() {
        // Explicit coverage for Snowflake types beyond the "classic" SQL set:
        // - Geospatial (GEOGRAPHY, GEOMETRY) — text-serialized WKT/GeoJSON/WKB.
        // - Semi-structured MAP — dynamic, collapses to JSON text.
        // - UUID / FILE — textual wire representations.
        // - DECFLOAT — dynamic-scale decimal with no lossless fixed-scale
        //   Arrow mapping; intentionally mapped to Utf8 to preserve exact values.
        for (sql_type, expected) in [
            ("GEOGRAPHY", DataType::Utf8),
            ("GEOMETRY", DataType::Utf8),
            ("MAP", DataType::Utf8),
            ("UUID", DataType::Utf8),
            ("FILE", DataType::Utf8),
            ("DECFLOAT", DataType::Utf8),
        ] {
            assert_eq!(
                map_snowflake_sql_type(sql_type, None, None),
                expected,
                "Mismatch for {sql_type}"
            );
        }
    }

    /// Builds the eight-column `information_schema.columns` Arrow response that
    /// `probe_snowflake_information_schema` selects, with caller-supplied
    /// precision and scale arrays so each Snowflake encoding can be exercised.
    fn information_schema_batch(
        names: &[&str],
        data_types: &[&str],
        nullables: &[&str],
        precisions: ArrayRef,
        scales: ArrayRef,
    ) -> RecordBatch {
        let rows = names.len();
        let empty = || Arc::new(StringArray::from(vec![None::<&str>; rows])) as ArrayRef;

        RecordBatch::try_from_iter(vec![
            (
                "COLUMN_NAME",
                Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            ),
            (
                "DATA_TYPE",
                Arc::new(StringArray::from(data_types.to_vec())) as ArrayRef,
            ),
            (
                "IS_NULLABLE",
                Arc::new(StringArray::from(nullables.to_vec())) as ArrayRef,
            ),
            ("NUMERIC_PRECISION", precisions),
            ("NUMERIC_SCALE", scales),
            ("COMMENT", empty()),
            ("TABLE_COMMENT", empty()),
            ("CLUSTERING_KEY", empty()),
        ])
        .expect("information_schema Arrow batch should build")
    }

    /// A `NUMBER(38,0)` metadata column carrying `values`, which is how
    /// Snowflake encodes `NUMERIC_PRECISION` and `NUMERIC_SCALE` when the
    /// result chunk uses a decimal representation.
    fn metadata_decimal(values: &[Option<i128>]) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values.to_vec())
                .with_precision_and_scale(38, 0)
                .expect("NUMBER(38,0) metadata column should build"),
        )
    }

    /// The `LINEITEM` columns from #13271: a scale-zero key, three
    /// `NUMBER(12,2)` measures, and a text column with no numeric metadata.
    const LINEITEM_COLUMNS: &[&str] = &[
        "L_ORDERKEY",
        "L_QUANTITY",
        "L_DISCOUNT",
        "L_TAX",
        "L_SHIPMODE",
    ];
    const LINEITEM_TYPES: &[&str] = &["NUMBER", "NUMBER", "NUMBER", "NUMBER", "TEXT"];
    const LINEITEM_NULLABLE: &[&str] = &["NO", "NO", "NO", "NO", "YES"];

    fn assert_lineitem_schema(schema: &Schema) {
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Decimal128(38, 0),
            "L_ORDERKEY is NUMBER(38,0)"
        );
        for index in 1..=3 {
            let field = schema.field(index);
            assert_eq!(
                field.data_type(),
                &DataType::Decimal128(12, 2),
                "{} must keep Snowflake's scale, or every fraction is rounded away",
                field.name()
            );
            assert_eq!(
                field
                    .metadata()
                    .get(SOURCE_TYPE_METADATA_KEY)
                    .map(String::as_str),
                Some("NUMBER(12,2)"),
                "{} source type",
                field.name()
            );
        }
        assert_eq!(
            schema.field(4).data_type(),
            &DataType::Utf8,
            "L_SHIPMODE has no numeric precision or scale"
        );
        assert!(
            !schema.field(0).is_nullable(),
            "L_ORDERKEY is declared NOT NULL"
        );
        assert!(schema.field(4).is_nullable(), "L_SHIPMODE is nullable");
    }

    /// Regression test for #13271: Snowflake returns `NUMERIC_PRECISION` and
    /// `NUMERIC_SCALE` as Arrow integers, and reading them as text only
    /// registered every `NUMBER(12,2)` column as `Decimal128(38, 0)` —
    /// rounding away the fraction the source stores, and permanently
    /// materializing zeros once the table is accelerated.
    #[test]
    fn information_schema_arrow_keeps_scale_from_integer_metadata_columns() {
        let batch = information_schema_batch(
            LINEITEM_COLUMNS,
            LINEITEM_TYPES,
            LINEITEM_NULLABLE,
            Arc::new(Int64Array::from(vec![
                Some(38),
                Some(12),
                Some(12),
                Some(12),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(0),
                Some(2),
                Some(2),
                Some(2),
                None,
            ])),
        );

        let schema = parse_information_schema_arrow(&[batch], "TPCH_SF1.LINEITEM")
            .expect("information_schema Arrow response should parse");

        assert_lineitem_schema(&schema);
    }

    /// The same metadata delivered as decimals, which is how Snowflake encodes
    /// the `NUMBER(38,0)` `information_schema` columns when the result chunk
    /// exceeds the width of an Arrow integer.
    #[test]
    fn information_schema_arrow_keeps_scale_from_decimal_metadata_columns() {
        let batch = information_schema_batch(
            LINEITEM_COLUMNS,
            LINEITEM_TYPES,
            LINEITEM_NULLABLE,
            metadata_decimal(&[Some(38), Some(12), Some(12), Some(12), None]),
            metadata_decimal(&[Some(0), Some(2), Some(2), Some(2), None]),
        );

        let schema = parse_information_schema_arrow(&[batch], "TPCH_SF1.LINEITEM")
            .expect("information_schema Arrow response should parse");

        assert_lineitem_schema(&schema);
    }

    /// A metadata cell wide enough to need 256 bits has to be rescaled before it
    /// is narrowed: a `Decimal256(76, 38)` cell holding 12 carries a coefficient
    /// of 12 followed by 38 zeros, which overruns an `i128` even though the
    /// number it represents is small.
    #[test]
    fn information_schema_arrow_keeps_scale_from_wide_decimal_metadata_columns() {
        let wide = |digits: &str| {
            Arc::new(
                Decimal256Array::from(vec![Some(
                    i256::from_string(digits).expect("wide decimal literal"),
                )])
                .with_precision_and_scale(76, 38)
                .expect("Decimal256(76,38) metadata column should build"),
            ) as ArrayRef
        };
        let scaled = |value: &str| wide(&format!("{value}{}", "0".repeat(38)));

        let batch = information_schema_batch(
            &["AMOUNT"],
            &["NUMBER"],
            &["YES"],
            scaled("12"),
            scaled("2"),
        );

        let schema = parse_information_schema_arrow(&[batch], "SALES.ORDERS")
            .expect("a wide decimal metadata column should parse");

        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(12, 2));

        // 12.5 is not the whole number `numeric_precision` declares, and must
        // not be truncated into one.
        let fractional = information_schema_batch(
            &["AMOUNT"],
            &["NUMBER"],
            &["YES"],
            wide(&format!("125{}", "0".repeat(37))),
            scaled("2"),
        );

        let error = parse_information_schema_arrow(&[fractional], "SALES.ORDERS")
            .expect_err("a fractional wide decimal must not register a schema");
        assert!(
            error.contains("numeric_precision") && error.contains("'AMOUNT'"),
            "error must name the metadata column and the column: {error}"
        );
    }

    /// Snowflake sizes an integer metadata column to the values it holds, so
    /// the same response can arrive as any Arrow integer width, signed or
    /// unsigned.
    #[test]
    fn information_schema_arrow_keeps_scale_from_every_integer_width() {
        let precisions: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![Some(12)])),
            Arc::new(Int16Array::from(vec![Some(12)])),
            Arc::new(Int32Array::from(vec![Some(12)])),
            Arc::new(Int64Array::from(vec![Some(12)])),
            Arc::new(UInt8Array::from(vec![Some(12)])),
            Arc::new(UInt16Array::from(vec![Some(12)])),
            Arc::new(UInt32Array::from(vec![Some(12)])),
            Arc::new(UInt64Array::from(vec![Some(12)])),
        ];

        for precision in precisions {
            let arrow_type = precision.data_type().clone();
            let batch = information_schema_batch(
                &["AMOUNT"],
                &["NUMBER"],
                &["YES"],
                precision,
                Arc::new(Int32Array::from(vec![Some(2)])),
            );

            let schema = parse_information_schema_arrow(&[batch], "SALES.ORDERS")
                .unwrap_or_else(|e| panic!("{arrow_type} precision should parse: {e}"));

            assert_eq!(
                schema.field(0).data_type(),
                &DataType::Decimal128(12, 2),
                "{arrow_type} precision dropped"
            );
        }
    }

    /// The text representation Snowflake uses in its JSON result format still
    /// has to be read, including a decimal-rendered integer such as `12.00`.
    #[test]
    fn information_schema_arrow_keeps_scale_from_string_metadata_columns() {
        let batch = information_schema_batch(
            &["AMOUNT", "TOTAL", "NAME"],
            &["NUMBER", "NUMBER", "VARCHAR"],
            &["YES", "YES", "YES"],
            Arc::new(StringArray::from(vec![Some("12"), Some("12.00"), None])),
            Arc::new(StringArray::from(vec![Some("2"), Some(" 2 "), None])),
        );

        let schema = parse_information_schema_arrow(&[batch], "SALES.ORDERS")
            .expect("string metadata should still parse");

        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(12, 2));
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(12, 2));
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    }

    /// An all-null metadata column arrives as an Arrow `Null` array, which
    /// carries no integers to read and must not be mistaken for unreadable
    /// metadata.
    #[test]
    fn information_schema_arrow_accepts_all_null_metadata_columns() {
        let batch = information_schema_batch(
            &["NAME"],
            &["VARCHAR"],
            &["YES"],
            Arc::new(NullArray::new(1)),
            Arc::new(NullArray::new(1)),
        );

        let schema = parse_information_schema_arrow(&[batch], "SALES.ORDERS")
            .expect("absent numeric metadata should parse");

        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    /// Metadata Spice cannot turn into a precision and scale must fail the
    /// probe so discovery falls back to `SHOW COLUMNS`, rather than register a
    /// silently wrong scale. Each case names the column and the table so the
    /// fallback warning identifies what to look at.
    #[test]
    fn information_schema_arrow_rejects_unusable_precision_and_scale() {
        let cases: Vec<(&str, ArrayRef, ArrayRef, &str)> = vec![
            (
                "precision above the Decimal128 maximum",
                Arc::new(Int64Array::from(vec![Some(99)])),
                Arc::new(Int64Array::from(vec![Some(2)])),
                "numeric_precision",
            ),
            (
                "zero precision",
                Arc::new(Int64Array::from(vec![Some(0)])),
                Arc::new(Int64Array::from(vec![Some(0)])),
                "numeric_precision",
            ),
            (
                "negative precision",
                Arc::new(Int64Array::from(vec![Some(-12)])),
                Arc::new(Int64Array::from(vec![Some(2)])),
                "numeric_precision",
            ),
            (
                "scale wider than the precision",
                Arc::new(Int64Array::from(vec![Some(12)])),
                Arc::new(Int64Array::from(vec![Some(20)])),
                "numeric_scale",
            ),
            (
                "negative scale",
                Arc::new(Int64Array::from(vec![Some(12)])),
                Arc::new(Int64Array::from(vec![Some(-2)])),
                "numeric_scale",
            ),
            (
                "unreadable Arrow type",
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(Int64Array::from(vec![Some(2)])),
                "numeric_precision",
            ),
            (
                "text that is not a number",
                Arc::new(StringArray::from(vec![Some("twelve")])),
                Arc::new(Int64Array::from(vec![Some(2)])),
                "numeric_precision",
            ),
            (
                "fractional decimal",
                Arc::new(
                    Decimal128Array::from(vec![Some(1250_i128)])
                        .with_precision_and_scale(38, 2)
                        .expect("fractional metadata column should build"),
                ),
                Arc::new(Int64Array::from(vec![Some(2)])),
                "numeric_precision",
            ),
        ];

        for (case, precision, scale, expected_column) in cases {
            let batch =
                information_schema_batch(&["AMOUNT"], &["NUMBER"], &["YES"], precision, scale);

            let error = parse_information_schema_arrow(&[batch], "SALES.ORDERS")
                .expect_err(&format!("{case} must not register a schema"));

            assert!(
                error.contains(expected_column),
                "{case}: error must name the metadata column it could not read: {error}"
            );
            assert!(
                error.contains("'AMOUNT'") && error.contains("'SALES.ORDERS'"),
                "{case}: error must name the column and table: {error}"
            );
        }
    }

    /// The JSON result format reaches the same conversion, so a precision and
    /// scale it renders as text, or as a whole number with a decimal point,
    /// must survive, and unusable metadata must fail there too.
    #[test]
    fn information_schema_json_reads_every_precision_representation() {
        let rows = serde_json::json!([
            ["A", "NUMBER", "NO", 12, 2, null, null, null],
            ["B", "NUMBER", "NO", "12", "2", null, null, null],
            ["C", "NUMBER", "NO", 12.0, 2.0, null, null, null],
            ["D", "VARCHAR", "YES", null, null, null, null, null]
        ]);

        let schema = parse_information_schema_json(&rows, "SALES.ORDERS")
            .expect("every precision representation should parse");

        for index in 0..3 {
            assert_eq!(
                schema.field(index).data_type(),
                &DataType::Decimal128(12, 2),
                "{} lost its scale",
                schema.field(index).name()
            );
        }
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);

        let invalid = serde_json::json!([["A", "NUMBER", "NO", 99, 2, null, null, null]]);
        let error = parse_information_schema_json(&invalid, "SALES.ORDERS")
            .expect_err("an out-of-range precision must not register a schema");
        assert!(
            error.contains("numeric_precision") && error.contains("'A'"),
            "error must name the metadata column and the column: {error}"
        );
    }

    /// A fixed-point column Snowflake reported no precision or scale for cannot
    /// be mapped without inventing a scale, so the probe must fail and let
    /// discovery fall back to `SHOW COLUMNS` instead of registering
    /// `Decimal128(38, 0)` and rounding every fraction away.
    #[test]
    fn information_schema_arrow_rejects_a_fixed_point_column_without_precision_or_scale() {
        let cases: Vec<(&str, ArrayRef, ArrayRef, &str)> = vec![
            (
                "neither reported",
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                "`numeric_precision` and `numeric_scale`",
            ),
            (
                "no scale",
                Arc::new(Int64Array::from(vec![Some(12)])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                "`numeric_scale`",
            ),
            (
                "no precision",
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Int64Array::from(vec![Some(2)])),
                "`numeric_precision`",
            ),
            (
                "empty text",
                Arc::new(StringArray::from(vec![Some("")])),
                Arc::new(StringArray::from(vec![Some("  ")])),
                "`numeric_precision` and `numeric_scale`",
            ),
        ];

        for (case, precision, scale, expected) in cases {
            let batch =
                information_schema_batch(&["AMOUNT"], &["NUMBER"], &["YES"], precision, scale);

            let error = parse_information_schema_arrow(&[batch], "SALES.ORDERS")
                .expect_err(&format!("{case} must not register a schema"));

            assert!(
                error.contains(expected),
                "{case}: error must name the missing metadata: {error}"
            );
            assert!(
                error.contains("'AMOUNT'") && error.contains("'SALES.ORDERS'"),
                "{case}: error must name the column and table: {error}"
            );
        }
    }

    /// A float, string, or timestamp column carries no decimal digits, so
    /// whatever `information_schema` reports in the numeric columns for it must
    /// neither be required nor rejected.
    ///
    /// Snowflake does not report a decimal's precision for these types, and what
    /// it does report is not one: holding those values to the 1-38 digits a
    /// decimal can hold would fail the preferred probe for any table with a
    /// `FLOAT` column and silently drop to `SHOW COLUMNS`, losing the
    /// nullability, comments, and clustering metadata this probe exists to read.
    #[test]
    fn information_schema_arrow_accepts_non_fixed_point_columns_whatever_the_numeric_columns_say() {
        let columns = &["RATING", "NAME", "CREATED_AT", "PRICE", "ACTIVE", "SHIPPED"];
        let types = &[
            "FLOAT",
            "VARCHAR",
            "TIMESTAMP_NTZ",
            "DOUBLE",
            "BOOLEAN",
            "DATE",
        ];
        let nullable = &["YES", "YES", "YES", "YES", "YES", "YES"];
        // A zero precision, a binary-radix precision that overruns a decimal's
        // 38 digits, and an absent one — none of which describe these types.
        let precisions: Vec<Option<i64>> = vec![Some(0), Some(99), None, Some(53), Some(0), None];
        let scales: Vec<Option<i64>> = vec![None, None, Some(9), Some(-2), None, None];

        for precision_kind in ["integer", "text"] {
            let (precision, scale): (ArrayRef, ArrayRef) = if precision_kind == "integer" {
                (
                    Arc::new(Int64Array::from(precisions.clone())),
                    Arc::new(Int64Array::from(scales.clone())),
                )
            } else {
                let render = |values: &Vec<Option<i64>>| {
                    Arc::new(StringArray::from(
                        values
                            .iter()
                            .map(|value| value.map(|value| value.to_string()))
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                };
                (render(&precisions), render(&scales))
            };
            let batch = information_schema_batch(columns, types, nullable, precision, scale);

            let schema =
                parse_information_schema_arrow(&[batch], "SALES.ORDERS").unwrap_or_else(|e| {
                    panic!("{precision_kind} metadata on non-fixed-point columns should parse: {e}")
                });

            assert_eq!(schema.field(0).data_type(), &DataType::Float64);
            assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
            assert_eq!(
                schema.field(2).data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, None)
            );
            assert_eq!(schema.field(3).data_type(), &DataType::Float64);
            assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
            assert_eq!(schema.field(5).data_type(), &DataType::Date32);
            for field in schema.fields() {
                assert_eq!(
                    field
                        .metadata()
                        .get(SOURCE_TYPE_METADATA_KEY)
                        .map(String::as_str),
                    Some(types[schema.index_of(field.name()).expect("field present")]),
                    "{} source type must not gain decimal digits",
                    field.name()
                );
            }
        }
    }

    /// The same rule on the JSON path: a `FLOAT` column's numeric metadata is
    /// not a decimal's, so it cannot fail the probe.
    #[test]
    fn information_schema_json_accepts_non_fixed_point_columns_whatever_the_numeric_columns_say() {
        let rows = serde_json::json!([
            ["RATING", "FLOAT", "YES", 0, null, null, null, null],
            ["PRICE", "DOUBLE", "YES", 53, -2, null, null, null],
            [
                "NAME",
                "VARCHAR",
                "YES",
                "not a number",
                "",
                null,
                null,
                null
            ]
        ]);

        let schema = parse_information_schema_json(&rows, "SALES.ORDERS")
            .expect("non-fixed-point numeric metadata should parse");

        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    }

    /// The JSON path applies the same requirement.
    #[test]
    fn information_schema_json_rejects_a_fixed_point_column_without_scale() {
        let rows = serde_json::json!([["AMOUNT", "NUMBER", "NO", 12, null, null, null, null]]);

        let error = parse_information_schema_json(&rows, "SALES.ORDERS")
            .expect_err("a fixed-point column without a scale must not register a schema");

        assert!(
            error.contains("`numeric_scale`") && error.contains("'AMOUNT'"),
            "error must name the missing metadata and the column: {error}"
        );
    }
}
