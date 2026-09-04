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

//! Mode A: `DataFusion` consumer baseline.
//!
//! Registers the IBM TPC-H CSVs as in-memory-backed listing tables and lowers
//! each suite plan with `datafusion-substrait::from_substrait_plan` — the same
//! consumer `spiced` uses on the `FlightSQL` path. This is a DF-fork signal, not
//! product CI.

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;
use snafu::ResultExt;

use crate::compare::{ColumnSpec, TableData, compare};
use crate::error::{self, Result};
use crate::report::{CaseResult, TestStatus};
use crate::schema::{TPCH_TABLES, schema_for};
use crate::suite::{InputTable, LoadedCase, LoadedSuite};

pub const ENGINE_NAME: &str = "DataFusion";
pub const ENGINE_VERSION: &str = "54.1";

pub struct ModeAEngine {
    ctx: SessionContext,
}

impl ModeAEngine {
    /// Register every TPC-H CSV under the Isthmus plan name (`LINEITEM`).
    ///
    /// `register_csv(&str, …)` goes through `TableReference::parse_str`, which
    /// lowercases even when `enable_ident_normalization` is off. The Substrait
    /// consumer then looks up the exact Isthmus name on a case-sensitive
    /// catalog, so we register via `TableReference::bare`.
    pub async fn with_tpch_data(data_dir: &Path) -> Result<Self> {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.enable_ident_normalization = false;
        let ctx = SessionContext::new_with_config(config);
        for table in TPCH_TABLES {
            let csv_path = data_dir.join(format!("{}.csv", table.file_stem));
            register_csv(&ctx, table.plan_name, &csv_path, table.file_stem).await?;
        }
        Ok(Self { ctx })
    }

    pub async fn run_suite(
        &self,
        suite: &LoadedSuite,
        only: Option<&str>,
    ) -> Result<Vec<CaseResult>> {
        let mut results = Vec::with_capacity(suite.cases.len());
        for case in &suite.cases {
            if let Some(filter) = only
                && !case.id.eq_ignore_ascii_case(filter)
            {
                continue;
            }
            results.push(self.run_case(case).await);
        }
        Ok(results)
    }

    async fn run_case(&self, case: &LoadedCase) -> CaseResult {
        let start = Instant::now();
        let Some(expected) = case.expected.as_ref() else {
            return CaseResult {
                test_id: case.id.clone(),
                description: case.description.clone(),
                status: TestStatus::Skipped,
                execution_time_ms: elapsed_ms(start),
                error_message: Some("No expected output — cannot verify correctness".to_string()),
            };
        };

        match self.execute(case).await {
            Ok(actual) => match compare(&actual, expected) {
                None => CaseResult {
                    test_id: case.id.clone(),
                    description: case.description.clone(),
                    status: TestStatus::Passed,
                    execution_time_ms: elapsed_ms(start),
                    error_message: None,
                },
                Some(mismatch) => CaseResult {
                    test_id: case.id.clone(),
                    description: case.description.clone(),
                    status: TestStatus::Failed,
                    execution_time_ms: elapsed_ms(start),
                    error_message: Some(mismatch.to_string()),
                },
            },
            Err(err) => CaseResult {
                test_id: case.id.clone(),
                description: case.description.clone(),
                status: TestStatus::Error,
                execution_time_ms: elapsed_ms(start),
                error_message: Some(err),
            },
        }
    }

    async fn execute(&self, case: &LoadedCase) -> std::result::Result<TableData, String> {
        ensure_inputs_registered(case)?;

        let proto = Plan::decode(case.plan_bytes.as_slice()).map_err(|e| {
            format!(
                "Failed to decode Substrait plan {}: {e}",
                case.plan_path.display()
            )
        })?;

        let state = self.ctx.state();
        let logical_plan = from_substrait_plan(&state, &proto)
            .await
            .map_err(|e| format!("from_substrait_plan: {e}"))?;

        let df = self
            .ctx
            .execute_logical_plan(logical_plan)
            .await
            .map_err(|e| format!("execute_logical_plan: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;

        Ok(batches_to_table(&batches))
    }
}

async fn register_csv(
    ctx: &SessionContext,
    table_name: &str,
    csv_path: &Path,
    file_stem: &str,
) -> Result<()> {
    let schema = schema_for(file_stem).ok_or_else(|| error::Error::UnknownTable {
        name: file_stem.to_string(),
        test_id: String::new(),
    })?;
    let options = CsvReadOptions::new()
        .delimiter(b'|')
        .has_header(false)
        .schema(schema.as_ref())
        .file_extension("csv");
    ctx.register_csv(
        TableReference::bare(table_name),
        csv_path.to_string_lossy().as_ref(),
        options,
    )
    .await
    .context(error::RegisterTableSnafu {
        table: table_name.to_string(),
        path: csv_path.to_path_buf(),
    })
}

fn ensure_inputs_registered(case: &LoadedCase) -> std::result::Result<(), String> {
    let known: HashSet<&str> = TPCH_TABLES
        .iter()
        .flat_map(|t| [t.file_stem, t.plan_name])
        .collect();
    for InputTable { name, csv_path } in &case.input_tables {
        if !csv_path.exists() {
            return Err(format!(
                "test '{}' input CSV '{}' does not exist",
                case.id,
                csv_path.display()
            ));
        }
        if !known.contains(name.as_str()) && !known.contains(name.to_ascii_uppercase().as_str()) {
            return Err(format!(
                "test '{}' references unknown TPC-H table '{name}'",
                case.id
            ));
        }
    }
    Ok(())
}

fn batches_to_table(batches: &[RecordBatch]) -> TableData {
    if batches.is_empty() {
        return TableData {
            columns: Vec::new(),
            rows: Vec::new(),
        };
    }
    let schema = batches[0].schema();
    let columns = schema
        .fields()
        .iter()
        .map(|f| ColumnSpec {
            name: f.name().clone(),
            type_token: arrow_type_token(f.data_type()).to_string(),
        })
        .collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                row.push(cell_to_string(batch.column(col_idx).as_ref(), row_idx));
            }
            rows.push(row);
        }
    }
    TableData { columns, rows }
}

fn arrow_type_token(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            "integer"
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "bigint",
        DataType::Float32 => "float",
        DataType::Float64 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "double",
        DataType::Boolean => "boolean",
        DataType::Date32 | DataType::Date64 => "date",
        _ => "string",
    }
}

fn cell_to_string(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return String::new();
    }
    match array.data_type() {
        DataType::Boolean => array.as_boolean().value(idx).to_string(),
        DataType::Int8 => array
            .as_primitive::<arrow::datatypes::Int8Type>()
            .value(idx)
            .to_string(),
        DataType::Int16 => array
            .as_primitive::<arrow::datatypes::Int16Type>()
            .value(idx)
            .to_string(),
        DataType::Int32 => array
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(idx)
            .to_string(),
        DataType::Int64 => array
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(idx)
            .to_string(),
        DataType::UInt8 => array
            .as_primitive::<arrow::datatypes::UInt8Type>()
            .value(idx)
            .to_string(),
        DataType::UInt16 => array
            .as_primitive::<arrow::datatypes::UInt16Type>()
            .value(idx)
            .to_string(),
        DataType::UInt32 => array
            .as_primitive::<arrow::datatypes::UInt32Type>()
            .value(idx)
            .to_string(),
        DataType::UInt64 => array
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(idx)
            .to_string(),
        DataType::Float32 => array
            .as_primitive::<arrow::datatypes::Float32Type>()
            .value(idx)
            .to_string(),
        DataType::Float64 => array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(idx)
            .to_string(),
        DataType::Decimal128(_, scale) => {
            let raw = array
                .as_primitive::<arrow::datatypes::Decimal128Type>()
                .value(idx);
            format_decimal(raw, i32::from(*scale))
        }
        DataType::Date32 => {
            let days = array
                .as_primitive::<arrow::datatypes::Date32Type>()
                .value(idx);
            format_date32(days)
        }
        DataType::Utf8 => array.as_string::<i32>().value(idx).to_string(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(idx).to_string(),
        DataType::Utf8View => array.as_string_view().value(idx).to_string(),
        other => format!("<{other:?}>"),
    }
}

fn format_decimal(raw: i128, scale: i32) -> String {
    if scale <= 0 {
        return raw.to_string();
    }
    let scale_u = u32::try_from(scale).unwrap_or(0);
    let factor = 10_u128.saturating_pow(scale_u);
    let sign = if raw < 0 { "-" } else { "" };
    let abs = raw.unsigned_abs();
    let whole = abs / factor;
    let frac = abs % factor;
    let frac_width = usize::try_from(scale).unwrap_or(0);
    format!("{sign}{whole}.{frac:0frac_width$}")
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn format_date32(days: i32) -> String {
    // Date32 is days since UNIX epoch. Keep ISO-8601 so it can match a golden
    // `date` column stored as a string.
    match chrono::DateTime::from_timestamp(i64::from(days) * 86_400, 0) {
        Some(ts) => ts.format("%Y-%m-%d").to_string(),
        None => days.to_string(),
    }
}
