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

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use datafusion::catalog::{ScanArgs, ScanResult, Session};
use datafusion::common::{Constraints, ScalarValue, SchemaExt, Statistics};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, dml::InsertOp};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, metrics::MetricsSet};
use datafusion::sql::TableReference;
use datafusion::sql::unparser::{Unparser, dialect::Dialect};
use datafusion_table_providers::util::count_exec::make_count_exec;
use datafusion_table_providers::util::dml::{DeletionExec, DeletionSink, UpdateExec, UpdateSink};
use futures::StreamExt;
use snafu::prelude::*;
use snowflake_api::{QueryResult, SnowflakeApi};
use tokio::sync::Mutex;

use datafusion_federation::{
    FederatedTableProviderAdaptor, FederationAnalyzerForLogicalPlan, FederationProvider,
};

use super::SnowflakeConnectionPool;

const INSERT_BATCH_ROWS: usize = 1_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Snowflake identifier '{value}': {reason}"))]
    InvalidIdentifier { value: String, reason: String },

    #[snafu(display("Failed to get Snowflake connection for table {table}: {source}"))]
    UnableToGetConnection {
        table: String,
        source: datafusion_table_providers::sql::db_connection_pool::Error,
    },

    #[snafu(display("Failed to access Snowflake connection for table {table}: {reason}"))]
    InvalidConnection { table: String, reason: String },

    #[snafu(display("Failed to execute Snowflake DML statement for table {table}: {source}"))]
    QueryFailed {
        table: String,
        source: snowflake_api::SnowflakeApiError,
    },

    #[snafu(display("Unexpected Snowflake DML response for table {table}: {reason}"))]
    UnexpectedDmlResponse { table: String, reason: String },

    #[snafu(display(
        "Snowflake DML count mismatch for table {table}: expected {expected}, got {actual}"
    ))]
    DmlCountMismatch {
        table: String,
        expected: u64,
        actual: u64,
    },

    #[snafu(display("Unsupported Snowflake DML value for column {column}: {data_type}"))]
    UnsupportedValue { column: String, data_type: String },

    #[snafu(display("Invalid Snowflake DML value for column {column}: {reason}"))]
    InvalidValue { column: String, reason: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DataFusionError {
    fn from(error: Error) -> Self {
        DataFusionError::External(Box::new(error))
    }
}

#[derive(Clone)]
pub struct SnowflakeTableProvider {
    read_provider: Arc<dyn TableProvider>,
    pool: Arc<SnowflakeConnectionPool>,
    table_reference: TableReference,
    schema: SchemaRef,
    dialect: Arc<dyn Dialect + Send + Sync>,
    write_lock: Arc<Mutex<()>>,
}

impl std::fmt::Debug for SnowflakeTableProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnowflakeTableProvider")
            .field("table_reference", &self.table_reference)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl SnowflakeTableProvider {
    #[must_use]
    pub fn new(
        read_provider: Arc<dyn TableProvider>,
        pool: Arc<SnowflakeConnectionPool>,
        table_reference: TableReference,
        schema: SchemaRef,
        dialect: Arc<dyn Dialect + Send + Sync>,
        write_lock: Arc<Mutex<()>>,
    ) -> Arc<dyn TableProvider> {
        let write_provider = Arc::new(Self {
            read_provider: Arc::clone(&read_provider),
            pool,
            table_reference,
            schema,
            dialect,
            write_lock,
        });

        // Re-wrap in a FederatedTableProviderAdaptor so that datafusion-federation's
        // analyzer still recognises this as a federated table (it only downcasts to
        // FederatedTableProviderAdaptor). The source carries the fully-qualified SQL;
        // the fallback provider is our write-capable SnowflakeTableProvider.
        if let Some(adaptor) = read_provider
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
        {
            return Arc::new(FederatedTableProviderAdaptor::new_with_provider(
                Arc::clone(&adaptor.source),
                write_provider,
            ));
        }

        write_provider
    }
}

impl FederationProvider for SnowflakeTableProvider {
    fn name(&self) -> &'static str {
        "SnowflakeTableProvider"
    }

    fn compute_context(&self) -> Option<String> {
        self.read_provider
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .and_then(|a| a.source.federation_provider().compute_context())
    }

    fn analyzer(&self, plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
        self.read_provider
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .and_then(|a| a.source.federation_provider().analyzer(plan))
    }
}

#[async_trait]
impl TableProvider for SnowflakeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.read_provider.constraints()
    }

    fn table_type(&self) -> TableType {
        self.read_provider.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.read_provider.get_table_definition()
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.read_provider.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.read_provider.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.read_provider.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.read_provider.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DataFusionResult<ScanResult> {
        self.read_provider.scan_with_args(state, args).await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if matches!(insert_op, InsertOp::Replace) {
            return Err(DataFusionError::Plan(
                "Snowflake tables do not support INSERT REPLACE semantics".to_string(),
            ));
        }

        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;

        let sink = Arc::new(SnowflakeDataSink {
            pool: Arc::clone(&self.pool),
            table_reference: self.table_reference.clone(),
            schema: Arc::clone(&self.schema),
            insert_op,
            write_lock: Arc::clone(&self.write_lock),
        });

        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let where_clause = filters_to_sql(&filters, self.dialect.as_ref())?;
        Ok(Arc::new(DeletionExec::new(Arc::new(
            SnowflakeDeletionSink {
                pool: Arc::clone(&self.pool),
                table_reference: self.table_reference.clone(),
                where_clause,
                write_lock: Arc::clone(&self.write_lock),
            },
        ))))
    }

    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if assignments.is_empty() {
            return make_count_exec(0);
        }

        let set_clause = assignments_to_sql(&assignments, self.dialect.as_ref())?;
        let where_clause = filters_to_sql(&filters, self.dialect.as_ref())?;

        Ok(Arc::new(UpdateExec::new(Arc::new(SnowflakeUpdateSink {
            pool: Arc::clone(&self.pool),
            table_reference: self.table_reference.clone(),
            set_clause,
            where_clause,
            write_lock: Arc::clone(&self.write_lock),
        }))))
    }
}

#[derive(Clone)]
struct SnowflakeDataSink {
    pool: Arc<SnowflakeConnectionPool>,
    table_reference: TableReference,
    schema: SchemaRef,
    insert_op: InsertOp,
    write_lock: Arc<Mutex<()>>,
}

impl std::fmt::Debug for SnowflakeDataSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnowflakeDataSink")
            .field("table_reference", &self.table_reference)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SnowflakeDataSink {
    fn fmt_as(
        &self,
        _display_type: DisplayFormatType,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            formatter,
            "SnowflakeDataSink(table={})",
            self.table_reference
        )
    }
}

#[async_trait]
impl DataSink for SnowflakeDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let _write_guard = self.write_lock.lock().await;
        let table_name = self.table_reference.to_quoted_string();
        let api = snowflake_api_from_pool(&self.pool, &table_name).await?;

        let mut transaction_started = false;

        let write_result = async {
            if matches!(self.insert_op, InsertOp::Overwrite) {
                execute_statement(&api, &table_name, "BEGIN").await?;
                transaction_started = true;
                let delete_sql = format!("DELETE FROM {table_name}");
                execute_dml_count(&api, &table_name, &delete_sql).await?;
            }

            let mut total_rows = 0u64;
            while let Some(batch) = data.next().await {
                let batch = batch?;
                self.schema
                    .logically_equivalent_names_and_types(batch.schema_ref())?;

                if batch.num_rows() == 0 {
                    continue;
                }

                if !transaction_started {
                    execute_statement(&api, &table_name, "BEGIN").await?;
                    transaction_started = true;
                }

                total_rows += insert_record_batch(&api, &table_name, &self.schema, &batch).await?;
            }

            if transaction_started {
                execute_statement(&api, &table_name, "COMMIT").await?;
                transaction_started = false;
            }

            Ok(total_rows)
        }
        .await;

        if write_result.is_err() && transaction_started {
            rollback_write(&api, &table_name).await;
        }

        write_result
    }
}

struct SnowflakeDeletionSink {
    pool: Arc<SnowflakeConnectionPool>,
    table_reference: TableReference,
    where_clause: Option<String>,
    write_lock: Arc<Mutex<()>>,
}

#[async_trait]
impl DeletionSink for SnowflakeDeletionSink {
    async fn delete_from(
        &self,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let _write_guard = self.write_lock.lock().await;
        let table_name = self.table_reference.to_quoted_string();
        let api = snowflake_api_from_pool(&self.pool, &table_name).await?;
        let sql = if let Some(where_clause) = &self.where_clause {
            format!("DELETE FROM {table_name} WHERE {where_clause}")
        } else {
            format!("DELETE FROM {table_name}")
        };

        Ok(execute_dml_count(&api, &table_name, &sql).await?)
    }
}

struct SnowflakeUpdateSink {
    pool: Arc<SnowflakeConnectionPool>,
    table_reference: TableReference,
    set_clause: String,
    where_clause: Option<String>,
    write_lock: Arc<Mutex<()>>,
}

#[async_trait]
impl UpdateSink for SnowflakeUpdateSink {
    async fn execute_update(
        &self,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let _write_guard = self.write_lock.lock().await;
        let table_name = self.table_reference.to_quoted_string();
        let api = snowflake_api_from_pool(&self.pool, &table_name).await?;
        let sql = if let Some(where_clause) = &self.where_clause {
            format!(
                "UPDATE {table_name} SET {} WHERE {where_clause}",
                self.set_clause
            )
        } else {
            format!("UPDATE {table_name} SET {}", self.set_clause)
        };

        Ok(execute_dml_count(&api, &table_name, &sql).await?)
    }
}

async fn snowflake_api_from_pool(
    pool: &Arc<SnowflakeConnectionPool>,
    table_name: &str,
) -> DataFusionResult<Arc<SnowflakeApi>> {
    let connection = pool.connect().await.context(UnableToGetConnectionSnafu {
        table: table_name.to_string(),
    })?;
    let snowflake_connection = connection
        .as_any()
        .downcast_ref::<db_connection_pool::dbconnection::snowflakeconn::SnowflakeConnection>()
        .context(InvalidConnectionSnafu {
            table: table_name.to_string(),
            reason: "connection was not a Snowflake connection",
        })?;

    Ok(Arc::clone(&snowflake_connection.api))
}

async fn insert_record_batch(
    api: &SnowflakeApi,
    table_name: &str,
    schema: &SchemaRef,
    batch: &RecordBatch,
) -> DataFusionResult<u64> {
    let mut inserted_rows = 0u64;
    let columns = schema
        .fields()
        .iter()
        .map(|field| quote_sql_identifier(field.name()))
        .collect::<Result<Vec<_>>>()?;
    let column_list = columns.join(", ");

    for start_row in (0..batch.num_rows()).step_by(INSERT_BATCH_ROWS) {
        let row_count = INSERT_BATCH_ROWS.min(batch.num_rows() - start_row);
        let values_sql = values_sql_for_rows(batch, schema, start_row, row_count)?;
        let sql = format!("INSERT INTO {table_name} ({column_list}) VALUES {values_sql}");
        let actual_inserted = execute_dml_count(api, table_name, &sql).await?;
        let expected_inserted = u64::try_from(row_count).map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to convert Snowflake insert row count: {error}"
            ))
        })?;

        ensure!(
            actual_inserted == expected_inserted,
            DmlCountMismatchSnafu {
                table: table_name.to_string(),
                expected: expected_inserted,
                actual: actual_inserted,
            }
        );

        inserted_rows += actual_inserted;
    }

    Ok(inserted_rows)
}

fn values_sql_for_rows(
    batch: &RecordBatch,
    schema: &SchemaRef,
    start_row: usize,
    row_count: usize,
) -> DataFusionResult<String> {
    let mut row_values = Vec::with_capacity(row_count);
    for row_index in start_row..start_row + row_count {
        let mut column_values = Vec::with_capacity(batch.num_columns());
        for column_index in 0..batch.num_columns() {
            let column = batch.column(column_index);
            let scalar = ScalarValue::try_from_array(column, row_index)?;
            let field = schema.field(column_index);
            column_values.push(scalar_to_sql_literal(scalar, field.name())?);
        }
        row_values.push(format!("({})", column_values.join(", ")));
    }

    Ok(row_values.join(", "))
}

async fn execute_statement(
    api: &SnowflakeApi,
    table_name: &str,
    sql: &str,
) -> DataFusionResult<()> {
    api.exec(sql).await.context(QueryFailedSnafu {
        table: table_name.to_string(),
    })?;
    Ok(())
}

async fn execute_dml_count(
    api: &SnowflakeApi,
    table_name: &str,
    sql: &str,
) -> DataFusionResult<u64> {
    let result = api.exec(sql).await.context(QueryFailedSnafu {
        table: table_name.to_string(),
    })?;
    extract_dml_count(table_name, result).map_err(Into::into)
}

async fn rollback_write(api: &SnowflakeApi, table_name: &str) {
    if let Err(error) = execute_statement(api, table_name, "ROLLBACK").await {
        tracing::debug!(%error, table = %table_name, "Failed to rollback Snowflake write transaction");
    }
}

fn extract_dml_count(table_name: &str, result: QueryResult) -> Result<u64> {
    match result {
        QueryResult::Json(json) => extract_count_from_json_value(&json.value).ok_or_else(|| {
            UnexpectedDmlResponseSnafu {
                table: table_name.to_string(),
                reason: format!(
                    "could not find an affected-row count in JSON response {}",
                    json.value
                ),
            }
            .build()
        }),
        QueryResult::Arrow(batches) => {
            for batch in batches {
                if batch.num_rows() == 0 || batch.num_columns() == 0 {
                    continue;
                }
                let scalar = ScalarValue::try_from_array(batch.column(0), 0).map_err(|error| {
                    UnexpectedDmlResponseSnafu {
                        table: table_name.to_string(),
                        reason: format!(
                            "failed to read affected-row count from Arrow response: {error}"
                        ),
                    }
                    .build()
                })?;
                if let Some(count) = scalar_to_u64(&scalar) {
                    return Ok(count);
                }
            }
            UnexpectedDmlResponseSnafu {
                table: table_name.to_string(),
                reason: "could not find an affected-row count in Arrow response".to_string(),
            }
            .fail()
        }
        QueryResult::Empty => UnexpectedDmlResponseSnafu {
            table: table_name.to_string(),
            reason: "Snowflake returned an empty response for DML statement".to_string(),
        }
        .fail(),
    }
}

fn extract_count_from_json_value(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Array(rows) => rows.iter().find_map(|row| match row {
            serde_json::Value::Array(columns) => columns.iter().find_map(json_value_to_u64),
            other => json_value_to_u64(other),
        }),
        serde_json::Value::Object(object) => object.values().find_map(json_value_to_u64),
        other => json_value_to_u64(other),
    }
}

fn json_value_to_u64(value: &serde_json::Value) -> Option<u64> {
    value.as_u64().or_else(|| {
        value
            .as_i64()
            .and_then(|number| u64::try_from(number).ok())
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|string_value| string_value.parse().ok())
            })
    })
}

fn scalar_to_u64(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::UInt8(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(*value),
        ScalarValue::Int8(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int16(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int32(Some(value)) | ScalarValue::Decimal32(Some(value), _, 0) => {
            u64::try_from(*value).ok()
        }
        ScalarValue::Int64(Some(value)) | ScalarValue::Decimal64(Some(value), _, 0) => {
            u64::try_from(*value).ok()
        }
        ScalarValue::Decimal128(Some(value), _, 0) => u64::try_from(*value).ok(),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => value.parse().ok(),
        _ => None,
    }
}

fn filters_to_sql(
    filters: &[Expr],
    dialect: &(dyn Dialect + Send + Sync),
) -> DataFusionResult<Option<String>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let unparser = Unparser::new(dialect);
    let parts = filters
        .iter()
        .map(|filter| unparser.expr_to_sql(filter).map(|sql| sql.to_string()))
        .collect::<DataFusionResult<Vec<_>>>()?;

    Ok(Some(
        parts
            .iter()
            .map(|p| format!("({p})"))
            .collect::<Vec<_>>()
            .join(" AND "),
    ))
}

fn assignments_to_sql(
    assignments: &[(String, Expr)],
    dialect: &(dyn Dialect + Send + Sync),
) -> DataFusionResult<String> {
    let unparser = Unparser::new(dialect);
    assignments
        .iter()
        .map(|(column, value)| {
            let column = quote_sql_identifier(column)?;
            let value = unparser.expr_to_sql(value)?.to_string();
            Ok(format!("{column} = {value}"))
        })
        .collect::<DataFusionResult<Vec<_>>>()
        .map(|parts| parts.join(", "))
}

fn quote_sql_identifier(value: &str) -> Result<String> {
    ensure!(
        !value.contains('\0'),
        InvalidIdentifierSnafu {
            value: value.to_string(),
            reason: "identifier contains NUL byte".to_string(),
        }
    );

    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

fn quote_sql_string_literal(value: &str, column: &str) -> Result<String> {
    ensure!(
        !value.contains('\0'),
        InvalidValueSnafu {
            column: column.to_string(),
            reason: "string contains NUL byte".to_string(),
        }
    );

    Ok(format!("'{}'", value.replace('\'', "''")))
}

fn scalar_to_sql_literal(value: ScalarValue, column: &str) -> Result<String> {
    if value.is_null() {
        return Ok("NULL".to_string());
    }

    match value {
        ScalarValue::Boolean(Some(value)) => Ok(if value { "TRUE" } else { "FALSE" }.to_string()),
        ScalarValue::Float16(Some(value)) => finite_float_literal(f32::from(value), column),
        ScalarValue::Float32(Some(value)) => finite_float_literal(value, column),
        ScalarValue::Float64(Some(value)) => finite_float_literal(value, column),
        ScalarValue::Decimal32(Some(value), _, scale) => Ok(decimal_to_sql_literal(&value, scale)),
        ScalarValue::Decimal64(Some(value), _, scale) => Ok(decimal_to_sql_literal(&value, scale)),
        ScalarValue::Decimal128(Some(value), _, scale) => Ok(decimal_to_sql_literal(&value, scale)),
        ScalarValue::Decimal256(Some(value), _, scale) => Ok(decimal_to_sql_literal(&value, scale)),
        ScalarValue::Int8(Some(value)) => Ok(value.to_string()),
        ScalarValue::Int16(Some(value)) => Ok(value.to_string()),
        ScalarValue::Int32(Some(value)) => Ok(value.to_string()),
        ScalarValue::Int64(Some(value)) => Ok(value.to_string()),
        ScalarValue::UInt8(Some(value)) => Ok(value.to_string()),
        ScalarValue::UInt16(Some(value)) => Ok(value.to_string()),
        ScalarValue::UInt32(Some(value)) => Ok(value.to_string()),
        ScalarValue::UInt64(Some(value)) => Ok(value.to_string()),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => quote_sql_string_literal(&value, column),
        ScalarValue::Binary(Some(value))
        | ScalarValue::BinaryView(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::FixedSizeBinary(_, Some(value)) => Ok(binary_to_sql_literal(&value)),
        ScalarValue::Date32(Some(value)) => date32_to_sql_literal(value, column),
        ScalarValue::Date64(Some(value)) => date64_to_sql_literal(value, column),
        ScalarValue::Time32Second(Some(value)) => {
            time_to_sql_literal(i64::from(value), TimeUnitForSql::Second, column)
        }
        ScalarValue::Time32Millisecond(Some(value)) => {
            time_to_sql_literal(i64::from(value), TimeUnitForSql::Millisecond, column)
        }
        ScalarValue::Time64Microsecond(Some(value)) => {
            time_to_sql_literal(value, TimeUnitForSql::Microsecond, column)
        }
        ScalarValue::Time64Nanosecond(Some(value)) => {
            time_to_sql_literal(value, TimeUnitForSql::Nanosecond, column)
        }
        ScalarValue::TimestampSecond(Some(value), timezone) => {
            timestamp_to_sql_literal(value, TimeUnitForSql::Second, timezone.as_deref(), column)
        }
        ScalarValue::TimestampMillisecond(Some(value), timezone) => timestamp_to_sql_literal(
            value,
            TimeUnitForSql::Millisecond,
            timezone.as_deref(),
            column,
        ),
        ScalarValue::TimestampMicrosecond(Some(value), timezone) => timestamp_to_sql_literal(
            value,
            TimeUnitForSql::Microsecond,
            timezone.as_deref(),
            column,
        ),
        ScalarValue::TimestampNanosecond(Some(value), timezone) => timestamp_to_sql_literal(
            value,
            TimeUnitForSql::Nanosecond,
            timezone.as_deref(),
            column,
        ),
        ScalarValue::Dictionary(_, value) => scalar_to_sql_literal(*value, column),
        other => UnsupportedValueSnafu {
            column: column.to_string(),
            data_type: other.data_type().to_string(),
        }
        .fail(),
    }
}

#[derive(Debug, Clone, Copy)]
enum TimeUnitForSql {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimeUnitForSql {
    fn nanos_per_unit(self) -> i64 {
        match self {
            Self::Second => NANOS_PER_SECOND,
            Self::Millisecond => 1_000_000,
            Self::Microsecond => 1_000,
            Self::Nanosecond => 1,
        }
    }
}

fn finite_float_literal(value: impl Into<f64>, column: &str) -> Result<String> {
    let value = value.into();
    ensure!(
        value.is_finite(),
        InvalidValueSnafu {
            column: column.to_string(),
            reason: "floating-point value is not finite".to_string(),
        }
    );
    Ok(value.to_string())
}

fn decimal_to_sql_literal(value: &impl ToString, scale: i8) -> String {
    let raw = value.to_string();
    let Some(unsigned_digits) = raw.strip_prefix('-') else {
        return unsigned_decimal_to_sql_literal(&raw, scale, "");
    };
    unsigned_decimal_to_sql_literal(unsigned_digits, scale, "-")
}

fn unsigned_decimal_to_sql_literal(digits: &str, scale: i8, sign: &str) -> String {
    if scale == 0 {
        return format!("{sign}{digits}");
    }

    let scale_digits = usize::from(scale.unsigned_abs());
    if scale < 0 {
        return format!("{sign}{digits}{}", "0".repeat(scale_digits));
    }

    if digits.len() <= scale_digits {
        let leading_zeros = "0".repeat(scale_digits - digits.len());
        format!("{sign}0.{leading_zeros}{digits}")
    } else {
        let split_at = digits.len() - scale_digits;
        let (whole, fractional) = digits.split_at(split_at);
        format!("{sign}{whole}.{fractional}")
    }
}

fn binary_to_sql_literal(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut hex = String::with_capacity(value.len() * 2);
    for byte in value {
        hex.push(char::from(HEX[usize::from(byte >> 4)]));
        hex.push(char::from(HEX[usize::from(byte & 0x0F)]));
    }
    format!("TO_BINARY('{hex}', 'HEX')")
}

fn date32_to_sql_literal(days: i32, column: &str) -> Result<String> {
    let epoch = epoch_date(column)?;
    let date = epoch
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .context(InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("date value {days} is out of range"),
        })?;
    Ok(format!("DATE '{}'", date.format("%Y-%m-%d")))
}

fn date64_to_sql_literal(milliseconds: i64, column: &str) -> Result<String> {
    let datetime =
        DateTime::<Utc>::from_timestamp_millis(milliseconds).context(InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("date64 value {milliseconds} is out of range"),
        })?;
    Ok(format!(
        "DATE '{}'",
        datetime.date_naive().format("%Y-%m-%d")
    ))
}

fn epoch_date(column: &str) -> Result<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1).context(InvalidValueSnafu {
        column: column.to_string(),
        reason: "failed to construct Unix epoch date".to_string(),
    })
}

fn time_to_sql_literal(value: i64, unit: TimeUnitForSql, column: &str) -> Result<String> {
    let nanos = value
        .checked_mul(unit.nanos_per_unit())
        .context(InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("time value {value} {unit:?} overflows nanosecond conversion"),
        })?;
    ensure!(
        (0..NANOS_PER_DAY).contains(&nanos),
        InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("time value {value} {unit:?} is outside a single day"),
        }
    );

    let seconds = u32::try_from(nanos / NANOS_PER_SECOND).map_err(|error| {
        InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("failed to convert time seconds: {error}"),
        }
        .build()
    })?;
    let subsecond_nanos = u32::try_from(nanos % NANOS_PER_SECOND).map_err(|error| {
        InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("failed to convert time nanoseconds: {error}"),
        }
        .build()
    })?;

    let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds, subsecond_nanos).context(
        InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("time value {value} {unit:?} is invalid"),
        },
    )?;

    Ok(format!("TIME '{}'", time.format("%H:%M:%S%.f")))
}

fn timestamp_to_sql_literal(
    value: i64,
    unit: TimeUnitForSql,
    timezone: Option<&str>,
    column: &str,
) -> Result<String> {
    let nanos = value
        .checked_mul(unit.nanos_per_unit())
        .context(InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("timestamp value {value} {unit:?} overflows nanosecond conversion"),
        })?;
    let seconds = nanos.div_euclid(NANOS_PER_SECOND);
    let subsecond_nanos = u32::try_from(nanos.rem_euclid(NANOS_PER_SECOND)).map_err(|error| {
        InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("failed to convert timestamp nanoseconds: {error}"),
        }
        .build()
    })?;

    let datetime =
        DateTime::<Utc>::from_timestamp(seconds, subsecond_nanos).context(InvalidValueSnafu {
            column: column.to_string(),
            reason: format!("timestamp value {value} {unit:?} is out of range"),
        })?;

    if timezone.is_some() {
        Ok(format!("TO_TIMESTAMP_TZ('{}')", datetime.to_rfc3339()))
    } else {
        Ok(format!(
            "TO_TIMESTAMP_NTZ('{}')",
            datetime.naive_utc().format("%Y-%m-%d %H:%M:%S%.f")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn quote_identifier_escapes_double_quotes() {
        assert_eq!(
            quote_sql_identifier("a\"b").expect("identifier should be quoted"),
            "\"a\"\"b\""
        );
    }

    #[test]
    fn quote_identifier_rejects_nul() {
        assert!(quote_sql_identifier("bad\0name").is_err());
    }

    #[test]
    fn decimal_literal_preserves_scale() {
        assert_eq!(decimal_to_sql_literal(&12345, 2), "123.45");
        assert_eq!(decimal_to_sql_literal(&-42, 4), "-0.0042");
        assert_eq!(decimal_to_sql_literal(&42, -2), "4200");
    }

    #[test]
    fn scalar_literals_cover_core_types() {
        assert_eq!(
            scalar_to_sql_literal(ScalarValue::Utf8(Some("O'Reilly".to_string())), "name")
                .expect("string should convert"),
            "'O''Reilly'"
        );
        assert_eq!(
            scalar_to_sql_literal(ScalarValue::Binary(Some(vec![0, 10, 255])), "payload")
                .expect("binary should convert"),
            "TO_BINARY('000AFF', 'HEX')"
        );
        assert_eq!(
            scalar_to_sql_literal(ScalarValue::Date32(Some(0)), "created_at")
                .expect("date should convert"),
            "DATE '1970-01-01'"
        );
        assert_eq!(
            scalar_to_sql_literal(
                ScalarValue::Time64Nanosecond(Some(3_723_000_000_000)),
                "time"
            )
            .expect("time should convert"),
            "TIME '01:02:03'"
        );
        assert_eq!(
            scalar_to_sql_literal(
                ScalarValue::TimestampNanosecond(Some(1_000_000_001), None),
                "ts"
            )
            .expect("timestamp should convert"),
            "TO_TIMESTAMP_NTZ('1970-01-01 00:00:01.000000001')"
        );
    }

    #[test]
    fn scalar_literal_rejects_non_finite_float() {
        assert!(scalar_to_sql_literal(ScalarValue::Float64(Some(f64::NAN)), "value").is_err());
    }

    #[test]
    fn extracts_count_from_json_shapes() {
        let response = serde_json::json!([["3"]]);
        assert_eq!(extract_count_from_json_value(&response), Some(3));

        let response = serde_json::json!({ "number of rows inserted": 12 });
        assert_eq!(extract_count_from_json_value(&response), Some(12));
    }

    #[test]
    fn builds_multi_row_values_sql() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .expect("record batch should be valid");

        assert_eq!(
            values_sql_for_rows(&batch, &schema, 0, 2).expect("values SQL should build"),
            "(1, 'a'), (2, NULL)"
        );
    }
}
