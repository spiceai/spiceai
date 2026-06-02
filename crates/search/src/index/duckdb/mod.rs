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

use std::{
    any::Any,
    sync::{Arc, Mutex, OnceLock},
};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::DefaultTableSource,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::LogicalPlan,
};
use datafusion_expr::LogicalPlanBuilder;
use datafusion_table_providers::{
    duckdb::{DuckDB, RelationName, TableDefinition},
    sql::db_connection_pool::{
        dbconnection::duckdbconn::DuckDbConnection, duckdbpool::DuckDbConnectionPool,
    },
};
use futures::future::try_join_all;
use llms::embeddings::Embed;
use runtime_datafusion_index::Index;
use snafu::ResultExt;

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    index::{SearchIndex, VectorIndex, duckdb::write::write_embeddings, embedding_col},
};

pub mod hnsw;
pub mod metric;
mod query_exec;
mod query_table;
mod sql;
mod write;

pub use hnsw::DuckDBHnswOptions;
pub use metric::DuckDBDistanceMetric;

use query_table::DuckDBVectorQueryTable;

static VSS_INSTALLED: OnceLock<()> = OnceLock::new();
static VSS_INSTALL_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone)]
pub struct DuckDBVectorQueryContext {
    pub pool: Arc<DuckDbConnectionPool>,
    pub table_definition: Arc<TableDefinition>,
}

#[derive(Debug, Clone)]
pub struct DuckDBVectorIndex {
    pub embedded_column: String,
    pub primary_key: Vec<Field>,
    pub compute_query: Arc<dyn Embed>,
    pub dims: i32,
    pub source_schema: SchemaRef,
    pub hnsw: DuckDBHnswOptions,
    pub query_context: Option<DuckDBVectorQueryContext>,
}

impl DuckDBVectorIndex {
    #[must_use]
    pub fn new(
        embedded_column: String,
        primary_key: Vec<Field>,
        compute_query: Arc<dyn Embed>,
        dims: i32,
        source_schema: SchemaRef,
        hnsw: DuckDBHnswOptions,
    ) -> Self {
        Self {
            embedded_column,
            primary_key,
            compute_query,
            dims,
            source_schema,
            hnsw,
            query_context: None,
        }
    }

    #[must_use]
    pub fn with_query_context(
        mut self,
        pool: &Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
    ) -> Self {
        // Clone the pool struct (cheap: the underlying r2d2 pool is Arc-shared) and inject
        // "LOAD vss" as a connection setup query so it runs exactly once per connection
        // acquisition rather than once per query.
        let pool_with_vss = Arc::new(
            (**pool)
                .clone()
                .with_connection_setup_queries(vec![Arc::from("LOAD vss")]),
        );
        self.query_context = Some(DuckDBVectorQueryContext {
            pool: pool_with_vss,
            table_definition,
        });
        self
    }

    /// Creates (or no-ops if already present) the HNSW index for this vector column on
    /// the given DuckDB table. Loads and installs VSS as needed.
    fn create_hnsw_index_on_table(
        &self,
        table_name: &str,
        conn: &DuckDbConnection,
    ) -> DataFusionResult<()> {
        let embedding_column = embedding_col(&self.embedded_column);
        install_vss_once(conn)?;
        conn.conn
            .execute("LOAD vss", [])
            .map_err(to_execution_error)?;
        conn.conn
            .execute("SET hnsw_enable_experimental_persistence = true", [])
            .map_err(to_execution_error)?;
        let index_name = DuckDBHnswOptions::index_name_for(table_name, &embedding_column);
        let create_sql = self
            .hnsw
            .create_index_sql(table_name, &embedding_column, &index_name);
        conn.conn
            .execute(&create_sql, [])
            .map_err(to_execution_error)?;

        tracing::debug!(
            table = %table_name,
            index = %index_name,
            column = %embedding_column,
            sql = %create_sql,
            "HNSW index created successfully"
        );

        Ok(())
    }

    fn query_result_schema(&self) -> Result<SchemaRef, DataFusionError> {
        if self
            .source_schema
            .column_with_name(SEARCH_SCORE_COLUMN_NAME)
            .is_some()
        {
            return Err(DataFusionError::Plan(format!(
                "DuckDB vector search cannot expose score column '{SEARCH_SCORE_COLUMN_NAME}' because the source table already has a column with that name."
            )));
        }

        let mut fields = self.source_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            DataType::Float64,
            false,
        )));
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            self.source_schema.metadata().clone(),
        )))
    }
}

#[async_trait]
impl SearchIndex for DuckDBVectorIndex {
    fn search_column(&self) -> String {
        self.embedded_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_key.clone()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        write_embeddings(self, record).await.boxed()
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let Some(query_context) = self.query_context.clone() else {
            return Err(DataFusionError::Plan(
                "DuckDB vector search requires a DuckDB-accelerated table. Configure the dataset with `acceleration.engine: duckdb`.".to_string(),
            ));
        };
        let schema = self.query_result_schema()?;
        let query_table = DuckDBVectorQueryTable {
            query_text: query.to_string(),
            embedded_column: embedding_col(&self.embedded_column),
            compute_query: Arc::clone(&self.compute_query),
            dims: self.dims,
            schema,
            hnsw: self.hnsw.clone(),
            context: query_context,
        };
        let table = Arc::new(query_table) as Arc<dyn datafusion::catalog::TableProvider>;

        Ok(
            LogicalPlanBuilder::scan("tbl", Arc::new(DefaultTableSource::new(table)), None)?
                .build()?
                .into(),
        )
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(Arc::clone(&self) as Arc<dyn VectorIndex>)
    }
}

impl VectorIndex for DuckDBVectorIndex {
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "DuckDBVectorIndex does not maintain a separate vector store — \
             embeddings are written into the DuckDB-accelerated table directly"
                .to_string(),
        ))
    }

    fn dimension(&self) -> i32 {
        self.dims
    }
}

#[async_trait]
impl Index for DuckDBVectorIndex {
    fn name(&self) -> &'static str {
        "duckdb_vector_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        let mut columns: Vec<_> = self.primary_key.iter().map(|f| f.name().clone()).collect();
        if !columns.contains(&self.embedded_column) {
            columns.push(self.embedded_column.clone());
        }
        columns
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let futs = batches
            .into_iter()
            .map(|rb| async { self.write(rb).await.map_err(DataFusionError::External) });
        try_join_all(futs).await
    }

    /// Creates (or verifies existence of) the HNSW index on the current underlying table
    /// after a full refresh completes. For CDC/append datasets the index is created once at
    /// init time; DuckDB VSS maintains it automatically on subsequent inserts.
    async fn on_write_complete(&self) -> DataFusionResult<()> {
        let Some(ctx) = &self.query_context else {
            tracing::debug!(
                column = %self.embedded_column,
                "on_write_complete skipped: no query context for HNSW index"
            );
            return Ok(());
        };
        let index = self.clone();
        let ctx = ctx.clone();
        tokio::task::spawn_blocking(move || {
            let mut db_conn = Arc::clone(&ctx.pool)
                .connect_sync()
                .map_err(to_execution_error)?;
            let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_execution_error)?;
            let table_name = resolve_current_table_name(ctx.table_definition.name(), duckdb_conn)?;
            index.create_hnsw_index_on_table(&table_name, duckdb_conn)
        })
        .await
        .map_err(|e| DataFusionError::Execution(format!("HNSW index creation task failed: {e}")))?
    }
}

// ---------------------------------------------------------------------------
// Shared utilities
// ---------------------------------------------------------------------------

fn install_vss_once(conn: &DuckDbConnection) -> DataFusionResult<()> {
    if VSS_INSTALLED.get().is_some() {
        return Ok(());
    }

    let _install_guard = VSS_INSTALL_LOCK.lock().map_err(|error| {
        DataFusionError::Execution(format!("Failed to lock DuckDB VSS install guard: {error}"))
    })?;
    if VSS_INSTALLED.get().is_none() {
        conn.conn
            .execute("INSTALL vss", [])
            .map_err(to_execution_error)?;
        let _ = VSS_INSTALLED.set(());
    }

    Ok(())
}

pub(super) fn resolve_current_table_name(
    table_rel_name: &RelationName,
    conn: &DuckDbConnection,
) -> DataFusionResult<String> {
    let definition_name = table_rel_name.to_string();
    let prefix = format!("__data_{definition_name}_");
    let mut stmt = conn
        .conn
        .prepare("SELECT table_name FROM duckdb_tables() WHERE starts_with(table_name, ?)")
        .map_err(to_execution_error)?;
    let rows = stmt
        .query_map([prefix], |row| row.get::<usize, String>(0))
        .map_err(to_execution_error)?;

    let mut internal_tables = Vec::new();
    for row in rows {
        let table_name = row.map_err(to_execution_error)?;
        if let Some(timestamp) = internal_table_timestamp(&table_name, &definition_name) {
            internal_tables.push((table_name, timestamp));
        }
    }
    internal_tables.sort_by_key(|(_, timestamp)| *timestamp);

    Ok(internal_tables
        .pop()
        .map_or(definition_name, |(table_name, _)| table_name))
}

fn internal_table_timestamp(table_name: &str, definition_name: &str) -> Option<u64> {
    let inner_name = table_name.strip_prefix("__data_")?;
    let (name, timestamp) = inner_name.rsplit_once('_')?;
    if name == definition_name {
        timestamp.parse().ok()
    } else {
        None
    }
}

pub(super) fn vector_literal(vector: &[f32], dims: i32) -> DataFusionResult<String> {
    validate_vector(vector, dims, "query")?;
    let values = vector
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("[{values}]::FLOAT[{dims}]"))
}

#[expect(clippy::cast_sign_loss)]
pub(super) fn validate_vector(vector: &[f32], dims: i32, context: &str) -> DataFusionResult<()> {
    if dims <= 0 {
        return Err(DataFusionError::Execution(format!(
            "DuckDB vector {context} dimension must be positive, got {dims}."
        )));
    }
    let expected = dims as usize;
    if vector.len() != expected {
        return Err(DataFusionError::Execution(format!(
            "DuckDB vector {context} dimension mismatch: expected {expected}, got {}.",
            vector.len()
        )));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(DataFusionError::Execution(format!(
            "DuckDB vector {context} contains a non-finite value."
        )));
    }
    Ok(())
}

pub(super) fn to_execution_error(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}
