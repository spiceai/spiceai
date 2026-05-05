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
    collections::HashSet,
    sync::{Arc, Mutex, OnceLock},
};

use arrow::array::{
    Array, FixedSizeListBuilder, Float32Builder, LargeStringArray, RecordBatch, RecordBatchOptions,
    StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::utils::quote_identifier,
    datasource::{DefaultTableSource, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{LogicalPlan, TableProviderFilterPushDown},
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
};
use datafusion_expr::LogicalPlanBuilder;
use datafusion_table_providers::{
    duckdb::{DuckDB, TableDefinition},
    sql::{
        db_connection_pool::duckdbpool::DuckDbConnectionPool,
        sql_provider_datafusion::expr::{self, Engine},
    },
};
use futures::{TryStreamExt, future::try_join_all};
use llms::embeddings::{Embed, EmbeddingInput};
use runtime_datafusion_index::Index;
use snafu::{ResultExt, Snafu};
use util::{convert_string_arrow_to_iterator, distribute_nulls};

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    index::{SearchIndex, VectorIndex, embedding_col},
};

static VSS_INSTALLED: OnceLock<()> = OnceLock::new();
static VSS_INSTALL_LOCK: Mutex<()> = Mutex::new(());
const DEFAULT_DUCKDB_VECTOR_SEARCH_LIMIT: usize = 1000;
const EMPTY_PROJECTION_ROW_COLUMN: &str = "__spice_empty_projection_row";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuckDBDistanceMetric {
    Cosine,
    L2,
    InnerProduct,
}

impl DuckDBDistanceMetric {
    #[must_use]
    pub fn duckdb_hnsw_metric(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::L2 => "l2sq",
            Self::InnerProduct => "ip",
        }
    }

    #[must_use]
    fn distance_expr(self, column: &str, vector_literal: &str) -> String {
        let column = quote_identifier(column);
        match self {
            Self::Cosine => format!("array_cosine_distance({column}, {vector_literal})"),
            Self::L2 => format!("array_distance({column}, {vector_literal})"),
            Self::InnerProduct => {
                format!("array_negative_inner_product({column}, {vector_literal})")
            }
        }
    }

    #[must_use]
    fn score_expr(self, column: &str, vector_literal: &str) -> String {
        let column = quote_identifier(column);
        match self {
            Self::Cosine => format!("1.0 - array_cosine_distance({column}, {vector_literal})"),
            Self::L2 => format!("-array_distance({column}, {vector_literal})"),
            Self::InnerProduct => format!("array_inner_product({column}, {vector_literal})"),
        }
    }

    /// Compute score from a pre-computed distance alias (used in the outer CTE query).
    #[must_use]
    fn cte_score_expr(self, distance_alias: &str) -> String {
        match self {
            Self::Cosine => format!("1.0 - {distance_alias}"),
            Self::L2 | Self::InnerProduct => format!("-{distance_alias}"),
        }
    }
}

impl TryFrom<&str> for DuckDBDistanceMetric {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cosine" => Ok(Self::Cosine),
            "l2" | "l2_norm" | "euclidean" | "l2sq" => Ok(Self::L2),
            "ip" | "inner_product" | "dot" | "dot_product" | "max_inner_product" => {
                Ok(Self::InnerProduct)
            }
            other => Err(format!(
                "Invalid DuckDB vector distance metric '{other}'. Expected one of: cosine | l2 | inner_product."
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDBHnswOptions {
    pub metric: DuckDBDistanceMetric,
    pub hnsw_m: Option<u32>,
    pub hnsw_ef_construction: Option<u32>,
    pub hnsw_ef_search: Option<u32>,
}

impl Default for DuckDBHnswOptions {
    fn default() -> Self {
        Self {
            metric: DuckDBDistanceMetric::Cosine,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
        }
    }
}

impl DuckDBHnswOptions {
    #[must_use]
    pub fn index_name_for(table_name: &str, embedding_column: &str) -> String {
        let mut raw = format!("__spice_vss_{table_name}_{embedding_column}");
        raw.retain(|c| c.is_ascii_alphanumeric() || c == '_');
        if raw.is_empty() {
            "__spice_vss_index".to_string()
        } else {
            raw
        }
    }

    #[must_use]
    pub fn create_index_sql(
        &self,
        table_name: &str,
        embedding_column: &str,
        index_name: &str,
    ) -> String {
        let mut with_options = vec![format!("metric = '{}'", self.metric.duckdb_hnsw_metric())];
        if let Some(m) = self.hnsw_m {
            with_options.push(format!("m = {m}"));
        }
        if let Some(ef) = self.hnsw_ef_construction {
            with_options.push(format!("ef_construction = {ef}"));
        }

        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} USING HNSW ({}) WITH ({})",
            quote_identifier(index_name),
            quote_identifier(table_name),
            quote_identifier(embedding_column),
            with_options.join(", ")
        )
    }
}

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
        pool: Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
    ) -> Self {
        self.query_context = Some(DuckDBVectorQueryContext {
            pool,
            table_definition,
        });
        self
    }

    /// Creates (or no-ops if already present) the HNSW index for this vector column on
    /// the given DuckDB table. Loads and installs VSS as needed.
    fn create_hnsw_index_on_table(
        &self,
        table_name: &str,
        conn: &duckdb::Connection,
    ) -> DataFusionResult<()> {
        let embedding_column = embedding_col(&self.embedded_column);
        install_vss_once(conn)?;
        conn.execute("LOAD vss", []).map_err(to_execution_error)?;
        conn.execute("SET hnsw_enable_experimental_persistence = true", [])
            .map_err(to_execution_error)?;
        let index_name = DuckDBHnswOptions::index_name_for(table_name, &embedding_column);
        let create_sql = self
            .hnsw
            .create_index_sql(table_name, &embedding_column, &index_name);
        conn.execute(&create_sql, []).map_err(to_execution_error)?;

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
        write_embeddings(self, record)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let Some(query_context) = self.query_context.clone() else {
            return Err(DataFusionError::Plan(
                "DuckDB vector search requires a DuckDB-accelerated table. Configure the dataset with `acceleration.engine: duckdb`.".to_string(),
            ));
        };
        let schema = self.query_result_schema()?;
        let table: Arc<dyn TableProvider> = Arc::new(DuckDBVectorQueryTable {
            query_text: query.to_string(),
            embedded_column: embedding_col(&self.embedded_column),
            compute_query: Arc::clone(&self.compute_query),
            dims: self.dims,
            schema,
            hnsw: self.hnsw.clone(),
            context: query_context,
        });

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
            let table_name = resolve_current_table_name(&ctx.table_definition, &duckdb_conn.conn)?;
            index.create_hnsw_index_on_table(&table_name, &duckdb_conn.conn)
        })
        .await
        .map_err(|e| DataFusionError::Execution(format!("HNSW index creation task failed: {e}")))?
    }
}

#[derive(Debug)]
struct DuckDBVectorQueryTable {
    query_text: String,
    embedded_column: String,
    compute_query: Arc<dyn Embed>,
    dims: i32,
    schema: SchemaRef,
    hnsw: DuckDBHnswOptions,
    context: DuckDBVectorQueryContext,
}

#[async_trait]
impl TableProvider for DuckDBVectorQueryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| duckdb_filter_pushdown(&self.schema, filter))
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        let projected_columns = match projection {
            Some(projection) => projection
                .iter()
                .map(|idx| self.schema.field(*idx).name().clone())
                .collect(),
            None => self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Arc::new(DuckDBVectorQueryExec {
            projected_schema,
            projected_columns,
            filters: filters.to_vec(),
            limit,
            query_text: self.query_text.clone(),
            embedded_column: self.embedded_column.clone(),
            compute_query: Arc::clone(&self.compute_query),
            dims: self.dims,
            hnsw: self.hnsw.clone(),
            context: self.context.clone(),
            properties,
        }))
    }
}

#[derive(Debug, Clone)]
struct DuckDBVectorQueryExec {
    projected_schema: SchemaRef,
    projected_columns: Vec<String>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    query_text: String,
    embedded_column: String,
    compute_query: Arc<dyn Embed>,
    dims: i32,
    hnsw: DuckDBHnswOptions,
    context: DuckDBVectorQueryContext,
    properties: PlanProperties,
}

impl DuckDBVectorQueryExec {
    async fn query_vector(&self) -> DataFusionResult<Vec<f32>> {
        let mut vectors = self
            .compute_query
            .embed(EmbeddingInput::String(self.query_text.clone()))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let vector = vectors.pop().ok_or_else(|| {
            DataFusionError::Execution("No embedding vector computed for query".to_string())
        })?;
        validate_vector(&vector, self.dims, "query")?;
        Ok(vector)
    }

    fn sql(&self, table_name: &str, query_vector: &[f32]) -> DataFusionResult<String> {
        let vector_literal = vector_literal(query_vector, self.dims)?;
        duckdb_vector_sql(
            table_name,
            &self.embedded_column,
            &self.projected_columns,
            &self.filters,
            self.limit,
            &self.hnsw,
            &vector_literal,
        )
    }
}

impl DisplayAs for DuckDBVectorQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DuckDBVectorQueryExec embedded_column={} limit={:?}",
            self.embedded_column, self.limit
        )
    }
}

impl ExecutionPlan for DuckDBVectorQueryExec {
    fn name(&self) -> &'static str {
        "DuckDBVectorQueryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let exec = self.clone();
        let schema = self.schema();
        let fut = async move {
            let query_vector = exec.query_vector().await?;
            let context = exec.context.clone();
            let sql_exec = exec.clone();
            let batches = tokio::task::spawn_blocking(move || {
                run_duckdb_vector_query(&context, &sql_exec, &query_vector)
            })
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("DuckDB vector query task failed: {e}"))
            })??;
            let stream =
                futures::stream::iter(batches.into_iter().map(Ok::<RecordBatch, DataFusionError>));
            Ok::<SendableRecordBatchStream, DataFusionError>(Box::pin(
                RecordBatchStreamAdapter::new(schema, stream),
            ))
        };
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn run_duckdb_vector_query(
    context: &DuckDBVectorQueryContext,
    exec: &DuckDBVectorQueryExec,
    query_vector: &[f32],
) -> DataFusionResult<Vec<RecordBatch>> {
    let mut db_conn = Arc::clone(&context.pool)
        .connect_sync()
        .map_err(to_execution_error)?;
    let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_execution_error)?;
    let conn = &duckdb_conn.conn;

    install_vss_once(conn)?;
    conn.execute("LOAD vss", []).map_err(to_execution_error)?;
    match exec.hnsw.hnsw_ef_search {
        Some(ef_search) => conn
            .execute(&format!("SET hnsw_ef_search = {ef_search}"), [])
            .map_err(to_execution_error)?,
        None => conn
            .execute("RESET hnsw_ef_search", [])
            .map_err(to_execution_error)?,
    };

    let table_name = resolve_current_table_name(&context.table_definition, conn)?;

    (|| -> DataFusionResult<Vec<RecordBatch>> {
        let sql = exec.sql(&table_name, query_vector)?;
        tracing::debug!(
            table_name = %table_name,
            embedded_column = %exec.embedded_column,
            query_vector_dimension = query_vector.len(),
            "Executing DuckDB vector query"
        );
        tracing::trace!("DuckDB vector query SQL: {sql}");
        let mut stmt = conn.prepare(&sql).map_err(to_execution_error)?;
        let result = stmt.query_arrow([]).map_err(to_execution_error)?;
        let batches = result.collect::<Vec<_>>();
        if exec.projected_columns.is_empty() {
            batches
                .into_iter()
                .map(|batch| {
                    empty_projected_batch(Arc::clone(&exec.projected_schema), batch.num_rows())
                })
                .collect()
        } else {
            batches
                .into_iter()
                .map(normalize_fixed_size_list_field_names)
                .collect()
        }
    })()
}

fn install_vss_once(conn: &duckdb::Connection) -> DataFusionResult<()> {
    if VSS_INSTALLED.get().is_some() {
        return Ok(());
    }

    let _install_guard = VSS_INSTALL_LOCK.lock().map_err(|error| {
        DataFusionError::Execution(format!("Failed to lock DuckDB VSS install guard: {error}"))
    })?;
    if VSS_INSTALLED.get().is_none() {
        conn.execute("INSTALL vss", [])
            .map_err(to_execution_error)?;
        let _ = VSS_INSTALLED.set(());
    }

    Ok(())
}

fn resolve_current_table_name(
    table_definition: &TableDefinition,
    conn: &duckdb::Connection,
) -> DataFusionResult<String> {
    let definition_name = table_definition.name().to_string();
    let prefix = format!("__data_{definition_name}_");
    let mut stmt = conn
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

/// CTE name used for the inner nearest-neighbor subquery.
const CTE_NAME: &str = "__spice_nn";
/// Alias for the pre-computed distance value inside the CTE.
const CTE_DISTANCE_ALIAS: &str = "__spice_dist";

/// Build vector search SQL for DuckDB.
///
/// When **no filters** are present, uses a CTE that preserves the clean
/// `TopN → Projection → SeqScan` plan shape required by the HNSW optimizer:
/// ```sql
/// WITH __spice_nn AS (
///     SELECT *, distance_func(col, vec) AS __spice_dist
///     FROM table
///     ORDER BY __spice_dist ASC LIMIT k
/// )
/// SELECT col1, col2, CAST(score_from_dist AS DOUBLE) AS _score
/// FROM __spice_nn ORDER BY __spice_dist ASC
/// ```
///
/// When **filters** are present, falls back to a flat query so filters are applied before the distance calculation.
fn duckdb_vector_sql(
    table_name: &str,
    embedding_column: &str,
    projected_columns: &[String],
    filters: &[Expr],
    limit: Option<usize>,
    hnsw: &DuckDBHnswOptions,
    vector_literal: &str,
) -> DataFusionResult<String> {
    let limit = limit.unwrap_or(DEFAULT_DUCKDB_VECTOR_SEARCH_LIMIT);

    if filters.is_empty() {
        // CTE path — activates HNSW index scan
        Ok(duckdb_vector_sql_cte(
            table_name,
            embedding_column,
            projected_columns,
            limit,
            hnsw,
            vector_literal,
        ))
    } else {
        // Flat query path — score calculation
        duckdb_vector_sql_flat(
            table_name,
            embedding_column,
            projected_columns,
            filters,
            limit,
            hnsw,
            vector_literal,
        )
    }
}

/// CTE path: no filters, clean plan shape for HNSW optimizer.
fn duckdb_vector_sql_cte(
    table_name: &str,
    embedding_column: &str,
    projected_columns: &[String],
    limit: usize,
    hnsw: &DuckDBHnswOptions,
    vector_literal: &str,
) -> String {
    let distance_expr = hnsw.metric.distance_expr(embedding_column, vector_literal);
    let score_expr = hnsw.metric.cte_score_expr(CTE_DISTANCE_ALIAS);
    let embedding_not_null = embedding_not_null_predicate(embedding_column);

    let cte = format!(
        "WITH {CTE_NAME} AS (\
         SELECT *, {distance_expr} AS {CTE_DISTANCE_ALIAS} \
         FROM {table} \
         WHERE {embedding_not_null} \
         ORDER BY {CTE_DISTANCE_ALIAS} ASC LIMIT {limit}\
         )",
        table = quote_identifier(table_name),
    );

    let select_exprs = build_select_exprs(projected_columns, &score_expr);

    format!(
        "{cte} SELECT {select_exprs} FROM {CTE_NAME} \
         ORDER BY {CTE_DISTANCE_ALIAS} ASC",
    )
}

/// Flat query path: filters applied via WHERE, no HNSW index (brute-force scan).
fn duckdb_vector_sql_flat(
    table_name: &str,
    embedding_column: &str,
    projected_columns: &[String],
    filters: &[Expr],
    limit: usize,
    hnsw: &DuckDBHnswOptions,
    vector_literal: &str,
) -> DataFusionResult<String> {
    let score_expr = hnsw.metric.score_expr(embedding_column, vector_literal);
    let distance_expr = hnsw.metric.distance_expr(embedding_column, vector_literal);

    let select_exprs = build_select_exprs(projected_columns, &score_expr);

    let filter_exprs: Vec<String> = filters
        .iter()
        .map(|filter| expr::to_sql_with_engine(filter, Some(Engine::DuckDB)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    let mut predicates = Vec::with_capacity(filter_exprs.len() + 1);
    predicates.push(embedding_not_null_predicate(embedding_column));
    predicates.extend(filter_exprs);
    let where_clause = format!(" WHERE {}", predicates.join(" AND "));

    Ok(format!(
        "SELECT {select_exprs} FROM {table}{where_clause} ORDER BY {distance_expr} ASC LIMIT {limit}",
        table = quote_identifier(table_name),
    ))
}

fn embedding_not_null_predicate(embedding_column: &str) -> String {
    format!("{} IS NOT NULL", quote_identifier(embedding_column))
}

/// Build the SELECT expression list, substituting `_score` with the given score expression.
fn build_select_exprs(projected_columns: &[String], score_expr: &str) -> String {
    if projected_columns.is_empty() {
        format!("1 AS {}", quote_identifier(EMPTY_PROJECTION_ROW_COLUMN))
    } else {
        projected_columns
            .iter()
            .map(|column| {
                if column == SEARCH_SCORE_COLUMN_NAME {
                    format!(
                        "CAST({score_expr} AS DOUBLE) AS {}",
                        quote_identifier(SEARCH_SCORE_COLUMN_NAME)
                    )
                } else {
                    quote_identifier(column).to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn duckdb_filter_pushdown(schema: &SchemaRef, filter: &Expr) -> TableProviderFilterPushDown {
    let pushdownable_columns: HashSet<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| *name != SEARCH_SCORE_COLUMN_NAME)
        .collect();

    if !filter
        .column_refs()
        .iter()
        .all(|column| pushdownable_columns.contains(column.name()))
    {
        return TableProviderFilterPushDown::Unsupported;
    }

    match expr::to_sql_with_engine(filter, Some(Engine::DuckDB)) {
        Ok(_) => TableProviderFilterPushDown::Exact,
        Err(_) => TableProviderFilterPushDown::Unsupported,
    }
}

fn empty_projected_batch(schema: SchemaRef, row_count: usize) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    match projection {
        Some(columns) => Ok(Arc::new(schema.project(columns)?)),
        None => Ok(Arc::clone(schema)),
    }
}

fn vector_literal(vector: &[f32], dims: i32) -> DataFusionResult<String> {
    validate_vector(vector, dims, "query")?;
    let values = vector
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("[{values}]::FLOAT[{dims}]"))
}

#[expect(clippy::cast_sign_loss)]
fn validate_vector(vector: &[f32], dims: i32, context: &str) -> DataFusionResult<()> {
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

#[derive(Debug, Snafu)]
enum WriteError {
    #[snafu(display(
        "Failed to compute DuckDB vector embeddings: embedded column '{column}' not found in record batch."
    ))]
    ColumnNotFound { column: String },

    #[snafu(display(
        "Failed to compute DuckDB vector embeddings: embedded column '{column}' has non-string type {data_type}; expected a Utf8/LargeUtf8/Utf8View column."
    ))]
    EmbeddedColumnNotString { column: String, data_type: String },

    #[snafu(display("Failed to compute DuckDB vector embeddings: {source}"))]
    FailedToEmbed { source: llms::embeddings::Error },

    #[snafu(display("Failed to build DuckDB vector embedding column: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display(
        "Failed to build DuckDB vector embedding column: embedding dimension mismatch at row {row_index}: expected {expected}, got {actual}."
    ))]
    EmbeddingDimensionMismatch {
        expected: usize,
        actual: usize,
        row_index: usize,
    },
}

async fn write_embeddings(
    index: &DuckDBVectorIndex,
    record: RecordBatch,
) -> Result<RecordBatch, WriteError> {
    let Some((embedded_column_idx, _)) = record
        .schema()
        .column_with_name(index.embedded_column.as_str())
    else {
        return ColumnNotFoundSnafu {
            column: index.embedded_column.clone(),
        }
        .fail();
    };

    let embedding_vectors = embed_column(
        &record,
        embedded_column_idx,
        index.embedded_column.as_str(),
        Arc::clone(&index.compute_query),
    )
    .await?;

    update_embedding_column_in_batch(
        &record,
        &index.embedded_column,
        &embedding_vectors,
        index.dims,
    )
}

async fn embed_column(
    rb: &RecordBatch,
    column_idx: usize,
    column_name: &str,
    model: Arc<dyn Embed>,
) -> Result<Vec<Option<Vec<f32>>>, WriteError> {
    let column_arr = rb.column(column_idx);
    let iter_opt: Option<Box<dyn Iterator<Item = Option<&str>> + Send>> =
        convert_string_arrow_to_iterator!(column_arr);
    let Some(data) = iter_opt else {
        return EmbeddedColumnNotStringSnafu {
            column: column_name.to_string(),
            data_type: column_arr.data_type().to_string(),
        }
        .fail();
    };

    let mut nulls = Vec::new();
    let mut column = Vec::new();
    for (i, value) in data.enumerate() {
        if value.is_none() || value.is_some_and(str::is_empty) {
            nulls.push(i);
        } else if let Some(s) = value {
            column.push(s.to_string());
        }
    }

    if column.is_empty() {
        return Ok(vec![None; rb.num_rows()]);
    }

    let embedded = model
        .embed(EmbeddingInput::StringArray(column))
        .await
        .context(FailedToEmbedSnafu)?;

    Ok(distribute_nulls(embedded, nulls))
}

fn update_embedding_column_in_batch(
    record: &RecordBatch,
    embedded_column_name: &str,
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<RecordBatch, WriteError> {
    let embedding_column_name = embedding_col(embedded_column_name);
    let schema = record.schema();
    let mut columns = record.columns().to_vec();
    let embedding_array = create_embedding_array(embedding_vectors, dimension)?;

    let target_schema = if let Some((idx, _)) = schema.column_with_name(&embedding_column_name) {
        columns[idx] = embedding_array;
        schema
    } else {
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            &embedding_column_name,
            embedding_array.data_type().clone(),
            true,
        )));
        columns.push(embedding_array);
        Arc::new(Schema::new(fields))
    };

    RecordBatch::try_new(target_schema, columns).context(ArrowSnafu)
}

#[expect(clippy::cast_sign_loss)]
fn create_embedding_array(
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<Arc<dyn Array>, WriteError> {
    let dim = if dimension > 0 {
        dimension
    } else {
        i32::try_from(
            embedding_vectors
                .iter()
                .find_map(|value| value.as_ref().map(Vec::len))
                .unwrap_or(1),
        )
        .unwrap_or(1)
        .max(1)
    };
    let expected = dim as usize;

    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim);
    builder = builder.with_field(Field::new_list_field(DataType::Float32, false));

    for (row, embedding) in embedding_vectors.iter().enumerate() {
        match embedding {
            Some(vector) if vector.len() == expected => {
                builder.values().append_slice(vector);
                builder.append(true);
            }
            Some(vector) => {
                return Err(WriteError::EmbeddingDimensionMismatch {
                    expected,
                    actual: vector.len(),
                    row_index: row,
                });
            }
            None => {
                builder.values().append_nulls(expected);
                builder.append(false);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Re-cast a batch's schema to match `target_schema`.
///
/// DuckDB serializes `FLOAT[N]` columns with an empty inner field name (`field: ''`),
/// whereas Arrow conventionally uses `"item"`. This restores the `"item"` name on any
/// `FixedSizeList` column whose inner field came back with an empty name.
fn normalize_fixed_size_list_field_names(batch: RecordBatch) -> DataFusionResult<RecordBatch> {
    let schema = batch.schema();
    let needs_fix = schema.fields().iter().any(
        |f| matches!(f.data_type(), DataType::FixedSizeList(inner, _) if inner.name().is_empty()),
    );
    if !needs_fix {
        return Ok(batch);
    }

    let new_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::FixedSizeList(inner, size) if inner.name().is_empty() => {
                let fixed_inner = Arc::new(Field::new(
                    "item",
                    inner.data_type().clone(),
                    inner.is_nullable(),
                ));
                Arc::new(Field::new(
                    f.name(),
                    DataType::FixedSizeList(fixed_inner, *size),
                    f.is_nullable(),
                ))
            }
            _ => Arc::clone(f),
        })
        .collect();
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn to_execution_error(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    #[test]
    fn hnsw_create_index_sql_includes_configured_options() {
        let options = DuckDBHnswOptions {
            metric: DuckDBDistanceMetric::L2,
            hnsw_m: Some(24),
            hnsw_ef_construction: Some(96),
            hnsw_ef_search: Some(40),
        };

        assert_eq!(
            options.create_index_sql("docs", "body_embedding", "idx_docs_embedding"),
            "CREATE INDEX IF NOT EXISTS idx_docs_embedding ON docs USING HNSW (body_embedding) WITH (metric = 'l2sq', m = 24, ef_construction = 96)"
        );
    }

    #[test]
    fn duckdb_vector_sql_orders_by_distance_and_projects_score() {
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            &[],
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert_eq!(
            sql,
            "WITH __spice_nn AS (\
             SELECT *, array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS __spice_dist \
             FROM docs \
             WHERE body_embedding IS NOT NULL \
             ORDER BY __spice_dist ASC LIMIT 10\
             ) SELECT id, CAST(1.0 - __spice_dist AS DOUBLE) AS _score \
             FROM __spice_nn \
             ORDER BY __spice_dist ASC"
        );
    }

    #[test]
    fn project_schema_honors_empty_projection() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let projection = Vec::new();

        let projected = project_schema(&schema, Some(&projection)).expect("schema should project");

        assert!(projected.fields().is_empty());
    }

    #[test]
    fn duckdb_vector_sql_handles_empty_projection() {
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &[],
            &[],
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert_eq!(
            sql,
            "WITH __spice_nn AS (\
             SELECT *, array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS __spice_dist \
             FROM docs \
             WHERE body_embedding IS NOT NULL \
             ORDER BY __spice_dist ASC LIMIT 10\
             ) SELECT 1 AS __spice_empty_projection_row \
             FROM __spice_nn \
             ORDER BY __spice_dist ASC"
        );
    }

    #[test]
    fn empty_projected_batch_preserves_row_count() {
        let schema = Arc::new(Schema::empty());
        let batch = empty_projected_batch(schema, 3).expect("batch should build");

        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn duckdb_filter_pushdown_rejects_score_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));
        let filter = col(SEARCH_SCORE_COLUMN_NAME).gt(lit(0.5));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn duckdb_filter_pushdown_allows_base_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));
        let filter = col("id").gt(lit(10_i64));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Exact
        );
    }

    #[test]
    fn duckdb_filter_pushdown_rejects_mixed_score_filter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));
        let filter = col("id")
            .gt(lit(10_i64))
            .and(col(SEARCH_SCORE_COLUMN_NAME).gt(lit(0.5)));

        assert_eq!(
            duckdb_filter_pushdown(&schema, &filter),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn duckdb_vector_sql_applies_default_limit() {
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string()],
            &[],
            None,
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert!(sql.contains("LIMIT 1000"));
    }

    #[test]
    fn duckdb_vector_sql_uses_flat_query_with_filters() {
        let filter = col("id").gt(lit(10_i64));
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            &[filter],
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        // Flat query: no CTE, direct SELECT with WHERE and inline score
        assert_eq!(
            sql,
            "SELECT id, CAST(1.0 - array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS DOUBLE) AS _score \
               FROM docs WHERE body_embedding IS NOT NULL AND \"id\" > 10 \
             ORDER BY array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) ASC LIMIT 10"
        );
    }

    #[test]
    fn duckdb_vector_sql_uses_cte_without_filters() {
        let sql = duckdb_vector_sql(
            "docs",
            "body_embedding",
            &["id".to_string(), SEARCH_SCORE_COLUMN_NAME.to_string()],
            &[],
            Some(10),
            &DuckDBHnswOptions::default(),
            "[1.0, 0.0]::FLOAT[2]",
        )
        .expect("SQL should build");

        assert_eq!(
            sql,
            "WITH __spice_nn AS (\
             SELECT *, array_cosine_distance(body_embedding, [1.0, 0.0]::FLOAT[2]) AS __spice_dist \
             FROM docs \
             WHERE body_embedding IS NOT NULL \
             ORDER BY __spice_dist ASC LIMIT 10\
             ) SELECT id, CAST(1.0 - __spice_dist AS DOUBLE) AS _score \
             FROM __spice_nn \
             ORDER BY __spice_dist ASC"
        );
    }
}
