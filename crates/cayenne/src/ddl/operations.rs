/*
Copyright 2026, Spice AI, Inc.

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

//! Core Cayenne DDL operations: `create_table`, `drop_table`, `create_schema`.
//!
//! These functions contain the single-node logic shared by both the simple
//! cayenne physical exec and the runtime's broadcast exec.  The broadcast exec
//! calls these functions and then adds distributed steps (executor forwarding,
//! partition metadata init, LIKE assignment copy).

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::catalog::SchemaProvider;
use datafusion::common::ToDFSchema;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::ExprSchemable;
use datafusion::prelude::{Expr, SessionContext};
use datafusion::sql::TableReference;
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::provider::PartitionTableProvider;

use crate::catalog::CatalogError;
use crate::catalog_provider::{CayenneCatalogProvider, CayenneSchemaProvider};
use crate::metadata::CreateTableOptions;
use crate::partition_creator::CayennePartitionCreator;
use crate::schema::transform_schema_for_vortex;
use crate::{CayenneTableProviderBuilder, MetadataCatalog};

// ── Output types ──────────────────────────────────────────────────────────────

/// Outcome of a `CREATE TABLE` operation.
#[derive(Debug)]
pub struct CreateTableOutcome {
    /// Human-readable result message returned as the DDL result batch.
    pub message: String,
}

/// Outcome of a `DROP TABLE` operation.
#[derive(Debug)]
pub struct DropTableOutcome {
    /// Human-readable result message.
    pub message: String,
}

// ── Parameter types ───────────────────────────────────────────────────────────

/// Parameters for [`create_table`].
pub struct CreateTableParams {
    /// Unqualified table name.
    pub table_name: String,
    /// `DataFusion` schema name.
    pub schema_name: String,
    /// `DataFusion` catalog name.
    pub catalog_name: String,
    /// Arrow schema for the new table.
    pub arrow_schema: Arc<Schema>,
    /// Primary key column names (empty if no primary key).
    pub primary_key: Vec<String>,
    /// Raw SQL text for the `PARTITION BY` expression.
    /// Parsed and validated at execution time inside [`CayenneCreateTableExec`].
    pub partition_expr_sql: Option<String>,
    /// If `true`, do not error when the table already exists.
    pub if_not_exists: bool,
    /// Source table for `CREATE TABLE … (LIKE …)`.
    pub like_source_table: Option<TableReference>,
    /// `SessionContext` used to parse the partition expression at execution time.
    pub ctx: Option<Arc<SessionContext>>,
}

// ── Operations ────────────────────────────────────────────────────────────────

/// Register a new table in the Cayenne metadata catalog and `DataFusion` schema provider.
///
/// Handles both partitioned and non-partitioned tables, and the `CREATE TABLE ... LIKE`
/// path.  Does **not** forward DDL to executor nodes or initialise partition metadata —
/// those are distribution concerns handled by the runtime's broadcast exec.
///
/// # Errors
///
/// Returns an error if the table already exists (and `if_not_exists` is `false`), if
/// schema transformation fails, or if the metadata catalog rejects the creation.
pub async fn create_table(
    params: CreateTableParams,
    cayenne_provider: &CayenneCatalogProvider,
    runtime_env: Arc<RuntimeEnv>,
) -> DFResult<CreateTableOutcome> {
    let metadata_catalog = Arc::clone(cayenne_provider.metadata_catalog());
    let data_base_path = cayenne_provider.data_base_path().to_string();
    let vortex_config = cayenne_provider.vortex_config().clone();

    let metadata_table_name = format!("{}/{}", params.schema_name, params.table_name);

    // ── Existence check ───────────────────────────────────────────────────────
    let exists = match metadata_catalog.get_table(&metadata_table_name).await {
        Ok(_) => true,
        Err(CatalogError::TableNotFound { .. }) => false,
        Err(e) => return Err(DataFusionError::External(Box::new(e))),
    };
    if exists {
        if params.if_not_exists {
            // Re-register in the in-memory schema provider if absent (e.g. after restart).
            let schema_provider = ensure_schema_provider(
                cayenne_provider,
                &params.schema_name,
                &metadata_catalog,
                &runtime_env,
            )?;
            if !schema_provider.table_exist(&params.table_name) {
                let builder = CayenneTableProviderBuilder::new(
                    Arc::clone(&metadata_catalog),
                    Arc::clone(&runtime_env),
                );
                if let Ok(provider) = builder.open(&metadata_table_name).await {
                    let wrapped: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(provider);
                    if let Err(e) =
                        schema_provider.register_table(params.table_name.clone(), wrapped)
                    {
                        tracing::error!(
                            table_name = %params.table_name,
                            error = %e,
                            "Failed to register existing Cayenne table in schema provider"
                        );
                    }
                }
            }
            return Ok(CreateTableOutcome {
                message: format!("Table '{}' already exists", params.table_name),
            });
        }
        return Err(DataFusionError::Execution(format!(
            "Table '{}' already exists in catalog '{}'",
            params.table_name, params.catalog_name
        )));
    }

    // ── Schema transformation ─────────────────────────────────────────────────
    let vortex_schema =
        transform_schema_for_vortex(&params.arrow_schema, UnsupportedTypeAction::Error).map_err(
            |e| DataFusionError::Execution(format!("Failed to transform schema for Vortex: {e}")),
        )?;

    let table_data_path = format!(
        "{}/{}/",
        data_base_path.trim_end_matches('/'),
        metadata_table_name
    );
    let vortex_schema = Arc::new(vortex_schema);
    let on_conflict = if params.primary_key.is_empty() {
        None
    } else {
        Some(OnConflict::Upsert(ColumnReference::new(
            params.primary_key.clone(),
        )))
    };

    // Derive partition label from the SQL expression string (needed for metadata storage).
    let partition_label = params
        .partition_expr_sql
        .as_ref()
        .map(|sql| parse_label_from_sql(sql).unwrap_or_else(|| "expr0".to_string()));

    // ── Metadata catalog registration ─────────────────────────────────────────
    let create_options = CreateTableOptions {
        table_name: metadata_table_name.clone(),
        schema: Arc::clone(&vortex_schema),
        primary_key: params.primary_key.clone(),
        on_conflict: on_conflict.clone(),
        base_path: table_data_path.clone(),
        partition_column: partition_label.clone(),
        vortex_config: vortex_config.clone(),
    };
    let table_id = metadata_catalog
        .create_table(create_options)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create Cayenne table '{}': {e}",
                params.table_name
            ))
        })?;

    // ── Build the table provider ──────────────────────────────────────────────
    let wrapped_provider: Arc<dyn datafusion::catalog::TableProvider> = if let Some(ref expr_sql) =
        params.partition_expr_sql
    {
        // LIKE path: partition_expr was not pre-parsed at plan time.
        let ctx_ref = params.ctx.as_ref().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "SessionContext required to parse partition expression '{expr_sql}' for LIKE table '{}'",
                    params.table_name
                ))
            })?;
        let df_schema = vortex_schema.as_ref().clone().to_dfschema()?;
        let parsed_expr = ctx_ref.parse_sql_expr(expr_sql, &df_schema).map_err(|e| {
            DataFusionError::Execution(format!(
                "Invalid PARTITION BY expression '{expr_sql}' for table '{}': {e}",
                params.table_name
            ))
        })?;
        parsed_expr.to_field(&df_schema).map_err(|e| {
            DataFusionError::Execution(format!("Invalid PARTITION BY expression '{expr_sql}': {e}"))
        })?;
        let label = partition_label
            .clone()
            .unwrap_or_else(|| partition_label_for_expr(&parsed_expr));
        build_partitioned_provider(
            &params.table_name,
            &metadata_table_name,
            &table_data_path,
            &parsed_expr,
            Some(&label),
            params.partition_expr_sql.as_ref(),
            &vortex_schema,
            &metadata_catalog,
            &table_id,
            &params.primary_key,
            on_conflict.clone(),
            &vortex_config,
            &runtime_env,
        )
        .await?
    } else {
        let builder = CayenneTableProviderBuilder::new(
            Arc::clone(&metadata_catalog),
            Arc::clone(&runtime_env),
        );
        let provider = builder.open(&metadata_table_name).await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to open Cayenne table '{}': {e}",
                params.table_name
            ))
        })?;
        Arc::new(provider) as Arc<dyn datafusion::catalog::TableProvider>
    };

    // ── Register in DataFusion schema provider ────────────────────────────────
    let schema_provider = ensure_schema_provider(
        cayenne_provider,
        &params.schema_name,
        &metadata_catalog,
        &runtime_env,
    )?;
    schema_provider.register_table(params.table_name.clone(), wrapped_provider)?;

    Ok(CreateTableOutcome {
        message: format!("Table '{}' created", params.table_name),
    })
}

/// Remove a table from the Cayenne metadata catalog and `DataFusion` schema provider.
///
/// Does **not** forward the DROP to executor nodes — that is a distribution concern
/// handled by the runtime's broadcast exec.
///
/// # Errors
///
/// Returns an error if the table does not exist (and `if_exists` is `false`), or if
/// the metadata catalog operation fails.
pub async fn drop_table(
    table_name: &str,
    schema_name: &str,
    catalog_name: &str,
    if_exists: bool,
    cayenne_provider: &CayenneCatalogProvider,
    df_catalog: &Arc<dyn datafusion::catalog::CatalogProvider>,
) -> DFResult<DropTableOutcome> {
    let metadata_catalog = Arc::clone(cayenne_provider.metadata_catalog());
    let metadata_table_name = format!("{schema_name}/{table_name}");

    let was_dropped = metadata_catalog
        .drop_table(&metadata_table_name)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to drop Cayenne table '{table_name}': {e}"))
        })?;

    if !was_dropped {
        if if_exists {
            return Ok(DropTableOutcome {
                message: format!("Table '{table_name}' does not exist"),
            });
        }
        return Err(DataFusionError::Execution(format!(
            "Table '{table_name}' does not exist in catalog '{catalog_name}'"
        )));
    }

    if let Some(schema_provider) = df_catalog.schema(schema_name)
        && let Err(err) = schema_provider.deregister_table(table_name)
    {
        tracing::error!(
            table_name,
            error = %err,
            "Failed to deregister Cayenne table from DataFusion schema provider"
        );
    }

    Ok(DropTableOutcome {
        message: format!("Table '{table_name}' dropped"),
    })
}

/// Create a new schema namespace in the Cayenne catalog.
///
/// # Errors
///
/// Returns an error if the schema already exists (and `if_not_exists` is `false`).
pub fn create_schema(
    schema_name: &str,
    catalog_name: &str,
    if_not_exists: bool,
    cayenne_provider: &CayenneCatalogProvider,
    runtime_env: Arc<RuntimeEnv>,
) -> DFResult<String> {
    if cayenne_provider.schema_provider(schema_name).is_some() {
        if if_not_exists {
            return Ok(format!("Schema '{schema_name}' already exists"));
        }
        return Err(DataFusionError::Execution(format!(
            "Schema '{schema_name}' already exists in catalog '{catalog_name}'"
        )));
    }

    let schema_provider = Arc::new(CayenneSchemaProvider::new_empty(
        Arc::clone(cayenne_provider.metadata_catalog()),
        schema_name.to_string(),
        runtime_env,
    ));
    cayenne_provider
        .register_schema_provider(
            schema_name,
            Arc::clone(&schema_provider) as Arc<dyn SchemaProvider>,
        )
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create schema '{schema_name}' in catalog '{catalog_name}': {e}"
            ))
        })?;

    Ok(format!("Schema '{schema_name}' created"))
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Build or fetch the `CayenneSchemaProvider` for `schema_name`, creating it on demand.
fn ensure_schema_provider(
    cayenne_provider: &CayenneCatalogProvider,
    schema_name: &str,
    metadata_catalog: &Arc<dyn MetadataCatalog>,
    runtime_env: &Arc<RuntimeEnv>,
) -> DFResult<Arc<dyn SchemaProvider>> {
    if let Some(s) = cayenne_provider.schema_provider(schema_name) {
        return Ok(s);
    }
    let new_schema = Arc::new(CayenneSchemaProvider::new_empty(
        Arc::clone(metadata_catalog),
        schema_name.to_string(),
        Arc::clone(runtime_env),
    ));
    cayenne_provider
        .register_schema_provider(
            schema_name,
            Arc::clone(&new_schema) as Arc<dyn SchemaProvider>,
        )
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to create schema '{schema_name}': {e}"))
        })?;
    Ok(Arc::clone(&new_schema) as Arc<dyn SchemaProvider>)
}

/// Build a partitioned table provider wrapping `CayennePartitionCreator` +
/// `PartitionTableProvider`.
#[expect(clippy::too_many_arguments)]
async fn build_partitioned_provider(
    table_name: &str,
    metadata_table_name: &str,
    table_data_path: &str,
    partition_expr: &Expr,
    partition_name: Option<&str>,
    partition_expr_sql: Option<&String>,
    vortex_schema: &Arc<Schema>,
    metadata_catalog: &Arc<dyn MetadataCatalog>,
    table_id: &str,
    primary_key: &[String],
    on_conflict: Option<OnConflict>,
    vortex_config: &crate::metadata::VortexConfig,
    runtime_env: &Arc<RuntimeEnv>,
) -> DFResult<Arc<dyn datafusion::catalog::TableProvider>> {
    let df_schema = vortex_schema.as_ref().clone().to_dfschema()?;
    let partition_expr_for_error =
        partition_expr_sql.map_or_else(|| partition_expr.to_string(), String::clone);

    tracing::info!(
        table = %table_name,
        partition_expr = %partition_expr_for_error,
        "CayenneCreateTableExec: validating partition expression"
    );
    partition_expr.to_field(&df_schema).map_err(|e| {
        DataFusionError::Execution(format!(
            "Invalid PARTITION BY expression '{partition_expr_for_error}': {e}"
        ))
    })?;

    let pname =
        partition_name.map_or_else(|| partition_label_for_expr(partition_expr), str::to_string);
    let partition_by = vec![PartitionedBy {
        name: pname,
        expression: partition_expr.clone(),
    }];

    let creator = Arc::new(CayennePartitionCreator::new(
        metadata_table_name.to_string(),
        PathBuf::from(table_data_path),
        partition_by.clone(),
        Arc::clone(vortex_schema),
        Arc::clone(metadata_catalog),
        table_id.to_string(),
        UnsupportedTypeAction::Error,
        Vec::new(),
        None,
        vortex_config.clone(),
        None,
        primary_key.to_vec(),
        on_conflict,
        Arc::clone(runtime_env),
    ));

    let partition_provider =
        PartitionTableProvider::new(creator, partition_by, Arc::clone(vortex_schema))
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to create partitioned table '{table_name}': {e}"
                ))
            })?;

    Ok(Arc::new(partition_provider) as Arc<dyn datafusion::catalog::TableProvider>)
}

/// Build a filesystem-safe label for a partition expression.
///
/// Column expressions use the column name; non-column expressions fall back to `"expr0"`.
#[must_use]
pub fn partition_label_for_expr(partition_expr: &Expr) -> String {
    let candidate = match partition_expr {
        Expr::Column(col) => col.name.as_str(),
        _ => "expr0",
    };
    let sanitized: String = candidate
        .chars()
        .map(|c: char| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        return "expr0".to_string();
    }
    sanitized
}

/// Parse a simple identifier from a SQL expression string for use as a partition label.
fn parse_label_from_sql(sql: &str) -> Option<String> {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql).ok()?;
    if let Ok(datafusion::sql::sqlparser::ast::Expr::Identifier(ident)) = parser.parse_expr() {
        Some(ident.value)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use tempfile::TempDir;

    use crate::CayenneCatalog;
    use crate::metadata::VortexConfig;

    /// End-to-end on local FS: a Cayenne table partitioned by a user-supplied
    /// `date_trunc` EXPRESSION over a Timestamp column (explicit `partition_by`
    /// with the user's own `date_trunc` — the supported time-bucketing path)
    /// must route each row to its `ts_day_bucket=<micros>/` partition and read
    /// every row back. This is the path the prior cayenne partition tests don't
    /// cover (they use string/Int column partitions, not an expression over
    /// timestamps); it directly exercises per-partition staging-write → move →
    /// scan for timestamp buckets on local FS, so no row is stranded in
    /// `_staging/` or lost, and bucket dir names stay filesystem-safe.
    #[tokio::test]
    async fn test_date_trunc_expression_partition_round_trips_on_local_fs() {
        let tmp = TempDir::new().expect("tempdir");
        let table_data_path = format!("{}/test/buckets/", tmp.path().to_string_lossy());
        let db_path = tmp.path().join("meta.db");

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("payload", DataType::Utf8, false),
        ]);
        let vortex_schema = Arc::new(
            transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
                .expect("schema transforms for vortex"),
        );

        let metadata_catalog: Arc<dyn MetadataCatalog> = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}", db_path.display())).expect("catalog opens"),
        );
        metadata_catalog.init().await.expect("catalog schema initializes");

        let metadata_table_name = "test/buckets".to_string();
        let create_options = CreateTableOptions {
            table_name: metadata_table_name.clone(),
            schema: Arc::clone(&vortex_schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: table_data_path.clone(),
            partition_column: Some("ts_day_bucket".to_string()),
            vortex_config: VortexConfig::default(),
        };
        let table_id = metadata_catalog
            .create_table(create_options)
            .await
            .expect("catalog create_table");

        let ctx = SessionContext::new();
        let df_schema = vortex_schema.as_ref().clone().to_dfschema().expect("df schema");
        let expr_sql = "date_trunc('day', ts)".to_string();
        let parsed_expr = ctx
            .parse_sql_expr(&expr_sql, &df_schema)
            .expect("date_trunc partition expression parses");
        let runtime_env = ctx.runtime_env();

        let provider = build_partitioned_provider(
            &metadata_table_name,
            &metadata_table_name,
            &table_data_path,
            &parsed_expr,
            Some("ts_day_bucket"),
            Some(&expr_sql),
            &vortex_schema,
            &metadata_catalog,
            &table_id,
            &[],
            None,
            &VortexConfig::default(),
            &runtime_env,
        )
        .await
        .expect("partitioned provider builds");

        ctx.register_table("buckets", provider)
            .expect("register partitioned table");

        // 4 rows across 3 distinct days (ids 1 & 4 share day 2025-01-01).
        // Integer micros via arrow_cast → exact Timestamp(Microsecond) values.
        ctx.sql(
            "INSERT INTO buckets VALUES \
             (1, arrow_cast(1735693200000000, 'Timestamp(Microsecond, None)'), 'a'), \
             (2, arrow_cast(1735783200000000, 'Timestamp(Microsecond, None)'), 'b'), \
             (3, arrow_cast(1735862400000000, 'Timestamp(Microsecond, None)'), 'c'), \
             (4, arrow_cast(1735689605000000, 'Timestamp(Microsecond, None)'), 'd')",
        )
        .await
        .expect("insert plans")
        .collect()
        .await
        .expect("insert executes");

        // Every row must round-trip — nothing stranded in `_staging/` or lost.
        let results = ctx
            .sql("SELECT id, payload FROM buckets ORDER BY id")
            .await
            .expect("select plans")
            .collect()
            .await
            .expect("select executes");
        let total: usize = results.iter().map(arrow::array::RecordBatch::num_rows).sum();
        assert_eq!(total, 4, "all 4 rows must round-trip across the day buckets");

        // Exactly 3 `ts_day_bucket=<micros>/` partition dirs, all FS-safe.
        let mut bucket_dirs = 0usize;
        for entry in std::fs::read_dir(&table_data_path).expect("read table dir") {
            let entry = entry.expect("dir entry");
            let name = entry.file_name().to_string_lossy().to_string();
            if entry.file_type().expect("file type").is_dir()
                && let Some(value) = name.strip_prefix("ts_day_bucket=")
            {
                bucket_dirs += 1;
                assert!(
                    value.chars().all(|c| c.is_ascii_digit()),
                    "bucket dir must be a filesystem-safe integer, got {name}"
                );
            }
        }
        assert_eq!(bucket_dirs, 3, "expected exactly 3 day-bucket partition dirs");
    }
}
