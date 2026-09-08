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

//! `CREATE TABLE` DDL extension extraction.
//!
//! Intercepts `CREATE TABLE` statements containing `WITH (...)` options
//! (`acceleration.*`, `dataset.*`) and/or `PARTITION BY` clauses. Extracts
//! these extensions from the AST, stores them in the [`DdlExtensionStore`]
//! for the analyzer rule to consume, strips them from the statement, and
//! delegates the cleaned statement to `DataFusion`'s standard planner.

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::{
    ColumnOption, CreateTable, CreateTableOptions, SqlOption, Statement as SQLStatement,
    TableConstraint,
};

use crate::datafusion::cayenne_ddl::is_cayenne_catalog;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use datafusion_ddl::{
    CreateTableStatementExtension, SharedDdlExtensionStore, parse_ddl_table_options,
};

// Returns `true` if the `CREATE TABLE` has extensions we need to intercept:
// `has_ddl_extensions` is re-exported from `datafusion_ddl` via the `use` above.

/// Plan a `CREATE TABLE` with DDL extensions.
///
/// Extracts recognized `WITH` options and `PARTITION BY` from the AST, stores
/// them in the [`DdlExtensionStore`], strips those clauses from the statement,
/// and delegates the cleaned statement to `session.statement_to_plan()`.
///
/// On planning failure, automatically cleans up the store entry.
pub(super) async fn plan_create_table(
    statement: Statement,
    session: &SessionState,
    ddl_store: &SharedDdlExtensionStore,
) -> DFResult<LogicalPlan> {
    // Decompose the Statement to take ownership of the CreateTable AST.
    let Statement::Statement(sql_stmt) = statement else {
        return Err(DataFusionError::Internal(
            "Expected Statement::Statement for CREATE TABLE".to_string(),
        ));
    };
    let SQLStatement::CreateTable(create_table) = *sql_stmt else {
        return Err(DataFusionError::Internal(
            "Expected SQLStatement::CreateTable".to_string(),
        ));
    };

    let (modified_ct, store_key) = extract_and_store_extensions(create_table, ddl_store)?;

    // Wrap the modified CreateTable back into a Statement for DataFusion.
    let modified_stmt = Statement::Statement(Box::new(SQLStatement::CreateTable(modified_ct)));

    match session.statement_to_plan(modified_stmt).await {
        Ok(plan) => Ok(plan),
        Err(e) => {
            // Clean up the store entry if planning fails.
            if let Some(ref key) = store_key
                && let Err(cleanup_err) = cleanup_store_entry(ddl_store, key)
            {
                tracing::warn!(
                    "Failed to clean up DDL extension store entry for {key} \
                     after planning failure: {cleanup_err}"
                );
            }
            Err(e)
        }
    }
}

/// Extract DDL extensions from a `CreateTable` AST, store them, and return
/// the modified `CreateTable` with extensions stripped.
///
/// Returns `(modified_create_table, optional_store_key)`. The store key is
/// `Some` when extensions were inserted and may need cleanup on error.
fn extract_and_store_extensions(
    mut create_table: CreateTable,
    ddl_store: &SharedDdlExtensionStore,
) -> DFResult<(CreateTable, Option<TableReference>)> {
    // Validate partition key constraints before extraction.
    if let Some(ref partition_expr) = create_table.partition_by {
        validate_partition_key_in_primary_key(
            partition_expr,
            &create_table.constraints,
            &create_table.columns,
        )?;
    }

    // Extract WITH options.
    let with_options = match &create_table.table_options {
        CreateTableOptions::With(options) => Some(options.clone()),
        _ => None,
    };

    let partition_by_expr = create_table.partition_by.clone();

    // Build the extension from WITH options.
    let mut extension = if let Some(ref options) = with_options {
        let (recognized, unrecognized) = classify_with_options(options);

        if !recognized.is_empty() && !unrecognized.is_empty() {
            return Err(DataFusionError::Plan(
                "Cannot mix 'acceleration.*' or 'dataset.*' options with other WITH options \
                 in CREATE TABLE. Use only 'acceleration.*' and/or 'dataset.*' options."
                    .to_string(),
            ));
        }

        if recognized.is_empty() {
            CreateTableStatementExtension::default()
        } else {
            let cleaned = clean_option_values(recognized);
            parse_ddl_table_options(&cleaned)?
        }
    } else {
        CreateTableStatementExtension::default()
    };

    // Attach partition_by expression.
    extension.partition_by = partition_by_expr;

    // Check if we actually extracted anything meaningful.
    let has_recognized_with = extension.acceleration.is_some()
        || extension.dataset.time_column.is_some()
        || extension.dataset.time_format.is_some();
    let has_partition = extension.partition_by.is_some();

    if !has_recognized_with && !has_partition {
        // Nothing to extract — return unmodified.
        return Ok((create_table, None));
    }

    // Store the extensions for the analyzer rule to consume.
    let table_name = create_table.name.to_string();
    let store_key = TableReference::parse_str(&table_name);

    {
        let mut guard = ddl_store.write().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire DDL extension store lock: {e}"))
        })?;
        guard.insert(store_key.clone(), extension);
    }

    // Strip the extracted clauses from the AST.
    if has_recognized_with {
        create_table.table_options = CreateTableOptions::None;
    }
    if has_partition {
        create_table.partition_by = None;
    }

    Ok((create_table, Some(store_key)))
}

/// Plan a `CREATE TABLE ... (LIKE ...)` statement.
///
/// Resolves the source table from the catalog, extracts its schema and
/// partition expression, and builds a [`CayenneCreateTableNode`] directly
/// (bypassing `DataFusion`'s standard planner which doesn't support LIKE).
pub(super) async fn plan_create_table_like(
    statement: Statement,
    session: &SessionState,
    planner_ctx: &super::PlannerContext,
) -> DFResult<LogicalPlan> {
    use std::sync::Arc;

    use datafusion::logical_expr::Extension;
    use datafusion::sql::sqlparser::ast::CreateTableLikeKind;

    // Decompose the Statement to take ownership of the CreateTable AST.
    let Statement::Statement(sql_stmt) = statement else {
        return Err(DataFusionError::Internal(
            "Expected Statement::Statement for CREATE TABLE LIKE".to_string(),
        ));
    };
    let SQLStatement::CreateTable(create_table) = *sql_stmt else {
        return Err(DataFusionError::Internal(
            "Expected SQLStatement::CreateTable for LIKE".to_string(),
        ));
    };

    // Extract the source table name from the LIKE clause.
    let like_kind = create_table.like.ok_or_else(|| {
        DataFusionError::Internal("Expected LIKE clause in CreateTable".to_string())
    })?;

    // OR REPLACE is not supported with LIKE.
    if create_table.or_replace {
        return Err(DataFusionError::Plan(
            "CREATE OR REPLACE TABLE ... LIKE is not supported. \
             Use DROP TABLE followed by CREATE TABLE ... LIKE instead."
                .to_string(),
        ));
    }

    let like = match like_kind {
        CreateTableLikeKind::Parenthesized(like) | CreateTableLikeKind::Plain(like) => like,
    };

    let source_name = like.name.to_string();
    let source_table_ref = TableReference::parse_str(&source_name);
    let source_catalog_name = source_table_ref
        .catalog()
        .unwrap_or(SPICE_DEFAULT_CATALOG)
        .to_string();
    let source_schema_name = source_table_ref
        .schema()
        .unwrap_or(SPICE_DEFAULT_SCHEMA)
        .to_string();
    let source_table_name = source_table_ref.table().to_string();

    // Validate the source catalog is Cayenne-backed.
    let catalog_list = session.catalog_list();
    let source_catalog = catalog_list.catalog(&source_catalog_name).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Source catalog '{source_catalog_name}' not found for LIKE"
        ))
    })?;

    if !is_cayenne_catalog(source_catalog.as_ref()) {
        return Err(DataFusionError::Plan(format!(
            "CREATE TABLE ... (LIKE ...) is only supported for Cayenne catalog tables. \
             Table '{source_name}' is not in a Cayenne catalog."
        )));
    }

    // Resolve the source table provider to get its schema.
    let source_schema_provider = source_catalog.schema(&source_schema_name).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Schema '{source_schema_name}' not found in catalog '{source_catalog_name}'"
        ))
    })?;

    let source_provider = source_schema_provider
        .table(&source_table_name)
        .await
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to resolve source table '{source_name}': {e}"
            ))
        })?
        .ok_or_else(|| DataFusionError::Plan(format!("Table '{source_name}' not found")))?;

    let arrow_schema = source_provider.schema();

    // Resolve the source table's partition expression from the Cayenne catalog.
    let partition_aware =
        crate::datafusion::cayenne_ddl::as_partition_aware(source_catalog.as_ref());
    let mut partition_expr_sql = if let Some(aware) = partition_aware {
        aware
            .table_partition_expr(&source_schema_name, &source_table_name)
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to get partition expression for source table '{source_name}': {e}"
                ))
            })?
    } else {
        None
    };

    // Build a fully-qualified source table reference (needed for partition metadata lookup).
    let source_full_ref = TableReference::full(
        source_catalog_name.clone(),
        source_schema_name.clone(),
        source_table_name.clone(),
    );

    // Resolve auto-generated labels (e.g. "expr0") to original SQL expressions
    // by looking up the partition manager metadata.
    if let Some(ref expr_str) = partition_expr_sql
        && let Some(Ok(idx)) = expr_str.strip_prefix("expr").map(str::parse::<usize>)
        && let Some(ref registry) = planner_ctx.executor_registry
    {
        let pm = registry.federated_partition_store();
        match pm.get_table_metadata(&source_full_ref).await {
            Ok(Some(metadata)) => {
                if let Some(original) = metadata.partition_expressions.get(idx) {
                    // Strip outer parentheses — the state store stores expressions
                    // like "(bucket(5, col))" but the SQL parser needs "bucket(5, col)".
                    let resolved = original.trim();
                    let resolved = if resolved.starts_with('(') && resolved.ends_with(')') {
                        &resolved[1..resolved.len() - 1]
                    } else {
                        resolved
                    };
                    tracing::info!(
                        source = %source_full_ref,
                        label = %expr_str,
                        resolved = %resolved,
                        "Resolved auto-generated partition label to original SQL expression"
                    );
                    partition_expr_sql = Some(resolved.to_string());
                } else {
                    tracing::warn!(
                        source = %source_full_ref,
                        label = %expr_str,
                        expressions = ?metadata.partition_expressions,
                        "Partition expression index {idx} not found in metadata"
                    );
                }
            }
            Ok(None) => {
                tracing::warn!(
                    source = %source_full_ref,
                    label = %expr_str,
                    "No partition metadata found for source table"
                );
            }
            Err(e) => {
                tracing::warn!(
                    source = %source_full_ref,
                    label = %expr_str,
                    error = %e,
                    "Failed to read partition metadata for source table"
                );
            }
        }
    }

    // Resolve the target table reference.
    let target_name = create_table.name.to_string();
    let target_table_ref = TableReference::parse_str(&target_name);
    let target_catalog_name = target_table_ref
        .catalog()
        .unwrap_or(SPICE_DEFAULT_CATALOG)
        .to_string();
    let target_schema_name = target_table_ref
        .schema()
        .unwrap_or(SPICE_DEFAULT_SCHEMA)
        .to_string();
    let target_table_name = target_table_ref.table().to_string();

    // Validate the target catalog is also Cayenne-backed.
    let target_catalog = catalog_list.catalog(&target_catalog_name).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Target catalog '{target_catalog_name}' not found for LIKE"
        ))
    })?;

    if !is_cayenne_catalog(target_catalog.as_ref()) {
        return Err(DataFusionError::Plan(format!(
            "CREATE TABLE ... (LIKE ...) is only supported for Cayenne catalog tables. \
             Table '{target_name}' is not in a Cayenne catalog."
        )));
    }

    // Validate source and target are in the same catalog.
    if source_catalog_name != target_catalog_name {
        return Err(DataFusionError::Plan(format!(
            "CREATE TABLE ... (LIKE ...) requires the source and target tables to be \
             in the same catalog. Source '{source_name}' is in catalog \
             '{source_catalog_name}', but target is in '{target_catalog_name}'."
        )));
    }

    let handler = planner_ctx.ddl_handler.clone().ok_or_else(|| {
        DataFusionError::Internal(
            "CREATE TABLE ... LIKE requires a DDL handler in PlannerContext".to_string(),
        )
    })?;

    let params = datafusion_ddl::CreateTableParams {
        catalog_name: target_catalog_name,
        schema_name: target_schema_name,
        table_name: target_table_name,
        arrow_schema,
        primary_key: vec![], // LIKE never copies primary keys
        extension: {
            // Encode the partition SQL back into the extension's partition_by field
            // as a bare identifier — the handler will call .to_string() on it.
            datafusion_ddl::CreateTableStatementExtension {
                partition_by: partition_expr_sql.map(|sql| {
                    use datafusion::sql::sqlparser::dialect::GenericDialect;
                    use datafusion::sql::sqlparser::parser::Parser;
                    // Parse back into a real AST expression so that function calls
                    // like `bucket(4, region)` are not mangled into an identifier.
                    Parser::new(&GenericDialect {})
                        .try_with_sql(&sql)
                        .and_then(|mut p| p.parse_expr())
                        .map_or_else(
                            |_| {
                                Box::new(datafusion::sql::sqlparser::ast::Expr::Identifier(
                                    datafusion::sql::sqlparser::ast::Ident::new(sql),
                                ))
                            },
                            Box::new,
                        )
                }),
                ..Default::default()
            }
        },
        if_not_exists: create_table.if_not_exists,
        or_replace: false,
        like_source_table: Some(source_full_ref),
    };

    let node = datafusion_ddl::DdlExtensionNode::new(
        datafusion_ddl::DdlNodeOp::CreateTable(Box::new(params)),
        handler,
    );

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}

/// Remove a store entry on error (best-effort).
fn cleanup_store_entry(store: &SharedDdlExtensionStore, key: &TableReference) -> DFResult<()> {
    let mut guard = store.write().map_err(|e| {
        DataFusionError::Execution(format!("Failed to acquire DDL extension store lock: {e}"))
    })?;
    let _ = guard.remove(key);
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

type OptionsClassification = (Vec<(String, String)>, Vec<SqlOption>);

/// Classify `WITH` options into recognized (`acceleration.*`/`dataset.*`) and others.
fn classify_with_options(options: &[SqlOption]) -> OptionsClassification {
    let mut recognized = Vec::new();
    let mut other = Vec::new();

    for opt in options {
        match opt {
            SqlOption::KeyValue { key, value } => {
                let key_str = key.value.clone();
                let value_str = match value {
                    datafusion::sql::sqlparser::ast::Expr::Value(v) => v.to_string(),
                    datafusion::sql::sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                    other => other.to_string(),
                };

                if key_str.starts_with("acceleration.") || key_str.starts_with("dataset.") {
                    recognized.push((key_str, value_str));
                } else {
                    other.push(opt.clone());
                }
            }
            _ => {
                other.push(opt.clone());
            }
        }
    }

    (recognized, other)
}

/// Strip surrounding quotes from option values.
fn clean_option_values(opts: Vec<(String, String)>) -> Vec<(String, String)> {
    opts.into_iter()
        .map(|(k, v)| {
            let v = v
                .trim_start_matches('\'')
                .trim_end_matches('\'')
                .trim_start_matches('"')
                .trim_end_matches('"')
                .to_string();
            (k, v)
        })
        .collect()
}

/// Validate that partition key columns are included in the primary key.
fn validate_partition_key_in_primary_key(
    partition_expr: &datafusion::sql::sqlparser::ast::Expr,
    constraints: &[TableConstraint],
    columns: &[datafusion::sql::sqlparser::ast::ColumnDef],
) -> DFResult<()> {
    let partition_cols = extract_partition_column_names(partition_expr);
    if partition_cols.is_empty() {
        return Ok(());
    }

    let pk_cols = extract_primary_key_columns(constraints, columns);
    if pk_cols.is_empty() {
        return Ok(());
    }

    let missing: Vec<&str> = partition_cols
        .iter()
        .filter(|p| !pk_cols.iter().any(|pk| pk.eq_ignore_ascii_case(p)))
        .map(String::as_str)
        .collect();

    if !missing.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "The primary key must include the partition column(s) '{}'. \
             Add the missing column(s) to the PRIMARY KEY constraint. \
             This is required for correct on-conflict behavior in distributed mode.",
            missing.join("', '")
        )));
    }

    Ok(())
}

/// Extract simple column names from a `PARTITION BY` expression.
fn extract_partition_column_names(expr: &datafusion::sql::sqlparser::ast::Expr) -> Vec<String> {
    use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

    match expr {
        Expr::Identifier(ident) => vec![ident.value.clone()],
        Expr::Nested(inner) => extract_partition_column_names(inner),
        Expr::Function(func) if func.name.to_string().eq_ignore_ascii_case("bucket") => {
            if let FunctionArguments::List(ref arg_list) = func.args {
                arg_list
                    .args
                    .get(1)
                    .and_then(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                            Some(vec![ident.value.clone()])
                        }
                        _ => None,
                    })
                    .unwrap_or_default()
            } else {
                vec![]
            }
        }
        _ => vec![],
    }
}

/// Extract primary key column names from constraints and column definitions.
fn extract_primary_key_columns(
    constraints: &[TableConstraint],
    columns: &[datafusion::sql::sqlparser::ast::ColumnDef],
) -> Vec<String> {
    let mut pk_cols = Vec::new();

    for constraint in constraints {
        if let TableConstraint::PrimaryKey(primary_key) = constraint {
            for idx_col in &primary_key.columns {
                if let datafusion::sql::sqlparser::ast::Expr::Identifier(ident) =
                    &idx_col.column.expr
                {
                    pk_cols.push(ident.value.clone());
                }
            }
        }
    }

    for col_def in columns {
        for opt_def in &col_def.options {
            if matches!(&opt_def.option, ColumnOption::PrimaryKey(_)) {
                pk_cols.push(col_def.name.value.clone());
            }
        }
    }

    pk_cols
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion_ddl::has_ddl_extensions;
    use datafusion_ddl::new_shared_store;

    /// Parse SQL into a `CreateTable` AST node for testing.
    fn parse_create_table(sql: &str) -> CreateTable {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("should parse");
        match stmts.into_iter().next().expect("should have a statement") {
            SQLStatement::CreateTable(ct) => ct,
            other => panic!("Expected CreateTable, got: {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // has_ddl_extensions
    // -----------------------------------------------------------------------

    #[test]
    fn test_no_extensions() {
        let ct = parse_create_table("CREATE TABLE foo (id INT, name VARCHAR)");
        assert!(!has_ddl_extensions(&ct));
    }

    #[test]
    fn test_acceleration_options() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT) WITH ("acceleration.engine" = 'arrow')"#,
        );
        assert!(has_ddl_extensions(&ct));
    }

    #[test]
    fn test_dataset_options() {
        let ct =
            parse_create_table(r#"CREATE TABLE foo (id INT) WITH ("dataset.time_column" = 'ts')"#);
        assert!(has_ddl_extensions(&ct));
    }

    #[test]
    fn test_partition_by_only() {
        let ct = parse_create_table("CREATE TABLE foo (id INT, region TEXT) PARTITION BY region");
        assert!(has_ddl_extensions(&ct));
    }

    #[test]
    fn test_unrecognized_with_options() {
        let ct = parse_create_table("CREATE TABLE foo (id INT) WITH (fillfactor = 70)");
        assert!(!has_ddl_extensions(&ct));
    }

    // -----------------------------------------------------------------------
    // extract_and_store_extensions
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_acceleration() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT, name VARCHAR) WITH ("acceleration.engine" = 'arrow', "acceleration.mode" = 'memory')"#,
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let (modified, store_key) =
            extract_and_store_extensions(ct, &store).expect("should succeed");

        assert!(store_key.is_some());
        assert!(matches!(modified.table_options, CreateTableOptions::None));

        let ext = store
            .write()
            .expect("store lock should not be poisoned")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have entry");
        let accel = ext.acceleration.expect("should have acceleration");
        assert_eq!(accel.engine.as_deref(), Some("arrow"));
        assert_eq!(accel.mode, spicepod::acceleration::Mode::Memory);
    }

    #[test]
    fn test_extract_dataset_options() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT, ts TIMESTAMP) WITH ("dataset.time_column" = 'ts', "dataset.time_format" = 'timestamp')"#,
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let (_, store_key) = extract_and_store_extensions(ct, &store).expect("should succeed");

        assert!(store_key.is_some());
        let ext = store
            .write()
            .expect("store lock should not be poisoned")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have entry");
        assert_eq!(ext.dataset.time_column.as_deref(), Some("ts"));
        assert_eq!(
            ext.dataset.time_format,
            Some(spicepod::component::dataset::TimeFormat::Timestamp)
        );
    }

    #[test]
    fn test_extract_mixed_accel_and_dataset() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT, ts TIMESTAMP) WITH ("acceleration.engine" = 'arrow', "acceleration.refresh_mode" = 'append', "dataset.time_column" = 'ts')"#,
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let (_, store_key) = extract_and_store_extensions(ct, &store).expect("should succeed");
        assert!(store_key.is_some());

        let ext = store
            .write()
            .expect("store lock should not be poisoned")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have entry");
        assert!(ext.acceleration.is_some());
        assert_eq!(ext.dataset.time_column.as_deref(), Some("ts"));
    }

    #[test]
    fn test_extract_unrecognized_only_returns_none() {
        let ct = parse_create_table("CREATE TABLE foo (id INT) WITH (fillfactor = 70)");
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let (_, store_key) = extract_and_store_extensions(ct, &store).expect("should succeed");
        assert!(store_key.is_none());
    }

    #[test]
    fn test_extract_mixed_recognized_and_unrecognized_errors() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT) WITH ("acceleration.engine" = 'arrow', fillfactor = 70)"#,
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let err = extract_and_store_extensions(ct, &store)
            .expect_err("should error")
            .to_string();
        assert!(err.contains("Cannot mix"));
    }

    #[test]
    fn test_extract_partition_by() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, region TEXT, ts TIMESTAMP) PARTITION BY region",
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let (modified, store_key) =
            extract_and_store_extensions(ct, &store).expect("should succeed");

        assert!(store_key.is_some());
        assert!(modified.partition_by.is_none());

        let ext = store
            .write()
            .expect("store lock should not be poisoned")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have entry");
        assert!(ext.partition_by.is_some());
    }

    #[test]
    fn test_extract_with_and_partition_by() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT, region TEXT) WITH ("acceleration.engine" = 'arrow') PARTITION BY region"#,
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let (modified, store_key) =
            extract_and_store_extensions(ct, &store).expect("should succeed");

        assert!(store_key.is_some());
        assert!(matches!(modified.table_options, CreateTableOptions::None));
        assert!(modified.partition_by.is_none());

        let ext = store
            .write()
            .expect("store lock should not be poisoned")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have entry");
        assert!(ext.acceleration.is_some());
        assert!(ext.partition_by.is_some());
    }

    // -----------------------------------------------------------------------
    // Partition key / primary key validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_key_in_primary_key() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, p TEXT, PRIMARY KEY (id, p)) PARTITION BY p",
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        extract_and_store_extensions(ct, &store)
            .expect("partition key in primary key should succeed");
    }

    #[test]
    fn test_partition_key_not_in_primary_key_errors() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, p TEXT, PRIMARY KEY (id)) PARTITION BY p",
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let err = extract_and_store_extensions(ct, &store)
            .expect_err("should error")
            .to_string();
        assert!(err.contains('p'), "should mention missing column: {err}");
        assert!(
            err.to_lowercase().contains("primary key"),
            "should mention primary key: {err}"
        );
    }

    #[test]
    fn test_column_level_primary_key_missing_partition() {
        let ct = parse_create_table("CREATE TABLE foo (id INT PRIMARY KEY, p TEXT) PARTITION BY p");
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let err = extract_and_store_extensions(ct, &store)
            .expect_err("should error")
            .to_string();
        assert!(err.contains('p'));
    }

    #[test]
    fn test_partition_by_no_primary_key_ok() {
        let ct = parse_create_table("CREATE TABLE foo (id INT, p TEXT) PARTITION BY p");
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        extract_and_store_extensions(ct, &store)
            .expect("partition by without primary key should succeed");
    }

    #[test]
    fn test_composite_primary_key_valid() {
        let ct = parse_create_table(
            "CREATE TABLE foo (a INT, b TEXT, c VARCHAR, PRIMARY KEY (a, b)) PARTITION BY b",
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        extract_and_store_extensions(ct, &store)
            .expect("composite primary key with partition should succeed");
    }

    #[test]
    fn test_bucket_partition_in_primary_key() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, region TEXT, PRIMARY KEY (id, region)) PARTITION BY bucket(4, region)",
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        extract_and_store_extensions(ct, &store)
            .expect("bucket partition in primary key should succeed");
    }

    #[test]
    fn test_bucket_partition_not_in_primary_key_errors() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, region TEXT, PRIMARY KEY (id)) PARTITION BY bucket(4, region)",
        );
        let store = new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let err = extract_and_store_extensions(ct, &store)
            .expect_err("should error")
            .to_string();
        assert!(err.contains("region"));
    }

    // -----------------------------------------------------------------------
    // LIKE detection
    // -----------------------------------------------------------------------

    #[test]
    fn test_like_plain_detected() {
        let ct = parse_create_table("CREATE TABLE staging LIKE source_table");
        assert!(
            ct.like.is_some(),
            "LIKE should be detected in CreateTable AST"
        );
    }

    #[test]
    fn test_like_if_not_exists_detected() {
        let ct = parse_create_table("CREATE TABLE IF NOT EXISTS staging LIKE source_table");
        assert!(ct.like.is_some());
        assert!(ct.if_not_exists);
    }

    #[test]
    fn test_like_qualified_source_detected() {
        let ct = parse_create_table(
            r#"CREATE TABLE IF NOT EXISTS "catalog"."schema"."staging" LIKE "catalog"."schema"."source""#,
        );
        assert!(ct.like.is_some());
        assert!(ct.if_not_exists);
    }

    #[test]
    fn test_like_not_treated_as_ddl_extension() {
        let ct = parse_create_table("CREATE TABLE staging LIKE source_table");
        assert!(
            !has_ddl_extensions(&ct),
            "LIKE should not be treated as a DDL extension"
        );
    }
}
