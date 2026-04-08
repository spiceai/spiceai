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
//! (`acceleration.*`, `dataset.*`) and/or distribution clauses (`PARTITION BY`,
//! `REPLICATED`). Extracts these extensions from the AST, stores them in the
//! [`DdlExtensionStore`] for the analyzer rule to consume, strips them from the
//! statement, and delegates the cleaned statement to `DataFusion`'s standard
//! planner.

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::{
    ColumnOption, CreateTable, CreateTableOptions, SqlOption, Statement as SQLStatement,
    TableConstraint,
};

use crate::datafusion::ddl::acceleration_options::{
    CreateTableStatementExtension, SharedDdlExtensionStore, TableDistribution,
    parse_ddl_table_options,
};

/// Returns `true` if the `CREATE TABLE` has extensions we need to intercept:
/// recognized `WITH (...)` options (`acceleration.*`, `dataset.*`) or a
/// distribution clause (`PARTITION BY` or `REPLICATED`).
pub(crate) fn has_ddl_extensions(ct: &CreateTable, is_replicated: bool) -> bool {
    if is_replicated || ct.partition_by.is_some() {
        return true;
    }

    if let CreateTableOptions::With(options) = &ct.table_options {
        return options.iter().any(|opt| {
            if let SqlOption::KeyValue { key, .. } = opt {
                key.value.starts_with("acceleration.") || key.value.starts_with("dataset.")
            } else {
                false
            }
        });
    }

    false
}

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
    is_replicated: bool,
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

    let (modified_ct, store_key) =
        extract_and_store_extensions(create_table, ddl_store, is_replicated)?;

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
    is_replicated: bool,
) -> DFResult<(CreateTable, Option<TableReference>)> {
    // Validate mutual exclusion: REPLICATED and PARTITION BY cannot be combined.
    if is_replicated && create_table.partition_by.is_some() {
        return Err(DataFusionError::Plan(
            "Cannot use both REPLICATED and PARTITION BY in CREATE TABLE. Use one or the other."
                .to_string(),
        ));
    }

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

    // Set distribution mode.
    extension.distribution = if is_replicated {
        Some(TableDistribution::Replicated)
    } else {
        partition_by_expr.map(TableDistribution::PartitionBy)
    };

    // Check if we actually extracted anything meaningful.
    let has_recognized_with = extension.acceleration.is_some()
        || extension.dataset.time_column.is_some()
        || extension.dataset.time_format.is_some();
    let has_distribution = extension.distribution.is_some();

    if !has_recognized_with && !has_distribution {
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
    if create_table.partition_by.is_some() {
        create_table.partition_by = None;
    }

    Ok((create_table, Some(store_key)))
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

/// Strip a trailing `REPLICATED` keyword from a `CREATE TABLE` SQL statement.
///
/// `sqlparser` does not recognize `REPLICATED` in this position, so we remove
/// it before parsing and keep an out-of-band flag for extension extraction.
/// Returns the (possibly modified) SQL and whether `REPLICATED` was stripped.
pub(crate) fn strip_replicated_keyword(sql: &str) -> (String, bool) {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("REPLICATED") {
        return (sql.to_string(), false);
    }

    let trimmed_start = sql.trim_start();
    let upper_trimmed = trimmed_start.to_ascii_uppercase();
    if !upper_trimmed.starts_with("CREATE TABLE")
        && !upper_trimmed.starts_with("CREATE OR REPLACE TABLE")
    {
        return (sql.to_string(), false);
    }

    fn is_identifier_char(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || byte == b'_'
    }

    let bytes = sql.as_bytes();
    let mut token_end = bytes.len();
    while token_end > 0 && bytes[token_end - 1].is_ascii_whitespace() {
        token_end -= 1;
    }
    if token_end == 0 {
        return (sql.to_string(), false);
    }

    // Optional trailing semicolon.
    if bytes[token_end - 1] == b';' {
        token_end -= 1;
        while token_end > 0 && bytes[token_end - 1].is_ascii_whitespace() {
            token_end -= 1;
        }
    }
    if token_end == 0 {
        return (sql.to_string(), false);
    }

    let mut token_start = token_end;
    while token_start > 0 && is_identifier_char(bytes[token_start - 1]) {
        token_start -= 1;
    }

    if !sql[token_start..token_end].eq_ignore_ascii_case("REPLICATED") {
        return (sql.to_string(), false);
    }

    let before = sql[..token_start].trim_end();
    let after = sql[token_end..].trim_start();
    (format!("{before}{after}"), true)
}

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
        if let TableConstraint::PrimaryKey { columns, .. } = constraint {
            for idx_col in columns {
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
            if matches!(
                &opt_def.option,
                ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            ) {
                pk_cols.push(col_def.name.value.clone());
            }
        }
    }

    pk_cols
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::ddl::acceleration_options::new_shared_store;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

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
        assert!(!has_ddl_extensions(&ct, false));
    }

    #[test]
    fn test_acceleration_options() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT) WITH ("acceleration.engine" = 'arrow')"#,
        );
        assert!(has_ddl_extensions(&ct, false));
    }

    #[test]
    fn test_dataset_options() {
        let ct =
            parse_create_table(r#"CREATE TABLE foo (id INT) WITH ("dataset.time_column" = 'ts')"#);
        assert!(has_ddl_extensions(&ct, false));
    }

    #[test]
    fn test_partition_by_only() {
        let ct = parse_create_table("CREATE TABLE foo (id INT, region TEXT) PARTITION BY region");
        assert!(has_ddl_extensions(&ct, false));
    }

    #[test]
    fn test_unrecognized_with_options() {
        let ct = parse_create_table("CREATE TABLE foo (id INT) WITH (fillfactor = 70)");
        assert!(!has_ddl_extensions(&ct, false));
    }

    #[test]
    fn test_replicated_only() {
        let ct = parse_create_table("CREATE TABLE foo (id INT)");
        assert!(has_ddl_extensions(&ct, true));
    }

    // -----------------------------------------------------------------------
    // extract_and_store_extensions
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_acceleration() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT, name VARCHAR) WITH ("acceleration.engine" = 'arrow', "acceleration.mode" = 'memory')"#,
        );
        let store = new_shared_store();
        let (modified, store_key) =
            extract_and_store_extensions(ct, &store, false).expect("should succeed");

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
        let store = new_shared_store();
        let (_, store_key) =
            extract_and_store_extensions(ct, &store, false).expect("should succeed");

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
        let store = new_shared_store();
        let (_, store_key) =
            extract_and_store_extensions(ct, &store, false).expect("should succeed");
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
        let store = new_shared_store();
        let (_, store_key) =
            extract_and_store_extensions(ct, &store, false).expect("should succeed");
        assert!(store_key.is_none());
    }

    #[test]
    fn test_extract_mixed_recognized_and_unrecognized_errors() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT) WITH ("acceleration.engine" = 'arrow', fillfactor = 70)"#,
        );
        let store = new_shared_store();
        let err = extract_and_store_extensions(ct, &store, false)
            .expect_err("should error")
            .to_string();
        assert!(err.contains("Cannot mix"));
    }

    #[test]
    fn test_extract_partition_by() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, region TEXT, ts TIMESTAMP) PARTITION BY region",
        );
        let store = new_shared_store();
        let (modified, store_key) =
            extract_and_store_extensions(ct, &store, false).expect("should succeed");

        assert!(store_key.is_some());
        assert!(modified.partition_by.is_none());

        let ext = store
            .write()
            .expect("store lock should not be poisoned")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have entry");
        assert!(matches!(
            ext.distribution,
            Some(TableDistribution::PartitionBy(_))
        ));
    }

    #[test]
    fn test_extract_with_and_partition_by() {
        let ct = parse_create_table(
            r#"CREATE TABLE foo (id INT, region TEXT) WITH ("acceleration.engine" = 'arrow') PARTITION BY region"#,
        );
        let store = new_shared_store();
        let (modified, store_key) =
            extract_and_store_extensions(ct, &store, false).expect("should succeed");

        assert!(store_key.is_some());
        assert!(matches!(modified.table_options, CreateTableOptions::None));
        assert!(modified.partition_by.is_none());

        let ext = store
            .write()
            .expect("store lock should not be poisoned")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have entry");
        assert!(ext.acceleration.is_some());
        assert!(matches!(
            ext.distribution,
            Some(TableDistribution::PartitionBy(_))
        ));
    }

    #[test]
    fn test_extract_replicated() {
        let ct = parse_create_table("CREATE TABLE foo (id INT, region TEXT)");
        let store = new_shared_store();
        let (modified, store_key) =
            extract_and_store_extensions(ct, &store, true).expect("should succeed");

        assert!(store_key.is_some());
        assert!(modified.partition_by.is_none());

        let ext = store
            .write()
            .expect("store lock should not be poisoned")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have entry");
        assert!(matches!(
            ext.distribution,
            Some(TableDistribution::Replicated)
        ));
    }

    #[test]
    fn test_replicated_and_partition_by_errors() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, region TEXT, PRIMARY KEY (id, region)) PARTITION BY region",
        );
        let store = new_shared_store();
        let err = extract_and_store_extensions(ct, &store, true)
            .expect_err("should error")
            .to_string();
        assert!(err.contains("Cannot use both REPLICATED and PARTITION BY"));
    }

    // -----------------------------------------------------------------------
    // Partition key / primary key validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_key_in_primary_key() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, p TEXT, PRIMARY KEY (id, p)) PARTITION BY p",
        );
        let store = new_shared_store();
        extract_and_store_extensions(ct, &store, false)
            .expect("partition key in primary key should succeed");
    }

    #[test]
    fn test_partition_key_not_in_primary_key_errors() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, p TEXT, PRIMARY KEY (id)) PARTITION BY p",
        );
        let store = new_shared_store();
        let err = extract_and_store_extensions(ct, &store, false)
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
        let store = new_shared_store();
        let err = extract_and_store_extensions(ct, &store, false)
            .expect_err("should error")
            .to_string();
        assert!(err.contains('p'));
    }

    #[test]
    fn test_partition_by_no_primary_key_ok() {
        let ct = parse_create_table("CREATE TABLE foo (id INT, p TEXT) PARTITION BY p");
        let store = new_shared_store();
        extract_and_store_extensions(ct, &store, false)
            .expect("partition by without primary key should succeed");
    }

    #[test]
    fn test_composite_primary_key_valid() {
        let ct = parse_create_table(
            "CREATE TABLE foo (a INT, b TEXT, c VARCHAR, PRIMARY KEY (a, b)) PARTITION BY b",
        );
        let store = new_shared_store();
        extract_and_store_extensions(ct, &store, false)
            .expect("composite primary key with partition should succeed");
    }

    #[test]
    fn test_bucket_partition_in_primary_key() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, region TEXT, PRIMARY KEY (id, region)) PARTITION BY bucket(4, region)",
        );
        let store = new_shared_store();
        extract_and_store_extensions(ct, &store, false)
            .expect("bucket partition in primary key should succeed");
    }

    #[test]
    fn test_bucket_partition_not_in_primary_key_errors() {
        let ct = parse_create_table(
            "CREATE TABLE foo (id INT, region TEXT, PRIMARY KEY (id)) PARTITION BY bucket(4, region)",
        );
        let store = new_shared_store();
        let err = extract_and_store_extensions(ct, &store, false)
            .expect_err("should error")
            .to_string();
        assert!(err.contains("region"));
    }

    // -----------------------------------------------------------------------
    // REPLICATED pre-parse stripping
    // -----------------------------------------------------------------------

    #[test]
    fn test_strip_replicated_keyword_create_table() {
        let sql = "CREATE TABLE foo (id INT) REPLICATED ;";
        let (stripped, is_replicated) = strip_replicated_keyword(sql);
        assert!(is_replicated);
        assert_eq!(stripped, "CREATE TABLE foo (id INT);");
    }

    #[test]
    fn test_strip_replicated_keyword_non_create_table() {
        let sql = "SELECT 'REPLICATED'";
        let (stripped, is_replicated) = strip_replicated_keyword(sql);
        assert!(!is_replicated);
        assert_eq!(stripped, sql);
    }

    #[test]
    fn test_strip_replicated_keyword_identifier_suffix_not_matched() {
        let sql = "CREATE TABLE foo (id INT, replicated_col INT)";
        let (stripped, is_replicated) = strip_replicated_keyword(sql);
        assert!(!is_replicated);
        assert_eq!(stripped, sql);
    }

    #[test]
    fn test_strip_replicated_keyword_string_literal_not_matched() {
        let sql = "CREATE TABLE foo (id TEXT DEFAULT 'REPLICATED')";
        let (stripped, is_replicated) = strip_replicated_keyword(sql);
        assert!(!is_replicated);
        assert_eq!(stripped, sql);
    }
}
