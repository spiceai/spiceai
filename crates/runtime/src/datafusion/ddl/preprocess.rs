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

//! Pre-processing for `CREATE TABLE` SQL statements with custom extensions.
//!
//! `DataFusion`'s `SqlToRel` does not support `WITH (...)` options on `CREATE TABLE`
//! (it only matches `table_options: CreateTableOptions::None`), and does not
//! support custom `PARTITION BY` clauses. This module intercepts such statements
//! before they reach `DataFusion`, extracts the extensions into the shared
//! [`DdlExtensionStore`], and returns modified SQL without those clauses so
//! `DataFusion` can plan it normally.
//!
//! ## `WITH (...)` options
//!
//! Keys must be double-quoted because dots are not valid in bare SQL identifiers:
//! ```sql
//! CREATE TABLE t (id INT) WITH (
//!     "acceleration.engine" = 'arrow',
//!     "dataset.time_column" = 'created_at'
//! )
//! ```
//!
//! ## `PARTITION BY` clause
//!
//! Partitioning expressions are extracted from the `CREATE TABLE` statement's
//! file options (where sqlparser places them):
//! ```sql
//! CREATE TABLE t (id INT, region TEXT) PARTITION BY region, year(ts)
//! ```

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{
    ColumnOption, CreateTableOptions, SqlOption, Statement, TableConstraint,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::acceleration_options::{
    CreateTableStatementExtension, SharedDdlExtensionStore, parse_ddl_table_options,
};

/// Result of pre-processing: either the original SQL unchanged, or modified SQL
/// with DDL extensions extracted.
#[derive(Debug)]
pub enum PreprocessResult {
    /// SQL was not a `CREATE TABLE` with recognized extensions — pass through unchanged.
    Unchanged,
    /// SQL was modified: extensions stripped, stored in the [`DdlExtensionStore`].
    Modified {
        /// The rewritten SQL string without the extracted clauses.
        sql: String,
        /// The store key used for the inserted extensions.
        store_key: TableReference,
    },
}

/// Remove a previously inserted DDL extension entry from the shared store.
///
/// Intended for error paths where preprocessing has inserted extensions but logical
/// planning failed before the analyzer could consume the entry.
///
/// # Errors
///
/// Returns an error if the store lock cannot be acquired.
pub fn cleanup_preprocessed_ddl_options(
    store: &SharedDdlExtensionStore,
    store_key: &TableReference,
) -> DFResult<()> {
    let mut guard = store.write().map_err(|e| {
        DataFusionError::Execution(format!("Failed to acquire DDL extension store lock: {e}"))
    })?;
    let _ = guard.remove(store_key);
    Ok(())
}

/// Pre-process a SQL string to extract `CREATE TABLE` extensions.
///
/// Extracts the following from `CREATE TABLE` statements:
/// - `WITH (...)` options containing `acceleration.*` or `dataset.*` keys
/// - `PARTITION BY` expressions
///
/// The extracted extensions are stored in the shared [`DdlExtensionStore`] and
/// the SQL is returned without those clauses so `DataFusion` can plan it normally.
/// If neither extension is present, the original SQL is returned unchanged.
///
/// # Errors
///
/// Returns an error if:
/// - The `WITH` options contain invalid keys or values.
/// - The `WITH` options contain keys with unrecognized prefixes (not `acceleration.*`
///   or `dataset.*`).
pub fn preprocess_create_table_with_options(
    sql: &str,
    ddl_store: &SharedDdlExtensionStore,
) -> DFResult<PreprocessResult> {
    // Quick check: if the SQL doesn't contain keywords we care about, skip parsing
    let upper = sql.to_uppercase();
    if !upper.contains("WITH") && !upper.contains("PARTITION") {
        return Ok(PreprocessResult::Unchanged);
    }

    let dialect = PostgreSqlDialect {};
    let statements = if let Ok(statements) = Parser::parse_sql(&dialect, sql) {
        statements
    } else if let Some(normalized_sql) = normalize_create_table_clause_order(sql) {
        let Ok(statements) = Parser::parse_sql(&dialect, &normalized_sql) else {
            // If sqlparser still can't parse it, let `DataFusion` handle the error.
            return Ok(PreprocessResult::Unchanged);
        };
        statements
    } else {
        // If sqlparser can't parse it, let `DataFusion` handle the error.
        return Ok(PreprocessResult::Unchanged);
    };

    // Only handle single-statement CREATE TABLE
    let [Statement::CreateTable(ref create_table)] = statements[..] else {
        return Ok(PreprocessResult::Unchanged);
    };

    // Extract WITH options if present
    let with_options = match create_table.table_options {
        CreateTableOptions::With(ref options) => Some(options.clone()),
        _ => None,
    };

    // Extract PARTITION BY expression if present
    let partition_by_expr = create_table.partition_by.clone();

    // Limitation: In distributed mode, primary keys must include the partitioning key
    // to ensure correct on-conflict behavior. When data is partitioned across different
    // executor nodes, on-conflict checks cannot work correctly if the partition key is
    // not part of the primary key — a conflicting row may reside on a different
    // executor's partition and be invisible during the conflict check.
    if let Some(ref partition_expr) = partition_by_expr {
        validate_partition_key_in_primary_key(
            partition_expr,
            &create_table.constraints,
            &create_table.columns,
        )?;
    }

    let has_with_options = with_options.is_some();
    let has_partition_by = partition_by_expr.is_some();

    if !has_with_options && !has_partition_by {
        return Ok(PreprocessResult::Unchanged);
    }

    // Process WITH options
    let mut extension = if let Some(ref options) = with_options {
        let (recognized_opts, other_opts) = classify_with_options(options);

        if recognized_opts.is_empty() && !has_partition_by {
            return Ok(PreprocessResult::Unchanged);
        }

        if !recognized_opts.is_empty() && !other_opts.is_empty() {
            return Err(DataFusionError::Plan(
                "Cannot mix 'acceleration.*' or 'dataset.*' options with other WITH options in CREATE TABLE. \
                 Use only 'acceleration.*' and/or 'dataset.*' options."
                    .to_string(),
            ));
        }

        if recognized_opts.is_empty() && other_opts.is_empty() {
            // Only PARTITION BY, no recognized WITH options — but has unrecognized WITH options
            // WITH has only non-recognized options, nothing for us to extract.
            // Only proceed if we have PARTITION BY.
            CreateTableStatementExtension::default()
        } else {
            let cleaned_opts = clean_option_values(recognized_opts);
            parse_ddl_table_options(&cleaned_opts)?
        }
    } else {
        CreateTableStatementExtension::default()
    };

    // Attach partition_by expression
    extension.partition_by = partition_by_expr;

    // Check if we actually have anything to extract
    let has_recognized_with = extension.acceleration.is_some()
        || extension.dataset.time_column.is_some()
        || extension.dataset.time_format.is_some();
    let has_partition = extension.partition_by.is_some();

    if !has_recognized_with && !has_partition {
        return Ok(PreprocessResult::Unchanged);
    }

    // Extract the table name for the store key
    let table_name = create_table.name.to_string();
    let store_key = TableReference::parse_str(&table_name);

    // Store the DDL extensions
    {
        let mut guard = ddl_store.write().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire DDL extension store lock: {e}"))
        })?;
        guard.insert(store_key.clone(), extension);
    }

    // Reconstruct the CREATE TABLE without the extracted clauses
    let mut modified = create_table.clone();

    // Remove WITH options if we extracted recognized ones
    if has_recognized_with {
        modified.table_options = CreateTableOptions::None;
    }

    // Remove PARTITION BY if we extracted it
    if has_partition {
        modified.partition_by = None;
    }

    let modified_sql = Statement::CreateTable(modified).to_string();

    Ok(PreprocessResult::Modified {
        sql: modified_sql,
        store_key,
    })
}

/// Normalize CREATE TABLE extension clause order for parser compatibility.
///
/// Some statements place `PARTITION BY ...` before `WITH (...)`. If parsing fails,
/// this rewrites to `WITH (...) PARTITION BY ...` and returns the rewritten SQL.
fn normalize_create_table_clause_order(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let partition_by_index = upper.find("PARTITION BY")?;

    // Only normalize the specific unsupported order where WITH appears after PARTITION BY.
    let with_index = upper[partition_by_index..]
        .find("WITH (")
        .map(|i| partition_by_index + i)?;

    if with_index <= partition_by_index {
        return None;
    }

    let before_partition = sql[..partition_by_index].trim_end();
    let partition_clause = sql[partition_by_index..with_index].trim();
    let with_clause = sql[with_index..].trim_start();

    if before_partition.is_empty() || partition_clause.is_empty() || with_clause.is_empty() {
        return None;
    }

    Some(format!(
        "{before_partition} {with_clause} {partition_clause}"
    ))
}

pub type OptionsClassification = (Vec<(String, String)>, Vec<SqlOption>);

/// Classify `WITH` options into recognized (`acceleration.*`/`dataset.*`) and other options.
fn classify_with_options(options: &[SqlOption]) -> OptionsClassification {
    let mut recognized_opts = Vec::new();
    let mut other_opts = Vec::new();

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
                    recognized_opts.push((key_str, value_str));
                } else {
                    other_opts.push(opt.clone());
                }
            }
            _ => {
                other_opts.push(opt.clone());
            }
        }
    }

    (recognized_opts, other_opts)
}

/// Strip surrounding quotes from option values (sqlparser includes them for string literals).
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

/// Extract simple column names from a `PARTITION BY` expression.
///
/// Handles:
/// - `Identifier` for single-column partition keys (e.g., `PARTITION BY region`)
/// - `bucket(num_buckets, column)` function calls (extracts the column argument)
/// - Nested/parenthesized expressions
///
/// Returns an empty vec for unrecognized complex expressions.
fn extract_partition_column_names(expr: &datafusion::sql::sqlparser::ast::Expr) -> Vec<String> {
    use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

    match expr {
        Expr::Identifier(ident) => {
            vec![ident.value.to_lowercase()]
        }
        Expr::Nested(inner) => extract_partition_column_names(inner),
        Expr::Function(func) if func.name.to_string().eq_ignore_ascii_case("bucket") => {
            // bucket(num_buckets, column) — extract the column from the second argument.
            if let FunctionArguments::List(ref arg_list) = func.args {
                arg_list
                    .args
                    .get(1)
                    .and_then(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                            Some(vec![ident.value.to_lowercase()])
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

/// Extract primary key column names from a `CREATE TABLE` statement.
///
/// Checks both table-level `PRIMARY KEY (col1, col2)` constraints and
/// column-level `col INT PRIMARY KEY` definitions.
fn extract_primary_key_columns(
    constraints: &[TableConstraint],
    columns: &[datafusion::sql::sqlparser::ast::ColumnDef],
) -> Vec<String> {
    let mut pk_cols = Vec::new();

    // Table-level PRIMARY KEY constraint
    for constraint in constraints {
        if let TableConstraint::PrimaryKey { columns, .. } = constraint {
            for idx_col in columns {
                if let datafusion::sql::sqlparser::ast::Expr::Identifier(ident) =
                    &idx_col.column.expr
                {
                    pk_cols.push(ident.value.to_lowercase());
                }
            }
        }
    }

    // Column-level PRIMARY KEY (e.g., `id INT PRIMARY KEY`)
    for col_def in columns {
        for opt_def in &col_def.options {
            if matches!(
                &opt_def.option,
                ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            ) {
                pk_cols.push(col_def.name.value.to_lowercase());
            }
        }
    }

    pk_cols
}

/// Validate that partition key columns are included in the primary key.
///
/// This is currently required to satisfy on-conflict behavior in distributed mode.
/// When data is partitioned across different executor nodes, on-conflict checks
/// cannot work correctly if the partition key is not part of the primary key,
/// because a conflicting row may reside on a different executor's partition and
/// be invisible during the conflict check.
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
        // No primary key defined — nothing to validate.
        return Ok(());
    }

    let missing: Vec<&str> = partition_cols
        .iter()
        .filter(|p| !pk_cols.iter().any(|pk| pk == *p))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::ddl::acceleration_options::new_shared_store;

    #[test]
    fn test_preprocess_no_with_clause() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT, name VARCHAR)";
        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Unchanged));
    }

    #[test]
    fn test_preprocess_non_create_table() {
        let store = new_shared_store();
        let sql = "SELECT * FROM foo";
        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Unchanged));
    }

    #[test]
    fn test_preprocess_with_acceleration_options() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT, name VARCHAR) WITH ("acceleration.engine" = 'arrow', "acceleration.mode" = 'memory')"#;

        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");

        match result {
            PreprocessResult::Modified {
                sql: modified_sql,
                store_key,
            } => {
                assert!(
                    !modified_sql.contains("acceleration."),
                    "Modified SQL should not contain acceleration options: {modified_sql}"
                );
                assert!(modified_sql.to_uppercase().contains("CREATE TABLE"));
                assert_eq!(store_key.to_string(), "foo");
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have extensions for 'foo'");
        let accel = ddl_ext.acceleration.expect("acceleration should be Some");
        assert_eq!(accel.engine.as_deref(), Some("arrow"));
        assert_eq!(accel.mode, spicepod::acceleration::Mode::Memory);
        assert!(ddl_ext.partition_by.is_none());
    }

    #[test]
    fn test_preprocess_with_dataset_options() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT, ts TIMESTAMP) WITH ("dataset.time_column" = 'ts', "dataset.time_format" = 'timestamp')"#;

        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");

        match result {
            PreprocessResult::Modified {
                sql: modified_sql,
                store_key,
            } => {
                assert!(
                    !modified_sql.contains("dataset."),
                    "Modified SQL should not contain dataset options: {modified_sql}"
                );
                assert_eq!(store_key.to_string(), "foo");
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have extensions for 'foo'");
        assert!(ddl_ext.acceleration.is_none());
        assert_eq!(ddl_ext.dataset.time_column.as_deref(), Some("ts"));
        assert_eq!(
            ddl_ext.dataset.time_format,
            Some(spicepod::component::dataset::TimeFormat::Timestamp)
        );
        assert!(ddl_ext.partition_by.is_none());
    }

    #[test]
    fn test_preprocess_with_mixed_accel_and_dataset_options() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT, ts TIMESTAMP) WITH ("acceleration.engine" = 'arrow', "acceleration.refresh_mode" = 'append', "dataset.time_column" = 'ts')"#;

        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Modified { .. }));

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have extensions for 'foo'");
        let accel = ddl_ext.acceleration.expect("acceleration should be Some");
        assert_eq!(accel.engine.as_deref(), Some("arrow"));
        assert_eq!(ddl_ext.dataset.time_column.as_deref(), Some("ts"));
        assert!(ddl_ext.partition_by.is_none());
    }

    #[test]
    fn test_preprocess_with_non_recognized_options_unchanged() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT) WITH (fillfactor = 70)";
        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Unchanged));
    }

    #[test]
    fn test_preprocess_mixed_recognized_and_other_options_errors() {
        let store = new_shared_store();
        let sql =
            r#"CREATE TABLE foo (id INT) WITH ("acceleration.engine" = 'arrow', fillfactor = 70)"#;
        let result = preprocess_create_table_with_options(sql, &store);
        let err = result.expect_err("should return an error").to_string();
        assert!(err.contains("Cannot mix"));
    }

    #[test]
    fn test_preprocess_invalid_acceleration_option_errors() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT) WITH ("acceleration.nonexistent" = 'value')"#;
        let result = preprocess_create_table_with_options(sql, &store);
        let err = result.expect_err("should return an error").to_string();
        assert!(err.contains("Unknown acceleration option"));
    }

    #[test]
    fn test_preprocess_invalid_dataset_option_errors() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT) WITH ("dataset.nonexistent" = 'value')"#;
        let result = preprocess_create_table_with_options(sql, &store);
        let err = result.expect_err("should return an error").to_string();
        assert!(err.contains("Unknown dataset option"));
    }

    #[test]
    fn test_preprocess_partition_by() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT, region TEXT, ts TIMESTAMP) PARTITION BY region";

        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");

        match result {
            PreprocessResult::Modified {
                sql: modified_sql,
                store_key,
            } => {
                assert!(
                    !modified_sql.to_uppercase().contains("PARTITION BY"),
                    "Modified SQL should not contain PARTITION BY: {modified_sql}"
                );
                assert_eq!(store_key.to_string(), "foo");
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have extensions for 'foo'");
        assert!(ddl_ext.acceleration.is_none());
        assert!(ddl_ext.partition_by.is_some());
    }

    #[test]
    fn test_preprocess_with_options_and_partition_by() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT, region TEXT) PARTITION BY region WITH ("acceleration.engine" = 'arrow')"#;

        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");

        match result {
            PreprocessResult::Modified {
                sql: modified_sql,
                store_key,
            } => {
                assert!(
                    !modified_sql.contains("acceleration."),
                    "Modified SQL should not contain acceleration options: {modified_sql}"
                );
                assert!(
                    !modified_sql.to_uppercase().contains("PARTITION BY"),
                    "Modified SQL should not contain PARTITION BY: {modified_sql}"
                );
                assert_eq!(store_key.to_string(), "foo");
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove(&TableReference::parse_str("foo"))
            .expect("should have extensions for 'foo'");
        assert!(ddl_ext.acceleration.is_some());
        assert!(ddl_ext.partition_by.is_some());
    }

    #[test]
    fn test_preprocess_partition_key_included_in_primary_key() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT, p_category TEXT, PRIMARY KEY (id, p_category)) PARTITION BY p_category";
        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Modified { .. }));
    }

    #[test]
    fn test_preprocess_partition_key_not_in_primary_key_errors() {
        let store = new_shared_store();
        let sql =
            "CREATE TABLE foo (id INT, p_category TEXT, PRIMARY KEY (id)) PARTITION BY p_category";
        let result = preprocess_create_table_with_options(sql, &store);
        let err = result.expect_err("should return an error").to_string();
        assert!(
            err.contains("p_category"),
            "Error should mention the missing partition column: {err}"
        );
        assert!(
            err.to_lowercase().contains("primary key"),
            "Error should mention primary key: {err}"
        );
    }

    #[test]
    fn test_preprocess_partition_key_with_column_level_primary_key_errors() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT PRIMARY KEY, p_category TEXT) PARTITION BY p_category";
        let result = preprocess_create_table_with_options(sql, &store);
        let err = result.expect_err("should return an error").to_string();
        assert!(
            err.contains("p_category"),
            "Error should mention the missing partition column: {err}"
        );
    }

    #[test]
    fn test_preprocess_partition_by_no_primary_key_succeeds() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT, p_category TEXT) PARTITION BY p_category";
        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Modified { .. }));
    }

    #[test]
    fn test_preprocess_partition_key_composite_primary_key_valid() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (p_id INT, p_category TEXT, name VARCHAR, PRIMARY KEY (p_id, p_category)) PARTITION BY p_category";
        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Modified { .. }));
    }

    #[test]
    fn test_preprocess_bucket_partition_key_in_primary_key() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT, region TEXT, PRIMARY KEY (id, region)) PARTITION BY bucket(4, region)";
        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Modified { .. }));
    }

    #[test]
    fn test_preprocess_bucket_partition_key_not_in_primary_key_errors() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT, region TEXT, PRIMARY KEY (id)) PARTITION BY bucket(4, region)";
        let result = preprocess_create_table_with_options(sql, &store);
        let err = result.expect_err("should return an error").to_string();
        assert!(
            err.contains("region"),
            "Error should mention the missing partition column: {err}"
        );
    }
}
