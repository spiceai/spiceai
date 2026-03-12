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
use datafusion::sql::sqlparser::ast::{CreateTableOptions, SqlOption, Statement};
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
        store_key: String,
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
    store_key: &str,
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
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        // If sqlparser can't parse it, let `DataFusion` handle the error
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

    let has_with_options = with_options.is_some();
    let has_partition_by = partition_by_expr.is_some();

    if !has_with_options && !has_partition_by {
        return Ok(PreprocessResult::Unchanged);
    }

    // Process WITH options
    let mut extension = if let Some(ref options) = with_options {
        let (recognized_opts, other_opts) = classify_with_options(options)?;

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

        if recognized_opts.is_empty() {
            // Only PARTITION BY, no recognized WITH options — but has unrecognized WITH options
            if !other_opts.is_empty() {
                // WITH has only non-recognized options, nothing for us to extract.
                // Only proceed if we have PARTITION BY.
                CreateTableStatementExtension::default()
            } else {
                CreateTableStatementExtension::default()
            }
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
    let store_key = table_name.clone();

    // Store the DDL extensions
    {
        let mut guard = ddl_store.write().map_err(|e| {
            DataFusionError::Execution(format!("Failed to acquire DDL extension store lock: {e}"))
        })?;
        guard.insert(table_name, extension);
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

/// Classify `WITH` options into recognized (`acceleration.*`/`dataset.*`) and other options.
fn classify_with_options(
    options: &[SqlOption],
) -> DFResult<(Vec<(String, String)>, Vec<SqlOption>)> {
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

    Ok((recognized_opts, other_opts))
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
                assert_eq!(store_key, "foo");
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove("foo")
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
                assert_eq!(store_key, "foo");
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove("foo")
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
            .remove("foo")
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
                assert_eq!(store_key, "foo");
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove("foo")
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
                assert_eq!(store_key, "foo");
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        let ddl_ext = store
            .write()
            .expect("store lock should be available")
            .remove("foo")
            .expect("should have extensions for 'foo'");
        assert!(ddl_ext.acceleration.is_some());
        assert!(ddl_ext.partition_by.is_some());
    }
}
