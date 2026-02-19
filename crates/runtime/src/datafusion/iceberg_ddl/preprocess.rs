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

//! Pre-processing for `CREATE TABLE ... WITH ("acceleration.*", "dataset.*")` SQL statements.
//!
//! `DataFusion`'s `SqlToRel` does not support `WITH (...)` options on `CREATE TABLE`
//! (it only matches `table_options: CreateTableOptions::None`). This module
//! intercepts such statements before they reach `DataFusion`, extracts the
//! DDL table options into the shared [`DdlOptionsStore`], and returns
//! modified SQL without the `WITH` clause so `DataFusion` can plan it normally.
//!
//! Keys must be double-quoted because dots are not valid in bare SQL identifiers:
//! ```sql
//! CREATE TABLE t (id INT) WITH (
//!     "acceleration.engine" = 'arrow',
//!     "dataset.time_column" = 'created_at'
//! )
//! ```

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::sql::sqlparser::ast::{CreateTableOptions, SqlOption, Statement};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::acceleration_options::{SharedDdlOptionsStore, parse_ddl_table_options};

/// Result of pre-processing: either the original SQL unchanged, or modified SQL
/// with DDL table options extracted.
#[derive(Debug)]
pub enum PreprocessResult {
    /// SQL was not a `CREATE TABLE ... WITH (acceleration.*|dataset.*)` — pass through unchanged.
    Unchanged,
    /// SQL was modified: `WITH` clause stripped, options stored.
    Modified {
        /// The rewritten SQL string without the `WITH` clause.
        sql: String,
        /// The store key used for the inserted options.
        store_key: String,
    },
}

/// Remove a previously inserted DDL option entry from the shared store.
///
/// Intended for error paths where preprocessing has inserted options but logical
/// planning failed before the analyzer could consume the entry.
///
/// # Errors
///
/// Returns an error if the store lock cannot be acquired.
pub fn cleanup_preprocessed_ddl_options(
    store: &SharedDdlOptionsStore,
    store_key: &str,
) -> DFResult<()> {
    let mut guard = store.write().map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to acquire DDL options store lock: {e}"
        ))
    })?;
    let _ = guard.remove(store_key);
    Ok(())
}

/// Pre-process a SQL string to extract `CREATE TABLE ... WITH (...)` options.
///
/// If the SQL is a `CREATE TABLE` with `WITH` options containing `acceleration.*`
/// or `dataset.*` keys, the options are parsed, stored in the shared store, and
/// the SQL is returned without the `WITH` clause so `DataFusion` can plan it
/// normally. Otherwise, the original SQL is returned unchanged.
///
/// # Errors
///
/// Returns an error if:
/// - The `WITH` options contain invalid keys or values.
/// - The `WITH` options contain keys with unrecognized prefixes (not `acceleration.*`
///   or `dataset.*`).
pub fn preprocess_create_table_with_options(
    sql: &str,
    ddl_store: &SharedDdlOptionsStore,
) -> DFResult<PreprocessResult> {
    // Quick check: if the SQL doesn't contain "WITH", skip parsing
    if !sql.to_uppercase().contains("WITH") {
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

    let CreateTableOptions::With(ref options) = create_table.table_options else {
        return Ok(PreprocessResult::Unchanged);
    };

    // Extract key-value pairs, separating recognized prefixes from unknown ones
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

    if recognized_opts.is_empty() {
        return Ok(PreprocessResult::Unchanged);
    }

    // Reject mixing recognized options with other WITH options for clarity
    if !other_opts.is_empty() {
        return Err(DataFusionError::Plan(
            "Cannot mix 'acceleration.*' or 'dataset.*' options with other WITH options in CREATE TABLE. \
             Use only 'acceleration.*' and/or 'dataset.*' options."
                .to_string(),
        ));
    }

    // Strip surrounding quotes from values (sqlparser includes them for string literals)
    let cleaned_opts: Vec<(String, String)> = recognized_opts
        .into_iter()
        .map(|(k, v)| {
            let v = v
                .trim_start_matches('\'')
                .trim_end_matches('\'')
                .trim_start_matches('"')
                .trim_end_matches('"')
                .to_string();
            (k, v)
        })
        .collect();

    let ddl_table_options = parse_ddl_table_options(&cleaned_opts)?;

    // Extract the table name for the store key
    let table_name = create_table.name.to_string();
    let store_key = table_name.clone();

    // Store the DDL table options
    {
        let mut guard = ddl_store.write().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to acquire DDL options store lock: {e}"
            ))
        })?;
        guard.insert(table_name, ddl_table_options);
    }

    // Reconstruct the CREATE TABLE without the WITH clause
    let mut modified = create_table.clone();
    modified.table_options = CreateTableOptions::None;
    let modified_sql = Statement::CreateTable(modified).to_string();

    Ok(PreprocessResult::Modified {
        sql: modified_sql,
        store_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::iceberg_ddl::acceleration_options::new_shared_store;

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

        let ddl_opts = store
            .write()
            .expect("store lock should be available")
            .remove("foo")
            .expect("should have options for 'foo'");
        let accel = ddl_opts.acceleration.expect("acceleration should be Some");
        assert_eq!(accel.engine.as_deref(), Some("arrow"));
        assert_eq!(accel.mode, spicepod::acceleration::Mode::Memory);
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

        let ddl_opts = store
            .write()
            .expect("store lock should be available")
            .remove("foo")
            .expect("should have options for 'foo'");
        assert!(ddl_opts.acceleration.is_none());
        assert_eq!(ddl_opts.dataset.time_column.as_deref(), Some("ts"));
        assert_eq!(
            ddl_opts.dataset.time_format,
            Some(spicepod::component::dataset::TimeFormat::Timestamp)
        );
    }

    #[test]
    fn test_preprocess_with_mixed_accel_and_dataset_options() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT, ts TIMESTAMP) WITH ("acceleration.engine" = 'arrow', "acceleration.refresh_mode" = 'append', "dataset.time_column" = 'ts')"#;

        let result = preprocess_create_table_with_options(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Modified { .. }));

        let ddl_opts = store
            .write()
            .expect("store lock should be available")
            .remove("foo")
            .expect("should have options for 'foo'");
        let accel = ddl_opts.acceleration.expect("acceleration should be Some");
        assert_eq!(accel.engine.as_deref(), Some("arrow"));
        assert_eq!(ddl_opts.dataset.time_column.as_deref(), Some("ts"));
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
}
