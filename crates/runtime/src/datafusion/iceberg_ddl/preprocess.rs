/*
Copyright 2024-2025, Spice AI, Inc.

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

//! Pre-processing for `CREATE TABLE ... WITH ("acceleration.*")` SQL statements.
//!
//! `DataFusion`'s `SqlToRel` does not support `WITH (...)` options on `CREATE TABLE`
//! (it only matches `table_options: CreateTableOptions::None`). This module
//! intercepts such statements before they reach `DataFusion`, extracts the
//! acceleration options into the shared [`AccelerationOptionsStore`], and returns
//! modified SQL without the `WITH` clause so `DataFusion` can plan it normally.
//!
//! Keys must be double-quoted because dots are not valid in bare SQL identifiers:
//! ```sql
//! CREATE TABLE t (id INT) WITH ("acceleration.engine" = 'arrow')
//! ```

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::sql::sqlparser::ast::{CreateTableOptions, SqlOption, Statement};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::acceleration_options::{SharedAccelerationOptionsStore, parse_acceleration_options};

/// Result of pre-processing: either the original SQL unchanged, or modified SQL
/// with acceleration options extracted.
#[derive(Debug)]
pub enum PreprocessResult {
    /// SQL was not a `CREATE TABLE ... WITH (acceleration.*)` — pass through unchanged.
    Unchanged,
    /// SQL was modified: `WITH` clause stripped, acceleration options stored.
    /// Contains the rewritten SQL string.
    Modified(String),
}

/// Pre-process a SQL string to extract `CREATE TABLE ... WITH (acceleration.*)` options.
///
/// If the SQL is a `CREATE TABLE` with `WITH` options containing `acceleration.*` keys,
/// the options are parsed, stored in `accel_store`, and the SQL is returned without
/// the `WITH` clause. Otherwise, the original SQL is returned unchanged.
///
/// # Errors
///
/// Returns an error if:
/// - The `WITH` options contain invalid `acceleration.*` keys or values.
/// - The `WITH` options mix `acceleration.*` keys with non-acceleration keys.
pub fn preprocess_create_table_acceleration(
    sql: &str,
    accel_store: &SharedAccelerationOptionsStore,
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

    // Separate acceleration options from other options
    let mut accel_opts = Vec::new();
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

                if key_str.starts_with("acceleration.") {
                    accel_opts.push((key_str, value_str));
                } else {
                    other_opts.push(opt.clone());
                }
            }
            _ => {
                other_opts.push(opt.clone());
            }
        }
    }

    if accel_opts.is_empty() {
        return Ok(PreprocessResult::Unchanged);
    }

    // Reject mixing acceleration and non-acceleration options for clarity
    if !other_opts.is_empty() {
        return Err(DataFusionError::Plan(
            "Cannot mix 'acceleration.*' options with other WITH options in CREATE TABLE. \
             Use only 'acceleration.*' options."
                .to_string(),
        ));
    }

    // Parse acceleration options
    // Strip surrounding quotes from values (sqlparser includes them for string literals)
    let cleaned_opts: Vec<(String, String)> = accel_opts
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

    let acceleration = parse_acceleration_options(&cleaned_opts)?;

    // Extract the table name for the store key
    let table_name = create_table.name.to_string();

    // Store the acceleration options
    {
        let mut store = accel_store.write().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to acquire acceleration options store lock: {e}"
            ))
        })?;
        store.insert(table_name, acceleration);
    }

    // Reconstruct the CREATE TABLE without the WITH clause
    let mut modified = create_table.clone();
    modified.table_options = CreateTableOptions::None;
    let modified_sql = Statement::CreateTable(modified).to_string();

    Ok(PreprocessResult::Modified(modified_sql))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::iceberg_ddl::acceleration_options::new_shared_store;

    #[test]
    fn test_preprocess_no_with_clause() {
        let store = new_shared_store();
        let sql = "CREATE TABLE foo (id INT, name VARCHAR)";
        let result = preprocess_create_table_acceleration(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Unchanged));
    }

    #[test]
    fn test_preprocess_non_create_table() {
        let store = new_shared_store();
        let sql = "SELECT * FROM foo";
        let result = preprocess_create_table_acceleration(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Unchanged));
    }

    #[test]
    fn test_preprocess_with_acceleration_options() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT, name VARCHAR) WITH ("acceleration.engine" = 'arrow', "acceleration.mode" = 'memory')"#;

        let result = preprocess_create_table_acceleration(sql, &store).expect("should succeed");

        match result {
            PreprocessResult::Modified(modified_sql) => {
                // The modified SQL should not contain acceleration options
                assert!(
                    !modified_sql.contains("acceleration."),
                    "Modified SQL should not contain acceleration options: {modified_sql}"
                );
                // Should still be valid CREATE TABLE
                assert!(modified_sql.to_uppercase().contains("CREATE TABLE"));
            }
            PreprocessResult::Unchanged => panic!("Expected Modified result"),
        }

        // Check that the store has the options
        let accel = store
            .write()
            .unwrap()
            .remove("foo")
            .expect("should have options for 'foo'");
        assert_eq!(accel.engine.as_deref(), Some("arrow"));
        assert_eq!(accel.mode, spicepod::acceleration::Mode::Memory);
    }

    #[test]
    fn test_preprocess_with_non_acceleration_options_unchanged() {
        let store = new_shared_store();
        // WITH options that don't have acceleration. prefix
        let sql = "CREATE TABLE foo (id INT) WITH (fillfactor = 70)";
        let result = preprocess_create_table_acceleration(sql, &store).expect("should succeed");
        assert!(matches!(result, PreprocessResult::Unchanged));
    }

    #[test]
    fn test_preprocess_mixed_options_errors() {
        let store = new_shared_store();
        let sql =
            r#"CREATE TABLE foo (id INT) WITH ("acceleration.engine" = 'arrow', fillfactor = 70)"#;
        let result = preprocess_create_table_acceleration(sql, &store);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Cannot mix"));
    }

    #[test]
    fn test_preprocess_invalid_acceleration_option_errors() {
        let store = new_shared_store();
        let sql = r#"CREATE TABLE foo (id INT) WITH ("acceleration.nonexistent" = 'value')"#;
        let result = preprocess_create_table_acceleration(sql, &store);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown acceleration option"));
    }
}
