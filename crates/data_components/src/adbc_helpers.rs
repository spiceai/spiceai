/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Shared helpers for ADBC data connectors and catalog connectors.
//!
//! This module provides functions that are used by both the ADBC data connector
//! and the ADBC catalog connector, avoiding circular dependencies between the
//! runtime and connector crates.

use adbc_core::options::OptionDatabase;
use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray};
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::sql::unparser::dialect::{BigQueryDialect, Dialect};
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::ADBCPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::sync::Arc;

use crate::{FieldMetadata, metadata_enriched_table_provider};

/// The `DuckDB` driver name used by ADBC connectors.
pub const DUCKDB_DRIVER: &str = "duckdb";

/// `DuckDB` exports this ADBC entry point instead of `AdbcDriverInit`.
pub const DUCKDB_ADBC_ENTRYPOINT: &[u8] = b"duckdb_adbc_init";

/// The settings every `DuckDB` database opens with, matching the `duckdb:`
/// connector: read-only, so Spice never takes the write lock or upgrades the
/// file, and a UTC session, so timestamps answer the same.
pub const DUCKDB_DB_DEFAULTS: &[(&str, &str)] =
    &[("access_mode", "read_only"), ("TimeZone", "UTC")];

/// Applies the `adbc.` prefix a driver-option key takes when it is written without
/// one. `bigquery.sql.dataset_id` and `adbc.bigquery.sql.dataset_id` name the same
/// option, so anything matching on a key has to agree on the spelling.
#[must_use]
pub fn normalize_driver_option_key(key: &str) -> String {
    if key.starts_with("adbc.") {
        key.to_string()
    } else {
        format!("adbc.{key}")
    }
}

/// Returns `duckdb_adbc_init` for `DuckDB`, which does not export `AdbcDriverInit`.
#[must_use]
pub fn driver_entrypoint(driver_name: &str) -> Option<&'static [u8]> {
    (driver_name == DUCKDB_DRIVER).then_some(DUCKDB_ADBC_ENTRYPOINT)
}

/// Builds the list of ADBC database options from connector parameters.
///
/// For `DuckDB` the file is passed as `path` (1.4.4 rejects `uri`; 1.4.4 and
/// 1.5.5 both accept `path`), followed by [`DUCKDB_DB_DEFAULTS`].
pub fn build_db_options(
    driver_name: &str,
    uri: &str,
    username: Option<&str>,
    password: Option<&str>,
    driver_options: Option<&str>,
) -> Vec<(OptionDatabase, adbc_core::options::OptionValue)> {
    let uri_option = if driver_name == DUCKDB_DRIVER {
        OptionDatabase::Other("path".to_string())
    } else {
        OptionDatabase::Uri
    };
    let mut opts = vec![(uri_option, uri.into())];
    if let Some(u) = username {
        opts.push((OptionDatabase::Username, u.into()));
    }
    if let Some(p) = password {
        opts.push((OptionDatabase::Password, p.into()));
    }
    if let Some(options_str) = driver_options {
        for pair in options_str.split(';') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((key, value)) = pair.split_once('=') {
                let key = key.trim();
                if key.is_empty() {
                    tracing::warn!("Ignoring ADBC driver option with empty key");
                    continue;
                }
                opts.push((
                    OptionDatabase::Other(normalize_driver_option_key(key)),
                    value.trim().into(),
                ));
            } else {
                tracing::warn!("Ignoring malformed ADBC driver option (expected 'key=value')");
            }
        }
    }
    if driver_name == DUCKDB_DRIVER {
        for &(key, value) in DUCKDB_DB_DEFAULTS {
            opts.push((OptionDatabase::Other(key.to_string()), value.into()));
        }
    }
    opts
}

/// Returns the SQL dialect for a given ADBC driver name, if a non-default one is
/// needed.
///
/// These are the stock `DataFusion` dialects, which rewrite no Spice function.
///
/// The catalog connector uses these for drivers other than `DuckDB`. Its
/// `DuckDB` dialect and function-support policy must stay paired in the runtime.
/// The dataset connector's `BigQuery` dialect rewrites Spice functions and must
/// only be used with its matching policy; the catalog keeps the stock dialect.
#[must_use]
pub fn dialect_for_driver(driver_name: &str) -> Option<Arc<dyn Dialect + Send + Sync>> {
    match driver_name {
        "bigquery" => Some(Arc::new(BigQueryDialect::new())),
        _ => None,
    }
}

/// Enriches a table provider with BigQuery-specific schema metadata (descriptions,
/// source types, partition flags, clustering ordinals) queried from the
/// `INFORMATION_SCHEMA` views. For non-BigQuery drivers this is a no-op.
pub async fn enrich_with_bigquery_metadata(
    driver_name: &str,
    pool: &Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    table_reference: &TableReference,
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    if !driver_name.eq_ignore_ascii_case("bigquery") {
        return provider;
    }

    match bigquery_schema_metadata(pool, table_reference).await {
        Ok((table_metadata, field_metadata)) => {
            if table_metadata.is_empty() && field_metadata.is_empty() {
                provider
            } else {
                metadata_enriched_table_provider(provider, table_metadata, field_metadata)
            }
        }
        Err(error) => {
            tracing::warn!(
                table = %table_reference,
                error = %error,
                "Failed to query BigQuery schema metadata via ADBC; registering without source metadata"
            );
            provider
        }
    }
}

async fn bigquery_schema_metadata(
    pool: &Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    table_reference: &TableReference,
) -> std::result::Result<
    (HashMap<String, String>, FieldMetadata),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let table_name = bigquery_string_literal(table_reference.table());
    let table_options = bigquery_information_schema_table(table_reference, "TABLE_OPTIONS");
    let column_field_paths =
        bigquery_information_schema_table(table_reference, "COLUMN_FIELD_PATHS");
    let columns = bigquery_information_schema_table(table_reference, "COLUMNS");

    let table_sql = format!(
        "SELECT option_value FROM {table_options} WHERE table_name = {table_name} AND option_name = 'description' AND option_value IS NOT NULL AND option_value != ''"
    );
    let comment_sql = format!(
        "SELECT field_path, description FROM {column_field_paths} WHERE table_name = {table_name} AND description IS NOT NULL AND description != ''"
    );
    let column_sql = format!(
        "SELECT column_name, data_type, CASE WHEN is_partitioning_column = 'YES' THEN 'true' ELSE NULL END, CAST(clustering_ordinal_position AS STRING) FROM {columns} WHERE table_name = {table_name}"
    );

    let mut table_metadata = HashMap::new();
    if let Some(comment) = first_string_result(pool, table_sql).await? {
        table_metadata.insert(crate::DESCRIPTION_METADATA_KEY.to_string(), comment);
    }

    let mut field_metadata = FieldMetadata::new();
    for row in string_column_results(pool, column_sql, 4).await? {
        let [column_name, source_type, partition, clustering] = row.as_slice() else {
            continue;
        };
        let Some(column_name) = column_name else {
            continue;
        };
        let metadata = field_metadata.entry(column_name.clone()).or_default();
        if let Some(source_type) = source_type {
            metadata.insert(
                crate::SOURCE_TYPE_METADATA_KEY.to_string(),
                source_type.clone(),
            );
        }
        if partition.as_deref() == Some("true") {
            metadata.insert(
                crate::PARTITION_METADATA_KEY.to_string(),
                "true".to_string(),
            );
        }
        if let Some(clustering) = clustering {
            metadata.insert(
                crate::CLUSTERING_METADATA_KEY.to_string(),
                clustering.clone(),
            );
        }
    }

    for row in string_column_results(pool, comment_sql, 2).await? {
        let [field_path, comment] = row.as_slice() else {
            continue;
        };
        let (Some(field_path), Some(comment)) = (field_path, comment) else {
            continue;
        };
        if field_path.contains('.') {
            continue;
        }
        field_metadata
            .entry(field_path.clone())
            .or_default()
            .insert(crate::DESCRIPTION_METADATA_KEY.to_string(), comment.clone());
    }

    Ok((table_metadata, field_metadata))
}

async fn first_string_result(
    pool: &Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    sql: String,
) -> std::result::Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let conn = Arc::clone(pool).connect().await?;
    let batches: Vec<_> = query_arrow(conn, sql, None).await?.try_collect().await?;
    for batch in &batches {
        if batch.num_columns() == 0 {
            continue;
        }
        let values = batch.column(0);
        for row in 0..batch.num_rows() {
            if let Some(value) = string_value(values, row) {
                return Ok(Some(value.to_string()));
            }
        }
    }
    Ok(None)
}

async fn string_column_results(
    pool: &Arc<ADBCPool<adbc_driver_manager::ManagedDatabase>>,
    sql: String,
    column_count: usize,
) -> std::result::Result<Vec<Vec<Option<String>>>, Box<dyn std::error::Error + Send + Sync>> {
    let conn = Arc::clone(pool).connect().await?;
    let batches: Vec<_> = query_arrow(conn, sql, None).await?.try_collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        if batch.num_columns() < column_count {
            continue;
        }
        for row in 0..batch.num_rows() {
            values.push(
                (0..column_count)
                    .map(|column| string_value(batch.column(column), row).map(ToString::to_string))
                    .collect(),
            );
        }
    }
    Ok(values)
}

fn string_value(array: &ArrayRef, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }

    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| array.value(row))
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|array| array.value(row))
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn bigquery_information_schema_table(table_reference: &TableReference, view: &str) -> String {
    let mut parts = Vec::new();
    if let Some(catalog) = table_reference.catalog() {
        parts.push(catalog.to_string());
    }
    if let Some(schema) = table_reference.schema() {
        parts.push(schema.to_string());
    }
    parts.push("INFORMATION_SCHEMA".to_string());
    parts.push(view.to_string());

    format!(
        "`{}`",
        parts
            .into_iter()
            .map(|part| part.replace('`', "\\`"))
            .collect::<Vec<_>>()
            .join(".")
    )
}

fn bigquery_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_driver_entrypoint() {
        assert_eq!(
            driver_entrypoint("duckdb"),
            Some(b"duckdb_adbc_init".as_slice())
        );
        for driver in ["sqlite", "postgresql", "snowflake", "other"] {
            assert_eq!(driver_entrypoint(driver), None, "driver: {driver}");
        }
    }

    fn assert_db_options_eq(
        actual: Vec<(OptionDatabase, adbc_core::options::OptionValue)>,
        expected: Vec<(OptionDatabase, &str)>,
    ) {
        assert_eq!(actual.len(), expected.len());
        for ((key, value), (expected_key, expected_value)) in actual.into_iter().zip(expected) {
            assert_eq!(key, expected_key);
            let adbc_core::options::OptionValue::String(value) = value else {
                panic!("expected a string database option, got {value:?}");
            };
            assert_eq!(value, expected_value);
        }
    }

    #[test]
    fn test_build_db_options_duckdb_defaults() {
        assert_db_options_eq(
            build_db_options("duckdb", "test.db", None, None, None),
            vec![
                (OptionDatabase::Other("path".into()), "test.db"),
                (OptionDatabase::Other("access_mode".into()), "read_only"),
                (OptionDatabase::Other("TimeZone".into()), "UTC"),
            ],
        );
    }

    #[test]
    fn test_build_db_options_duckdb_driver_options_keep_the_adbc_prefix() {
        assert_db_options_eq(
            build_db_options(
                "duckdb",
                "test.db",
                None,
                None,
                Some("access_mode=read_write;adbc.custom=value"),
            ),
            vec![
                (OptionDatabase::Other("path".into()), "test.db"),
                (
                    OptionDatabase::Other("adbc.access_mode".into()),
                    "read_write",
                ),
                (OptionDatabase::Other("adbc.custom".into()), "value"),
                (OptionDatabase::Other("access_mode".into()), "read_only"),
                (OptionDatabase::Other("TimeZone".into()), "UTC"),
            ],
        );
    }

    #[test]
    fn test_build_db_options_uri_only() {
        let opts = build_db_options("snowflake", "file:test.db", None, None, None);
        assert_eq!(opts.len(), 1);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert!(
            matches!(&opts[0].1, adbc_core::options::OptionValue::String(s) if s == "file:test.db")
        );
    }

    #[test]
    fn test_build_db_options_with_username_password() {
        let opts = build_db_options(
            "postgresql",
            "postgres://host/db",
            Some("admin"),
            Some("secret"),
            None,
        );
        assert_eq!(opts.len(), 3);

        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert!(
            matches!(&opts[0].1, adbc_core::options::OptionValue::String(s) if s == "postgres://host/db")
        );

        assert_eq!(opts[1].0, OptionDatabase::Username);
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "admin"));

        assert_eq!(opts[2].0, OptionDatabase::Password);
        assert!(matches!(&opts[2].1, adbc_core::options::OptionValue::String(s) if s == "secret"));
    }

    #[test]
    fn test_build_db_options_username_only() {
        let opts = build_db_options("sqlite", "sqlite:test.db", Some("user"), None, None);
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert_eq!(opts[1].0, OptionDatabase::Username);
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "user"));
    }

    #[test]
    fn test_build_db_options_with_driver_options_unprefixed() {
        let opts = build_db_options(
            "snowflake",
            "uri://db",
            None,
            None,
            Some("snowflake.sql.db=MY_DB;snowflake.sql.schema=PUBLIC"),
        );
        assert_eq!(opts.len(), 3);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert_eq!(
            opts[1].0,
            OptionDatabase::Other("adbc.snowflake.sql.db".to_string())
        );
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "MY_DB"));
        assert_eq!(
            opts[2].0,
            OptionDatabase::Other("adbc.snowflake.sql.schema".to_string())
        );
        assert!(matches!(&opts[2].1, adbc_core::options::OptionValue::String(s) if s == "PUBLIC"));
    }

    #[test]
    fn test_build_db_options_with_driver_options_prefixed() {
        let opts = build_db_options(
            "snowflake",
            "uri://db",
            None,
            None,
            Some("adbc.snowflake.sql.db=MY_DB;adbc.snowflake.sql.schema=PUBLIC"),
        );
        assert_eq!(opts.len(), 3);
        assert_eq!(
            opts[1].0,
            OptionDatabase::Other("adbc.snowflake.sql.db".to_string())
        );
        assert_eq!(
            opts[2].0,
            OptionDatabase::Other("adbc.snowflake.sql.schema".to_string())
        );
    }

    #[test]
    fn test_build_db_options_driver_options_trailing_semicolon() {
        let opts = build_db_options("snowflake", "uri://db", None, None, Some("key=value;"));
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[1].0, OptionDatabase::Other("adbc.key".to_string()));
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "value"));
    }

    #[test]
    fn test_build_db_options_driver_options_malformed_ignored() {
        let opts = build_db_options(
            "snowflake",
            "uri://db",
            None,
            None,
            Some("good=val;bad_no_equals;another=ok"),
        );
        assert_eq!(opts.len(), 3); // uri + good + another (bad_no_equals skipped)
    }

    #[test]
    fn test_build_db_options_driver_options_empty_key_ignored() {
        let opts = build_db_options("snowflake", "uri://db", None, None, Some("=value;good=ok"));
        assert_eq!(opts.len(), 2); // uri + good (empty key skipped)
        assert_eq!(opts[1].0, OptionDatabase::Other("adbc.good".to_string()));
    }
}
