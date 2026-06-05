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

//! Shared ADBC helper functions used by both the `connector-adbc` data-connector crate
//! and the ADBC catalog connector in the runtime.
//!
//! Lives here (in `data_components`) rather than in either `runtime` or `connector-adbc`
//! to avoid a circular dependency: `connector-adbc` depends on `runtime`, and the ADBC
//! catalog connector lives inside `runtime`.

use adbc_core::options::OptionDatabase;
use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::{BigQueryDialect, Dialect};
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::ADBCPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use crate::FieldMetadata;

/// Builds the list of ADBC database options from connector parameters.
pub fn build_db_options(
    uri: &str,
    username: Option<&str>,
    password: Option<&str>,
    driver_options: Option<&str>,
) -> Vec<(OptionDatabase, adbc_core::options::OptionValue)> {
    let mut opts = vec![(OptionDatabase::Uri, uri.into())];
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
                let key = if key.starts_with("adbc.") {
                    key.to_string()
                } else {
                    format!("adbc.{key}")
                };
                opts.push((OptionDatabase::Other(key), value.trim().into()));
            } else {
                tracing::warn!("Ignoring malformed ADBC driver option (expected 'key=value')");
            }
        }
    }
    opts
}

/// Builds a hashed join-pushdown context identifier for ADBC connections.
///
/// Hashes all identity-relevant parameters so secrets never appear in `EXPLAIN` plans,
/// while still uniquely identifying a connection target for federation decisions.
pub fn build_join_context(
    uri: &str,
    username: Option<&str>,
    catalog: Option<&str>,
    schema: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(uri.as_bytes());
    hasher.update(b"\0");
    if let Some(u) = username {
        hasher.update(u.as_bytes());
    }
    hasher.update(b"\0");
    if let Some(c) = catalog {
        hasher.update(c.as_bytes());
    }
    hasher.update(b"\0");
    if let Some(s) = schema {
        hasher.update(s.as_bytes());
    }
    hasher.finalize().iter().fold(String::new(), |mut hash, b| {
        let _ = write!(hash, "{b:02x}");
        hash
    })
}

/// Returns the SQL dialect for the given ADBC driver name, if one is known.
pub fn dialect_for_driver(driver_name: &str) -> Option<Arc<dyn Dialect + Send + Sync>> {
    match driver_name {
        "bigquery" => Some(Arc::new(BigQueryDialect::new())),
        _ => None,
    }
}

/// Enriches a `TableProvider` with BigQuery-specific schema metadata retrieved via ADBC
/// information-schema queries (descriptions, source types, partitioning/clustering columns).
/// For non-BigQuery drivers this is a no-op and returns the provider unchanged.
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
                crate::metadata_enriched_table_provider(provider, table_metadata, field_metadata)
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
) -> Result<(HashMap<String, String>, FieldMetadata), Box<dyn std::error::Error + Send + Sync>> {
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
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
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
) -> Result<Vec<Vec<Option<String>>>, Box<dyn std::error::Error + Send + Sync>> {
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
        .map(|a| a.value(row))
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|a| a.value(row))
        })
        .map(str::trim)
        .filter(|v| !v.is_empty())
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
