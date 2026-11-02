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

/// Builds raw database settings for a Spice-managed `DuckDB` source.
///
/// Direct `adbc:` connections use [`build_db_options`] instead, preserving the
/// driver's access mode and host time zone defaults.
#[must_use]
pub fn managed_duckdb_options(
    path: &str,
    read_only: bool,
) -> Vec<(OptionDatabase, adbc_core::options::OptionValue)> {
    let mut options = vec![(OptionDatabase::Other("path".into()), path.into())];
    if read_only {
        options.push((
            OptionDatabase::Other("access_mode".into()),
            "read_only".into(),
        ));
    }
    options.push((OptionDatabase::Other("TimeZone".into()), "UTC".into()));
    options
}

/// Errors during blocking ADBC initialization.
#[derive(Debug, snafu::Snafu)]
pub enum AdbcInitError {
    #[snafu(display("Failed to load ADBC driver '{driver_location}': {source}"))]
    LoadDriver {
        driver_location: String,
        source: adbc_core::error::Error,
    },
    #[snafu(display("Failed to create ADBC database: {source}"))]
    CreateDatabase { source: adbc_core::error::Error },
    #[snafu(display("ADBC driver initialization timed out after 120 seconds"))]
    Timeout,
    #[snafu(display("ADBC initialization task failed: {source}"))]
    Join { source: tokio::task::JoinError },
    #[snafu(display("{source}"))]
    Initialize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Loads an installed driver and opens one database. Call on a blocking thread.
///
/// # Errors
/// Returns the driver loading or database initialization error.
pub fn load_adbc_database(
    driver_location: &str,
    entrypoint: Option<&[u8]>,
    options: Vec<(OptionDatabase, adbc_core::options::OptionValue)>,
) -> Result<adbc_driver_manager::ManagedDatabase, AdbcInitError> {
    use adbc_core::{Driver as _, LOAD_FLAG_DEFAULT, options::AdbcVersion};
    use snafu::ResultExt as _;
    let mut driver = adbc_driver_manager::ManagedDriver::load_from_name(
        driver_location,
        entrypoint,
        AdbcVersion::V110,
        LOAD_FLAG_DEFAULT,
        None,
    )
    .context(LoadDriverSnafu { driver_location })?;
    driver
        .new_database_with_opts(options)
        .context(CreateDatabaseSnafu)
}

/// Errors opening the installed `DuckDB` for a managed connector.
#[derive(Debug, snafu::Snafu)]
pub enum ManagedDuckDbError {
    #[snafu(display(
        "Failed to load the installed DuckDB library. Install libduckdb on the system library path or register the 'duckdb' ADBC driver manifest. For details, visit: https://spiceai.org/docs/components/data-connectors/{connector}"
    ))]
    MissingLibrary {
        connector: &'static str,
        source: AdbcInitError,
    },
    #[snafu(display(
        "Failed to load the installed DuckDB library: version {found} is older than the minimum supported version 1.5.0. Install DuckDB 1.5.0 or later. For details, visit: https://spiceai.org/docs/components/data-connectors/{connector}"
    ))]
    UnsupportedVersion {
        found: String,
        connector: &'static str,
    },
    #[snafu(display(
        "Failed to determine the installed DuckDB version '{found}'. Install DuckDB 1.5.0 or later. For details, visit: https://spiceai.org/docs/components/data-connectors/{connector}"
    ))]
    InvalidVersion {
        found: String,
        connector: &'static str,
    },
    #[snafu(display("Failed to query the installed DuckDB version: {source}"))]
    QueryVersion {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Compares a `DuckDB` version with the minimum supported version, 1.5.0.
/// Returns `None` when the version cannot be parsed.
#[must_use]
pub fn duckdb_version_is_supported(version: &str) -> Option<bool> {
    let version = version.strip_prefix('v').unwrap_or(version);
    let mut parts = version.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    let patch = parts.next()?.split('-').next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch) >= (1, 5, 0))
}

/// Checks the installed version once. Call on a blocking thread after opening
/// a managed database; direct `adbc:` connections retain their driver defaults.
///
/// # Errors
/// Returns an error for an old, unknown, or unreadable version.
pub fn check_duckdb_version(
    database: &adbc_driver_manager::ManagedDatabase,
    connector: &'static str,
) -> Result<(), ManagedDuckDbError> {
    use adbc_core::{Connection as _, Database as _, Statement as _};
    let query = || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut connection = database.new_connection()?;
        let mut statement = connection.new_statement()?;
        statement.set_sql_query("SELECT version()")?;
        let mut reader = statement.execute()?;
        let batch = reader.next().ok_or("DuckDB returned no version")??;
        if batch.num_rows() != 1 || batch.num_columns() != 1 {
            return Err("DuckDB returned an invalid version result".into());
        }
        let column = batch.column(0);
        if column.is_null(0) {
            return Err("DuckDB returned a NULL version".into());
        }
        if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(values.value(0).to_owned());
        }
        if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(values.value(0).to_owned());
        }
        Err("DuckDB returned a non-string version".into())
    };
    let found = query().map_err(|source| ManagedDuckDbError::QueryVersion { source })?;
    match duckdb_version_is_supported(&found) {
        Some(true) => Ok(()),
        Some(false) => Err(ManagedDuckDbError::UnsupportedVersion { found, connector }),
        None => Err(ManagedDuckDbError::InvalidVersion { found, connector }),
    }
}

/// Bounds the wait for blocking driver initialization to two minutes.
///
/// Aborting cannot interrupt FFI already executing on the blocking thread.
///
/// # Errors
/// Returns the initialization, task, or timeout error.
pub async fn await_adbc_initialization<T, E>(
    handle: tokio::task::JoinHandle<Result<T, E>>,
) -> Result<T, AdbcInitError>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let abort = handle.abort_handle();
    match tokio::time::timeout(std::time::Duration::from_mins(2), handle).await {
        Ok(Ok(result)) => result.map_err(|source| AdbcInitError::Initialize {
            source: source.into(),
        }),
        Ok(Err(source)) => Err(AdbcInitError::Join { source }),
        Err(_) => {
            abort.abort();
            Err(AdbcInitError::Timeout)
        }
    }
}

/// Moves driver-owned resources onto the shared blocking cleanup worker.
pub fn offload_adbc_drop(resource: impl Send + 'static) {
    match adbc_cleanup_sender().try_send(Box::new(resource)) {
        Ok(()) => {}
        Err(std::sync::mpsc::TrySendError::Full(resource)) => offload_adbc_factory_drop(
            resource,
            "ADBC cleanup queue full; offloading drop to overflow thread",
        ),
        Err(std::sync::mpsc::TrySendError::Disconnected(resource)) => offload_adbc_factory_drop(
            resource,
            "ADBC cleanup channel closed; offloading drop to overflow thread",
        ),
    }
}

const ADBC_CLEANUP_QUEUE_CAPACITY: usize = 64;

fn offload_adbc_factory_drop(factory: Box<dyn Send>, reason: &str) {
    tracing::warn!("{reason}");
    if std::thread::Builder::new()
        .name("adbc-cleanup-overflow".to_string())
        .spawn(move || {
            drop(factory);
        })
        .is_err()
    {
        tracing::warn!("Failed to spawn overflow ADBC cleanup thread; dropping inline");
    }
}

/// Returns a sender for offloading ADBC factory cleanup to a dedicated
/// background thread. The worker thread is created once (on first use) and
/// processes drop work sequentially with bounded buffering, avoiding both
/// unbounded thread spawns and unbounded cleanup backlog growth.
fn adbc_cleanup_sender() -> &'static std::sync::mpsc::SyncSender<Box<dyn Send>> {
    static SENDER: std::sync::OnceLock<std::sync::mpsc::SyncSender<Box<dyn Send>>> =
        std::sync::OnceLock::new();
    SENDER.get_or_init(|| {
        let (tx, rx) =
            std::sync::mpsc::sync_channel::<Box<dyn Send>>(ADBC_CLEANUP_QUEUE_CAPACITY);
        if std::thread::Builder::new()
            .name("adbc-cleanup".to_string())
            .spawn(move || {
                for factory in rx {
                    drop(factory);
                }
            })
            .is_err()
        {
            tracing::warn!(
                "Failed to spawn ADBC cleanup worker thread; subsequent cleanup will use the overflow thread path or be performed inline as a last resort"
            );
        }
        tx
    })
}

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

/// Values of secret-looking keys shorter than this are not collected: a short
/// value under a key that merely contains `key` or `auth` is more likely a
/// setting than a credential, and replacing it everywhere would make the
/// message unreadable. Explicit passwords are always collected.
const MIN_HEURISTIC_SECRET_LEN: usize = 4;

/// Secrets shorter than this are only replaced where they stand alone (not
/// inside a longer word), so a short password does not mangle the message.
const MIN_UNBOUNDED_SECRET_LEN: usize = 4;

/// Query-string and `driver_options` keys (after any `adbc.` style prefix)
/// that name a credential outright, so their values are collected at any
/// length. Keys that only contain `key` or `auth` keep the length cutoff.
const CREDENTIAL_KEY_MARKERS: &[&str] =
    &["password", "passwd", "pwd", "secret", "token", "credential"];

/// Query-string and `driver_options` keys whose values are treated as secrets.
const SECRET_KEY_MARKERS: &[&str] = &[
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "key",
    "auth",
    "credential",
];

fn percent_decoded(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

/// The credential values an ADBC connection was configured with: the whole URI,
/// the `password` parameter, the password (or token-only user) in the URI's
/// userinfo, and the values of secret-looking query-string and
/// `driver_options` keys. Each value is included both as written and
/// percent-decoded, because a driver may echo either form.
///
/// Drivers do not all split a URI the way a URL parser does (`libpq` takes the
/// userinfo up to the last `@` before the path even when the password holds a
/// raw `?`, `#` or `\\`, and splits the query only on `&`), so the userinfo and
/// query are collected under each reading.
///
/// For `DuckDB` the URI is the database file's path, which a user needs in an
/// error to fix a wrong path, so the whole URI is kept; its query-string
/// credentials (a `MotherDuck` token) are still collected.
///
/// Pass the result to [`scrub_connection_secrets`] before a driver error is
/// shown to the user or logged.
#[must_use]
pub fn connection_secrets(
    driver_name: &str,
    uri: &str,
    password: Option<&str>,
    driver_options: Option<&str>,
) -> Vec<String> {
    let mut secrets = Vec::new();
    let mut push = |value: &str, min_len: usize| {
        for candidate in [value.to_string(), percent_decoded(value)] {
            if !candidate.is_empty() && candidate.len() >= min_len && !secrets.contains(&candidate)
            {
                secrets.push(candidate);
            }
        }
    };

    if driver_name != DUCKDB_DRIVER {
        push(uri, 1);
    }
    if let Some(password) = password {
        push(password, 1);
    }

    // Userinfo: everything before the last `@` of the authority that follows
    // `://`, with the authority ending at the path (driver reading) or at the
    // first of `/ \\ ? #` (URL reading).
    if let Some(idx) = uri.find("://") {
        let after = &uri[idx + "://".len()..];
        for delimiters in [&['/'][..], &['/', '\\', '?', '#'][..]] {
            let authority_end = after.find(delimiters).unwrap_or(after.len());
            if let Some(at) = after[..authority_end].rfind('@') {
                let userinfo = &after[..at];
                match userinfo.split_once(':') {
                    Some((_user, pass)) => push(pass, 1),
                    // A user with no password is usually a token.
                    None => push(userinfo, 1),
                }
            }
        }
    }

    let normalized_key = |key: &str| percent_decoded(key.trim()).to_ascii_lowercase();
    // The query both with the URL fragment removed and, as libpq reads it,
    // with `#` kept as data.
    let query = uri.split_once('?').map(|(_, q)| q);
    let queries = query
        .into_iter()
        .flat_map(|q| [q, q.split('#').next().unwrap_or_default()]);
    for pair in queries
        .flat_map(|q| q.split('&').chain(q.split(['&', ';'])))
        .chain(driver_options.into_iter().flat_map(|o| o.split(';')))
    {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        let key = normalized_key(key);
        let key = key.rsplit('.').next().unwrap_or_default();
        if key == "pass"
            || CREDENTIAL_KEY_MARKERS
                .iter()
                .any(|marker| key.contains(marker))
        {
            push(value.trim(), 1);
        } else if SECRET_KEY_MARKERS.iter().any(|marker| key.contains(marker)) {
            push(value.trim(), MIN_HEURISTIC_SECRET_LEN);
        }
    }

    // Longest first, so a secret that contains another is replaced whole.
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    secrets
}

/// `message` with every value from [`connection_secrets`] replaced by `***`.
/// A secret shorter than four bytes is replaced only where it is not part of a
/// longer alphanumeric word.
#[must_use]
pub fn scrub_connection_secrets(message: &str, secrets: &[String]) -> String {
    secrets.iter().fold(message.to_string(), |message, secret| {
        if secret.len() >= MIN_UNBOUNDED_SECRET_LEN {
            return message.replace(secret.as_str(), "***");
        }
        let is_word = |c: Option<char>| c.is_some_and(char::is_alphanumeric);
        let mut out = String::with_capacity(message.len());
        let mut last = 0;
        for (start, _) in message.match_indices(secret.as_str()) {
            if start < last {
                continue;
            }
            let end = start + secret.len();
            if is_word(message[..start].chars().next_back())
                || is_word(message[end..].chars().next())
            {
                continue;
            }
            out.push_str(&message[last..start]);
            out.push_str("***");
            last = end;
        }
        out.push_str(&message[last..]);
        out
    })
}

/// Builds the list of ADBC database options from connector parameters.
///
/// For `DuckDB` the file is passed as `path` (1.4.4 rejects `uri`; 1.4.4 and
/// 1.5.5 both accept `path`). Access mode and time zone retain driver defaults.
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
    #[test]
    fn managed_duckdb_version_floor() {
        for (version, expected) in [
            ("v1.4.4", Some(false)),
            ("v1.5.0", Some(true)),
            ("v2.0.0", Some(true)),
            ("v1.5.6-dev123", Some(true)),
            ("unknown", None),
        ] {
            assert_eq!(
                super::duckdb_version_is_supported(version),
                expected,
                "{version}"
            );
        }
    }

    #[test]
    fn managed_duckdb_version_error_text() {
        for connector in ["duckdb", "ducklake"] {
            assert_eq!(
                super::ManagedDuckDbError::UnsupportedVersion {
                    found: "v1.4.4".into(),
                    connector
                }
                .to_string(),
                format!(
                    "Failed to load the installed DuckDB library: version v1.4.4 is older than the minimum supported version 1.5.0. Install DuckDB 1.5.0 or later. For details, visit: https://spiceai.org/docs/components/data-connectors/{connector}"
                )
            );
        }
    }

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
    fn test_build_db_options_duckdb_has_no_managed_defaults() {
        assert_db_options_eq(
            build_db_options("duckdb", "test.db", None, None, None),
            vec![(OptionDatabase::Other("path".into()), "test.db")],
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
            ],
        );
    }

    #[test]
    fn managed_duckdb_file_is_read_only_and_utc() {
        assert_db_options_eq(
            managed_duckdb_options("test.db", true),
            vec![
                (OptionDatabase::Other("path".into()), "test.db"),
                (OptionDatabase::Other("access_mode".into()), "read_only"),
                (OptionDatabase::Other("TimeZone".into()), "UTC"),
            ],
        );
    }

    #[test]
    fn managed_duckdb_memory_is_writable_and_utc() {
        assert_db_options_eq(
            managed_duckdb_options(":memory:", false),
            vec![
                (OptionDatabase::Other("path".into()), ":memory:"),
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

#[cfg(test)]
mod scrub_tests {
    use super::*;

    #[test]
    fn scrub_connection_secrets_removes_uri_and_credentials() {
        let uri = "postgresql://alice:CANARY%ZZTOKEN@db.example.com:5432/app?sslmode=require&access_token=tok-secret-1";
        let secrets = connection_secrets(
            "postgresql",
            uri,
            Some("param-secret"),
            Some("adbc.token=opt-secret;adbc.timeout=30"),
        );
        for message in [
            format!("connect failed for {uri}"),
            "invalid percent-encoded token: \"CANARY%ZZTOKEN\"".to_string(),
            "auth failed for password param-secret".to_string(),
            "bad token tok-secret-1 / opt-secret".to_string(),
        ] {
            let scrubbed = scrub_connection_secrets(&message, &secrets);
            for secret in [
                "CANARY",
                "param-secret",
                "tok-secret-1",
                "opt-secret",
                "alice:",
            ] {
                assert!(!scrubbed.contains(secret), "{secret} leaked: {scrubbed}");
            }
        }
        // Non-secret query values and options are kept.
        let message = "sslmode=require timeout 30 at db.example.com";
        assert_eq!(scrub_connection_secrets(message, &secrets), message);
    }

    #[test]
    fn scrub_connection_secrets_handles_token_user_and_decoded_forms() {
        let secrets =
            connection_secrets("snowflake", "snowflake://tok%40en123@acct/db", None, None);
        assert_eq!(
            scrub_connection_secrets("rejected tok@en123 and tok%40en123", &secrets),
            "rejected *** and ***"
        );
        // Delimiters a URL parser would stop at, but libpq keeps in the password.
        for uri in [
            "postgresql://alice:CANARY?SECRET%ZZ@localhost/db",
            "postgresql://alice:CANARY#SECRET%ZZ@localhost/db",
            "postgresql://alice:CANARY\\SECRET%ZZ@localhost/db",
            "postgresql://localhost/db?password=CANARY;SECRET%ZZ",
            "postgresql://localhost/db?pass%77ord=CANARY%ZZSECRET",
        ] {
            let secrets = connection_secrets("postgresql", uri, None, None);
            let start = uri.find("CANARY").expect("canary");
            let echoed = &uri[start..uri[start..].find('@').map_or(uri.len(), |i| start + i)];
            let scrubbed =
                scrub_connection_secrets(&format!("invalid token: \"{echoed}\""), &secrets);
            assert_eq!(scrubbed, "invalid token: \"***\"", "{uri}");
        }
        // Short explicit credentials, echoed on their own.
        let secrets = connection_secrets(
            "postgresql",
            "postgresql://localhost/db?token=t1",
            Some("abc"),
            Some("adbc.secret=s2;adbc.api_key=k3"),
        );
        assert_eq!(
            scrub_connection_secrets(
                "driver rejected password abc, token t1, secret s2, key k3",
                &secrets
            ),
            "driver rejected password ***, token ***, secret ***, key k3"
        );
        // A `#` that libpq keeps as data does not hide a later password.
        let uri = "postgresql://localhost/db?application_name=x#y&password=CANARY%ZZSECRET";
        let secrets = connection_secrets("postgresql", uri, None, None);
        assert_eq!(
            scrub_connection_secrets("invalid token: \"CANARY%ZZSECRET\"", &secrets),
            "invalid token: \"***\""
        );
        // A short password is scrubbed where it stands alone, not inside words.
        for uri in [
            "postgresql://alice:%Z@localhost/db",
            "postgresql://localhost/db?password=%Z",
        ] {
            let secrets = connection_secrets("postgresql", uri, None, None);
            assert_eq!(
                scrub_connection_secrets("invalid percent-encoded token: \"%Z\" (%Zeta)", &secrets),
                "invalid percent-encoded token: \"***\" (%Zeta)",
                "{uri}"
            );
        }
        let secrets = connection_secrets("sqlite", "file:test.db", None, None);
        assert_eq!(
            scrub_connection_secrets("cannot open test.db", &secrets),
            "cannot open test.db"
        );
    }

    #[test]
    fn scrub_connection_secrets_keeps_a_duckdb_file_path() {
        let secrets = connection_secrets("duckdb", "/data/sales.db", Some("pw-secret"), None);
        assert_eq!(
            scrub_connection_secrets(
                "IO Error: Cannot open file \"/data/sales.db\": No such file or directory (pw-secret)",
                &secrets
            ),
            "IO Error: Cannot open file \"/data/sales.db\": No such file or directory (***)"
        );
        // A MotherDuck token in the query string is still a secret.
        let secrets = connection_secrets(
            "duckdb",
            "md:sales?motherduck_token=tok-secret-1",
            None,
            None,
        );
        assert_eq!(
            scrub_connection_secrets("rejected md:sales?motherduck_token=tok-secret-1", &secrets),
            "rejected md:sales?motherduck_token=***"
        );
        // Other drivers keep the whole URI secret.
        let secrets = connection_secrets("sqlite", "/data/sales.db", None, None);
        assert_eq!(
            scrub_connection_secrets("Cannot open file \"/data/sales.db\"", &secrets),
            "Cannot open file \"***\""
        );
    }
}
