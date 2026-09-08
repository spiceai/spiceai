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

//! `DuckLake` catalog provider implementation.
//!
//! `DuckLake` is an open Lakehouse format that stores metadata in SQL tables and data in Parquet files.
//! This module provides a catalog provider that connects to a `DuckLake` catalog using `DuckDB`
//! with the `ducklake` extension.

pub mod provider;
pub mod writer;

/// S3 credential parameters for `DuckLake` connectors.
///
/// When provided, these are used to configure `DuckDB`'s `httpfs` extension for
/// accessing Parquet data files stored on S3-compatible storage. If omitted, `DuckDB`
/// falls back to its built-in `credential_chain` provider (env vars, IAM roles, etc.).
#[derive(Default)]
pub struct DuckLakeS3Params {
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Session token accompanying temporary (STS) credentials. `DuckDB` rejects
    /// temporary `ASIA…` keys without it, because `SigV4` only validates when the
    /// token is sent alongside the key and secret.
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
    pub allow_http: bool,
}

/// Installs and loads `httpfs` in `DuckDB`, then optionally creates an S3 secret
/// from explicit parameters. If no explicit credentials are given, `DuckDB` uses
/// its `credential_chain` provider which reads standard `AWS_*` environment variables.
///
/// # Errors
///
/// Returns a `duckdb::Error` if extension installation or secret creation fails.
pub fn configure_duckdb_httpfs(
    conn: &duckdb::Connection,
    s3: &DuckLakeS3Params,
) -> Result<(), duckdb::Error> {
    conn.execute("INSTALL httpfs", [])?;
    conn.execute("LOAD httpfs", [])?;

    if let Some(secret_sql) = build_ducklake_s3_secret_sql(s3) {
        conn.execute(&secret_sql, [])?;
    }

    Ok(())
}

/// Whether a configured session token will be dropped rather than sent to `DuckDB`.
///
/// `SESSION_TOKEN` is only meaningful next to an explicit `KEY_ID`, so a token supplied
/// without `aws_access_key_id` is ignored however the rest of the parameters are set —
/// including the token-only case, which resolves no secret at all.
fn session_token_is_ignored(s3: &DuckLakeS3Params) -> bool {
    s3.session_token.is_some() && s3.access_key_id.is_none()
}

/// Builds the `CREATE SECRET` statement configuring `DuckDB`'s `httpfs` extension for
/// S3 access, or `None` when no explicit S3 parameters are set (`DuckDB` then resolves
/// credentials through its own `credential_chain` provider).
///
/// Values are escaped for single-quoted literals. `SESSION_TOKEN` is emitted whenever a
/// session token is configured, which is what makes temporary (STS) `ASIA…` credentials
/// usable; long-lived `AKIA…` credentials carry no token and are unaffected.
#[must_use]
pub fn build_ducklake_s3_secret_sql(s3: &DuckLakeS3Params) -> Option<String> {
    // Warn before the early return below: a session token on its own is not an explicit
    // credential, so a token-only configuration returns early and would never reach a
    // check placed further down — the very case where the token is most silently dropped.
    if session_token_is_ignored(s3) {
        tracing::warn!(
            "DuckLake: 'aws_session_token' provided without 'aws_access_key_id'. Set all three of 'aws_access_key_id', 'aws_secret_access_key', and 'aws_session_token' to use temporary credentials."
        );
    }

    let has_explicit_creds =
        s3.access_key_id.is_some() || s3.endpoint.is_some() || s3.region.is_some();
    if !has_explicit_creds {
        return None;
    }

    let region = s3.region.as_deref().unwrap_or("us-east-1");
    let use_ssl = !s3.allow_http;

    let mut secret_parts = vec![
        "TYPE s3".to_string(),
        format!("REGION '{}'", region.replace('\'', "''")),
        format!("USE_SSL {use_ssl}"),
    ];

    if let Some(key_id) = &s3.access_key_id {
        secret_parts.push("PROVIDER config".to_string());
        secret_parts.push(format!("KEY_ID '{}'", key_id.replace('\'', "''")));
        if let Some(secret) = &s3.secret_access_key {
            secret_parts.push(format!("SECRET '{}'", secret.replace('\'', "''")));
        } else {
            tracing::warn!(
                "DuckLake: 'aws_access_key_id' provided without 'aws_secret_access_key'. Both must be set for S3 authentication."
            );
        }
        if let Some(session_token) = &s3.session_token {
            secret_parts.push(format!(
                "SESSION_TOKEN '{}'",
                session_token.replace('\'', "''")
            ));
        }
    } else {
        secret_parts.push("PROVIDER credential_chain".to_string());
    }

    if let Some(endpoint) = &s3.endpoint {
        let endpoint = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        secret_parts.push(format!("ENDPOINT '{}'", endpoint.replace('\'', "''")));
        secret_parts.push("URL_STYLE 'path'".to_string());
    }

    Some(format!(
        "CREATE OR REPLACE SECRET __ducklake_s3 ({})",
        secret_parts.join(", ")
    ))
}

/// Builds the `ATTACH` statement used to attach a `DuckLake` catalog in `DuckDB`.
///
/// The connection string is escaped for a single-quoted literal and the catalog
/// name for a double-quoted identifier. When `automatic_migration` is `true`, the
/// `AUTOMATIC_MIGRATION` attach option is appended so that `DuckDB` migrates an
/// older `DuckLake` catalog schema in place instead of failing with a
/// `catalog version mismatch ... the extension requires version` error. Migration
/// is disabled by default because it rewrites the catalog's metadata and cannot be
/// undone, so it is gated behind an explicit opt-in.
#[must_use]
pub fn build_ducklake_attach_sql(
    connection_string: &str,
    catalog_name: &str,
    automatic_migration: bool,
) -> String {
    let escaped_connection_string = connection_string.replace('\'', "''");
    let escaped_catalog_name = catalog_name.replace('"', "\"\"");
    let mut attach_sql =
        format!("ATTACH 'ducklake:{escaped_connection_string}' AS \"{escaped_catalog_name}\"");
    if automatic_migration {
        attach_sql.push_str(" (AUTOMATIC_MIGRATION TRUE)");
    }
    attach_sql
}

#[cfg(test)]
mod tests {
    use super::{
        DuckLakeS3Params, build_ducklake_attach_sql, build_ducklake_s3_secret_sql,
        session_token_is_ignored,
    };

    #[test]
    fn s3_secret_sql_is_none_without_explicit_parameters() {
        assert_eq!(
            build_ducklake_s3_secret_sql(&DuckLakeS3Params::default()),
            None
        );
    }

    #[test]
    fn a_session_token_without_a_key_id_is_reported_ignored() {
        // A token on its own resolves no secret at all, so this is the configuration
        // whose token is dropped most quietly — it must still be flagged.
        assert!(session_token_is_ignored(&DuckLakeS3Params {
            session_token: Some("FwoSessionToken".to_string()),
            ..DuckLakeS3Params::default()
        }));

        // …as must a token alongside other explicit parameters but still no key id.
        assert!(session_token_is_ignored(&DuckLakeS3Params {
            region: Some("us-east-1".to_string()),
            session_token: Some("FwoSessionToken".to_string()),
            ..DuckLakeS3Params::default()
        }));
    }

    #[test]
    fn a_usable_or_absent_session_token_is_not_reported_ignored() {
        // Sent as SESSION_TOKEN next to the key id.
        assert!(!session_token_is_ignored(&DuckLakeS3Params {
            access_key_id: Some("ASIAEXAMPLE".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: Some("FwoSessionToken".to_string()),
            ..DuckLakeS3Params::default()
        }));

        // Nothing to ignore when no token was configured.
        assert!(!session_token_is_ignored(&DuckLakeS3Params {
            region: Some("us-east-1".to_string()),
            ..DuckLakeS3Params::default()
        }));
    }

    #[test]
    fn a_session_token_alone_still_resolves_no_secret() {
        // Warning aside, a token is not an explicit credential: DuckDB keeps using its
        // own credential_chain rather than gaining a half-populated secret.
        assert_eq!(
            build_ducklake_s3_secret_sql(&DuckLakeS3Params {
                session_token: Some("FwoSessionToken".to_string()),
                ..DuckLakeS3Params::default()
            }),
            None
        );
    }

    #[test]
    fn s3_secret_sql_includes_session_token_for_temporary_credentials() {
        let sql = build_ducklake_s3_secret_sql(&DuckLakeS3Params {
            region: Some("us-east-1".to_string()),
            access_key_id: Some("ASIAEXAMPLE".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: Some("FwoSessionToken".to_string()),
            endpoint: None,
            allow_http: false,
        })
        .expect("explicit credentials should produce a secret");

        assert_eq!(
            sql,
            "CREATE OR REPLACE SECRET __ducklake_s3 (TYPE s3, REGION 'us-east-1', USE_SSL true, PROVIDER config, KEY_ID 'ASIAEXAMPLE', SECRET 'secret', SESSION_TOKEN 'FwoSessionToken')"
        );
    }

    #[test]
    fn s3_secret_sql_omits_session_token_when_unset() {
        let sql = build_ducklake_s3_secret_sql(&DuckLakeS3Params {
            region: Some("us-east-1".to_string()),
            access_key_id: Some("AKIAEXAMPLE".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: None,
            endpoint: None,
            allow_http: false,
        })
        .expect("explicit credentials should produce a secret");

        assert!(
            !sql.contains("SESSION_TOKEN"),
            "long-lived credentials must not carry a session token: {sql}"
        );
    }

    #[test]
    fn s3_secret_sql_escapes_session_token() {
        let sql = build_ducklake_s3_secret_sql(&DuckLakeS3Params {
            region: Some("us-east-1".to_string()),
            access_key_id: Some("ASIAEXAMPLE".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: Some("tok'en".to_string()),
            endpoint: None,
            allow_http: false,
        })
        .expect("explicit credentials should produce a secret");

        assert!(
            sql.contains("SESSION_TOKEN 'tok''en'"),
            "session token must be escaped for a single-quoted literal: {sql}"
        );
    }

    #[test]
    fn s3_secret_sql_falls_back_to_credential_chain_without_a_key() {
        let sql = build_ducklake_s3_secret_sql(&DuckLakeS3Params {
            region: Some("us-east-1".to_string()),
            session_token: Some("FwoSessionToken".to_string()),
            ..DuckLakeS3Params::default()
        })
        .expect("an explicit region should produce a secret");

        assert!(
            sql.contains("PROVIDER credential_chain") && !sql.contains("SESSION_TOKEN"),
            "a session token without a key id must not be sent: {sql}"
        );
    }

    #[test]
    fn attach_sql_without_migration_is_unchanged() {
        assert_eq!(
            build_ducklake_attach_sql("metadata.ducklake", "ducklake", false),
            "ATTACH 'ducklake:metadata.ducklake' AS \"ducklake\""
        );
    }

    #[test]
    fn attach_sql_with_migration_appends_option() {
        assert_eq!(
            build_ducklake_attach_sql("metadata.ducklake", "ducklake", true),
            "ATTACH 'ducklake:metadata.ducklake' AS \"ducklake\" (AUTOMATIC_MIGRATION TRUE)"
        );
    }

    #[test]
    fn attach_sql_escapes_connection_string_and_catalog_name() {
        assert_eq!(
            build_ducklake_attach_sql("s3://b/o'brien.ducklake", "my\"lake", true),
            "ATTACH 'ducklake:s3://b/o''brien.ducklake' AS \"my\"\"lake\" (AUTOMATIC_MIGRATION TRUE)"
        );
    }
}
