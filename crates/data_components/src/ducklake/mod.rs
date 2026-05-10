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

    let has_explicit_creds =
        s3.access_key_id.is_some() || s3.endpoint.is_some() || s3.region.is_some();
    if !has_explicit_creds {
        return Ok(());
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

    let secret_sql = format!(
        "CREATE OR REPLACE SECRET __ducklake_s3 ({})",
        secret_parts.join(", ")
    );
    conn.execute(&secret_sql, [])?;

    Ok(())
}
