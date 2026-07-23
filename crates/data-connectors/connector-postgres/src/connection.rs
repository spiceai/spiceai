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

//! Resolve Postgres connection identity for CDC using the same rules as
//! `PostgresConnectionPool` in `datafusion-table-providers`.
//!
//! Keep [`parse_connection_string`] in sync with
//! `datafusion_table_providers::sql::db_connection_pool::postgrespool`.

use runtime::parameters::{ExposedParamLookup, Parameters};
use std::fmt::Write as _;
use std::str::FromStr;

/// Connection identity for the CDC stream — same override rule as the read pool
/// (`connection_string` overrides discrete host/user/db/…).
pub(crate) struct PgConnectionIdentity {
    pub host: String,
    pub port: u16,
    pub user: String,
    /// Optional, mirroring the connection pool (`pg_pass` is `.secret()` but not
    /// `.required()`): passwordless sources (e.g. `trust` auth) must not be
    /// forced to set a password here when bootstrap already connected without one.
    pub password: String,
    pub database: String,
    pub sslmode: Option<String>,
    pub sslrootcert: Option<String>,
}

impl std::fmt::Debug for PgConnectionIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConnectionIdentity")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("sslmode", &self.sslmode)
            .field("sslrootcert", &self.sslrootcert)
            .finish_non_exhaustive()
    }
}

pub(crate) fn connection_identity_from_params(
    params: &Parameters,
) -> Result<PgConnectionIdentity, String> {
    if let Some(connection_string) = optional_string(params, "connection_string") {
        return connection_identity_from_connection_string(params, &connection_string);
    }

    Ok(PgConnectionIdentity {
        host: required_string(params, "host")?,
        port: optional_port(params)?,
        user: required_string(params, "user")?,
        password: optional_string(params, "pass").unwrap_or_default(),
        database: required_string(params, "db")?,
        sslmode: optional_string(params, "sslmode"),
        sslrootcert: optional_string(params, "sslrootcert"),
    })
}

fn connection_identity_from_connection_string(
    params: &Parameters,
    connection_string: &str,
) -> Result<PgConnectionIdentity, String> {
    let user_param = params.user_param("connection_string").to_string();

    // Same steps as `PostgresConnectionPool::new_inner` when `connection_string`
    // is set: peel password/sslmode/sslrootcert, build a stripped libpq string,
    // then parse with `tokio_postgres::Config`.
    let (mut stripped, mut ssl_mode, mut ssl_rootcert, password) =
        parse_connection_string(connection_string);

    // Validate the mode from the connection string before discrete overrides so
    // a typo is attributed to `pg_connection_string`, not `pg_sslmode`.
    validate_sslmode(&ssl_mode, &user_param)?;

    // Discrete sslmode/sslrootcert override the connection string (pool order).
    if let Some(mode) = optional_string(params, "sslmode") {
        validate_sslmode(&mode, &params.user_param("sslmode").to_string())?;
        ssl_mode = mode;
    }
    if let Some(cert) = optional_string(params, "sslrootcert") {
        ssl_rootcert = Some(cert);
    }

    // tokio_postgres Config only accepts disable/prefer/require; verify-* map
    // to require for the Config string, while the full token is kept for TLS.
    let mode_for_config = match ssl_mode.as_str() {
        "disable" => "disable",
        "prefer" => "prefer",
        _ => "require",
    };
    let _ = write!(stripped, "sslmode={mode_for_config} ");

    let config = tokio_postgres::Config::from_str(stripped.as_str())
        .map_err(|e| format!("invalid `{user_param}`: {e}"))?;

    let host = match config.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
        Some(tokio_postgres::config::Host::Unix(path)) => {
            return Err(format!(
                "parameter `{user_param}` must specify a TCP host; \
                 Unix sockets are not supported for replication ({})",
                path.display()
            ));
        }
        None => {
            return Err(format!("parameter `{user_param}` is missing host"));
        }
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let user = config
        .get_user()
        .filter(|u| !u.is_empty())
        .ok_or_else(|| format!("parameter `{user_param}` is missing user"))?
        .to_string();
    let database = config
        .get_dbname()
        .filter(|d| !d.is_empty())
        .ok_or_else(|| format!("parameter `{user_param}` is missing dbname"))?
        .to_string();

    Ok(PgConnectionIdentity {
        host,
        port,
        user,
        password: password.unwrap_or_default(),
        database,
        // Always set: `parse_connection_string` defaults to `verify-full` when
        // the string omits sslmode (same as the pool).
        sslmode: Some(ssl_mode),
        sslrootcert: ssl_rootcert,
    })
}

/// Parses a connection string into components, extracting `sslmode`, `sslrootcert`,
/// and `password` separately so they can be handled by the caller.
///
/// Keep in sync with `datafusion-table-providers`
/// `sql::db_connection_pool::postgrespool::parse_connection_string`.
fn parse_connection_string(
    pg_connection_string: &str,
) -> (String, String, Option<String>, Option<String>) {
    let mut connection_string = String::new();
    let mut ssl_mode = "verify-full".to_string();
    let mut ssl_rootcert_path: Option<String> = None;
    let mut password: Option<String> = None;

    let str_params: Vec<&str> = pg_connection_string.split_whitespace().collect();
    for param in str_params {
        let param = param.split('=').collect::<Vec<&str>>();
        if let (Some(&name), Some(&value)) = (param.first(), param.get(1)) {
            match name {
                "sslmode" => {
                    ssl_mode = value.to_string();
                }
                "sslrootcert" => {
                    ssl_rootcert_path = Some(value.to_string());
                }
                "password" => {
                    password = Some(value.to_string());
                }
                _ => {
                    let _ = write!(connection_string, "{name}={value} ");
                }
            }
        }
    }

    (connection_string, ssl_mode, ssl_rootcert_path, password)
}

fn validate_sslmode(mode: &str, user_param: &str) -> Result<(), String> {
    match mode.to_lowercase().as_str() {
        "disable" | "require" | "prefer" | "verify-ca" | "verify-full" => Ok(()),
        _ => Err(format!(
            "parameter `{user_param}` must be one of disable, prefer, require, verify-ca, verify-full, got {mode:?}"
        )),
    }
}

fn required_string(params: &Parameters, key: &str) -> Result<String, String> {
    match params.get(key).expose() {
        ExposedParamLookup::Present(v) => Ok(v.to_string()),
        ExposedParamLookup::Absent(name) => Err(format!("missing required parameter `{name}`")),
    }
}

fn optional_string(params: &Parameters, key: &str) -> Option<String> {
    match params.get(key).expose() {
        ExposedParamLookup::Present(v) => Some(v.to_string()),
        ExposedParamLookup::Absent(_) => None,
    }
}

fn optional_port(params: &Parameters) -> Result<u16, String> {
    let Some(raw) = optional_string(params, "port") else {
        return Ok(5432);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(5432);
    }
    trimmed.parse::<u16>().map_err(|_| {
        let user_param = params.user_param("port");
        format!("parameter `{user_param}` must be a port number (0-65535), got {raw:?}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;

    fn params_with_pairs(pairs: &[(&str, &str)]) -> Parameters {
        Parameters::new(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), SecretString::from(*v)))
                .collect(),
            "pg",
            crate::PARAMETERS,
        )
    }

    #[test]
    fn parse_connection_string_extracts_password_and_defaults_sslmode() {
        let (conn_str, ssl_mode, cert_path, password) = parse_connection_string(
            "host=localhost user=postgres password=secret dbname=mydb",
        );
        assert_eq!(conn_str.trim(), "host=localhost user=postgres dbname=mydb");
        assert_eq!(ssl_mode, "verify-full");
        assert!(cert_path.is_none());
        assert_eq!(password.as_deref(), Some("secret"));
    }

    #[test]
    fn connection_string_overrides_discrete_params() {
        let params = params_with_pairs(&[
            (
                "connection_string",
                "host=db.internal port=5433 dbname=csdb user=csuser password=secret sslmode=require",
            ),
            ("host", "ignored"),
            ("user", "ignored"),
            ("db", "ignored"),
            ("port", "1111"),
        ]);
        let identity =
            connection_identity_from_params(&params).expect("valid connection_string should parse");
        assert_eq!(identity.host, "db.internal");
        assert_eq!(identity.port, 5433);
        assert_eq!(identity.user, "csuser");
        assert_eq!(identity.database, "csdb");
        assert_eq!(identity.password, "secret");
        assert_eq!(identity.sslmode.as_deref(), Some("require"));
    }

    #[test]
    fn connection_string_omitted_sslmode_defaults_to_verify_full() {
        let params = params_with_pairs(&[(
            "connection_string",
            "host=db.internal dbname=csdb user=csuser",
        )]);
        let identity =
            connection_identity_from_params(&params).expect("valid connection_string should parse");
        assert_eq!(identity.sslmode.as_deref(), Some("verify-full"));
    }

    #[test]
    fn discrete_sslmode_overrides_connection_string() {
        let params = params_with_pairs(&[
            (
                "connection_string",
                "host=db.internal dbname=csdb user=csuser sslmode=require",
            ),
            ("sslmode", "disable"),
        ]);
        let identity = connection_identity_from_params(&params).expect("valid params should parse");
        assert_eq!(identity.sslmode.as_deref(), Some("disable"));
    }

    #[test]
    fn connection_string_missing_user_errors() {
        let params = params_with_pairs(&[("connection_string", "host=db.internal dbname=csdb")]);
        let err = connection_identity_from_params(&params)
            .expect_err("connection_string without user must error");
        assert_eq!(
            err,
            "parameter `pg_connection_string` is missing user".to_string()
        );
    }

    #[test]
    fn connection_string_invalid_sslmode_errors_on_connection_string_param() {
        let params = params_with_pairs(&[(
            "connection_string",
            "host=db.internal dbname=csdb user=csuser sslmode=verify-ful",
        )]);
        let err = connection_identity_from_params(&params)
            .expect_err("typo'd sslmode in connection_string must error");
        assert!(
            err.contains("pg_connection_string"),
            "error should attribute to connection_string, got: {err}"
        );
        assert!(err.contains("verify-ful"), "got: {err}");
    }

    #[test]
    fn discrete_params_without_connection_string() {
        let params = params_with_pairs(&[
            ("host", "localhost"),
            ("user", "postgres"),
            ("db", "app"),
            ("pass", "pw"),
            ("port", "6543"),
        ]);
        let identity =
            connection_identity_from_params(&params).expect("discrete params should parse");
        assert_eq!(identity.host, "localhost");
        assert_eq!(identity.port, 6543);
        assert_eq!(identity.user, "postgres");
        assert_eq!(identity.database, "app");
        assert_eq!(identity.password, "pw");
        assert!(identity.sslmode.is_none());
    }
}
