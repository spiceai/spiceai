/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use bb8::CustomizeConnection;
use bb8_oracle::OracleConnectionManager;
use oracle::{Connection, Connector};
use snafu::ResultExt;
use std::sync::Arc;

use crate::oracle::{ConnectionSnafu, OracleInitSnafu};

#[derive(Debug)]
pub struct OracleConnectionPool {
    pool: bb8::Pool<OracleConnectionManager>,
}

impl OracleConnectionPool {
    pub async fn get(&self) -> super::Result<bb8::PooledConnection<'_, OracleConnectionManager>> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| super::Error::ConnectionPoolError { source: err.into() })?;
        Ok(conn)
    }
}

#[derive(Debug)]
pub struct SetTimezoneCustomizer {
    pub timezone: String,
}

#[async_trait::async_trait]
impl CustomizeConnection<Arc<Connection>, bb8_oracle::Error> for SetTimezoneCustomizer {
    fn on_acquire<'a>(
        &'a self,
        conn: &'a mut Arc<Connection>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), bb8_oracle::Error>> + Send + 'a>> {
        let sql = format!("ALTER SESSION SET TIME_ZONE = '{}'", self.timezone);
        Box::pin(async move {
            let _ = conn.execute(&sql, &[]);
            Ok(())
        })
    }
}

#[derive(Debug)]
pub struct OracleConnectionParams {
    pub username: String,
    pub password: String,
    pub connect_string: String,
}

impl OracleConnectionParams {
    #[must_use]
    pub fn new(username: &str, password: &str, connect_string: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            connect_string: connect_string.to_string(),
        }
    }
}

/// Default TCP port for Oracle Database (commonly used in on-prem/self-managed installations).
const DEFAULT_PORT: u16 = 1521;

/// `XEPDB1` is the default pluggable database (PDB) name in Oracle Database XE 18c/21c and later versions.
static DEFAULT_SERVICE_NAME: &str = "XEPDB1";

/// Builds Oracle connection parameters for direct TCP connections using host, port, and service name.
pub struct OracleDirectConnectionParamsBuilder {
    host: String,
    username: String,
    password: String,
    port: Option<u16>,
    service_name: Option<String>,
}

impl OracleDirectConnectionParamsBuilder {
    pub fn new(
        host: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            username: username.into(),
            password: password.into(),
            port: None,
            service_name: None,
        }
    }

    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port);
        self
    }

    pub fn service_name(&mut self, service_name: impl Into<String>) -> &mut Self {
        self.service_name = Some(service_name.into());
        self
    }

    #[must_use]
    pub fn build(self) -> OracleConnectionParams {
        let connect_string = format!(
            "//{}:{}/{}",
            self.host,
            self.port.unwrap_or(DEFAULT_PORT),
            self.service_name
                .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string())
        );

        OracleConnectionParams {
            username: self.username,
            password: self.password,
            connect_string,
        }
    }
}

pub async fn connect(
    params: &OracleConnectionParams,
    wallet_path: Option<&str>,
) -> super::Result<OracleConnectionPool> {
    if let Some(wallet_path) = wallet_path {
        // Initializes Oracle client library with the specified wallet directory
        // Note: this is applied for the first connection only, if library is already initialized, dynamically changing wallet directory has no effect
        let initialized_here = oracle::InitParams::new()
            .oracle_client_config_dir(wallet_path)
            .context(OracleInitSnafu)?
            .init()
            .context(OracleInitSnafu)?;

        if initialized_here {
            tracing::info!("Using wallet directory for Oracle data connector: {wallet_path}");
        } else {
            tracing::debug!(
                "Oracle client library was already initialized, using existing configuration"
            );
        }
    }

    let connector = Connector::new(
        params.username.clone(),
        params.password.clone(),
        params.connect_string.clone(),
    );

    // verify connection to an Oracle server
    let _ = connector.connect().context(ConnectionSnafu)?;

    let manager = OracleConnectionManager::from_connector(connector);

    let pool = bb8::Pool::builder()
        // Spice uses UTC timezone for timestamp data. Set preferred timezone for automated datatype conversion to correctly handle TIMESTAMP WITH LOCAL TIME ZONE data types
        .connection_customizer(Box::new(SetTimezoneCustomizer {
            timezone: "UTC".to_string(),
        }))
        .build(manager)
        .await
        .map_err(|err| super::Error::ConnectionPoolError { source: err.into() })?;

    Ok(OracleConnectionPool { pool })
}
