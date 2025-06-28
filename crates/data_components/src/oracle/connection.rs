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

use crate::oracle::ConnectionSnafu;

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
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub service_name: String,
}

pub struct OracleConnectionParamsBuilder {
    host: String,
    username: String,
    password: String,
    port: Option<u16>,
    service_name: Option<String>,
}

impl OracleConnectionParamsBuilder {
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
        OracleConnectionParams {
            host: self.host,
            username: self.username,
            password: self.password,
            port: self.port.unwrap_or(1521),
            service_name: self.service_name.unwrap_or_else(|| "XEPDB1".to_string()),
        }
    }
}

pub async fn connect(params: &OracleConnectionParams) -> super::Result<OracleConnectionPool> {
    let connect_string = format!("//{}:{}/{}", params.host, params.port, params.service_name);
    let connector = Connector::new(
        params.username.clone(),
        params.password.clone(),
        connect_string,
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
