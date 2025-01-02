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

use crate::dbconnection::odbcconn::ODBCConnection;
use crate::dbconnection::odbcconn::{ODBCDbConnection, ODBCParameter};
use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::{DbConnectionPool, JoinPushDown};
use odbc_api::{sys::AttrConnectionPooling, Connection, ConnectionOptions, Environment};
use secrecy::{ExposeSecret, Secret, SecretString};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

static ENV: LazyLock<Environment> = LazyLock::new(|| unsafe {
    // Enable connection pooling. Let driver decide wether the attributes of two connection
    // are similar enough to change the attributes of a pooled one, to fit the requested
    // connection, or if it is cheaper to create a new Connection from scratch.
    // See <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/driver-aware-connection-pooling>
    if let Err(e) = Environment::set_connection_pooling(AttrConnectionPooling::DriverAware) {
        tracing::error!("Failed to set ODBC connection pooling: {e}");
    };
    match Environment::new() {
        Ok(env) => env,
        Err(e) => {
            panic!("Failed to create ODBC environment: {e}");
        }
    }
});

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing ODBC connection string parameter: odbc_connection_string"))]
    MissingConnectionString {},

    #[snafu(display("Invalid parameter: {parameter_name}"))]
    InvalidParameterError { parameter_name: String },
}

pub struct ODBCPool {
    pool: &'static Environment,
    params: Arc<HashMap<String, SecretString>>,
    connection_string: String,
    connection_id: String,
}

fn hash_string(val: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(val);
    hasher.finalize().iter().fold(String::new(), |mut hash, b| {
        hash.push_str(&format!("{b:02x}"));
        hash
    })
}

impl ODBCPool {
    // Creates a new instance of `ODBCPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub fn new(params: HashMap<String, SecretString>) -> Result<Self, Error> {
        let connection_string = params
            .get("connection_string")
            .map(Secret::expose_secret)
            .map(ToString::to_string)
            .context(MissingConnectionStringSnafu)?;

        // hash the connection string to get a comparable connection ID
        // we do this to prevent exposing secrets in the EXPLAIN ... plan when using federated JoinPushDown
        let connection_id = hash_string(&connection_string);

        Ok(Self {
            params: params.into(),
            connection_string,
            connection_id,
            pool: &ENV,
        })
    }

    #[must_use]
    pub fn odbc_environment(&self) -> &'static Environment {
        self.pool
    }
}

#[async_trait]
impl<'a> DbConnectionPool<Connection<'a>, ODBCParameter> for ODBCPool
where
    'a: 'static,
{
    async fn connect(
        &self,
    ) -> Result<Box<ODBCDbConnection<'a>>, Box<dyn std::error::Error + Send + Sync>> {
        let cxn = self.pool.connect_with_connection_string(
            &self.connection_string,
            ConnectionOptions::default(),
        )?;

        let odbc_cxn = ODBCConnection {
            conn: Arc::new(cxn.into()),
            params: Arc::clone(&self.params),
        };

        Ok(Box::new(odbc_cxn))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::AllowedFor(self.connection_id.clone())
    }
}
