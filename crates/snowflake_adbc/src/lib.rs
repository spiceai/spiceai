/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Pure Rust implementation of ADBC driver for Snowflake.
//!
//! This driver provides Arrow-native access to Snowflake data without
//! requiring Go runtime or CGO bindings.

mod auth;
mod client;
mod connection;
mod error;
mod statement;
mod types;

pub use connection::Connection;
pub use error::{Error, Result};
pub use statement::Statement;

use std::collections::HashMap;

/// Snowflake ADBC Database handle.
///
/// This represents connection configuration that can be used to
/// create multiple connections.
#[derive(Debug, Clone)]
pub struct Database {
    account: String,
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    auth_config: auth::AuthConfig,
}

impl Database {
    /// Create a new Database with connection parameters.
    pub fn new(params: HashMap<String, String>) -> Result<Self> {
        let account = params
            .get("account")
            .ok_or_else(|| Error::InvalidArgument {
                message: "account parameter is required".to_string(),
            })?
            .clone();

        let auth_config = auth::AuthConfig::from_params(&params)?;

        Ok(Self {
            account,
            warehouse: params.get("warehouse").cloned(),
            database: params.get("database").cloned(),
            schema: params.get("schema").cloned(),
            role: params.get("role").cloned(),
            auth_config,
        })
    }

    /// Create a new connection from this database configuration.
    pub async fn connect(&self) -> Result<Connection> {
        Connection::new(self.clone()).await
    }

    pub(crate) fn account(&self) -> &str {
        &self.account
    }

    pub(crate) fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    pub(crate) fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub(crate) fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub(crate) fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    pub(crate) fn auth_config(&self) -> &auth::AuthConfig {
        &self.auth_config
    }
}
