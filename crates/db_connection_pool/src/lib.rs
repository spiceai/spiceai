/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::collections::HashMap;

use crate::dbconnection::DbConnection;
use async_trait::async_trait;
use secrets::Secret;
use spicepod::component::dataset::acceleration;

#[cfg(feature = "clickhouse")]
pub mod clickhousepool;
pub mod dbconnection;
#[cfg(feature = "duckdb")]
pub mod duckdbpool;
#[cfg(feature = "mysql")]
pub mod mysqlpool;
#[cfg(feature = "odbc")]
pub mod odbcpool;
#[cfg(feature = "postgres")]
pub mod postgrespool;
#[cfg(feature = "sqlite")]
pub mod sqlitepool;

#[cfg(feature = "snowflake")]
pub mod snowflakepool;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait DbConnectionPool<T, P: 'static> {
    async fn connect(&self) -> Result<Box<dyn DbConnection<T, P>>>;
}

#[derive(Default)]
pub enum Mode {
    #[default]
    Memory,
    File,
}

impl From<&str> for Mode {
    fn from(m: &str) -> Self {
        match m {
            "file" => Mode::File,
            "memory" => Mode::Memory,
            _ => Mode::default(),
        }
    }
}

impl From<acceleration::Mode> for Mode {
    fn from(m: acceleration::Mode) -> Self {
        match m {
            acceleration::Mode::File => Mode::File,
            acceleration::Mode::Memory => Mode::Memory,
        }
    }
}

#[must_use]
#[allow(clippy::implicit_hasher)]
pub(crate) fn get_secret_or_param(
    params: Option<&HashMap<String, String>>,
    secret: &Option<Secret>,
    secret_param_key: &str,
    param_key: &str,
) -> Option<String> {
    let secret_param_val = match params.and_then(|p| p.get(secret_param_key)) {
        Some(val) => val,
        None => param_key,
    };

    if let Some(secrets) = secret {
        if let Some(secret_val) = secrets.get(secret_param_val) {
            return Some(secret_val.to_string());
        };
    };

    if let Some(param_val) = params.and_then(|p| p.get(param_key)) {
        return Some(param_val.to_string());
    };

    None
}
