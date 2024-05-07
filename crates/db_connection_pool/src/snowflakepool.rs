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

use async_trait::async_trait;
use secrets::Secret;
use snafu::prelude::*;
use snowflake_api::{SnowflakeApi, SnowflakeApiError};
use std::{collections::HashMap, fs, sync::Arc};

use super::{DbConnectionPool, Result};

use crate::{
    dbconnection::{snowflakeconn::SnowflakeConnection, DbConnection},
    get_secret_or_param,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required secret: {name}"))]
    MissingRequiredSecret { name: String },

    #[snafu(display("Unable to connect to Snowflake: {source}"))]
    UnableToConnect {
        source: snowflake_api::SnowflakeApiError,
    },

    #[snafu(display("Snowflake authentication failed with error: {source}"))]
    UnableToAuthenticate {
        source: snowflake_api::SnowflakeApiError,
    },

    #[snafu(display("Snowflake authentication failed. Validate account and warehouse parameters using the SnowSQL tool."))]
    UnableToAuthenticateGeneric {},

    #[snafu(display("Error reading private key file {file_path}: {source}."))]
    ErrorReadingPrivateKeyFile {
        source: std::io::Error,
        file_path: String,
    },

    #[snafu(display("Parameter {param_key} has invalid value: {param_value}"))]
    InvalidParameterValue {
        param_key: String,
        param_value: String,
    },
}

pub struct SnowflakeConnectionPool {
    pub api: Arc<SnowflakeApi>,
}

fn get_param(
    params: &Arc<Option<HashMap<String, String>>>,
    secret: &Option<Secret>,
    param_name: &str,
) -> Option<String> {
    return get_secret_or_param(
        params.as_ref().as_ref(),
        secret,
        &format!("{param_name}_key"),
        param_name,
    );
}

impl SnowflakeConnectionPool {
    // Creates a new instance of `SnowflakeConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(
        params: &Arc<Option<HashMap<String, String>>>,
        secret: &Option<Secret>,
    ) -> Result<Self> {
        let api = init_snowflake_api(params, secret)?;

        if let Err(err) = api.exec("SELECT 1").await {
            match err {
                snowflake_api::SnowflakeApiError::AuthError(auth_err) => {
                    // for incorrect werehouse or account param the library fails
                    // with response decoding message that confuses, so we return a generic error
                    if auth_err
                        .to_string()
                        .contains("error decoding response body")
                    {
                        return Err(Box::new(Error::UnableToAuthenticateGeneric {}));
                    };

                    return Err(Box::new(Error::UnableToAuthenticate {
                        source: SnowflakeApiError::AuthError(auth_err),
                    }));
                }
                _ => {
                    return Err(Box::new(Error::UnableToConnect { source: err }));
                }
            }
        }

        Ok(Self { api: Arc::new(api) })
    }
}

fn init_snowflake_api(
    params: &Arc<Option<HashMap<String, String>>>,
    secret: &Option<Secret>,
) -> Result<SnowflakeApi> {
    let username = get_param(params, secret, "username")
        .context(MissingRequiredSecretSnafu { name: "username" })?;
    let account = get_param(params, secret, "account")
        .context(MissingRequiredSecretSnafu { name: "account" })?;
    // account identifier can be in <orgname.account_name> format but API requires it as <orgname-account_name>
    let account = account.replace('.', "-");

    let warehouse = get_param(params, secret, "snowflake_warehouse");
    let role = get_param(params, secret, "snowflake_role");
    let auth_type = get_param(params, secret, "snowflake_auth_type")
        .unwrap_or_else(|| "snowflake".to_string())
        .to_lowercase();

    if auth_type == "snowflake" {
        // default username/password auth
        let password = get_param(params, secret, "password")
            .context(MissingRequiredSecretSnafu { name: "password" })?;
        let api = SnowflakeApi::with_password_auth(
            &account,
            warehouse.as_deref(),
            None,
            None,
            &username,
            role.as_deref(),
            &password,
        )
        .context(UnableToConnectSnafu)?;

        return Ok(api);
    }

    if auth_type == "keypair" {
        let private_key_path = get_param(params, secret, "snowflake_private_key_path").context(
            MissingRequiredSecretSnafu {
                name: "snowflake_private_key_path",
            },
        )?;

        let private_key_pem: String =
            fs::read_to_string(&private_key_path).context(ErrorReadingPrivateKeyFileSnafu {
                file_path: private_key_path,
            })?;

        let api = SnowflakeApi::with_certificate_auth(
            &account,
            warehouse.as_deref(),
            None,
            None,
            &username,
            role.as_deref(),
            &private_key_pem,
        )
        .context(UnableToConnectSnafu)?;

        return Ok(api);
    }

    InvalidParameterValueSnafu {
        param_key: "snowflake_auth_type",
        param_value: auth_type,
    }
    .fail()?
}

#[async_trait]
impl DbConnectionPool<Arc<SnowflakeApi>, &'static (dyn Sync)> for SnowflakeConnectionPool {
    async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<Arc<SnowflakeApi>, &'static (dyn Sync)>>> {
        let api = Arc::clone(&self.api);

        let conn = SnowflakeConnection { api };

        Ok(Box::new(conn))
    }
}
