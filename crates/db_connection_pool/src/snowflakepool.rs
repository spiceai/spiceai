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

use std::{collections::HashMap, sync::Arc};

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
        let username = get_param(params, secret, "username")
            .context(MissingRequiredSecretSnafu { name: "username" })?;
        let password = get_param(params, secret, "password")
            .context(MissingRequiredSecretSnafu { name: "password" })?;
        let account = get_param(params, secret, "account")
            .context(MissingRequiredSecretSnafu { name: "account" })?;
        let warehouse = get_param(params, secret, "snowflake_warehouse");
        let role = get_param(params, secret, "snowflake_role");

        // account identifier can be in <orgname.account_name> format but API requires it as <orgname-account_name>
        let account = account.replace('.', "-");

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

        // auth happens on the first request; test auth and connection
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
