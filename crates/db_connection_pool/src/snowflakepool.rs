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
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::DbConnection, DbConnectionPool, JoinPushDown,
};
use pkcs8::{LineEnding, SecretDocument};
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::prelude::*;
use snowflake_api::{SnowflakeApi, SnowflakeApiError};
use std::{collections::HashMap, fs, sync::Arc};

use crate::dbconnection::snowflakeconn::SnowflakeConnection;

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

    #[snafu(display("Unable to parse private key file: {source}"))]
    UnableToParsePrivateKey { source: pkcs8::der::Error },

    #[snafu(display("Unable to decrypt private key file: {source}. Is the passphrase correct?"))]
    UnableToDecryptPrivateKey { source: pkcs8::Error },

    #[snafu(display("Failed to save decrypted private key content as PEM: {source}"))]
    FailedToCreatePem { source: pkcs8::der::Error },
}

pub struct SnowflakeConnectionPool {
    pub api: Arc<SnowflakeApi>,
    join_push_down: JoinPushDown,
}

impl SnowflakeConnectionPool {
    // Creates a new instance of `SnowflakeConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: &HashMap<String, SecretString>) -> Result<Self, Error> {
        let username = params
            .get("username")
            .map(Secret::expose_secret)
            .context(MissingRequiredSecretSnafu { name: "username" })?;

        let account = params
            .get("account")
            .map(Secret::expose_secret)
            .context(MissingRequiredSecretSnafu { name: "account" })?;
        // account identifier can be in <orgname.account_name> format but API requires it as <orgname-account_name>
        let account = account.replace('.', "-");

        let warehouse = params
            .get("warehouse")
            .map(Secret::expose_secret)
            .map(ToString::to_string);
        let role = params
            .get("role")
            .map(Secret::expose_secret)
            .map(ToString::to_string);

        let auth_type = params
            .get("auth_type")
            .map(Secret::expose_secret)
            .map_or_else(|| "snowflake".to_string(), ToString::to_string)
            .to_lowercase();

        let api = match auth_type.as_str() {
            "snowflake" => init_snowflake_api_with_password_auth(
                &account, username, &warehouse, &role, params,
            )?,
            "keypair" => {
                init_snowflake_api_with_keypair_auth(&account, username, &warehouse, &role, params)?
            }
            _ => InvalidParameterValueSnafu {
                param_key: "snowflake_auth_type",
                param_value: auth_type,
            }
            .fail()?,
        };

        if let Err(err) = api.exec("SELECT 1").await {
            match err {
                snowflake_api::SnowflakeApiError::AuthError(auth_err) => {
                    // for incorrect werehouse or account param the library fails
                    // with response decoding message that confuses, so we return a generic error
                    if auth_err
                        .to_string()
                        .contains("error decoding response body")
                    {
                        return Err(Error::UnableToAuthenticateGeneric {});
                    };

                    return Err(Error::UnableToAuthenticate {
                        source: SnowflakeApiError::AuthError(auth_err),
                    });
                }
                _ => {
                    return Err(Error::UnableToConnect { source: err });
                }
            }
        }

        let mut join_push_context_str = format!("username={username},account={account}");
        if let Some(warehouse) = warehouse {
            join_push_context_str.push_str(&format!(",warehouse={warehouse}"));
        }
        if let Some(role) = role {
            join_push_context_str.push_str(&format!(",role={role}"));
        }

        Ok(Self {
            api: Arc::new(api),
            join_push_down: JoinPushDown::AllowedFor(join_push_context_str),
        })
    }
}

fn init_snowflake_api_with_password_auth(
    account: &str,
    username: &str,
    warehouse: &Option<String>,
    role: &Option<String>,
    params: &HashMap<String, SecretString>,
) -> Result<SnowflakeApi, Error> {
    let password = params
        .get("password")
        .map(Secret::expose_secret)
        .context(MissingRequiredSecretSnafu { name: "password" })?;
    let api = SnowflakeApi::with_password_auth(
        account,
        warehouse.as_deref(),
        None,
        None,
        username,
        role.as_deref(),
        password,
    )
    .context(UnableToConnectSnafu)?;

    Ok(api)
}

fn init_snowflake_api_with_keypair_auth(
    account: &str,
    username: &str,
    warehouse: &Option<String>,
    role: &Option<String>,
    params: &HashMap<String, SecretString>,
) -> Result<SnowflakeApi, Error> {
    let private_key_path = params
        .get("private_key_path")
        .map(Secret::expose_secret)
        .context(MissingRequiredSecretSnafu {
            name: "snowflake_private_key_path",
        })?;

    let mut private_key_pem: String =
        fs::read_to_string(private_key_path).context(ErrorReadingPrivateKeyFileSnafu {
            file_path: private_key_path,
        })?;

    let (label, data) =
        SecretDocument::from_pem(&private_key_pem).context(UnableToParsePrivateKeySnafu)?;

    if label.to_uppercase() == "ENCRYPTED PRIVATE KEY" {
        let passphrase = params
            .get("private_key_passphrase")
            .map(Secret::expose_secret)
            .context(MissingRequiredSecretSnafu {
                name: "snowflake_private_key_passphrase",
            })?;

        private_key_pem = decode_pkcs8_encrypted_data(&data, passphrase)?;
    }

    let api = SnowflakeApi::with_certificate_auth(
        account,
        warehouse.as_deref(),
        None,
        None,
        username,
        role.as_deref(),
        &private_key_pem,
    )
    .context(UnableToConnectSnafu)?;

    Ok(api)
}

#[async_trait]
impl DbConnectionPool<Arc<SnowflakeApi>, &'static (dyn Sync)> for SnowflakeConnectionPool {
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<Arc<SnowflakeApi>, &'static (dyn Sync)>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let api = Arc::clone(&self.api);

        let conn = SnowflakeConnection { api };

        Ok(Box::new(conn))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

fn decode_pkcs8_encrypted_data(data: &SecretDocument, password: &str) -> Result<String, Error> {
    let encrypted_key_info = data
        .decode_msg::<pkcs8::EncryptedPrivateKeyInfo>()
        .context(UnableToParsePrivateKeySnafu)?;
    let decrypted_key_info = encrypted_key_info
        .decrypt(password)
        .context(UnableToDecryptPrivateKeySnafu)?;
    let decrypted_pem = decrypted_key_info
        .to_pem("PRIVATE KEY", LineEnding::CRLF)
        .context(FailedToCreatePemSnafu)?;

    Ok(decrypted_pem.to_string())
}
