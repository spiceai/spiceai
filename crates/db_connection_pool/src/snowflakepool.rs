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

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::{
    DbConnectionPool, JoinPushDown, dbconnection::DbConnection,
};
use pkcs8::{LineEnding, SecretDocument};
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::prelude::*;
use snowflake_api::{SnowflakeApi, SnowflakeApiError};
use std::{collections::HashMap, fmt::Write, fs, str::FromStr, sync::Arc, time::Instant};

use crate::dbconnection::snowflakeconn::SnowflakeConnection;

/// Snowflake account identifier formats.
///
/// The org-based format (`orgname.account_name`) uses a dot separator that must
/// become a dash in the API URL: `orgname-account_name.snowflakecomputing.com`.
///
/// The legacy format (`account_locator` with optional `.region.cloud` suffix)
/// uses dots as subdomain separators and must be preserved as-is:
/// `account_locator.region.cloud.snowflakecomputing.com`.
///
/// See: <https://docs.snowflake.com/en/user-guide/admin-account-identifier>
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnowflakeAccountIdentifier {
    /// `orgname.account_name` — dot is replaced with dash for the API URL.
    OrgBased {
        orgname: String,
        account_name: String,
    },
    /// `account_locator[.region[.cloud]]` — used as-is in the API URL.
    Legacy {
        account_locator: String,
        region: Option<String>,
        cloud: Option<String>,
    },
}

impl SnowflakeAccountIdentifier {
    /// Returns the account identifier formatted for the Snowflake API URL.
    #[must_use]
    pub fn api_account(&self) -> String {
        match self {
            Self::OrgBased {
                orgname,
                account_name,
            } => format!("{orgname}-{account_name}"),
            Self::Legacy {
                account_locator,
                region: Some(r),
                cloud: Some(c),
            } => format!("{account_locator}.{r}.{c}"),
            Self::Legacy {
                account_locator,
                region: Some(r),
                cloud: None,
            } => format!("{account_locator}.{r}"),

            Self::Legacy {
                account_locator,
                region: _,
                cloud: _,
            } => account_locator.clone(),
        }
    }
}

fn is_valid_org_identifier_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

impl FromStr for SnowflakeAccountIdentifier {
    type Err = String;

    /// Parses a Snowflake account identifier string.
    ///
    /// Org-based identifiers (`orgname.account_name`) have exactly one dot and
    /// no dashes — both org and account names are alphanumeric + underscores.
    ///
    /// Legacy identifiers contain dashes (from region names like `eu-central-1`)
    /// or multiple dots (`locator.region.cloud`), or no dots at all (bare locator).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err("account identifier cannot be empty".to_string());
        }

        let dot_count = s.matches('.').count();

        // Exactly one dot and no dashes → org-based format.
        // Org names and account names are alphanumeric + underscores only.
        if dot_count == 1 && !s.contains('-') {
            let (orgname, account_name) = s
                .split_once('.')
                .ok_or_else(|| format!("invalid org-based account identifier: {s}"))?;
            if !is_valid_org_identifier_segment(orgname)
                || !is_valid_org_identifier_segment(account_name)
            {
                return Err(format!("invalid org-based account identifier: {s}"));
            }
            return Ok(Self::OrgBased {
                orgname: orgname.to_string(),
                account_name: account_name.to_string(),
            });
        }

        // Legacy format: account_locator[.region[.cloud]]
        let parts: Vec<&str> = s.splitn(3, '.').collect();
        let has_empty_segment = parts.iter().any(|segment| segment.is_empty());
        if has_empty_segment {
            return Err(format!("invalid account identifier: {s}"));
        }

        match parts.as_slice() {
            [locator] => Ok(Self::Legacy {
                account_locator: (*locator).to_string(),
                region: None,
                cloud: None,
            }),
            [locator, region] => Ok(Self::Legacy {
                account_locator: (*locator).to_string(),
                region: Some((*region).to_string()),
                cloud: None,
            }),
            [locator, region, cloud] => Ok(Self::Legacy {
                account_locator: (*locator).to_string(),
                region: Some((*region).to_string()),
                cloud: Some((*cloud).to_string()),
            }),
            _ => Err(format!("invalid account identifier: {s}")),
        }
    }
}

impl std::fmt::Display for SnowflakeAccountIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OrgBased {
                orgname,
                account_name,
            } => write!(f, "{orgname}.{account_name}"),
            Self::Legacy {
                account_locator,
                region,
                cloud,
            } => match (region, cloud) {
                (Some(r), Some(c)) => write!(f, "{account_locator}.{r}.{c}"),
                (Some(r), None) => write!(f, "{account_locator}.{r}"),
                _ => write!(f, "{account_locator}"),
            },
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required secret: {name}. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/snowflake#auth"
    ))]
    MissingRequiredSecret { name: String },

    #[snafu(display(
        "Failed to connect to Snowflake. Verify your Snowflake configuration, and try again. {source}"
    ))]
    UnableToConnect {
        source: snowflake_api::SnowflakeApiError,
    },

    #[snafu(display(
        "Failed to authenticate with Snowflake. Verify your credentials, and try again. {source}"
    ))]
    UnableToAuthenticate {
        source: snowflake_api::SnowflakeApiError,
    },

    #[snafu(display(
        "Failed to authenticate with Snowflake. Verify your credentials and warehouse parameters using the SnowSQL tool: https://docs.snowflake.com/en/user-guide/snowsql"
    ))]
    UnableToAuthenticateGeneric {},

    #[snafu(display(
        "Failed to read private key file {file_path}. Verify the key file exists with the necessary permissions, and try again. {source}"
    ))]
    ErrorReadingPrivateKeyFile {
        source: std::io::Error,
        file_path: String,
    },

    #[snafu(display(
        "Invalid value for parameter '{param_key}': {param_value}. For details, visit: https://spiceai.org/docs/components/data-connectors/snowflake#parameters"
    ))]
    InvalidParameterValue {
        param_key: String,
        param_value: String,
    },

    #[snafu(display(
        "Failed to parse private key file. Verify the file is a private key file, and try again. {source}"
    ))]
    UnableToParsePrivateKey { source: pkcs8::der::Error },

    #[snafu(display(
        "Unable to decrypt private key file. Verify the passphrase, and try again. {source}"
    ))]
    UnableToDecryptPrivateKey { source: pkcs8::Error },

    #[snafu(display(
        "Failed to save decrypted private key content as PEM. Verify filesystem permissions, and try again. {source}"
    ))]
    FailedToCreatePem { source: pkcs8::der::Error },

    #[snafu(display(
        "Both 'snowflake_private_key' and 'snowflake_private_key_path' are specified. Only one of these options can be specified for a given dataset. For details, visit: https://spiceai.org/docs/components/data-connectors/snowflake#auth"
    ))]
    MutuallyExclusivePrivateKeyParams,
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
        let pool_start = Instant::now();

        let username = params
            .get("username")
            .map(SecretBox::expose_secret)
            .context(MissingRequiredSecretSnafu { name: "username" })?;

        let account_raw = params
            .get("account")
            .map(SecretBox::expose_secret)
            .context(MissingRequiredSecretSnafu { name: "account" })?;
        let account_id = SnowflakeAccountIdentifier::from_str(account_raw).map_err(|e| {
            Error::InvalidParameterValue {
                param_key: "account".to_string(),
                param_value: e,
            }
        })?;
        let account = account_id.api_account();

        let warehouse = params
            .get("warehouse")
            .map(SecretBox::expose_secret)
            .map(ToString::to_string);
        let role = params
            .get("role")
            .map(SecretBox::expose_secret)
            .map(ToString::to_string);

        let auth_type = params
            .get("auth_type")
            .map(SecretBox::expose_secret)
            .map_or_else(|| "snowflake".to_string(), ToString::to_string)
            .to_lowercase();

        let api = match auth_type.as_str() {
            "snowflake" => init_snowflake_api_with_password_auth(
                &account,
                username,
                warehouse.as_ref(),
                role.as_ref(),
                params,
            )?,
            "keypair" => init_snowflake_api_with_keypair_auth(
                &account,
                username,
                warehouse.as_ref(),
                role.as_ref(),
                params,
            )?,
            _ => InvalidParameterValueSnafu {
                param_key: "snowflake_auth_type",
                param_value: auth_type,
            }
            .fail()?,
        };

        tracing::debug!("Snowflake API client created, validating connectivity...");
        let validation_start = Instant::now();
        if let Err(err) = api.exec("SELECT 1").await {
            tracing::warn!(duration_ms = validation_start.elapsed().as_millis(), error = %err, "Snowflake connectivity validation failed");
            match err {
                snowflake_api::SnowflakeApiError::AuthError(auth_err) => {
                    // for incorrect werehouse or account param the library fails
                    // with response decoding message that confuses, so we return a generic error
                    if auth_err
                        .to_string()
                        .contains("error decoding response body")
                    {
                        return Err(Error::UnableToAuthenticateGeneric {});
                    }

                    return Err(Error::UnableToAuthenticate {
                        source: SnowflakeApiError::AuthError(auth_err),
                    });
                }
                _ => {
                    return Err(Error::UnableToConnect { source: err });
                }
            }
        }

        tracing::debug!(
            duration_ms = validation_start.elapsed().as_millis(),
            "Snowflake connectivity validation succeeded"
        );

        let mut join_push_context_str = format!("username={username},account={account}");
        if let Some(warehouse) = warehouse {
            let _ = write!(join_push_context_str, ",warehouse={warehouse}");
        }
        if let Some(role) = role {
            let _ = write!(join_push_context_str, ",role={role}");
        }

        tracing::info!(
            duration_ms = pool_start.elapsed().as_millis(),
            "Snowflake connection pool created"
        );

        Ok(Self {
            api: Arc::new(api),
            join_push_down: JoinPushDown::AllowedFor(join_push_context_str),
        })
    }
}

fn init_snowflake_api_with_password_auth(
    account: &str,
    username: &str,
    warehouse: Option<&String>,
    role: Option<&String>,
    params: &HashMap<String, SecretString>,
) -> Result<SnowflakeApi, Error> {
    let password = params
        .get("password")
        .map(SecretBox::expose_secret)
        .context(MissingRequiredSecretSnafu { name: "password" })?;
    let api = SnowflakeApi::with_password_auth(
        account,
        warehouse.map(String::as_str),
        None,
        None,
        username,
        role.map(String::as_str),
        password,
    )
    .context(UnableToConnectSnafu)?;

    Ok(api)
}

fn init_snowflake_api_with_keypair_auth(
    account: &str,
    username: &str,
    warehouse: Option<&String>,
    role: Option<&String>,
    params: &HashMap<String, SecretString>,
) -> Result<SnowflakeApi, Error> {
    let private_key_content = params.get("private_key").map(SecretBox::expose_secret);
    let private_key_path = params.get("private_key_path").map(SecretBox::expose_secret);

    let mut private_key_pem: String = match (private_key_content, private_key_path) {
        (Some(_), Some(_)) => {
            return MutuallyExclusivePrivateKeyParamsSnafu.fail();
        }
        (Some(content), None) => content.to_string(),
        (None, Some(path)) => {
            fs::read_to_string(path).context(ErrorReadingPrivateKeyFileSnafu { file_path: path })?
        }
        (None, None) => {
            return MissingRequiredSecretSnafu {
                name: "snowflake_private_key or snowflake_private_key_path",
            }
            .fail();
        }
    };

    let (label, data) =
        SecretDocument::from_pem(&private_key_pem).context(UnableToParsePrivateKeySnafu)?;

    if label.to_uppercase() == "ENCRYPTED PRIVATE KEY" {
        let passphrase = params
            .get("private_key_passphrase")
            .map(SecretBox::expose_secret)
            .context(MissingRequiredSecretSnafu {
                name: "snowflake_private_key_passphrase",
            })?;

        private_key_pem = decode_pkcs8_encrypted_data(&data, passphrase)?;
    }

    let api = SnowflakeApi::with_certificate_auth(
        account,
        warehouse.map(String::as_str),
        None,
        None,
        username,
        role.map(String::as_str),
        &private_key_pem,
    )
    .context(UnableToConnectSnafu)?;

    Ok(api)
}

#[async_trait]
impl DbConnectionPool<Arc<SnowflakeApi>, &'static dyn Sync> for SnowflakeConnectionPool {
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<Arc<SnowflakeApi>, &'static dyn Sync>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_invalid_account_identifier(account_identifier: &str) {
        let _error = account_identifier
            .parse::<SnowflakeAccountIdentifier>()
            .expect_err("reject invalid Snowflake account identifier");
    }

    #[test]
    fn test_org_based_format() {
        let id: SnowflakeAccountIdentifier = "myorg.myaccount"
            .parse()
            .expect("parse org-based Snowflake account identifier");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::OrgBased {
                orgname: "myorg".to_string(),
                account_name: "myaccount".to_string(),
            }
        );
        assert_eq!(id.api_account(), "myorg-myaccount");
        assert_eq!(id.to_string(), "myorg.myaccount");
    }

    #[test]
    fn test_org_based_with_underscores() {
        let id: SnowflakeAccountIdentifier = "my_org.my_account"
            .parse()
            .expect("parse org-based Snowflake account identifier with underscores");
        assert_eq!(id.api_account(), "my_org-my_account");
    }

    #[test]
    fn test_legacy_locator_with_region() {
        let id: SnowflakeAccountIdentifier = "sb70577.eu-central-1"
            .parse()
            .expect("parse legacy Snowflake account identifier with region");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::Legacy {
                account_locator: "sb70577".to_string(),
                region: Some("eu-central-1".to_string()),
                cloud: None,
            }
        );
        assert_eq!(id.api_account(), "sb70577.eu-central-1");
    }

    #[test]
    fn test_legacy_locator_with_region_and_cloud() {
        let id: SnowflakeAccountIdentifier = "xy12345.us-east-2.aws"
            .parse()
            .expect("parse legacy Snowflake account identifier with region and cloud");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::Legacy {
                account_locator: "xy12345".to_string(),
                region: Some("us-east-2".to_string()),
                cloud: Some("aws".to_string()),
            }
        );
        assert_eq!(id.api_account(), "xy12345.us-east-2.aws");
    }

    #[test]
    fn test_legacy_bare_locator() {
        let id: SnowflakeAccountIdentifier = "xy12345"
            .parse()
            .expect("parse legacy Snowflake account locator");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::Legacy {
                account_locator: "xy12345".to_string(),
                region: None,
                cloud: None,
            }
        );
        assert_eq!(id.api_account(), "xy12345");
    }

    #[test]
    fn test_already_dashed_org_format() {
        let id: SnowflakeAccountIdentifier = "myorg-myaccount"
            .parse()
            .expect("parse dashed Snowflake account identifier as legacy locator");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::Legacy {
                account_locator: "myorg-myaccount".to_string(),
                region: None,
                cloud: None,
            }
        );
        assert_eq!(id.api_account(), "myorg-myaccount");
    }

    #[test]
    fn test_empty_is_err() {
        assert_invalid_account_identifier("");
    }

    #[test]
    fn test_leading_dot_is_err() {
        assert_invalid_account_identifier(".myaccount");
    }

    #[test]
    fn test_trailing_dot_is_err() {
        assert_invalid_account_identifier("myorg.");
    }

    #[test]
    fn test_org_based_with_invalid_character_is_err() {
        assert_invalid_account_identifier("myorg.my$account");
    }

    #[test]
    fn test_legacy_with_empty_region_segment_is_err() {
        assert_invalid_account_identifier("xy12345..aws");
    }

    #[test]
    fn test_legacy_with_empty_cloud_segment_is_err() {
        assert_invalid_account_identifier("xy12345.us-east-1.");
    }
}
