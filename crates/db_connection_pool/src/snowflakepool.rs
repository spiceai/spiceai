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

const SNOWFLAKE_ACCOUNT_IDENTIFIER_DOCS: &str =
    "https://docs.snowflake.com/en/user-guide/admin-account-identifier";
const SNOWFLAKE_COMPUTING_COM_SUFFIX: &str = ".snowflakecomputing.com";
const SNOWFLAKE_COMPUTING_CN_SUFFIX: &str = ".snowflakecomputing.cn";
const ACCOUNT_IDENTIFIER_EXAMPLES: &str = "Use a Snowflake account identifier such as `myorg-myaccount`, `myorg.myaccount`, `https://myorg-myaccount.snowflakecomputing.com`, `xy12345`, `xy12345.us-east-2.aws`, or `xy12345.fhplus.us-gov-west-1.aws`.";
const MISSING_KEYPAIR_CREDENTIALS_REASON: &str = "keypair authentication requires either `snowflake_private_key` or `snowflake_private_key_path`";

/// Snowflake account identifier formats accepted by the connector.
///
/// The preferred client/driver format (`orgname-account_name`) and legacy
/// locator formats are already API URL host labels. The SQL/data sharing format
/// (`orgname.account_name`) uses a dot separator that must become a dash for the
/// API URL: `orgname-account_name.snowflakecomputing.com`.
///
/// See: <https://docs.snowflake.com/en/user-guide/admin-account-identifier>
#[derive(Debug, Clone, PartialEq, Eq)]
enum SnowflakeAccountIdentifier {
    /// `orgname.account_name` - dot is replaced with dash for the API URL.
    OrgQualified {
        orgname: String,
        account_name: String,
    },
    /// `orgname-account_name`, a bare locator, or a Snowflake account URL host stripped to its account identifier.
    AccountName { account_identifier: String },
    /// `account_locator[.region[.cloud]]` or `account_locator.gov_compliance.region.cloud`.
    Legacy { segments: Vec<String> },
}

impl SnowflakeAccountIdentifier {
    /// Returns the account identifier formatted for the Snowflake API URL.
    #[must_use]
    fn api_account(&self) -> String {
        match self {
            Self::OrgQualified {
                orgname,
                account_name,
            } => format!("{orgname}-{account_name}"),
            Self::AccountName { account_identifier } => account_identifier.clone(),
            Self::Legacy { segments } => segments.join("."),
        }
    }
}

fn account_identifier_error(reason: &str) -> String {
    format!("{reason}. {ACCOUNT_IDENTIFIER_EXAMPLES} See: {SNOWFLAKE_ACCOUNT_IDENTIFIER_DOCS}")
}

fn starts_with_ascii_letter(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphabetic)
}

fn validate_org_name(orgname: &str) -> std::result::Result<(), String> {
    if orgname.is_empty() {
        return Err(account_identifier_error(
            "the organization name before `.` cannot be empty",
        ));
    }

    if !starts_with_ascii_letter(orgname) {
        return Err(account_identifier_error(
            "the organization name before `.` must start with a letter",
        ));
    }

    if !orgname.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        return Err(account_identifier_error(
            "the organization name before `.` can contain only letters and digits",
        ));
    }

    Ok(())
}

fn validate_account_name(account_name: &str) -> std::result::Result<(), String> {
    if account_name.is_empty() {
        return Err(account_identifier_error(
            "the account name after `.` cannot be empty",
        ));
    }

    if !starts_with_ascii_letter(account_name) {
        return Err(account_identifier_error(
            "the account name after `.` must start with a letter",
        ));
    }

    if account_name.ends_with('_') {
        return Err(account_identifier_error(
            "the account name after `.` cannot end with `_`",
        ));
    }

    if !account_name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(account_identifier_error(
            "the account name after `.` can contain only letters, digits, and underscores",
        ));
    }

    Ok(())
}

fn validate_org_qualified_identifier_length(
    orgname: &str,
    account_name: &str,
) -> std::result::Result<(), String> {
    if orgname.len() + 1 + account_name.len() > 63 {
        return Err(account_identifier_error(
            "the preferred account identifier must be 63 characters or fewer, including the organization name, account name, and separator",
        ));
    }

    Ok(())
}

fn validate_account_host_label(account_identifier: &str) -> std::result::Result<(), String> {
    if account_identifier.is_empty() {
        return Err(account_identifier_error(
            "the account identifier cannot be empty",
        ));
    }

    if account_identifier.len() > 63 {
        return Err(account_identifier_error(
            "the preferred account identifier must be 63 characters or fewer",
        ));
    }

    if !starts_with_ascii_letter(account_identifier) {
        return Err(account_identifier_error(
            "the account identifier must start with a letter",
        ));
    }

    if account_identifier.ends_with('_') || account_identifier.ends_with('-') {
        return Err(account_identifier_error(
            "the account identifier cannot end with `_` or `-`",
        ));
    }

    if !account_identifier
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(account_identifier_error(
            "the account identifier can contain only letters, digits, underscores, and hyphens",
        ));
    }

    Ok(())
}

fn validate_legacy_segment(segment: &str) -> std::result::Result<(), String> {
    if segment.is_empty() {
        return Err(account_identifier_error(
            "legacy account locators cannot contain empty segments; remove extra dots",
        ));
    }

    if !starts_with_ascii_letter(segment) {
        return Err(account_identifier_error(
            "each legacy account locator segment must start with a letter",
        ));
    }

    if segment.ends_with('-') {
        return Err(account_identifier_error(
            "legacy account locator segments cannot end with `-`",
        ));
    }

    if !segment
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
    {
        return Err(account_identifier_error(
            "legacy account locator segments can contain only letters, digits, and hyphens",
        ));
    }

    Ok(())
}

fn validate_legacy_cloud_segment(segment: &str) -> std::result::Result<(), String> {
    if segment.eq_ignore_ascii_case("aws")
        || segment.eq_ignore_ascii_case("azure")
        || segment.eq_ignore_ascii_case("gcp")
    {
        return Ok(());
    }

    Err(account_identifier_error(
        "legacy account locators with a cloud segment must use `aws`, `azure`, or `gcp`",
    ))
}

fn validate_legacy_compliance_segment(segment: &str) -> std::result::Result<(), String> {
    if segment.eq_ignore_ascii_case("fhplus") || segment.eq_ignore_ascii_case("dod") {
        return Ok(());
    }

    Err(account_identifier_error(
        "legacy SnowGov account locators with four segments must use `fhplus` or `dod` as the compliance segment",
    ))
}

fn validate_legacy_locator_segments(parts: &[&str]) -> std::result::Result<(), String> {
    match parts {
        [_, _, cloud] => validate_legacy_cloud_segment(cloud),
        [_, compliance, _, cloud] => {
            validate_legacy_compliance_segment(compliance)?;
            validate_legacy_cloud_segment(cloud)
        }
        _ => Ok(()),
    }
}

fn extract_account_identifier_from_host(host: &str) -> std::result::Result<String, String> {
    if host.is_empty() {
        return Err(account_identifier_error(
            "Snowflake account URL is missing a host",
        ));
    }

    let host = host.strip_suffix('.').unwrap_or(host);
    let host_lowercase = host.to_ascii_lowercase();
    if host_lowercase.contains(".privatelink.") {
        return Err(account_identifier_error(
            "Snowflake PrivateLink account URLs are not supported by this connector because the Snowflake client connects to the public snowflakecomputing.com endpoint",
        ));
    }

    if host_lowercase.ends_with(SNOWFLAKE_COMPUTING_COM_SUFFIX) {
        let account_identifier = &host[..host.len() - SNOWFLAKE_COMPUTING_COM_SUFFIX.len()];
        if account_identifier.is_empty() {
            return Err(account_identifier_error(
                "Snowflake account URL is missing the account identifier before snowflakecomputing.com",
            ));
        }
        return Ok(account_identifier.to_string());
    }

    if host_lowercase.ends_with(SNOWFLAKE_COMPUTING_CN_SUFFIX) {
        return Err(account_identifier_error(
            "Snowflake account URLs on snowflakecomputing.cn are not supported by this connector because the Snowflake client connects to snowflakecomputing.com",
        ));
    }

    Err(account_identifier_error(
        "Snowflake account URLs must end with snowflakecomputing.com",
    ))
}

fn split_url_authority(input: &str) -> (&str, Option<&str>) {
    input.find(['/', '?', '#']).map_or((input, None), |index| {
        (&input[..index], Some(&input[index..]))
    })
}

fn normalize_account_identifier_input(input: &str) -> std::result::Result<String, String> {
    if input.is_empty() {
        return Err(account_identifier_error(
            "account identifier cannot be empty",
        ));
    }

    if input.trim() != input {
        return Err(account_identifier_error(
            "account identifier must not include leading or trailing whitespace",
        ));
    }

    if input.chars().any(char::is_whitespace) {
        return Err(account_identifier_error(
            "account identifier cannot contain whitespace",
        ));
    }

    if input.contains("://") {
        let Some((scheme, without_scheme)) = input.split_once("://") else {
            return Err(account_identifier_error("invalid Snowflake account URL"));
        };

        if !scheme.eq_ignore_ascii_case("https") {
            return Err(account_identifier_error(
                "Snowflake account URLs must use the https:// scheme",
            ));
        }

        let (authority, tail) = split_url_authority(without_scheme);
        if let Some(tail) = tail
            && tail != "/"
        {
            return Err(account_identifier_error(
                "Snowflake account URLs in `snowflake_account` must not include a path, query, or fragment",
            ));
        }

        if authority.contains('@') {
            return Err(account_identifier_error(
                "Snowflake account URLs in `snowflake_account` must not include credentials",
            ));
        }

        if authority.contains(':') {
            return Err(account_identifier_error(
                "Snowflake account URLs in `snowflake_account` must not include a port",
            ));
        }

        return extract_account_identifier_from_host(authority);
    }

    if input.contains(['/', '?', '#', '@', ':']) {
        return Err(account_identifier_error(
            "account identifier must not include URL paths, query strings, fragments, credentials, or ports",
        ));
    }

    if input.to_ascii_lowercase().contains("snowflakecomputing.") {
        return extract_account_identifier_from_host(input);
    }

    Ok(input.to_string())
}

impl FromStr for SnowflakeAccountIdentifier {
    type Err = String;

    /// Parses a Snowflake account identifier string.
    ///
    /// Org-qualified identifiers for SQL/data sharing (`orgname.account_name`)
    /// have exactly one dot and no dashes. They are converted to the
    /// client/driver form (`orgname-account_name`) before creating the
    /// Snowflake API client.
    ///
    /// Client/driver identifiers (`orgname-account_name`), account URLs, and
    /// legacy locators (`locator[.region[.cloud]]`) are preserved as-is after
    /// URL normalization.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = normalize_account_identifier_input(s)?;
        let parts: Vec<&str> = normalized.split('.').collect();

        if parts.len() > 4 {
            return Err(account_identifier_error(
                "legacy account locators can contain at most four dot-separated segments",
            ));
        }

        if parts.len() == 1 {
            let account_identifier = parts[0];
            validate_account_host_label(account_identifier)?;
            return Ok(Self::AccountName {
                account_identifier: account_identifier.to_string(),
            });
        }

        if parts.len() == 2 && !normalized.contains('-') {
            let orgname = parts[0];
            let account_name = parts[1];
            validate_org_name(orgname)?;
            validate_account_name(account_name)?;
            validate_org_qualified_identifier_length(orgname, account_name)?;
            return Ok(Self::OrgQualified {
                orgname: orgname.to_string(),
                account_name: account_name.to_string(),
            });
        }

        for segment in &parts {
            validate_legacy_segment(segment)?;
        }
        validate_legacy_locator_segments(&parts)?;

        if parts
            .iter()
            .any(|segment| segment.eq_ignore_ascii_case("cn-northwest-1"))
        {
            return Err(account_identifier_error(
                "Snowflake China region locators use snowflakecomputing.cn, which is not supported by this connector",
            ));
        }

        Ok(Self::Legacy {
            segments: parts.into_iter().map(ToString::to_string).collect(),
        })
    }
}

impl std::fmt::Display for SnowflakeAccountIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OrgQualified {
                orgname,
                account_name,
            } => write!(f, "{orgname}.{account_name}"),
            Self::AccountName { account_identifier } => write!(f, "{account_identifier}"),
            Self::Legacy { segments } => write!(f, "{}", segments.join(".")),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required Snowflake parameter `{name}`. Add `{name}` to the Snowflake dataset or catalog params, or configure a secret named `{name}`. For data connector details, visit: https://spiceai.org/docs/components/data-connectors/snowflake#auth. For catalog details, visit: https://spiceai.org/docs/components/catalogs/snowflake#auth"
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
        "Invalid value for Snowflake parameter `{parameter}`: {reason}. For data connector parameters, visit: https://spiceai.org/docs/components/data-connectors/snowflake#parameters. For catalog parameters, visit: https://spiceai.org/docs/components/catalogs/snowflake#parameters"
    ))]
    InvalidParameterValue { parameter: String, reason: String },

    #[snafu(display(
        "Failed to parse private key. Verify the key file or `snowflake_private_key` content is a valid PEM private key, and try again. {source}"
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
        "Both `snowflake_private_key` and `snowflake_private_key_path` are specified. Only one of these options can be specified for a given dataset. For details, visit: https://spiceai.org/docs/components/data-connectors/snowflake#auth"
    ))]
    MutuallyExclusivePrivateKeyParams,

    #[snafu(display(
        "Failed to set the Snowflake session timezone to UTC, which is required for correct `AT TIME ZONE` predicate pushdown. Verify the role can run `ALTER SESSION SET TIMEZONE`, and try again. {source}"
    ))]
    UnableToSetSessionTimezone {
        source: snowflake_api::SnowflakeApiError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn prefixed_parameter(parameter: &str) -> String {
    if parameter.starts_with("snowflake_") {
        parameter.to_string()
    } else {
        format!("snowflake_{parameter}")
    }
}

fn invalid_parameter(parameter: &str, reason: impl Into<String>) -> Error {
    Error::InvalidParameterValue {
        parameter: prefixed_parameter(parameter),
        reason: reason.into(),
    }
}

fn validate_non_empty_parameter(parameter: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(invalid_parameter(parameter, "value must not be empty"));
    }

    Ok(())
}

fn validate_text_parameter(parameter: &str, value: &str) -> Result<()> {
    validate_non_empty_parameter(parameter, value)?;

    if value.trim() != value {
        return Err(invalid_parameter(
            parameter,
            "value must not include leading or trailing whitespace",
        ));
    }

    Ok(())
}

fn required_parameter<'a>(
    params: &'a HashMap<String, SecretString>,
    parameter: &str,
) -> Result<&'a str> {
    let value = params
        .get(parameter)
        .map(SecretBox::expose_secret)
        .context(MissingRequiredSecretSnafu {
            name: prefixed_parameter(parameter),
        })?;

    Ok(value)
}

fn required_text_parameter<'a>(
    params: &'a HashMap<String, SecretString>,
    parameter: &str,
) -> Result<&'a str> {
    let value = required_parameter(params, parameter)?;
    validate_text_parameter(parameter, value)?;
    Ok(value)
}

fn optional_text_parameter(
    params: &HashMap<String, SecretString>,
    parameter: &str,
) -> Result<Option<String>> {
    let Some(value) = params.get(parameter).map(SecretBox::expose_secret) else {
        return Ok(None);
    };

    validate_text_parameter(parameter, value)?;

    Ok(Some(value.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnowflakeAuthType {
    Password,
    KeyPair,
}

impl SnowflakeAuthType {
    fn from_params(params: &HashMap<String, SecretString>) -> Result<Self> {
        let Some(auth_type) = params.get("auth_type").map(SecretBox::expose_secret) else {
            if !params.contains_key("password")
                && (params.contains_key("private_key") || params.contains_key("private_key_path"))
            {
                return Ok(Self::KeyPair);
            }

            return Ok(Self::Password);
        };

        validate_text_parameter("auth_type", auth_type)?;

        match auth_type.to_ascii_lowercase().as_str() {
            "snowflake" | "password" => Ok(Self::Password),
            "keypair" | "snowflake_jwt" => Ok(Self::KeyPair),
            _ => Err(invalid_parameter(
                "auth_type",
                "supported values are `password` (or `snowflake`) and `keypair` (or `snowflake_jwt`)",
            )),
        }
    }
}

fn validate_authentication_parameters(
    params: &HashMap<String, SecretString>,
    auth_type: SnowflakeAuthType,
) -> Result<()> {
    match auth_type {
        SnowflakeAuthType::Password => {
            if params.contains_key("private_key")
                || params.contains_key("private_key_path")
                || params.contains_key("private_key_passphrase")
            {
                return Err(invalid_parameter(
                    "auth_type",
                    "keypair parameters were provided but password authentication is selected; remove `snowflake_private_key`, `snowflake_private_key_path`, and `snowflake_private_key_passphrase`, or set `snowflake_auth_type: keypair`",
                ));
            }
        }
        SnowflakeAuthType::KeyPair => {
            if params.contains_key("password") {
                return Err(invalid_parameter(
                    "auth_type",
                    "`snowflake_password` was provided but keypair authentication is selected; remove `snowflake_password` or set `snowflake_auth_type: password`",
                ));
            }

            if !params.contains_key("private_key") && !params.contains_key("private_key_path") {
                return Err(invalid_parameter(
                    "auth_type",
                    MISSING_KEYPAIR_CREDENTIALS_REASON,
                ));
            }
        }
    }

    Ok(())
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
    pub async fn new(params: &HashMap<String, SecretString>) -> Result<Self> {
        let pool_start = Instant::now();

        let username = required_text_parameter(params, "username")?;

        let account_raw = required_parameter(params, "account")?;
        let account_id = SnowflakeAccountIdentifier::from_str(account_raw).map_err(|reason| {
            Error::InvalidParameterValue {
                parameter: prefixed_parameter("account"),
                reason,
            }
        })?;
        let account = account_id.api_account();

        let warehouse = optional_text_parameter(params, "warehouse")?;
        let role = optional_text_parameter(params, "role")?;

        let auth_type = SnowflakeAuthType::from_params(params)?;
        validate_authentication_parameters(params, auth_type)?;

        let api = match auth_type {
            SnowflakeAuthType::Password => init_snowflake_api_with_password_auth(
                &account,
                username,
                warehouse.as_deref(),
                role.as_deref(),
                params,
            )?,
            SnowflakeAuthType::KeyPair => init_snowflake_api_with_keypair_auth(
                &account,
                username,
                warehouse.as_deref(),
                role.as_deref(),
                params,
            )?,
        };

        tracing::debug!("Snowflake API client created, validating connectivity...");
        let validation_start = Instant::now();
        if let Err(err) = api.exec("SELECT 1").await {
            tracing::warn!(duration_ms = validation_start.elapsed().as_millis(), error = %err, "Snowflake connectivity validation failed");
            match err {
                snowflake_api::SnowflakeApiError::AuthError(auth_err) => {
                    // For incorrect warehouse or account params the library fails
                    // with a response decoding message that confuses, so return a generic error.
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

        // Pin the session timezone to UTC. Federated `AT TIME ZONE` predicates are
        // unparsed to `CAST(CONVERT_TIMEZONE('tz', ...) AS TIMESTAMP_NTZ)` chains that
        // interpret the intermediate naive timestamp in the session zone, so a non-UTC
        // session would silently shift predicate boundaries. Fail rather than risk
        // returning wrong results — `ALTER SESSION SET TIMEZONE` is available to any role.
        api.exec("ALTER SESSION SET TIMEZONE = 'UTC'")
            .await
            .context(UnableToSetSessionTimezoneSnafu)?;

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
    warehouse: Option<&str>,
    role: Option<&str>,
    params: &HashMap<String, SecretString>,
) -> Result<SnowflakeApi> {
    let password = required_parameter(params, "password")?;
    validate_non_empty_parameter("password", password)?;

    let api =
        SnowflakeApi::with_password_auth(account, warehouse, None, None, username, role, password)
            .context(UnableToConnectSnafu)?;

    Ok(api)
}

fn init_snowflake_api_with_keypair_auth(
    account: &str,
    username: &str,
    warehouse: Option<&str>,
    role: Option<&str>,
    params: &HashMap<String, SecretString>,
) -> Result<SnowflakeApi> {
    let private_key_content = params.get("private_key").map(SecretBox::expose_secret);
    let private_key_path = params.get("private_key_path").map(SecretBox::expose_secret);

    let mut private_key_pem: String = match (private_key_content, private_key_path) {
        (Some(_), Some(_)) => {
            return MutuallyExclusivePrivateKeyParamsSnafu.fail();
        }
        (Some(content), None) => {
            validate_non_empty_parameter("private_key", content)?;
            content.to_string()
        }
        (None, Some(path)) => {
            validate_text_parameter("private_key_path", path)?;
            fs::read_to_string(path).context(ErrorReadingPrivateKeyFileSnafu {
                file_path: path.to_string(),
            })?
        }
        (None, None) => {
            return Err(invalid_parameter(
                "auth_type",
                MISSING_KEYPAIR_CREDENTIALS_REASON,
            ));
        }
    };

    let (label, data) =
        SecretDocument::from_pem(&private_key_pem).context(UnableToParsePrivateKeySnafu)?;

    if label.to_uppercase() == "ENCRYPTED PRIVATE KEY" {
        let passphrase = params
            .get("private_key_passphrase")
            .map(SecretBox::expose_secret)
            .context(MissingRequiredSecretSnafu {
                name: "snowflake_private_key_passphrase".to_string(),
            })?;

        private_key_pem = decode_pkcs8_encrypted_data(&data, passphrase)?;
    }

    let api = SnowflakeApi::with_certificate_auth(
        account,
        warehouse,
        None,
        None,
        username,
        role,
        &private_key_pem,
    )
    .context(UnableToConnectSnafu)?;

    Ok(api)
}

#[async_trait]
impl DbConnectionPool<Arc<SnowflakeApi>, &'static dyn Sync> for SnowflakeConnectionPool {
    async fn connect(
        &self,
    ) -> std::result::Result<
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

fn decode_pkcs8_encrypted_data(data: &SecretDocument, password: &str) -> Result<String> {
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

    fn assert_invalid_account_identifier(account_identifier: &str) -> String {
        account_identifier
            .parse::<SnowflakeAccountIdentifier>()
            .expect_err("reject invalid Snowflake account identifier")
    }

    fn secret_params(params: &[(&str, &str)]) -> HashMap<String, SecretString> {
        params
            .iter()
            .map(|(key, value)| ((*key).to_string(), SecretString::from(*value)))
            .collect()
    }

    #[test]
    fn org_qualified_format_uses_dash_for_api_account() {
        let id: SnowflakeAccountIdentifier = "myorg.myaccount"
            .parse()
            .expect("parse org-qualified Snowflake account identifier");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::OrgQualified {
                orgname: "myorg".to_string(),
                account_name: "myaccount".to_string(),
            }
        );
        assert_eq!(id.api_account(), "myorg-myaccount");
        assert_eq!(id.to_string(), "myorg.myaccount");
    }

    #[test]
    fn org_qualified_format_supports_account_name_underscores() {
        let id: SnowflakeAccountIdentifier = "myorg.my_account"
            .parse()
            .expect("parse account name with underscores");
        assert_eq!(id.api_account(), "myorg-my_account");
    }

    #[test]
    fn preferred_client_driver_format_is_preserved() {
        let id: SnowflakeAccountIdentifier = "myorg-myaccount"
            .parse()
            .expect("parse preferred client-driver account identifier");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::AccountName {
                account_identifier: "myorg-myaccount".to_string(),
            }
        );
        assert_eq!(id.api_account(), "myorg-myaccount");
    }

    #[test]
    fn account_urls_are_normalized_to_api_account() {
        let id: SnowflakeAccountIdentifier = "https://myorg-myaccount.snowflakecomputing.com"
            .parse()
            .expect("parse Snowflake account URL");
        assert_eq!(id.api_account(), "myorg-myaccount");

        let id: SnowflakeAccountIdentifier = "xy12345.us-east-2.aws.snowflakecomputing.com"
            .parse()
            .expect("parse Snowflake account host");
        assert_eq!(id.api_account(), "xy12345.us-east-2.aws");
    }

    #[test]
    fn legacy_locator_with_region_is_preserved() {
        let id: SnowflakeAccountIdentifier = "sb70577.eu-central-1"
            .parse()
            .expect("parse legacy Snowflake account identifier with region");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::Legacy {
                segments: vec!["sb70577".to_string(), "eu-central-1".to_string()],
            }
        );
        assert_eq!(id.api_account(), "sb70577.eu-central-1");
    }

    #[test]
    fn legacy_locator_with_region_and_cloud_is_preserved() {
        let id: SnowflakeAccountIdentifier = "xy12345.us-east-2.aws"
            .parse()
            .expect("parse legacy Snowflake account identifier with region and cloud");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::Legacy {
                segments: vec![
                    "xy12345".to_string(),
                    "us-east-2".to_string(),
                    "aws".to_string(),
                ],
            }
        );
        assert_eq!(id.api_account(), "xy12345.us-east-2.aws");
    }

    #[test]
    fn legacy_snowgov_locator_with_compliance_segment_is_preserved() {
        let id: SnowflakeAccountIdentifier = "xy12345.fhplus.us-gov-west-1.aws"
            .parse()
            .expect("parse legacy SnowGov Snowflake account identifier");
        assert_eq!(id.api_account(), "xy12345.fhplus.us-gov-west-1.aws");
    }

    #[test]
    fn legacy_bare_locator_is_preserved() {
        let id: SnowflakeAccountIdentifier = "xy12345"
            .parse()
            .expect("parse legacy Snowflake account locator");
        assert_eq!(
            id,
            SnowflakeAccountIdentifier::AccountName {
                account_identifier: "xy12345".to_string(),
            }
        );
        assert_eq!(id.api_account(), "xy12345");
    }

    #[test]
    fn invalid_account_identifiers_are_rejected_with_actionable_message() {
        for account_identifier in [
            "",
            " myorg-myaccount",
            "my org-myaccount",
            ".myaccount",
            "myorg.",
            "my_org.myaccount",
            "myorg.my_account_",
            "myorg.my$account",
            "xy12345..aws",
            "xy12345.us-east-1.",
            "xy12345.us-east-2.notcloud",
            "xy12345.us-east-2.aws.extra",
            "xy12345.invalid.us-gov-west-1.aws",
            "xy12345.us-east-2.aws.extra.segment",
            "myorganizationname.myaccountnamewithmorethanallowedidentifierchars",
            "https://xy12345.us-east-2.aws.snowflakecomputing.com/console",
            "https://xy12345.us-east-2.aws.privatelink.snowflakecomputing.com",
            "https://xy12345.cn-northwest-1.aws.snowflakecomputing.cn",
            "xy12345.cn-northwest-1.aws",
        ] {
            let error = assert_invalid_account_identifier(account_identifier);
            assert!(
                error.contains("account identifier") || error.contains("Snowflake account URL"),
                "error should describe the invalid account identifier: {error}"
            );
            assert!(
                error.contains(SNOWFLAKE_ACCOUNT_IDENTIFIER_DOCS),
                "error should link to Snowflake account identifier docs: {error}"
            );
        }
    }

    #[test]
    fn auth_type_accepts_documented_and_legacy_aliases() {
        let params = secret_params(&[]);
        assert_eq!(
            SnowflakeAuthType::from_params(&params).expect("default auth type"),
            SnowflakeAuthType::Password
        );

        let params = secret_params(&[("auth_type", "password")]);
        assert_eq!(
            SnowflakeAuthType::from_params(&params).expect("password auth type"),
            SnowflakeAuthType::Password
        );

        let params = secret_params(&[("auth_type", "snowflake")]);
        assert_eq!(
            SnowflakeAuthType::from_params(&params).expect("legacy password auth type"),
            SnowflakeAuthType::Password
        );

        let params = secret_params(&[("auth_type", "keypair")]);
        assert_eq!(
            SnowflakeAuthType::from_params(&params).expect("keypair auth type"),
            SnowflakeAuthType::KeyPair
        );

        let params = secret_params(&[("auth_type", "snowflake_jwt")]);
        assert_eq!(
            SnowflakeAuthType::from_params(&params).expect("Snowflake JWT auth type"),
            SnowflakeAuthType::KeyPair
        );
    }

    #[test]
    fn auth_type_defaults_to_keypair_when_only_keypair_credentials_are_present() {
        let params = secret_params(&[("private_key_path", "/path/to/key.pem")]);
        assert_eq!(
            SnowflakeAuthType::from_params(&params).expect("infer keypair auth type"),
            SnowflakeAuthType::KeyPair
        );
    }

    #[test]
    fn invalid_auth_type_is_rejected_with_user_facing_parameter_name() {
        let params = secret_params(&[("auth_type", "oauth")]);
        let error = SnowflakeAuthType::from_params(&params)
            .expect_err("reject unsupported auth type")
            .to_string();

        assert!(error.contains("snowflake_auth_type"));
        assert!(error.contains("password"));
        assert!(error.contains("keypair"));
    }

    #[test]
    fn conflicting_authentication_parameters_are_rejected() {
        let params = secret_params(&[("auth_type", "password"), ("private_key_path", "key.pem")]);
        let error = validate_authentication_parameters(&params, SnowflakeAuthType::Password)
            .expect_err("reject keypair params with password auth")
            .to_string();
        assert!(error.contains("snowflake_auth_type"));
        assert!(error.contains("snowflake_private_key_path"));

        let params = secret_params(&[("auth_type", "keypair"), ("password", "secret")]);
        let error = validate_authentication_parameters(&params, SnowflakeAuthType::KeyPair)
            .expect_err("reject password with keypair auth")
            .to_string();
        assert!(error.contains("snowflake_auth_type"));
        assert!(error.contains("snowflake_password"));
    }

    #[test]
    fn keypair_auth_requires_private_key_or_path() {
        let params = secret_params(&[("auth_type", "keypair")]);
        let error = validate_authentication_parameters(&params, SnowflakeAuthType::KeyPair)
            .expect_err("reject missing keypair credentials")
            .to_string();

        assert!(error.contains("snowflake_auth_type"));
        assert!(error.contains("snowflake_private_key"));
        assert!(error.contains("snowflake_private_key_path"));
        assert!(
            !error.contains("secret named `snowflake_private_key or snowflake_private_key_path`")
        );
    }
}
