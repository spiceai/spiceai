/*
Copyright 2024-2026 The Spice.ai OSS Authors

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
use std::str::FromStr;

use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// IAM role credential source restriction for a Bedrock model, from the
/// `aws_iam_role_source` model param.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IamRoleSource {
    /// The default AWS credential chain.
    Auto,
    /// Only instance/container metadata (IMDS, ECS, EKS/IRSA).
    Metadata,
    /// Only environment variables.
    Env,
}

impl IamRoleSource {
    /// The values accepted in a Spicepod. The parameter spec validates against this same
    /// slice, so the documented vocabulary and the parsed one cannot drift.
    pub const VALUES: &'static [&'static str] = &["auto", "metadata", "env"];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Metadata => "metadata",
            Self::Env => "env",
        }
    }
}

impl FromStr for IamRoleSource {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "metadata" => Ok(Self::Metadata),
            "env" => Ok(Self::Env),
            other => Err(format!(
                "must be one of: {}. Found {other}",
                Self::VALUES.join(", ")
            )),
        }
    }
}

/// Trace behavior for a Bedrock guardrail, from the `trace` model param.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardrailTraceMode {
    Enabled,
    Disabled,
    EnabledFull,
}

impl GuardrailTraceMode {
    /// The values accepted in a Spicepod. The parameter spec validates against this same
    /// slice, so the documented vocabulary and the parsed one cannot drift.
    pub const VALUES: &'static [&'static str] = &["enabled", "disabled", "enabled_full"];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::EnabledFull => "enabled_full",
        }
    }
}

impl FromStr for GuardrailTraceMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            "enabled_full" => Ok(Self::EnabledFull),
            other => Err(format!(
                "must be one of: {}. Found {other}",
                Self::VALUES.join(", ")
            )),
        }
    }
}

/// Parameters for `from: bedrock` chat models.
#[derive(TypedParams)]
#[params(
    prefix = "bedrock",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct BedrockModelParams {
    /// The AWS access key ID to use for Bedrock models.
    #[param(runtime, autoload_secret)]
    pub aws_access_key_id: Option<SecretString>,
    /// The AWS secret access key to use for Bedrock models.
    #[param(runtime, autoload_secret)]
    pub aws_secret_access_key: Option<SecretString>,
    /// The AWS session token to use for Bedrock models.
    #[param(runtime, autoload_secret)]
    pub aws_session_token: Option<SecretString>,
    /// The AWS region to use for Bedrock models.
    #[param(runtime)]
    pub aws_region: Option<String>,
    /// IAM role credential source. 'auto' uses the default AWS credential chain, 'metadata' uses only instance/container metadata (IMDS, ECS, EKS/IRSA), 'env' uses only environment variables.
    #[param(runtime)]
    pub aws_iam_role_source: Option<IamRoleSource>,
    /// Identifier for the guardrail.
    pub guardrail_identifier: Option<String>,
    /// Guardrail version.
    pub guardrail_version: Option<String>,
    /// Trace behavior for the guardrail. Valid values: `enabled`, `disabled`, `enabled_full`.
    pub trace: Option<GuardrailTraceMode>,
}

impl BedrockModelParams {
    /// Builds the AWS-credential map consumed by
    /// [`crate::model::util::create_bedrock_client`]. Keys mirror the historical
    /// runtime params so credential resolution is unchanged.
    #[must_use]
    pub fn runtime_params(&self) -> HashMap<String, SecretString> {
        let mut params = HashMap::new();
        if let Some(value) = &self.aws_access_key_id {
            params.insert("aws_access_key_id".to_string(), value.clone());
        }
        if let Some(value) = &self.aws_secret_access_key {
            params.insert("aws_secret_access_key".to_string(), value.clone());
        }
        if let Some(value) = &self.aws_session_token {
            params.insert("aws_session_token".to_string(), value.clone());
        }
        if let Some(value) = &self.aws_region {
            params.insert("aws_region".to_string(), SecretString::from(value.clone()));
        }
        if let Some(value) = self.aws_iam_role_source {
            params.insert(
                "aws_iam_role_source".to_string(),
                SecretString::from(value.as_str().to_string()),
            );
        }
        params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iam_role_source_parses_the_documented_values_case_insensitively() {
        for (raw, expected) in [
            ("auto", IamRoleSource::Auto),
            ("Auto", IamRoleSource::Auto),
            ("metadata", IamRoleSource::Metadata),
            ("env", IamRoleSource::Env),
        ] {
            assert_eq!(
                raw.parse::<IamRoleSource>()
                    .unwrap_or_else(|e| panic!("{raw:?} should parse: {e}")),
                expected,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn iam_role_source_rejects_values_outside_the_spec() {
        for raw in ["", "role", "instance"] {
            assert!(
                raw.parse::<IamRoleSource>().is_err(),
                "{raw:?} should be rejected"
            );
        }
    }

    #[test]
    fn guardrail_trace_mode_parses_the_documented_values_case_insensitively() {
        for (raw, expected) in [
            ("enabled", GuardrailTraceMode::Enabled),
            ("Disabled", GuardrailTraceMode::Disabled),
            ("enabled_full", GuardrailTraceMode::EnabledFull),
        ] {
            assert_eq!(
                raw.parse::<GuardrailTraceMode>()
                    .unwrap_or_else(|e| panic!("{raw:?} should parse: {e}")),
                expected,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn guardrail_trace_mode_rejects_values_outside_the_spec() {
        for raw in ["", "verbose", "true"] {
            assert!(
                raw.parse::<GuardrailTraceMode>().is_err(),
                "{raw:?} should be rejected"
            );
        }
    }
}
