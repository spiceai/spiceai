use aws_sdk_bedrockruntime::{
    error::BuildError,
    types::{
        GuardrailConfiguration, GuardrailStreamConfiguration, GuardrailTrace,
        builders::{GuardrailConfigurationBuilder, GuardrailStreamConfigurationBuilder},
    },
};
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
use regex::Regex;
use snafu::Snafu;
use std::sync::LazyLock;

pub struct GuardRail {
    trace: GuardrailTrace,
    id: GuardrailIdentifier,
    version: GuardrailVersion,
}

impl GuardRail {
    pub fn try_new(
        id: &str,
        version: &str,
        trace: Option<&str>,
    ) -> Result<GuardRail, GuardrailError> {
        let trace = trace
            .map(|t| {
                GuardrailTrace::try_parse(t).map_err(|_| GuardrailError::UnknownTrace {
                    variant: t.to_string(),
                })
            })
            .transpose()?
            .unwrap_or(GuardrailTrace::Disabled);

        Ok(Self {
            id: validate_guardrail_identifier(id)?,
            trace,
            version: validate_guardrail_version(version)?,
        })
    }
}

impl TryFrom<&GuardRail> for GuardrailStreamConfiguration {
    type Error = BuildError;

    fn try_from(value: &GuardRail) -> Result<Self, Self::Error> {
        let GuardRail { trace, id, version } = value;
        GuardrailStreamConfigurationBuilder::default()
            .guardrail_identifier(id)
            .trace(trace.clone())
            .guardrail_version(version.to_string())
            .build()
    }
}

impl TryFrom<&GuardRail> for GuardrailConfiguration {
    type Error = BuildError;

    fn try_from(value: &GuardRail) -> Result<Self, Self::Error> {
        let GuardRail { trace, id, version } = value;
        GuardrailConfigurationBuilder::default()
            .guardrail_identifier(id)
            .trace(trace.clone())
            .guardrail_version(version.to_string())
            .build()
    }
}

// Pattern: `(([a-z0-9]+) | (arn:aws(-[^:]+)?:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:guardrail/[a-z0-9]+))`. Length: 0-2048."
pub type GuardrailIdentifier = String;
static GUARDRAIL_ID_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)                                   # allow whitespace and comments
        ^                                         # beginning of string
        (
            [a-z0-9]+                             # simple id
            |
            arn:aws(-[^:]+)?:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:guardrail/[a-z0-9]+
        )
        $                                         # end of string
        ",
    )
    .unwrap_or_else(|_| unreachable!("The regex is a valid regex, so it should compile"))
});

#[derive(Debug, Snafu)]
pub enum GuardrailError {
    #[snafu(display("Identifier does not match the required pattern. Got: {id}"))]
    IdentifierPatternMismatch { id: String },
    #[snafu(display("Identifier is not within length bounds (0-2048). Got: {len}"))]
    IdentifierLengthError { len: usize },
    #[snafu(display("Version does not match required pattern. Got: {version}"))]
    VersionPatternMismatch { version: String },
    #[snafu(display("Version number out of range (must fit in u32): {number}"))]
    VersionOutOfRange { number: String },
    #[snafu(display("Guardrail trace is not valid: {variant}"))]
    UnknownTrace { variant: String },
}

pub fn validate_guardrail_identifier(
    s: impl Into<String>,
) -> Result<GuardrailIdentifier, GuardrailError> {
    let value = s.into();
    let len = value.len();
    if len == 0 || len > 2048 {
        return Err(GuardrailError::IdentifierLengthError { len });
    }
    if !GUARDRAIL_ID_RE.is_match(&value) {
        return Err(GuardrailError::IdentifierPatternMismatch { id: value });
    }
    Ok(value)
}

static GUARDRAIL_VERSION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:([1-9][0-9]{0,7})|(DRAFT))$")
        .unwrap_or_else(|_| unreachable!("The regex is a valid regex, so it should compile"))
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardrailVersion {
    Number(u32),
    Draft,
}

impl std::fmt::Display for GuardrailVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GuardrailVersion::Number(n) => write!(f, "{n}"),
            GuardrailVersion::Draft => write!(f, "DRAFT"),
        }
    }
}

pub fn validate_guardrail_version(
    s: impl Into<String>,
) -> Result<GuardrailVersion, GuardrailError> {
    let value = s.into();
    if let Some(caps) = GUARDRAIL_VERSION_RE.captures(&value) {
        // Check if it matched a number
        if let Some(number_str) = caps.get(1) {
            // Parse to u32, but ensure within range
            let number = number_str.as_str();
            let num: u32 = number
                .parse()
                .map_err(|_| GuardrailError::VersionOutOfRange {
                    number: number.to_string(),
                })?;
            return Ok(GuardrailVersion::Number(num));
        }
        // Check if it matched "DRAFT"
        if let Some(draft) = caps.get(2)
            && draft.as_str() == "DRAFT"
        {
            return Ok(GuardrailVersion::Draft);
        }
    }
    Err(GuardrailError::VersionPatternMismatch { version: value })
}
