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

use std::fmt;

/// ARN resource types whose resource ID is a foundation model ID or an inference profile ID.
const MODEL_ARN_RESOURCE_TYPES: [&str; 2] = ["foundation-model", "inference-profile"];

/// A Bedrock `modelId`. Bedrock takes a foundation model ID, a cross-Region inference profile
/// ID, or an ARN:
///
/// ```text
/// anthropic.claude-sonnet-4-5-20250929-v1:0                    <provider>.<name>
/// us.anthropic.claude-sonnet-4-5-20250929-v1:0                 <geography>.<provider>.<name>
/// arn:aws:bedrock:<region>:<account>:inference-profile/<one of the IDs above>
/// arn:aws:bedrock:<region>:<account>:application-inference-profile/a1b2c3
/// ```
///
/// Some ARNs, such as an application inference profile or a provisioned model, do not name
/// the model that serves them. These have no [`FoundationModel`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelId {
    id: String,
    model: Option<FoundationModel>,
}

/// The foundation model that a [`ModelId`] names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FoundationModel {
    pub(crate) provider: Provider,
    /// The ID after the provider, for example `claude-sonnet-4-5-20250929-v1:0`.
    pub(crate) name: String,
}

/// The provider part of a Bedrock model ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Provider {
    Anthropic,
    Amazon,
    OpenAi,
    /// A provider without provider-specific behavior in Spice, for example `meta`.
    Other,
}

impl Provider {
    fn parse(provider: &str) -> Self {
        match provider {
            "anthropic" => Self::Anthropic,
            "amazon" => Self::Amazon,
            "openai" => Self::OpenAi,
            _ => Self::Other,
        }
    }
}

impl FoundationModel {
    /// Parses `[<geography>.]<provider>.<name>`.
    fn parse(id: &str) -> Option<Self> {
        let (first, rest) = id.split_once('.')?;
        // A known provider is never a geography, so only an unknown first part can be one.
        let (provider, name) = match (Provider::parse(first), rest.split_once('.')) {
            (Provider::Other, Some((provider, name))) => (Provider::parse(provider), name),
            (provider, _) => (provider, rest),
        };
        (!name.is_empty()).then(|| Self {
            provider,
            name: name.to_string(),
        })
    }
}

impl ModelId {
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.id
    }

    /// The foundation model this ID names, or `None` when the ID does not name one.
    #[must_use]
    pub(crate) fn foundation_model(&self) -> Option<&FoundationModel> {
        self.model.as_ref()
    }
}

impl From<String> for ModelId {
    fn from(id: String) -> Self {
        let model = match id.strip_prefix("arn:") {
            // `<partition>:bedrock:<region>:<account>:<resource type>/<resource ID>`. The
            // resource ID can hold a `:`, so split at most five times.
            Some(arn) => arn
                .splitn(5, ':')
                .nth(4)
                .and_then(|resource| resource.split_once('/'))
                .filter(|(resource_type, _)| MODEL_ARN_RESOURCE_TYPES.contains(resource_type))
                .and_then(|(_, resource_id)| FoundationModel::parse(resource_id)),
            None => FoundationModel::parse(&id),
        };
        Self { id, model }
    }
}

impl From<&str> for ModelId {
    fn from(id: &str) -> Self {
        Self::from(id.to_string())
    }
}

impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(provider: Provider, name: &str) -> Option<FoundationModel> {
        Some(FoundationModel {
            provider,
            name: name.to_string(),
        })
    }

    #[test]
    fn model_ids_name_their_foundation_model() {
        // IDs from the Bedrock supported models and inference profiles pages.
        let cases = [
            (
                "anthropic.claude-3-7-sonnet-20250219-v1:0",
                model(Provider::Anthropic, "claude-3-7-sonnet-20250219-v1:0"),
            ),
            (
                "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                model(Provider::Anthropic, "claude-sonnet-4-5-20250929-v1:0"),
            ),
            (
                "global.anthropic.claude-haiku-4-5-20251001-v1:0",
                model(Provider::Anthropic, "claude-haiku-4-5-20251001-v1:0"),
            ),
            (
                "us-gov.anthropic.claude-3-5-sonnet-20240620-v1:0",
                model(Provider::Anthropic, "claude-3-5-sonnet-20240620-v1:0"),
            ),
            (
                "us.amazon.nova-2-lite-v1:0",
                model(Provider::Amazon, "nova-2-lite-v1:0"),
            ),
            (
                "openai.gpt-oss-120b-1:0",
                model(Provider::OpenAi, "gpt-oss-120b-1:0"),
            ),
            (
                "meta.llama3-1-8b-instruct-v1:0",
                model(Provider::Other, "llama3-1-8b-instruct-v1:0"),
            ),
            ("us.deepseek.r1-v1:0", model(Provider::Other, "r1-v1:0")),
            (
                "arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                model(Provider::Anthropic, "claude-sonnet-4-5-20250929-v1:0"),
            ),
            (
                "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-opus-4-6-v1",
                model(Provider::Anthropic, "claude-opus-4-6-v1"),
            ),
            (
                "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/a1b2c3",
                None,
            ),
            (
                "arn:aws:bedrock:us-east-1:123456789012:provisioned-model/a1b2c3",
                None,
            ),
            ("my-model", None),
            ("anthropic.", None),
        ];
        for (id, expected) in cases {
            let model_id = ModelId::from(id);
            assert_eq!(model_id.foundation_model(), expected.as_ref(), "{id}");
            assert_eq!(model_id.as_str(), id);
            assert_eq!(model_id.to_string(), id);
        }
    }
}
