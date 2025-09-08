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

use super::{COMMON_MODEL_PARAMETERS_WITH_DEPRECATED, PARAM_WITH_DEPRE_LEN, concat_arrays};
use crate::parameters::ParameterSpec;

pub(crate) const BEDROCK_PARAMETERS: [ParameterSpec; 7] = [
    ParameterSpec::runtime("aws_access_key_id")
        .description("The AWS access key ID to use for Bedrock models")
        .secret(),
    ParameterSpec::runtime("aws_secret_access_key")
        .description("The AWS secret access key to use for Bedrock models")
        .secret(),
    ParameterSpec::runtime("aws_session_token")
        .description("The AWS session token to use for Bedrock models.")
        .secret(),
    ParameterSpec::runtime("aws_region").description("The AWS region to use for Bedrock models."),
    ParameterSpec::component("guardrail_identifier").description("Identifier for the guardrail. Pattern: `(([a-z0-9]+) | (arn:aws(-[^:]+)?:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:guardrail/[a-z0-9]+))`. Length: 0-2048."),
    ParameterSpec::component("guardrail_version").description("Guardrail version. Pattern: `(([1-9][0-9]{0,7})|(DRAFT))`"),
    ParameterSpec::component("trace").description("Trace behavior for the guardrail. Valid values: `enabled`, `disabled`, `enabled_full`").one_of(&["enabled", "disabled", "enabled_full"]),
];
pub(crate) const PARAMETERS: &[ParameterSpec] =
    &concat_arrays::<ParameterSpec, 7, PARAM_WITH_DEPRE_LEN, { 7 + PARAM_WITH_DEPRE_LEN }>(
        BEDROCK_PARAMETERS,
        COMMON_MODEL_PARAMETERS_WITH_DEPRECATED,
    );
