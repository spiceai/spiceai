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

use crate::parameters::ParameterSpec;

const BEDROCK_PARAM_LEN: usize = 11;

pub const PARAMETERS: &[ParameterSpec] = &BEDROCK_PARAMETERS;

pub(crate) const BEDROCK_PARAMETERS: [ParameterSpec; BEDROCK_PARAM_LEN] = [
    // AWS credential/config params — runtime (no prefix), matching the LLM bedrock convention.
    ParameterSpec::runtime("aws_access_key_id")
        .description("The AWS access key ID.")
        .secret(),
    ParameterSpec::runtime("aws_secret_access_key")
        .description("The AWS secret access key.")
        .secret(),
    ParameterSpec::runtime("aws_session_token")
        .description("The AWS session token.")
        .secret(),
    ParameterSpec::runtime("aws_region")
        .description("The AWS region to use for Bedrock embeddings."),
    ParameterSpec::runtime("aws_iam_role_source")
        .description("IAM role credential source. 'auto' uses the default AWS credential chain, 'metadata' uses only instance/container metadata (IMDS, ECS, EKS/IRSA), 'env' uses only environment variables.")
        .one_of(&["auto", "metadata", "env"]),
    // Model-specific params — runtime (no prefix) to preserve backward compatibility with
    // pre-#10853 configs where these were bare keys.
    ParameterSpec::runtime("dimensions")
        .description("The number of dimensions for the embedding output."),
    ParameterSpec::runtime("normalize")
        .description("Whether to normalize the embedding output.")
        .one_of(&["true", "false"]),
    ParameterSpec::runtime("truncate_mode")
        .description("Truncation mode for input text that exceeds the model's token limit."),
    ParameterSpec::runtime("truncate")
        .description("Alias for `truncate_mode`; prefer `truncate_mode`.")
        .deprecated("Use `truncate_mode` instead."),
    ParameterSpec::runtime("input_type")
        .description("The input type for Cohere embedding models."),
    ParameterSpec::runtime("embedding_purpose")
        .description("The embedding purpose for Nova multimodal embedding models.")
        .one_of(&["GENERIC_INDEX", "GENERIC_RETRIEVAL", "TEXT_RETRIEVAL", "IMAGE_RETRIEVAL", "VIDEO_RETRIEVAL", "DOCUMENT_RETRIEVAL", "AUDIO_RETRIEVAL", "CLASSIFICATION", "CLUSTERING"]),
];
