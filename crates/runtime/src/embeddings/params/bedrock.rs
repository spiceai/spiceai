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

const BEDROCK_PARAM_LEN: usize = 5;

pub const PARAMETERS: &[ParameterSpec] = &BEDROCK_PARAMETERS;

pub(crate) const BEDROCK_PARAMETERS: [ParameterSpec; BEDROCK_PARAM_LEN] = [
    ParameterSpec::component("dimensions")
        .description("The number of dimensions for the embedding output."),
    ParameterSpec::component("normalize")
        .description("Whether to normalize the embedding output.")
        .one_of(&["true", "false"]),
    ParameterSpec::component("truncate_mode")
        .description("Truncation mode for input text that exceeds the model's token limit."),
    ParameterSpec::component("input_type")
        .description("The input type for Cohere embedding models."),
    ParameterSpec::component("embedding_purpose")
        .description("The embedding purpose for Nova multimodal embedding models."),
];
