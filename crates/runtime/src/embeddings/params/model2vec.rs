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

const MODEL2VEC_PARAM_LEN: usize = 6;

pub const PARAMETERS: &[ParameterSpec] = &MODEL2VEC_PARAMETERS;

pub(crate) const MODEL2VEC_PARAMETERS: [ParameterSpec; MODEL2VEC_PARAM_LEN] = [
    ParameterSpec::component("hf_token")
        .secret()
        .description("The Hugging Face access token."),
    ParameterSpec::component("subfolder")
        .description("The subfolder within the Hugging Face repo containing the model."),
    ParameterSpec::component("normalize")
        .description("Whether to normalize the embedding output.")
        .one_of(&["true", "false"]),
    ParameterSpec::runtime("parallelism")
        .description("The number of threads to use for parallel inference."),
    ParameterSpec::runtime("embed_max_token_length")
        .description("The maximum token length for embedding input."),
    ParameterSpec::runtime("embed_custom_batch_size")
        .description("The custom batch size for embedding inference."),
];
