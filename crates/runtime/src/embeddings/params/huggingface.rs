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

const HF_PARAM_LEN: usize = 3;

pub const PARAMETERS: &[ParameterSpec] = &HF_PARAMETERS;

pub(crate) const HF_PARAMETERS: [ParameterSpec; HF_PARAM_LEN] = [
    ParameterSpec::component("hf_token")
        .secret()
        .description("The Hugging Face access token."),
    ParameterSpec::runtime("pooling")
        .description("The pooling strategy for the embedding model.")
        .one_of(&["cls", "mean", "splade"]),
    ParameterSpec::runtime("max_seq_length")
        .description("The maximum sequence length for the embedding model."),
];
