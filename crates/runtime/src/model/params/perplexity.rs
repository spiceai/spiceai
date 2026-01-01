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

pub const PARAMETERS: &[ParameterSpec] = &concat_arrays::<
    ParameterSpec,
    PERPLEXITY_PARAM_LEN,
    PARAM_WITH_DEPRE_LEN,
    { PERPLEXITY_PARAM_LEN + PARAM_WITH_DEPRE_LEN },
>(
    PERPLEXITY_PARAMETERS,
    COMMON_MODEL_PARAMETERS_WITH_DEPRECATED,
);

const PERPLEXITY_PARAM_LEN: usize = 5;

pub(crate) const PERPLEXITY_PARAMETERS: [ParameterSpec; PERPLEXITY_PARAM_LEN] = [
    ParameterSpec::component("auth_token").description("The Anthropic API key."),
    ParameterSpec::component("return_images"),
    ParameterSpec::component("return_related_questions"),
    ParameterSpec::component("search_domain_filter"),
    ParameterSpec::component("search_recency_filter"),
];
