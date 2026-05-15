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

const OPENAI_PARAM_LEN: usize = 5;

pub const PARAMETERS: &[ParameterSpec] = &OPENAI_PARAMETERS;

pub(crate) const OPENAI_PARAMETERS: [ParameterSpec; OPENAI_PARAM_LEN] = [
    ParameterSpec::runtime("endpoint")
        .description("The OpenAI API base endpoint.")
        .default("https://api.openai.com/v1"),
    ParameterSpec::component("api_key")
        .secret()
        .description("The OpenAI API key."),
    ParameterSpec::component("org_id")
        .description("The OpenAI organization ID."),
    ParameterSpec::component("project_id")
        .description("The OpenAI project ID."),
    ParameterSpec::component("usage_tier")
        .description("The current usage tier for the OpenAI account: 'free', 'tier1'-'tier5'.")
        .one_of(&["free", "tier1", "tier2", "tier3", "tier4", "tier5"])
        .default("tier1"),
];
