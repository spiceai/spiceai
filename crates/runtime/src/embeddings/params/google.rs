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

const GOOGLE_PARAM_LEN: usize = 2;

pub const PARAMETERS: &[ParameterSpec] = &GOOGLE_PARAMETERS;

pub(crate) const GOOGLE_PARAMETERS: [ParameterSpec; GOOGLE_PARAM_LEN] = [
    ParameterSpec::component("api_key")
        .secret()
        .description("The Google API key."),
    ParameterSpec::component("dimensions")
        .description("The number of dimensions for the embedding output."),
];
