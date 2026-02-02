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

use util::concat_arrays;

use super::{COMMON_MODEL_PARAMETERS_WITH_DEPRECATED, PARAM_WITH_DEPRE_LEN};
use crate::parameters::ParameterSpec;

pub const PARAMETERS: &[ParameterSpec] =
    &concat_arrays::<
        ParameterSpec,
        AZURE_PARAM_LEN,
        PARAM_WITH_DEPRE_LEN,
        { AZURE_PARAM_LEN + PARAM_WITH_DEPRE_LEN },
    >(AZURE_PARAMETERS, COMMON_MODEL_PARAMETERS_WITH_DEPRECATED);

const AZURE_PARAM_LEN: usize = 7;

pub(crate) const AZURE_PARAMETERS: [ParameterSpec; AZURE_PARAM_LEN] = [
    ParameterSpec::runtime("endpoint").description(
        "The Azure OpenAI resource endpoint, e.g., https://resource-name.openai.azure.com.",
    ),
    ParameterSpec::component("api_version")
        .description("The API version used for the Azure OpenAI service."),
    ParameterSpec::component("deployment_name").description("The name of the model deployment."),
    ParameterSpec::component("api_key")
        .description("The Azure OpenAI API key from the models deployment page."),
    ParameterSpec::component("entra_token")
        .description("The Azure Entra token for authentication."),
    ParameterSpec::component("openai_responses_tools")
        .description(
            "Comma-separated list of OpenAI-hosted tools exposed via the Responses API for this model.",
        )
        .default(""),
    ParameterSpec::runtime("responses_api")
        .description(
            "Whether to enable use of this model via the Responses API. `disabled` by default.",
        )
        .default("disabled"),
];
