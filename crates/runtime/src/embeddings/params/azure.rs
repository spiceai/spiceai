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

const AZURE_PARAM_LEN: usize = 5;

pub const PARAMETERS: &[ParameterSpec] = &AZURE_PARAMETERS;

pub(crate) const AZURE_PARAMETERS: [ParameterSpec; AZURE_PARAM_LEN] = [
    ParameterSpec::runtime("endpoint")
        .description("The Azure OpenAI resource endpoint, e.g., https://resource-name.openai.azure.com."),
    ParameterSpec::component("api_version")
        .description("The API version used for the Azure OpenAI service."),
    ParameterSpec::component("deployment_name")
        .description("The name of the model deployment."),
    ParameterSpec::component("api_key")
        .secret()
        .description("The Azure OpenAI API key."),
    ParameterSpec::component("entra_token")
        .secret()
        .description("The Azure Entra token for authentication."),
];
