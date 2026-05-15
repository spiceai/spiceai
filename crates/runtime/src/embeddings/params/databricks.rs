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

const DATABRICKS_PARAM_LEN: usize = 4;

pub const PARAMETERS: &[ParameterSpec] = &DATABRICKS_PARAMETERS;

pub(crate) const DATABRICKS_PARAMETERS: [ParameterSpec; DATABRICKS_PARAM_LEN] = [
    ParameterSpec::component("endpoint")
        .description("The Databricks workspace endpoint, e.g., dbc-a12cd3e4-56f7.cloud.databricks.com."),
    ParameterSpec::component("token")
        .secret()
        .description("The Databricks API token."),
    ParameterSpec::component("client_id")
        .description("The Databricks Service Principal Client ID. Cannot be used with databricks_token."),
    ParameterSpec::component("client_secret")
        .secret()
        .description("The Databricks Service Principal Client Secret. Cannot be used with databricks_token."),
];
