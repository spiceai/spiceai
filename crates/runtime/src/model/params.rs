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

use spicepod::component::model::ModelSource;

use crate::parameters::ParameterSpec;

pub(crate) fn get_params_spec(source: ModelSource) -> &'static [ParameterSpec] {
    match source {
        ModelSource::OpenAi => OPENAI_PARAMS,
        _ => unimplemented!("Model source {:?} does not have parameters defined", source),
    }
}

pub(crate) const OPENAI_PARAMS: &[ParameterSpec] = &[
    ParameterSpec::component("endpoint")
        .description("The OpenAI API base endpoint. Can be overridden to use a compatible provider (i.e. Nvidia NIM).")
        .default("https://api.openai.com/v1"),
    ParameterSpec::runtime("tools")
        .description("Which tools should be made available to the model. Set to 'auto' to use all available tools."),
    ParameterSpec::runtime("system_prompt")
        .description("An additional system prompt used for all chat completions to this model."),
    ParameterSpec::component("api_key")
        .secret()
        .description("The OpenAI API key."),
    ParameterSpec::component("org_id")
        .description("The OpenAI organization ID."),
    ParameterSpec::component("project_id")
        .description("The OpenAI project ID."),
    ParameterSpec::component("temperature")
        .description("Set the default temperature to use on chat completions."),
    ParameterSpec::component("response_format")
        .description("An object specifying the format that the model must output, see structured outputs."),
    ParameterSpec::component("reasoning_effort")
        .description("For reasoning models, like o1, this parameter specifies the reasoning effort used for the model."),
];
