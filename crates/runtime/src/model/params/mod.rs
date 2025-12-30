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

pub mod anthropic;
pub mod azure;
pub mod bedrock;
pub mod databricks;
pub mod file;
pub mod google;
pub mod huggingface;
pub mod openai;
pub mod perplexity;
pub mod xai;

use spicepod::component::model::ModelSource;

pub use crate::parameters::ParameterSpec;

const DEPRECATED_MESSAGE: &str = "The `openai_<param>` language model overrides parameter is deprecated and will be removed in a future release. Please use `<model_prefix>_<param>` parameter name instead.";

/// Returns the parameter specifications for a given model source.
///
/// This function is used by the schema generator to collect all model parameters.
#[must_use]
pub fn get_params_spec(source: &ModelSource) -> Option<&'static [ParameterSpec]> {
    match source {
        ModelSource::OpenAi => Some(openai::PARAMETERS),
        ModelSource::Azure => Some(azure::PARAMETERS),
        ModelSource::File => Some(file::PARAMETERS),
        ModelSource::Databricks => Some(databricks::PARAMETERS),
        ModelSource::HuggingFace => Some(huggingface::PARAMETERS),
        ModelSource::Anthropic => Some(anthropic::PARAMETERS),
        ModelSource::Perplexity => Some(perplexity::PARAMETERS),
        ModelSource::Xai => Some(xai::PARAMETERS),
        ModelSource::Bedrock => Some(bedrock::PARAMETERS),
        ModelSource::SpiceAI => None,
        ModelSource::Google => Some(google::PARAMETERS),
    }
}

// Use the const function to reduce the duplicated common model parameters definition in each model provider param spec.
pub const fn concat_arrays<T: Copy, const N: usize, const M: usize, const S: usize>(
    a: [T; N],
    b: [T; M],
) -> [T; S] {
    let mut out = [a[0]; S];
    let mut i = 0;
    while i < N {
        out[i] = a[i];
        i += 1;
    }
    let mut j = 0;
    while j < M {
        out[N + j] = b[j];
        j += 1;
    }
    out
}

pub const PARAM_LEN: usize = 44;
pub const PARAM_WITH_DEPRE_LEN: usize = 45;

// Model parameters that are used for openai model provider. Those parameters are supported by other (non-openai) models as well.
// OpenAI model is prefixed with `openai_`, use separate PARAMETERS constant to avoid confusion with other model providers.
pub const COMMON_MODEL_PARAMETERS: [ParameterSpec; PARAM_LEN] = [
    // Common parameters for all models
    ParameterSpec::runtime("tools")
        .description("Which tools should be made available to the model. Set to 'auto' to use all available tools."),
    ParameterSpec::runtime("system_prompt")
        .description("An additional system prompt used for all chat completions to this model."),
    ParameterSpec::runtime("parameterized_prompt"),
    // OpenAI compatible default override parameters for all models
    ParameterSpec::runtime("frequency_penalty"),
    ParameterSpec::runtime("logit_bias"),
    ParameterSpec::runtime("logprobs"),
    ParameterSpec::runtime("top_logprobs"),
    ParameterSpec::runtime("max_completion_tokens"),
    ParameterSpec::runtime("reasoning_effort"),
    ParameterSpec::runtime("store"),
    ParameterSpec::runtime("metadata"),
    ParameterSpec::runtime("n"),
    ParameterSpec::runtime("presence_penalty"),
    ParameterSpec::runtime("response_format"),
    ParameterSpec::runtime("seed"),
    ParameterSpec::runtime("stop"),
    ParameterSpec::runtime("stream"),
    ParameterSpec::runtime("stream_options"),
    ParameterSpec::runtime("temperature"),
    ParameterSpec::runtime("top_p"),
    ParameterSpec::runtime("tool_choice"),
    ParameterSpec::runtime("parallel_tool_calls"),
    ParameterSpec::runtime("user"),
    ParameterSpec::component("frequency_penalty").deprecated("Use 'frequency_penalty' without prefix"),
    ParameterSpec::component("logit_bias").deprecated("Use 'logit_bias' without prefix"),
    ParameterSpec::component("logprobs").deprecated("Use 'logprobs' without prefix"),
    ParameterSpec::component("top_logprobs").deprecated("Use 'top_logprobs' without prefix"),
    ParameterSpec::component("max_completion_tokens").deprecated("Use 'max_completion_tokens' without prefix"),
    ParameterSpec::component("reasoning_effort").deprecated("Use 'reasoning_effort' without prefix"),
    ParameterSpec::component("store").deprecated("Use 'store' without prefix"),
    ParameterSpec::component("metadata").deprecated("Use 'metadata' without prefix"),
    ParameterSpec::component("n").deprecated("Use 'n' without prefix"),
    ParameterSpec::component("presence_penalty").deprecated("Use 'presence_penalty' without prefix"),
    ParameterSpec::component("response_format").deprecated("Use 'response_format' without prefix"),
    ParameterSpec::component("seed").deprecated("Use 'seed' without prefix"),
    ParameterSpec::component("stop").deprecated("Use 'stop' without prefix"),
    ParameterSpec::component("stream").deprecated("Use 'stream' without prefix"),
    ParameterSpec::component("stream_options").deprecated("Use 'stream_options' without prefix"),
    ParameterSpec::component("temperature").deprecated("Use 'temperature' without prefix"),
    ParameterSpec::component("top_p").deprecated("Use 'top_p' without prefix"),
    ParameterSpec::component("tools").deprecated("Use 'tools' without prefix"),
    ParameterSpec::component("tool_choice").deprecated("Use 'tool_choice' without prefix"),
    ParameterSpec::component("parallel_tool_calls").deprecated("Use 'parallel_tool_calls' without prefix"),
    ParameterSpec::component("user").deprecated("Use 'user' without prefix"),
];

// Common model parameters that are used for all model providers except openai.
pub const COMMON_MODEL_PARAMETERS_WITH_DEPRECATED: [ParameterSpec; PARAM_WITH_DEPRE_LEN] = [
    // Common parameters for all models
    ParameterSpec::runtime("tools")
        .description("Which tools should be made available to the model. Set to 'auto' to use all available tools."),
    ParameterSpec::runtime("system_prompt")
        .description("An additional system prompt used for all chat completions to this model."),
    ParameterSpec::runtime("parameterized_prompt"),
    // OpenAI compatible default override parameters for all models
    ParameterSpec::component("frequency_penalty"),
    ParameterSpec::component("logit_bias"),
    ParameterSpec::component("logprobs"),
    ParameterSpec::component("top_logprobs"),
    ParameterSpec::component("max_completion_tokens"),
    ParameterSpec::component("reasoning_effort"),
    ParameterSpec::component("store"),
    ParameterSpec::component("metadata"),
    ParameterSpec::component("n"),
    ParameterSpec::component("presence_penalty"),
    ParameterSpec::component("response_format"),
    ParameterSpec::component("seed"),
    ParameterSpec::component("stop"),
    ParameterSpec::component("stream"),
    ParameterSpec::component("stream_options"),
    ParameterSpec::component("temperature"),
    ParameterSpec::component("top_p"),
    ParameterSpec::component("tools"),
    ParameterSpec::component("tool_choice"),
    ParameterSpec::component("parallel_tool_calls"),
    ParameterSpec::component("user"),
    // For model providers that are not OpenAI
    // The default Override parameters start with `openai_` is deprecated and will be removed in a future release.
    // Keep the `openai_` for backward compatibility, but recommend user using `<model_prefix>_<param>` instead.
    ParameterSpec::runtime("openai_frequency_penalty").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_logit_bias").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_logprobs").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_top_logprobs").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_max_completion_tokens").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_reasoning_effort").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_store").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_metadata").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_n").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_presence_penalty").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_response_format").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_seed").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_stop").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_stream").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_stream_options").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_temperature").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_top_p").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_tools").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_tool_choice").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_parallel_tool_calls").deprecated(DEPRECATED_MESSAGE),
    ParameterSpec::runtime("openai_user").deprecated(DEPRECATED_MESSAGE),
];
