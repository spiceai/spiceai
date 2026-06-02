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

pub const PARAMETERS: &[ParameterSpec] = &concat_arrays::<
    ParameterSpec,
    HF_PARAM_LEN,
    PARAM_WITH_DEPRE_LEN,
    { HF_PARAM_LEN + PARAM_WITH_DEPRE_LEN },
>(HF_PARAMETERS, COMMON_MODEL_PARAMETERS_WITH_DEPRECATED);

const HF_PARAM_LEN: usize = 3;

pub(crate) const HF_PARAMETERS: [ParameterSpec; HF_PARAM_LEN] = [
    ParameterSpec::runtime("model_type")
        .description("The architecture to load the model as. Supported text architectures: mistral, gemma, mixtral, llama, phi2, phi3, qwen2, gemma2, starcoder2, phi3.5moe, deepseekv2, deepseekv3, qwen3, glm4, glm4moelite, glm4moe, qwen3moe, smollm3, granitemoehybrid, gpt_oss, qwen3next. Supported multimodal architectures: phi3v, idefics2, llava_next, llava, vllama, qwen2vl, idefics3, minicpmo, phi4mm, qwen2_5vl, gemma3, mistral3, llama4, gemma3n, gemma4, qwen3vl, qwen3vlmoe, qwen3_5, qwen3_5moe, voxtral."),
    ParameterSpec::runtime("chat_template")
        .description("Customizes the transformation of OpenAI chat messages into a character stream for the model."),
    ParameterSpec::component("token").description("The Huggingface access token.")
];
