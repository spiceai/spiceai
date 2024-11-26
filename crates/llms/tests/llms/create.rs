/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use async_openai::error::OpenAIError;
use llms::{
    anthropic::{Anthropic, AnthropicConfig},
    chat::{create_hf_model, Chat, Error as ChatError},
    openai::Openai,
};
use secrecy::Secret;

pub(crate) fn create_openai(model_id: &str) -> Arc<Box<dyn Chat>> {
    let api_key = std::env::var("SPICE_OPENAI_API_KEY").ok();
    Arc::new(Box::new(Openai::new(
        model_id.to_string(),
        None,
        api_key,
        None,
        None,
    )))
}

pub(crate) fn create_anthropic(model_id: Option<&str>) -> Result<Arc<Box<dyn Chat>>, OpenAIError> {
    let cfg = AnthropicConfig::default()
        .with_api_key(std::env::var("SPICE_ANTHROPIC_API_KEY").ok())
        .with_auth_token(std::env::var("SPICE_ANTHROPIC_AUTH_TOKEN").ok());
    let model = Anthropic::new(cfg, model_id)?;

    Ok(Arc::new(Box::new(model)))
}

pub(crate) fn create_hf(model_id: &str) -> Result<Arc<Box<dyn Chat>>, ChatError> {
    let hf_token = std::env::var("HF_TOKEN").ok().map(Secret::new);
    let model_type = None;

    Ok(Arc::new(create_hf_model(
        model_id,
        &model_type,
        hf_token.as_ref(),
    )?))
}
