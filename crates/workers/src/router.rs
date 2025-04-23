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

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    },
};
use llms::chat::{Chat, nsql::SqlGeneration};
use rand::{
    distr::{Distribution, weighted::WeightedIndex},
    rng,
};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicUsize},
};
use tokio::sync::RwLock;

use spicepod::component::worker;

pub struct RouterModel {
    router_name: String,
    models_cfg: Vec<worker::RouterConfig>,
    state: RouterState,
    models: Arc<RwLock<HashMap<String, Arc<dyn Chat>>>>,
}

pub enum RouterState {
    None,
    RoundRobin { incr: AtomicUsize },
}

impl RouterModel {
    /// Assumes all `models_cfg` to be of same enum type.
    pub fn new(
        router_name: String,
        models_cfg: Vec<worker::RouterConfig>,
        models: Arc<RwLock<HashMap<String, Arc<dyn Chat>>>>,
    ) -> Self {
        let initial_state = match models_cfg.first() {
            Some(worker::RouterConfig::RoundRobin { .. }) => RouterState::RoundRobin {
                incr: AtomicUsize::default(),
            },
            _ => RouterState::None,
        };

        Self {
            router_name,
            models_cfg,
            models,
            state: initial_state,
        }
    }

    pub fn select_from_round_robin(&self) -> Option<String> {
        let RouterState::RoundRobin { incr } = &self.state else {
            return None;
        };
        let idx = incr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.models_cfg
            .get(idx % self.models_cfg.len())
            .map(spicepod::component::worker::RouterConfig::from)
    }
}

#[async_trait::async_trait]
impl Chat for RouterModel {
    #[allow(deprecated)]
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        match self.models_cfg.first() {
            Some(worker::RouterConfig::RoundRobin { .. }) => {
                // This cannot be `None` as by this point, there is at least one model.
                let name = self.select_from_round_robin().unwrap_or_default();

                let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
                    return Err(OpenAIError::InvalidArgument(format!(
                        "Model router '{}' expects a model '{name}' to exist, but does not",
                        self.router_name
                    )));
                };

                model.chat_stream(req).await
            }
            Some(worker::RouterConfig::Weighted { .. }) => {
                let name = select_from_weighted(&self.models_cfg);
                let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
                    return Err(OpenAIError::InvalidArgument(format!(
                        "Model router '{}' expects a model '{name}' to exist, but does not",
                        self.router_name
                    )));
                };

                model.chat_stream(req).await
            }
            Some(worker::RouterConfig::Fallback { .. }) => {
                let fallbacks = into_ordered_fallbacks(&self.models_cfg);
                for (name, _) in fallbacks {
                    let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
                        return Err(OpenAIError::InvalidArgument(format!(
                            "Model router '{}' expects a model '{name}' to exist, but does not",
                            self.router_name
                        )));
                    };

                    if let Ok(resp) = model.chat_stream(req.clone()).await {
                        return Ok(resp);
                    }
                }
                Err(OpenAIError::ApiError(ApiError {
                    message: format!(
                        "All models in model router '{}' failed. Check logging for error details",
                        self.router_name
                    ),
                    r#type: None,
                    param: None,
                    code: None,
                }))
            }
            None => Err(OpenAIError::ApiError(ApiError {
                message: format!("No models within model router '{}'.", self.router_name),
                r#type: None,
                param: None,
                code: None,
            })),
        }
    }

    #[allow(deprecated)]
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        match self.models_cfg.first() {
            Some(worker::RouterConfig::RoundRobin { .. }) => {
                // This cannot be `None` as by this point, there is at least one model.
                let name = self.select_from_round_robin().unwrap_or_default();

                let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
                    return Err(OpenAIError::InvalidArgument(format!(
                        "Model router '{}' expects a model '{name}' to exist, but does not",
                        self.router_name
                    )));
                };

                model.chat_request(req).await
            }
            Some(worker::RouterConfig::Weighted { .. }) => {
                let name = select_from_weighted(&self.models_cfg);
                let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
                    return Err(OpenAIError::InvalidArgument(format!(
                        "Model router '{}' expects a model '{name}' to exist, but does not",
                        self.router_name
                    )));
                };

                model.chat_request(req).await
            }
            Some(worker::RouterConfig::Fallback { .. }) => {
                let fallbacks = into_ordered_fallbacks(&self.models_cfg);
                for (name, _) in fallbacks {
                    let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
                        return Err(OpenAIError::InvalidArgument(format!(
                            "Model router '{}' expects a model '{name}' to exist, but does not",
                            self.router_name
                        )));
                    };

                    if let Ok(resp) = model.chat_request(req.clone()).await {
                        return Ok(resp);
                    }
                }
                Err(OpenAIError::ApiError(ApiError {
                    message: format!(
                        "All models in model router '{}' failed. Check logging for error details",
                        self.router_name
                    ),
                    r#type: None,
                    param: None,
                    code: None,
                }))
            }
            None => Err(OpenAIError::ApiError(ApiError {
                message: format!("No models within model router '{}'.", self.router_name),
                r#type: None,
                param: None,
                code: None,
            })),
        }
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
}

/// Assumes all elements of `cfg` are [`worker::RouterConfig::Weighted`].
/// Panics if no elements of `cfg` are [`worker::RouterConfig::Weighted`].
fn select_from_weighted(cfg: &[worker::RouterConfig]) -> String {
    let weighted: Vec<(String, u32)> = cfg
        .iter()
        .filter_map(|c| {
            if let worker::RouterConfig::Weighted { from, weight } = c {
                Some((from.clone(), *weight))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let index = if let Ok(dist) = WeightedIndex::new(weighted.iter().map(|(_, w)| w)) {
        let mut rng = rng();
        dist.sample(&mut rng)
    } else {
        0
    };

    weighted[index].0.clone()
}

/// Assumes all elements of `cfg` are [`worker::RouterConfig::Fallback`].
fn into_ordered_fallbacks(cfg: &[worker::RouterConfig]) -> Vec<(String, u32)> {
    let mut fallbacks = cfg
        .iter()
        .filter_map(|c| {
            if let worker::RouterConfig::Fallback { from, order } = c {
                Some((from.clone(), *order))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    fallbacks.sort_by(|(_, a), (_, b)| a.cmp(b));
    fallbacks
}
