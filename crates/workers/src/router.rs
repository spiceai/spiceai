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
use futures::{TryStreamExt, stream::StreamExt};
use llms::{
    chat::{Chat, nsql::SqlGeneration},
    progress::Progress,
};
use rand::{
    distr::{Distribution, weighted::WeightedIndex},
    rng,
};
use spicepod::component::worker;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicUsize},
};
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

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

    async fn select_from_weighted(&self) -> Result<Arc<dyn Chat>, OpenAIError> {
        let Some(name) = select_from_weighted(&self.models_cfg) else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model router '{}' incorrectly initialized",
                self.router_name
            )));
        };
        tracing::info!(
            target: "task_history",
            progress = Progress::log().title(format!(
                "Worker '{}' deferring request to model '{name}'", self.router_name.clone()
            )).to_jsonl(),
        );
        let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model router '{}' expects a model '{name}' to exist, but does not",
                self.router_name
            )));
        };
        Ok(model)
    }

    async fn select_from_round_robin(&self) -> Result<Arc<dyn Chat>, OpenAIError> {
        let RouterState::RoundRobin { incr } = &self.state else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model router '{}' incorrectly initialized",
                self.router_name
            )));
        };

        let Some(name) = select_from_round_robin(incr, self.models_cfg.as_slice()) else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model router '{}' incorrectly initialized",
                self.router_name
            )));
        };

        tracing::info!(
            target: "task_history",
            progress = Progress::log().title(format!(
                "Worker '{}' deferring request to model '{name}'", self.router_name.clone()
            )).to_jsonl(),
        );
        let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model router '{}' expects a model '{name}' to exist, but does not",
                self.router_name
            )));
        };
        Ok(model)
    }
}

#[async_trait::async_trait]
impl Chat for RouterModel {
    #[allow(deprecated)]
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let span = Span::current();
        let public_name = self.router_name.clone();
        Ok(Box::pin(match self.models_cfg.first() {
            Some(worker::RouterConfig::RoundRobin { .. }) => {
                self.select_from_round_robin().await?.chat_stream(req).instrument(span.clone()).await
            }
            Some(worker::RouterConfig::Weighted { .. }) => {
                self.select_from_weighted().await?.chat_stream(req).instrument(span.clone()).await
            }
            Some(worker::RouterConfig::Fallback { .. }) => {
                let fallbacks = into_ordered_fallbacks(&self.models_cfg);
                for (name, _) in fallbacks {
                    tracing::info!(
                        target: "task_history",
                        progress = Progress::log().title(format!(
                            "Worker '{}' deferring request to model '{name}'", self.router_name.clone()
                        )).to_jsonl(),
                    );
                    let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
                        return Err(OpenAIError::InvalidArgument(format!(
                            "Model router '{}' expects a model '{name}' to exist, but does not",
                            self.router_name.clone()
                        )));
                    };

                    match model.chat_stream(req.clone()).instrument(span.clone()).await {
                        Err(e) => {
                            tracing::error!(
                                target: "task_history",
                                progress = Progress::error()
                                .title(format!(
                                    "Error occured in model '{name}'"
                                ))
                                .content(e.to_string())
                                .to_jsonl(),
                            );
                        },

                        // Check if first item in stream is `Err` since this is a common error by providers.
                        Ok(stream) => {
                            let mut peekable = Box::pin(Box::pin(stream).peekable());
                            match peekable.as_mut().peek().await.as_ref() {
                                Some(Err(e)) => {
                                    tracing::error!(
                                        target: "task_history",
                                        progress = Progress::error()
                                        .title(format!(
                                            "Error occured in model '{name}'"
                                        ))
                                        .content(e.to_string())
                                        .to_jsonl(),
                                    );
                                },
                                None | Some(Ok(_)) => return Ok(peekable),
                            }
                        }
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
        }?
        .map_ok(move |mut ss| {
            ss.model.clone_from(&public_name);
            ss
        })))
    }

    #[allow(deprecated)]
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let span = Span::current();
        match self.models_cfg.first() {
            Some(worker::RouterConfig::RoundRobin { .. }) => {
                self.select_from_round_robin().await?.chat_request(req).instrument(span.clone()).await
            }
            Some(worker::RouterConfig::Weighted { .. }) => {
                self.select_from_weighted().await?.chat_request(req).instrument(span.clone()).await
            }
            Some(worker::RouterConfig::Fallback { .. }) => {
                let fallbacks = into_ordered_fallbacks(&self.models_cfg);
                for (name, _) in fallbacks {
                    tracing::info!(
                        target: "task_history",
                        progress = Progress::log().title(format!(
                            "Worker '{}' deferring request to model '{name}'", self.router_name.clone()
                        )).to_jsonl(),
                    );
                    let Some(model) = self.models.read().await.get(&name).map(Arc::clone) else {
                        return Err(OpenAIError::InvalidArgument(format!(
                            "Model router '{}' expects a model '{name}' to exist, but does not",
                            self.router_name.clone()
                        )));
                    };

                    match model.chat_request(req.clone()).instrument(span.clone()).await {
                        Err(e) => {
                            tracing::error!(
                                target: "task_history",
                                progress = Progress::error()
                                .title(format!(
                                    "Error occured in model '{name}'"
                                ))
                                .content(e.to_string())
                                .to_jsonl(),
                            );
                        },

                        Ok(resp) => return Ok(resp),
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
        .map(|mut r| {
            r.model.clone_from(&self.router_name);
            r
        })
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
}

/// Assumes all elements of `cfg` are [`worker::RouterConfig::Weighted`].
/// Returns `None` if `cfg.len() <=0`
fn select_from_weighted(cfg: &[worker::RouterConfig]) -> Option<String> {
    let weighted: Vec<(&String, u32)> = cfg
        .iter()
        .filter_map(|c| {
            if let worker::RouterConfig::Weighted { from, weight } = c {
                Some((from, *weight))
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

    weighted.get(index).map(|&(a, _)| a.clone())
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

fn select_from_round_robin(incr: &AtomicUsize, models: &[worker::RouterConfig]) -> Option<String> {
    let idx = incr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    models
        .get(idx % models.len())
        .map(spicepod::component::worker::RouterConfig::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    mod weighted {

        use super::*;
        #[test]
        fn test_select_from_weighted() {
            let cfg = vec![
                worker::RouterConfig::Weighted {
                    from: "example1.com".to_string(),
                    weight: 1,
                },
                worker::RouterConfig::Weighted {
                    from: "example2.com".to_string(),
                    weight: 2,
                },
                worker::RouterConfig::Weighted {
                    from: "example3.com".to_string(),
                    weight: 4,
                },
            ];
            let mut count_1 = 0;
            let mut count_2 = 0;
            let mut count_4 = 0;
            let n = 1000;
            for _ in 1..n {
                let Some(v) = select_from_weighted(&cfg) else {
                    continue;
                };
                match v.as_str() {
                    "example1.com" => count_1 += 1,
                    "example2.com" => count_2 += 1,
                    "example3.com" => count_4 += 1,
                    _ => {}
                }
            }

            assert!(count_2 > count_1);
            assert!(count_4 > count_2);

            // An integer approximation to check that weights are approximately correct.
            assert!(count_2 - 2 * count_1 < n / 10);
            assert!(count_4 - 2 * count_2 < n / 10);
        }
    }

    mod roundrobin {
        use super::*;
        #[test]
        fn test_select_from_round_robin() {
            let cfg = vec![
                worker::RouterConfig::RoundRobin {
                    from: "example1.com".to_string(),
                },
                worker::RouterConfig::RoundRobin {
                    from: "example2.com".to_string(),
                },
            ];
            let incr = AtomicUsize::new(0);
            assert_eq!(
                select_from_round_robin(&incr, cfg.as_slice()),
                Some("example1.com".to_string())
            );
            assert_eq!(
                select_from_round_robin(&incr, cfg.as_slice()),
                Some("example2.com".to_string())
            );
            assert_eq!(
                select_from_round_robin(&incr, cfg.as_slice()),
                Some("example1.com".to_string())
            );
            assert_eq!(
                select_from_round_robin(&incr, cfg.as_slice()),
                Some("example2.com".to_string())
            );
        }
    }

    mod fallback {

        use super::*;

        #[test]
        fn test_into_ordered_fallbacks_empty() {
            assert_eq!(into_ordered_fallbacks(&[]), vec![]);
        }

        #[test]
        fn test_into_ordered_fallbacks_single_fallback() {
            assert_eq!(
                into_ordered_fallbacks(&[worker::RouterConfig::Fallback {
                    from: "example.com".to_string(),
                    order: 1,
                }]),
                vec![("example.com".to_string(), 1)]
            );
        }

        #[test]
        fn test_into_ordered_fallbacks_multiple_fallbacks() {
            assert_eq!(
                into_ordered_fallbacks(&[
                    worker::RouterConfig::Fallback {
                        from: "example1.com".to_string(),
                        order: 2,
                    },
                    worker::RouterConfig::Fallback {
                        from: "example2.com".to_string(),
                        order: 1,
                    },
                    worker::RouterConfig::Fallback {
                        from: "example3.com".to_string(),
                        order: 3,
                    }
                ]),
                vec![
                    ("example2.com".to_string(), 1),
                    ("example1.com".to_string(), 2),
                    ("example3.com".to_string(), 3),
                ]
            );
        }

        #[test]
        fn test_into_ordered_fallbacks_mixed_configs() {
            assert_eq!(
                into_ordered_fallbacks(&[
                    worker::RouterConfig::Fallback {
                        from: "example1.com".to_string(),
                        order: 2,
                    },
                    worker::RouterConfig::Weighted {
                        from: "example2.com".to_string(),
                        weight: 50,
                    },
                    worker::RouterConfig::Fallback {
                        from: "example3.com".to_string(),
                        order: 1,
                    },
                    worker::RouterConfig::RoundRobin {
                        from: "example4.com".to_string(),
                    }
                ]),
                vec![
                    ("example3.com".to_string(), 1),
                    ("example1.com".to_string(), 2),
                ]
            );
        }

        #[test]
        fn test_into_ordered_fallbacks_no_fallbacks() {
            assert_eq!(
                into_ordered_fallbacks(&[
                    worker::RouterConfig::Weighted {
                        from: "example1.com".to_string(),
                        weight: 50,
                    },
                    worker::RouterConfig::RoundRobin {
                        from: "example2.com".to_string(),
                    }
                ]),
                vec![]
            );
        }

        #[test]
        fn test_into_ordered_fallbacks_equal_orders() {
            let result = into_ordered_fallbacks(&[
                worker::RouterConfig::Fallback {
                    from: "example1.com".to_string(),
                    order: 1,
                },
                worker::RouterConfig::Fallback {
                    from: "example2.com".to_string(),
                    order: 1,
                },
                worker::RouterConfig::Fallback {
                    from: "example3.com".to_string(),
                    order: 2,
                },
            ]);

            assert_eq!(result.len(), 3);

            assert!(result.contains(&("example1.com".to_string(), 1)));
            assert!(result.contains(&("example2.com".to_string(), 1)));
            assert!(result.contains(&("example3.com".to_string(), 2)));

            let order_1_count = result.iter().take_while(|(_, order)| *order == 1).count();
            assert_eq!(order_1_count, 2);
        }
    }
}
