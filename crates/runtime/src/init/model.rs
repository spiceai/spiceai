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

use std::{collections::HashMap, sync::Arc};

use crate::{
    metrics, model::ENABLE_MODEL_SUPPORT_MESSAGE, status, timing::TimeMeasurement, Runtime,
};
use app::App;
use model_components::model::Model;
use opentelemetry::KeyValue;
use spicepod::component::model::{Model as SpicepodModel, ModelType};

impl Runtime {
    pub(crate) async fn load_models(&self) {
        let app_lock = self.app.read().await;

        if !cfg!(feature = "models") && app_lock.as_ref().is_some_and(|s| !s.models.is_empty()) {
            tracing::error!("Cannot load models without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}");
            return;
        }

        // Load tools before loading models.
        self.load_tools().await;

        if let Some(app) = app_lock.as_ref() {
            for model in &app.models {
                self.status
                    .update_model(&model.name, status::ComponentStatus::Initializing);
                self.load_model(model).await;
            }
        }
    }

    // Caller must set `status::update_model(...` before calling `load_model`. This function will set error/ready statues appropriately.`
    async fn load_model(&self, m: &SpicepodModel) {
        let source = m.get_source();
        let source_str = source.clone().map(|s| s.to_string()).unwrap_or_default();
        let model = m.clone();
        let _guard = TimeMeasurement::new(
            &metrics::models::LOAD_DURATION_MS,
            &[
                KeyValue::new("model", m.name.clone()),
                KeyValue::new("source", source_str.clone()),
            ],
        );

        tracing::info!("Loading model [{}] from {}...", m.name, m.from);

        // TODO: Have downstream code using model parameters to accept `Hashmap<String, Value>`.
        // This will require handling secrets with `Value` type.
        let p = m
            .params
            .clone()
            .iter()
            .map(|(k, v)| {
                let k = k.clone();
                match v.as_str() {
                    Some(s) => (k, s.to_string()),
                    None => (k, v.to_string()),
                }
            })
            .collect::<HashMap<_, _>>();
        let params = self.get_params_with_secrets(&p).await;

        let model_type = m.model_type();
        tracing::trace!("Model type for {} is {:#?}", m.name, model_type.clone());
        let result: Result<(), String> = match model_type {
            Some(ModelType::Llm) => match self.load_llm(m.clone(), params).await {
                Ok(l) => {
                    let mut llm_map = self.llms.write().await;
                    llm_map.insert(m.name.clone(), l);
                    Ok(())
                }
                Err(e) => Err(format!(
                    "Unable to load LLM from spicepod {}, error: {}",
                    m.name, e,
                )),
            },
            Some(ModelType::Ml) => match Model::load(m.clone(), params).await {
                Ok(in_m) => {
                    let mut model_map = self.models.write().await;
                    model_map.insert(m.name.clone(), in_m);
                    Ok(())
                }
                Err(e) => Err(format!(
                    "Unable to load runnable model from spicepod {}, error: {}",
                    m.name, e,
                )),
            },
            None => Err(format!(
                "Unable to load model {} from spicepod. Unable to determine model type.",
                m.name,
            )),
        };
        match result {
            Ok(()) => {
                tracing::info!("Model [{}] deployed, ready for inferencing", m.name);
                metrics::models::COUNT.add(
                    1,
                    &[
                        KeyValue::new("model", m.name.clone()),
                        KeyValue::new("source", source_str),
                    ],
                );
                self.status
                    .update_model(&model.name, status::ComponentStatus::Ready);
            }
            Err(e) => {
                metrics::models::LOAD_ERROR.add(1, &[]);
                self.status
                    .update_model(&model.name, status::ComponentStatus::Error);
                tracing::warn!(e);
            }
        }
    }

    async fn remove_model(&self, m: &SpicepodModel) {
        match m.model_type() {
            Some(ModelType::Ml) => {
                let mut ml_map = self.models.write().await;
                ml_map.remove(&m.name);
            }
            Some(ModelType::Llm) => {
                let mut llm_map = self.llms.write().await;
                llm_map.remove(&m.name);
            }
            None => return,
        };

        tracing::info!("Model [{}] has been unloaded", m.name);
        let source_str = m.get_source().map(|s| s.to_string()).unwrap_or_default();
        metrics::models::COUNT.add(
            -1,
            &[
                KeyValue::new("model", m.name.clone()),
                KeyValue::new("source", source_str),
            ],
        );
    }

    async fn update_model(&self, m: &SpicepodModel) {
        self.status
            .update_model(&m.name, status::ComponentStatus::Refreshing);
        self.remove_model(m).await;
        self.load_model(m).await;
    }

    pub(crate) async fn apply_model_diff(&self, current_app: &Arc<App>, new_app: &Arc<App>) {
        for model in &new_app.models {
            if let Some(current_model) = current_app.models.iter().find(|m| m.name == model.name) {
                if current_model != model {
                    self.update_model(model).await;
                }
            } else {
                self.status
                    .update_model(&model.name, status::ComponentStatus::Initializing);
                self.load_model(model).await;
            }
        }

        // Remove models that are no longer in the app
        for model in &current_app.models {
            if !new_app.models.iter().any(|m| m.name == model.name) {
                self.status
                    .update_model(&model.name, status::ComponentStatus::Disabled);
                self.remove_model(model).await;
            }
        }
    }
}
