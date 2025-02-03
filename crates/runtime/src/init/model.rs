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

use std::{collections::HashMap, sync::Arc};

use crate::{
    get_params_with_secrets, metrics, model::ENABLE_MODEL_SUPPORT_MESSAGE, status,
    timing::TimeMeasurement, Runtime,
};
use app::App;
use model_components::model::Model;
use opentelemetry::KeyValue;
use snafu::prelude::*;
use spicepod::component::model::{Model as SpicepodModel, ModelSource, ModelType};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load LLM: {name}.\n{source}"))]
    FailedToLoadLLM {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to load runnable model: {name}.\n{source}"))]
    FailedToLoadRunnableModel {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to load model {name} from spicepod.\nUnable to determine model type. Verify the model source and try again.\nFor details, visit https://spiceai.org/docs/components/models",
    ))]
    UnableToDetermineModelType { name: String },

    #[snafu(display(
        "Model {name} includes a non-existent path: {path}.\nVerify the model configuration and ensure all paths are correct.\nFor details, visit https://spiceai.org/docs/components/models",
    ))]
    ReferencedPathDoesNotExist { name: String, path: String },
}

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
        let params = get_params_with_secrets(self.secrets(), &p).await;

        if matches!(source, Some(ModelSource::File)) {
            // Verify all referenced local files exist before attempting to load the model and determine its type.
            // Otherwise, we will fail to determine the model type and the error will be confusing.
            if let Err(err) = verify_local_files_exist(m) {
                metrics::models::LOAD_ERROR.add(1, &[]);
                self.status
                    .update_model(&model.name, status::ComponentStatus::Error);
                tracing::warn!("{err}");
                return;
            }
        }

        let model_type = m.model_type();
        tracing::trace!("Model type for {} is {:#?}", m.name, model_type.clone());
        let result: Result<(), Error> = match model_type {
            Some(ModelType::Llm) => match self.load_llm(m.clone(), params).await {
                Ok(l) => {
                    let mut llm_map = self.llms.write().await;
                    llm_map.insert(m.name.clone(), l);
                    Ok(())
                }
                Err(e) => Err(Error::FailedToLoadLLM {
                    name: m.name.clone(),
                    source: Box::new(e),
                }),
            },
            Some(ModelType::Ml) => match Model::load(m.clone(), params).await {
                Ok(in_m) => {
                    let mut model_map = self.models.write().await;
                    model_map.insert(m.name.clone(), in_m);
                    Ok(())
                }
                Err(e) => Err(Error::FailedToLoadRunnableModel {
                    name: m.name.clone(),
                    source: Box::new(e),
                }),
            },
            None => Err(Error::UnableToDetermineModelType {
                name: m.name.clone(),
            }),
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
                tracing::warn!("{e}");
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

fn verify_local_files_exist(m: &SpicepodModel) -> Result<(), Error> {
    for f in m.get_all_files() {
        if !std::path::Path::new(&f.path).exists() {
            return Err(Error::ReferencedPathDoesNotExist {
                name: m.name.clone(),
                path: f.path.clone(),
            });
        };
    }
    Ok(())
}
