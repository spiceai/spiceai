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
    Runtime, get_params_with_secrets, metrics, model::ENABLE_MODEL_SUPPORT_MESSAGE, status,
    timing::TimeMeasurement,
};
use app::App;
use model_components::model::Model;
use opentelemetry::KeyValue;
use snafu::prelude::*;
use spicepod::component::model::{Model as SpicepodModel, ModelSource, ModelType};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load LLM: {name}. {source}"))]
    FailedToLoadLLM {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to load runnable model: {name}. {source}"))]
    FailedToLoadRunnableModel {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to load model {name} from spicepod. Unable to determine model type. Verify the model source and try again. For details, visit https://spiceai.org/docs/components/models",
    ))]
    UnableToDetermineModelType { name: String },

    #[snafu(display(
        "Model {name} includes a non-existent path: {path}. Verify the model configuration and ensure all paths are correct. For details, visit https://spiceai.org/docs/components/models",
    ))]
    ReferencedPathDoesNotExist { name: String, path: String },
}

impl Runtime {
    pub(crate) async fn load_models(self: Arc<Self>) {
        let app_lock = self.app.read().await;

        if !cfg!(feature = "models") && app_lock.as_ref().is_some_and(|s| !s.models.is_empty()) {
            tracing::error!(
                "Cannot load models without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}"
            );
            return;
        }

        // Load tools before loading models.
        Arc::clone(&self).load_tools().await;

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

        // Prepare parameters with secrets
        let params = self.prepare_model_params(m).await;

        // Validate local files if needed
        if matches!(source, Some(ModelSource::File)) && verify_local_files_exist(m).is_err() {
            self.handle_model_load_error(&model.name, "Local file verification failed");
            return;
        }

        // Load the model based on its type
        let result = self.load_model_by_type(m, params).await;

        // Handle success or failure
        self.finalize_model_load(&model, &source_str, result).await;
    }

    /// Prepare model parameters by resolving secrets
    async fn prepare_model_params(
        &self,
        m: &SpicepodModel,
    ) -> HashMap<String, secrecy::SecretString> {
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
        get_params_with_secrets(self.secrets(), &p).await
    }

    /// Load a model based on its type (LLM or ML)
    async fn load_model_by_type(
        &self,
        m: &SpicepodModel,
        params: HashMap<String, secrecy::SecretString>,
    ) -> Result<(), Error> {
        let model_type = m.model_type();
        tracing::trace!("Model type for {} is {:#?}", m.name, model_type.clone());

        match model_type {
            Some(ModelType::Llm) => self.load_and_register_llm(m, params).await,
            Some(ModelType::Ml) => self.load_and_register_ml(m, params).await,
            None => Err(Error::UnableToDetermineModelType {
                name: m.name.clone(),
            }),
        }
    }

    /// Load an LLM model and register it in the appropriate maps
    async fn load_and_register_llm(
        &self,
        m: &SpicepodModel,
        params: HashMap<String, secrecy::SecretString>,
    ) -> Result<(), Error> {
        match self.load_llm(m.clone(), params).await {
            Ok((completions_model, Some(responses_model))) => {
                self.completion_llms
                    .write()
                    .await
                    .insert(m.name.clone(), completions_model);
                self.responses_llms
                    .write()
                    .await
                    .insert(m.name.clone(), responses_model);
                self.register_model_in_registry(m);
                Ok(())
            }
            Ok((model, None)) => {
                self.completion_llms
                    .write()
                    .await
                    .insert(m.name.clone(), model);
                self.register_model_in_registry(m);
                Ok(())
            }
            Err(e) => Err(Error::FailedToLoadLLM {
                name: m.name.clone(),
                source: Box::new(e),
            }),
        }
    }

    /// Load an ML model and register it
    async fn load_and_register_ml(
        &self,
        m: &SpicepodModel,
        params: HashMap<String, secrecy::SecretString>,
    ) -> Result<(), Error> {
        match Model::load(m.clone(), params).await {
            Ok(in_m) => {
                self.models.write().await.insert(m.name.clone(), in_m);
                Ok(())
            }
            Err(e) => Err(Error::FailedToLoadRunnableModel {
                name: m.name.clone(),
                source: Box::new(e),
            }),
        }
    }

    /// Register a model in the AI UDF partitioning registry
    #[cfg(feature = "models")]
    fn register_model_in_registry(&self, m: &SpicepodModel) {
        // Populate the model registry for AI UDF partitioning
        // Pre-compute the source string once during model loading
        if let Some(source) = m.get_source()
            && let Ok(mut registry) = self.model_registry.write()
        {
            registry.insert(m.name.clone(), source.to_string());
        }
    }

    #[cfg(not(feature = "models"))]
    fn register_model_in_registry(&self, _m: &SpicepodModel) {
        // No-op when models feature is disabled
    }

    /// Handle model load error by updating metrics and status
    fn handle_model_load_error(&self, model_name: &str, message: &str) {
        metrics::models::LOAD_ERROR.add(1, &[]);
        self.status
            .update_model(model_name, status::ComponentStatus::Error);
        tracing::warn!("{message}");
    }

    /// Finalize model loading by updating metrics and status based on result
    async fn finalize_model_load(
        &self,
        model: &SpicepodModel,
        source_str: &str,
        result: Result<(), Error>,
    ) {
        match result {
            Ok(()) => {
                tracing::info!("Model [{}] deployed, ready for inferencing", model.name);
                metrics::models::COUNT.add(
                    1,
                    &[
                        KeyValue::new("model", model.name.clone()),
                        KeyValue::new("source", source_str.to_string()),
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
                let mut llm_map = self.completion_llms.write().await;
                llm_map.remove(&m.name);
            }
            None => return,
        }

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
        }
    }
    Ok(())
}
