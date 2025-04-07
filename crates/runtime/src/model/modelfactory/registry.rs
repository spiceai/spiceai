use crate::parameters::{ParameterSpec, Parameters};
use llms::chat::{Chat, Error as LlmError};
use llms::embeddings::Embed;
use llms::embeddings::Error as EmbedError;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;

use super::xai::XaiFactory;

static CHAT_MODEL_REGISTERY: LazyLock<Mutex<HashMap<String, Arc<dyn ChatModelFactory>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static EMBEDDING_MODEL_REGISTRY: LazyLock<Mutex<HashMap<String, Arc<dyn EmbedModelFactory>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub trait ChatModelFactory: Send + Sync {
    fn create(
        &self,
        model_id: Option<&str>,
        component: &spicepod::component::model::Model,
        params: Parameters,
    ) -> Result<Arc<dyn Chat>, LlmError>;
    fn parameters(&self) -> &'static [ParameterSpec];
    fn prefix(&self) -> &'static str;
}

pub trait EmbedModelFactory: Send + Sync {
    fn create(
        &self,
        model_id: Option<&str>,
        component: &spicepod::component::model::Model,
        params: Parameters,
    ) -> Result<Arc<dyn Embed>, EmbedError>;
    fn parameters(&self) -> &'static [ParameterSpec];
    fn prefix(&self) -> &'static str;
}

pub async fn register_chat_model_factory(factory: Arc<dyn ChatModelFactory>) {
    let mut registry = CHAT_MODEL_REGISTERY.lock().await;
    registry.insert(factory.prefix().to_string(), factory);
}

pub async fn register_embedding_model_factory(factory: Arc<dyn EmbedModelFactory>) {
    let mut registry = EMBEDDING_MODEL_REGISTRY.lock().await;
    registry.insert(factory.prefix().to_string(), factory);
}

pub async fn register_all_chats() {
    register_chat_model_factory(XaiFactory::new_arc()).await;
}

pub async fn register_all() {
    register_all_chats().await;
}

pub async fn get_chat_model_factory(prefix: &str) -> Result<Arc<dyn ChatModelFactory>, LlmError> {
    let registry = CHAT_MODEL_REGISTERY.lock().await;
    registry
        .get(prefix)
        .cloned()
        .ok_or_else(|| LlmError::UnsupportedTaskForModel {
            from: prefix.into(),
            task: "llm".into(),
        })
}

pub async fn get_embedding_model_factory(
    prefix: &str,
) -> Result<Arc<dyn EmbedModelFactory>, EmbedError> {
    let registry = EMBEDDING_MODEL_REGISTRY.lock().await;
    registry
        .get(prefix)
        .cloned()
        .ok_or_else(|| EmbedError::UnknownModelSource {
            from: prefix.into(),
        })
}
