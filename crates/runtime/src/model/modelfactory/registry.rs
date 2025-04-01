use crate::parameters::{ParameterSpec, Parameters};
use llms::chat::{Chat, Error as LlmError};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;

use super::xai::XaiFactory;

static MODEL_FACTORY_REGISTRY: LazyLock<Mutex<HashMap<String, Arc<dyn ModelFactory>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub trait ModelFactory: Send + Sync {
    fn create_chat(
        &self,
        model_id: Option<&str>,
        component: &spicepod::component::model::Model,
        params: Parameters,
    ) -> Result<Arc<dyn Chat>, LlmError>;
    fn create_embedding(
        &self,
        model_id: Option<&str>,
        component: &spicepod::component::model::Model,
        params: Parameters,
    ) -> Result<Arc<dyn Chat>, LlmError>;
    fn parameters(&self) -> &'static [ParameterSpec];
    fn prefix(&self) -> &'static str;
}

pub async fn register_model_factory(name: &str, factory: Arc<dyn ModelFactory>) {
    let mut registry = MODEL_FACTORY_REGISTRY.lock().await;
    registry.insert(name.to_string(), factory);
}

pub async fn register_all() {
    register_model_factory("xai", XaiFactory::new_arc()).await;
}
