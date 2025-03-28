use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use async_openai::types::Embedding;
use llms::chat::{Chat, Error as LlmError};
use llms::embeddings::{Embed, Error as EmbedError};
use secrecy::{SecretBox, SecretString};
use spicepod::component::embeddings::EmbeddingPrefix;
use spicepod::component::model::ModelSource;

use crate::secrets::Secrets;

pub trait ChatModel {
    fn create_chat(
        &self,
        model_id: &Option<String>,
        component: &spicepod::component::model::Model,
        params: &HashMap<String, SecretString>,
    ) -> Result<Box<dyn Chat>, LlmError>;
}

pub trait EmbedModel {
    fn create_embed(
        &self,
        model_id: &Option<String>,
        component: &spicepod::component::model::Model,
        params: &HashMap<String, SecretBox<str>>,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Result<Box<dyn Embed>, EmbedError>;
}

pub struct ModelFactory {
    chat_models: HashMap<String, Box<dyn ChatModel>>,
    embed_models: HashMap<String, Box<dyn EmbedModel>>,
}

impl ModelFactory {
    pub fn new() -> Self {
        Self {
            chat_models: HashMap::new(),
            embed_models: HashMap::new(),
        }
    }

    pub fn register_chat_model(&mut self, source: ModelSource, model: Box<dyn ChatModel>) {
        self.chat_models.insert(source.to_string(), model);
    }

    pub fn register_embed_model(&mut self, source: EmbeddingPrefix, model: Box<dyn EmbedModel>) {
        self.embed_models.insert(source.to_string(), model);
    }

    pub fn get_chat_model(&self, source: &ModelSource) -> Result<&Box<dyn ChatModel>, LlmError> {
        let model = self.chat_models.get(&source.to_string());
        match model {
            Some(m) => Ok(m),
            None => Err(LlmError::UnknownModelSource {
                from: source.to_string(),
            }),
        }
    }

    pub fn get_embed_model(
        &self,
        source: &EmbeddingPrefix,
    ) -> Result<&Box<dyn EmbedModel>, EmbedError> {
        let model = self.embed_models.get(&source.to_string());
        match model {
            Some(m) => Ok(m),
            None => Err(EmbedError::UnknownModelSource {
                from: source.to_string(),
            }),
        }
    }
}
