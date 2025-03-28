use llms::chat::{Chat, Error as LlmError};
use secrecy::SecretString;
use spicepod::component::model::ModelSource;
use std::collections::HashMap;

use super::xai::XaiProvider;

pub struct ModelFactory {}

impl ModelFactory {
    pub fn create_chat_model(
        model_source: &ModelSource,
        model_id: Option<&str>,
        component: &spicepod::component::model::Model,
        params: &HashMap<String, SecretString>,
    ) -> Result<Box<dyn Chat>, LlmError> {
        let mut provided_params = params.clone();
        match model_source {
            ModelSource::Xai => {
                XaiProvider::new().create_chat(model_id.as_deref(), &mut provided_params)
            }
            _ => Err(LlmError::UnknownModelSource {
                from: model_source.to_string(),
            }),
        }
    }
}
