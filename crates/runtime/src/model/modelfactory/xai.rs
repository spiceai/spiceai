use super::registry::ModelFactory;
use crate::parameters::{ParameterSpec, Parameters};
use llms::chat::{Chat, Error as LlmError};
use llms::xai::Xai;
use std::sync::Arc;

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::component("api_key").required().secret()];

pub struct XaiFactory {}

impl ModelFactory for XaiFactory {
    fn create_chat(
        &self,
        model_id: Option<&str>,
        _component: &spicepod::component::model::Model,
        params: Parameters,
    ) -> Result<std::sync::Arc<dyn Chat>, LlmError> {
        let api_key =
            params
                .get("api_key")
                .expose()
                .ok_or_else(|_| LlmError::MissingParameter {
                    param: "api_key".to_string(),
                    component: "xai".to_string(),
                })?;

        Ok(Arc::new(Xai::new(model_id, api_key)) as Arc<dyn Chat>)
    }

    fn create_embedding(
        &self,
        _model_id: Option<&str>,
        _component: &spicepod::component::model::Model,
        _params: Parameters,
    ) -> Result<std::sync::Arc<dyn Chat>, LlmError> {
        Err(LlmError::UnknownModelSource {
            from: String::from("xai"),
        })
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    fn prefix(&self) -> &'static str {
        "xai"
    }
}

impl XaiFactory {
    pub fn new_arc() -> Arc<dyn ModelFactory> {
        Arc::new(XaiFactory {}) as Arc<dyn ModelFactory>
    }
}
