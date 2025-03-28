use super::utils::convert_params_for_validation;
use crate::extract_secret;
use crate::parameters::{Error as ParameterError, ParameterSpec, Parameters};
use llms::xai::Xai;
use secrecy::SecretString;
use std::collections::HashMap;

use llms::chat::{Chat, Error as LlmError};

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::component("xai_api_key").required().secret()];

pub struct XaiProvider {
    api_key: String,
}

impl XaiProvider {
    fn validate_params(params: &mut HashMap<String, SecretString>) -> Result<(), LlmError> {
        Parameters::check_for_deprecated_parameters(
            "xai",
            &convert_params_for_validation(params),
            PARAMETERS,
        );
        Parameters::ensure_required_parameters(
            "xai",
            &mut convert_params_for_validation(params),
            "xai",
            PARAMETERS,
        )
        .map_err(|e| match *e {
            ParameterError::MissingParameter { param, component } => {
                LlmError::MissingParameter { param, component }
            }
            ParameterError::InvalidConfigurationNoSource { component, message } => {
                LlmError::InvalidConfiguration { component, message }
            }
        })?;
        Ok(())
    }

    fn extract_params(
        &mut self,
        params: &mut HashMap<String, SecretString>,
    ) -> Result<(), LlmError> {
        if let Some(api_key) = extract_secret!(params, "xai_api_key") {
            self.api_key = api_key.to_string();
        } else {
            return Err(LlmError::MissingParameter {
                param: "xai_api_key".to_string(),
                component: "xai".to_string(),
            });
        }
        Ok(())
    }

    pub fn new() -> Self {
        Self {
            api_key: String::new(),
        }
    }

    pub fn create_chat(
        mut self,
        model_id: Option<&str>,
        params: &mut HashMap<String, SecretString>,
    ) -> Result<Box<dyn Chat>, LlmError> {
        Self::validate_params(params)?;
        self.extract_params(params)?;

        Ok(Box::new(Xai::new(model_id, &self.api_key)) as Box<dyn Chat>)
    }
}
