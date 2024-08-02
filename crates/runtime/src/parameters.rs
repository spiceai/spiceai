use std::{collections::HashMap, fmt::Display, sync::Arc};

use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use tokio::sync::RwLock;

pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
use crate::secrets::Secrets;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid configuration for {component}. {message}"))]
    InvalidConfigurationNoSource { component: String, message: String },
}
impl Parameters {
    pub async fn try_new(
        component_name: &str,
        params: Vec<(String, SecretString)>,
        prefix: &'static str,
        secrets: Arc<RwLock<Secrets>>,
        all_params: &'static [ParameterSpec],
    ) -> AnyErrorResult<Self> {
        let full_prefix = format!("{prefix}_");

        // Convert the user-provided parameters into the format expected by the data connector
        let mut params: Vec<(String, SecretString)> = params
            .into_iter()
            .filter_map(|(key, value)| {
                let mut unprefixed_key = key.as_str();
                let mut has_prefix = false;
                if key.starts_with(&full_prefix) {
                    has_prefix = true;
                    unprefixed_key = &key[full_prefix.len()..];
                }

                let spec = all_params.iter().find(|p| p.name == unprefixed_key);

                let Some(spec) = spec else {
                    tracing::warn!("Ignoring parameter {key}: not supported for {component_name}.");
                    return None;
                };

                if !has_prefix && spec.r#type.is_prefixed() {
                    tracing::warn!(
                    "Ignoring parameter {key}: must be prefixed with `{full_prefix}` for {component_name}."
                );
                    return None;
                }

                if has_prefix && !spec.r#type.is_prefixed() {
                    tracing::warn!(
                    "Ignoring parameter {key}: must not be prefixed with `{full_prefix}` for {component_name}."
                );
                    return None;
                }

                Some((unprefixed_key.to_string(), value))
            })
            .collect();
        let secret_guard = secrets.read().await;

        // Try to autoload secrets that might be missing from params.
        for secret_key in all_params.iter().filter(|p| p.secret) {
            let secret_key_with_prefix = format!("{prefix}_{}", secret_key.name);
            tracing::debug!(
                "Attempting to autoload secret for {component_name}: {secret_key_with_prefix}",
            );
            if params.iter().any(|p| p.0 == secret_key.name) {
                continue;
            }
            let secret = secret_guard.get_secret(&secret_key_with_prefix).await;
            if let Ok(Some(secret)) = secret {
                tracing::debug!(
                    "Autoloading secret for {component_name}: {secret_key_with_prefix}",
                );
                // Insert without the prefix into the params
                params.push((secret_key.name.to_string(), secret));
            }
        }

        // Check if all required parameters are present
        for parameter in all_params {
            // If the parameter is missing and has a default value, add it to the params
            let missing = !params.iter().any(|p| p.0 == parameter.name);
            if missing {
                if let Some(default_value) = parameter.default {
                    params.push((parameter.name.to_string(), default_value.to_string().into()));
                    continue;
                }
            }

            if parameter.required && missing {
                let param = if parameter.r#type.is_prefixed() {
                    format!("{full_prefix}{}", parameter.name)
                } else {
                    parameter.name.to_string()
                };

                return Err(Box::new(Error::InvalidConfigurationNoSource {
                    component: component_name.to_string(),
                    message: format!("Missing required parameter: {param}"),
                }));
            }
        }

        Ok(Parameters::new(params, prefix, all_params))
    }

    #[must_use]
    pub fn new(
        params: Vec<(String, SecretString)>,
        prefix: &'static str,
        all_params: &'static [ParameterSpec],
    ) -> Self {
        Self {
            params,
            prefix,
            all_params,
        }
    }

    #[must_use]
    pub fn to_secret_map(&self) -> HashMap<String, SecretString> {
        self.params.iter().cloned().collect()
    }

    /// Returns the `SecretString` for the given parameter, or the user-facing parameter name of the missing parameter.
    #[must_use]
    pub fn get<'a>(&'a self, name: &str) -> ParamLookup<'a> {
        if let Some(param_value) = self.params.iter().find(|p| p.0 == name) {
            ParamLookup::Present(&param_value.1)
        } else {
            ParamLookup::Absent(self.user_param(name))
        }
    }

    /// Gets the `ParameterSpec` for the given parameter name.
    ///
    /// # Panics
    ///
    /// Panics if the parameter is not found in the `all_params` list, as this is a programming error.
    #[must_use]
    pub fn describe(&self, name: &str) -> &ParameterSpec {
        if let Some(spec) = self.all_params.iter().find(|p| p.name == name) {
            spec
        } else {
            panic!("Parameter `{name}` not found in parameters list. Add it to the parameters() list on the DataConnectorFactory or DataAccelerator.");
        }
    }

    /// Retrieves the user-facing parameter name for the given parameter.
    #[must_use]
    pub fn user_param(&self, name: &str) -> UserParam {
        let spec = self.describe(name);

        if self.prefix.is_empty() || !spec.r#type.is_prefixed() {
            UserParam(spec.name.to_string())
        } else {
            UserParam(format!("{}_{}", self.prefix, spec.name))
        }
    }
}

#[derive(Clone)]
pub struct Parameters {
    params: Vec<(String, SecretString)>,
    prefix: &'static str,
    all_params: &'static [ParameterSpec],
}

#[derive(Debug, Clone)]
pub struct UserParam(pub String);

impl Display for UserParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub enum ParamLookup<'a> {
    Present(&'a SecretString),
    Absent(UserParam),
}

impl<'a> ParamLookup<'a> {
    #[must_use]
    pub fn ok(&self) -> Option<&'a SecretString> {
        match self {
            ParamLookup::Present(s) => Some(*s),
            ParamLookup::Absent(_) => None,
        }
    }

    #[must_use]
    pub fn expose(self) -> ExposedParamLookup<'a> {
        match self {
            ParamLookup::Present(s) => ExposedParamLookup::Present(s.expose_secret()),
            ParamLookup::Absent(s) => ExposedParamLookup::Absent(s),
        }
    }

    pub fn ok_or_else<E>(self, f: impl FnOnce(UserParam) -> E) -> Result<&'a SecretString, E> {
        match self {
            ParamLookup::Present(s) => Ok(s),
            ParamLookup::Absent(s) => Err(f(s)),
        }
    }
}

pub enum ExposedParamLookup<'a> {
    Present(&'a str),
    Absent(UserParam),
}

impl<'a> ExposedParamLookup<'a> {
    #[must_use]
    pub fn ok(self) -> Option<&'a str> {
        match self {
            ExposedParamLookup::Present(s) => Some(s),
            ExposedParamLookup::Absent(_) => None,
        }
    }

    pub fn ok_or_else<E>(self, f: impl FnOnce(UserParam) -> E) -> Result<&'a str, E> {
        match self {
            ExposedParamLookup::Present(s) => Ok(s),
            ExposedParamLookup::Absent(s) => Err(f(s)),
        }
    }
}

pub struct ParameterSpec {
    pub name: &'static str,
    pub required: bool,
    pub default: Option<&'static str>,
    pub secret: bool,
    pub description: &'static str,
    pub help_link: &'static str,
    pub examples: &'static [&'static str],
    pub r#type: ParameterType,
}

impl ParameterSpec {
    #[must_use]
    pub const fn connector(name: &'static str) -> Self {
        Self {
            name,
            required: false,
            default: None,
            secret: false,
            description: "",
            help_link: "",
            examples: &[],
            r#type: ParameterType::Connector,
        }
    }

    #[must_use]
    pub const fn runtime(name: &'static str) -> Self {
        Self {
            name,
            required: false,
            default: None,
            secret: false,
            description: "",
            help_link: "",
            examples: &[],
            r#type: ParameterType::Runtime,
        }
    }

    #[must_use]
    pub const fn accelerator(name: &'static str) -> Self {
        Self {
            name,
            required: false,
            default: None,
            secret: false,
            description: "",
            help_link: "",
            examples: &[],
            r#type: ParameterType::Accelerator,
        }
    }

    #[must_use]
    pub const fn required(mut self) -> Self {
        self.required = true;
        self
    }

    #[must_use]
    pub const fn default(mut self, default: &'static str) -> Self {
        self.default = Some(default);
        self
    }

    #[must_use]
    pub const fn secret(mut self) -> Self {
        self.secret = true;
        self
    }

    #[must_use]
    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = description;
        self
    }

    #[must_use]
    pub const fn help_link(mut self, help_link: &'static str) -> Self {
        self.help_link = help_link;
        self
    }

    #[must_use]
    pub const fn examples(mut self, examples: &'static [&'static str]) -> Self {
        self.examples = examples;
        self
    }
}

#[derive(Default, Clone, Copy, PartialEq)]
pub enum ParameterType {
    /// A parameter which tells Spice how to connect to the underlying data source.
    ///
    /// These parameters are automatically prefixed with the data connector's prefix.
    ///
    /// # Examples
    ///
    /// In Postgres, the `host` is a Connector parameter and would be auto-prefixed with `pg_`.
    #[default]
    Connector,

    /// A parameter which tells Spice how to connect to the underlying data accelerator.
    ///
    /// These parameters are automatically prefixed with the data accelerator's prefix.
    ///
    /// # Examples
    ///
    /// In Postgres, the `host` is an Accelerator parameter and would be auto-prefixed with `pg_`.
    Accelerator,

    /// Other parameters which control how the runtime interacts with the data source, but does
    /// not affect the actual connection.
    ///
    /// These parameters are not prefixed with the data connector's prefix.
    ///
    /// # Examples
    ///
    /// In Databricks, the `mode` parameter is used to select which connection to use, and thus is
    /// not a Connector parameter.
    Runtime,
}

// Display implementation for ParameterType
impl Display for ParameterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParameterType::Connector => write!(f, "Connector"),
            ParameterType::Accelerator => write!(f, "Accelerator"),
            ParameterType::Runtime => write!(f, "Runtime"),
        }
    }
}

impl ParameterType {
    #[must_use]
    pub const fn is_prefixed(self) -> bool {
        matches!(self, Self::Connector | Self::Accelerator)
    }
}
