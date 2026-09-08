/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Static parameter specifications used by runtime components.
//!
//! Both [`runtime-parameters`](https://docs.rs/runtime-parameters) (data
//! connectors, accelerators, models, etc.) and `runtime-secrets` (secret
//! stores) describe their accepted parameters with [`ParameterSpec`].
//! Splitting the spec types into this leaf crate avoids a dependency cycle
//! between `runtime-parameters` (which already depends on `runtime-secrets`
//! to support `${ store:KEY }` resolution) and `runtime-secrets` (which now
//! consumes [`ParameterSpec`] to validate per-store params such as `region`
//! on AWS Secrets Manager).

use std::fmt::Display;

#[derive(Debug, Clone, Copy)]
pub struct ParameterSpec {
    pub name: &'static str,
    pub required: bool,
    pub default: Option<&'static str>,
    pub secret: bool,
    pub description: &'static str,
    pub help_link: &'static str,
    pub examples: &'static [&'static str],
    pub one_of: Option<&'static [&'static str]>,
    pub one_of_ignore_ascii_case: bool,
    pub deprecation_message: Option<&'static str>,
    pub r#type: ParameterType,
}

impl ParameterSpec {
    /// A parameter passed through to the underlying component, automatically
    /// prefixed with the component's prefix (e.g. `pg_host` for Postgres).
    #[must_use]
    pub const fn component(name: &'static str) -> Self {
        Self {
            name,
            required: false,
            default: None,
            secret: false,
            description: "",
            help_link: "",
            examples: &[],
            deprecation_message: None,
            r#type: ParameterType::Component,
            one_of: None,
            one_of_ignore_ascii_case: false,
        }
    }

    /// A parameter that controls how the runtime interacts with the component
    /// and is not prefixed (e.g. Databricks `mode`).
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
            deprecation_message: None,
            r#type: ParameterType::Runtime,
            one_of: None,
            one_of_ignore_ascii_case: false,
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

    #[must_use]
    pub const fn deprecated(mut self, deprecation_message: &'static str) -> Self {
        self.deprecation_message = Some(deprecation_message);
        self
    }

    #[must_use]
    pub const fn one_of(mut self, options: &'static [&'static str]) -> Self {
        self.one_of = Some(options);
        self
    }

    #[must_use]
    pub const fn one_of_ignore_ascii_case(mut self, options: &'static [&'static str]) -> Self {
        self.one_of = Some(options);
        self.one_of_ignore_ascii_case = true;
        self
    }

    #[must_use]
    pub const fn is_boolean(mut self) -> Self {
        self = self.one_of(&["true", "false"]);
        self
    }

    /// Returns the user-facing name for this parameter. Component params are
    /// rendered as `{prefix}_{name}`; runtime params keep their bare name.
    #[must_use]
    pub fn display_name(&self, prefix: &str) -> String {
        if self.r#type.is_prefixed() && !prefix.is_empty() {
            format!("{prefix}_{}", self.name)
        } else {
            self.name.to_string()
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub enum ParameterType {
    /// A parameter which tells Spice how to configure the underlying component.
    /// Auto-prefixed with the component's prefix when surfaced to users.
    #[default]
    Component,

    /// A parameter that controls runtime behavior around the component, not
    /// the component configuration itself. Not prefixed.
    Runtime,
}

impl Display for ParameterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Component => write!(f, "Component"),
            Self::Runtime => write!(f, "Runtime"),
        }
    }
}

impl ParameterType {
    #[must_use]
    pub const fn is_prefixed(self) -> bool {
        matches!(self, Self::Component)
    }
}
