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

use async_trait::async_trait;
use secrecy::SecretString;
use spicepod::component::tool::Tool;
use std::{collections::HashMap, sync::Arc};
use tools::SpiceModelTool;

use crate::catalog::SpiceToolCatalog;

/// A factory that can create individual [`SpiceModelTool`]s from a spicepod [`Tool`] component.
pub trait IndividualToolFactory: Send + Sync {
    fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>>;
}

/// A factory that can create [`SpiceToolCatalog`]s from a spicepod [`Tool`] component.
#[async_trait]
pub trait ToolCatalogFactory: Send + Sync {
    async fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
        env: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceToolCatalog>, Box<dyn std::error::Error + Send + Sync>>;
}
