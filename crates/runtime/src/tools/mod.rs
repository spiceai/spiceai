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

use async_trait::async_trait;
use catalog::SpiceToolCatalog;
use serde_json::Value;
use std::{borrow::Cow, sync::Arc};

use crate::Runtime;

pub mod builtin;
pub mod catalog;
pub mod factory;
pub mod memory;
pub mod options;
pub mod utils;

/// [`Tooling`] define several ways to access and load tools into the runtime.
/// Tools can be defined singularly, or as a set of tools a user may want to
/// include all together (i.e. a catalog).
pub enum Tooling {
    Tool(Arc<dyn SpiceModelTool>),
    Catalog(Arc<dyn SpiceToolCatalog>),
}

impl Tooling {
    #[must_use]
    pub async fn tools(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        match self {
            Tooling::Tool(t) => vec![Arc::clone(t)],
            Tooling::Catalog(c) => c.all().await,
        }
    }
}

impl From<Arc<dyn SpiceModelTool>> for Tooling {
    fn from(tool: Arc<dyn SpiceModelTool>) -> Self {
        Tooling::Tool(tool)
    }
}

impl From<Arc<dyn SpiceToolCatalog>> for Tooling {
    fn from(catalog: Arc<dyn SpiceToolCatalog>) -> Self {
        Tooling::Catalog(catalog)
    }
}

/// Tools that implement the [`SpiceModelTool`] trait can automatically be used by LLMs in the runtime.
#[async_trait]
pub trait SpiceModelTool: Sync + Send {
    fn name(&self) -> Cow<'_, str>;
    fn description(&self) -> Option<Cow<'_, str>>;
    fn strict(&self) -> Option<bool> {
        None
    }
    fn parameters(&self) -> Option<Value>;
    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}
