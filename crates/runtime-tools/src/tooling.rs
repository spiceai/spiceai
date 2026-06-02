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

use std::{borrow::Cow, sync::Arc};
use tools::{SpiceModelTool, rename::with_name};

use crate::catalog::SpiceToolCatalog;

/// [`Tooling`] defines several ways to access and load tools into the runtime.
/// Tools can be defined singularly, or as a set of tools a user may want to
/// include all together (i.e. a catalog).
pub enum Tooling {
    Tool(Arc<dyn SpiceModelTool>),
    FunctionTool(Arc<dyn SpiceModelTool>),
    Catalog {
        tools: Arc<dyn SpiceToolCatalog>,
        default_catalog_names: Vec<String>,
    },
}

impl Tooling {
    #[must_use]
    pub fn name(&self) -> Cow<'_, str> {
        match self {
            Tooling::Tool(t) | Tooling::FunctionTool(t) => t.name(),
            Tooling::Catalog { tools, .. } => Cow::Borrowed(tools.name()),
        }
    }

    /// Extension methods for [`Tooling`] that require runtime-local knowledge
    /// (e.g. which catalogs are "default" and therefore don't prefix tool names).
    pub async fn tools(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        match self {
            Tooling::Tool(t) | Tooling::FunctionTool(t) => vec![Arc::clone(t)],
            Tooling::Catalog {
                tools,
                default_catalog_names,
            } => {
                let catalog_name = tools.name();
                if default_catalog_names.contains(&catalog_name.to_string()) {
                    return tools.all().await;
                }

                // If non-default catalog, tool name must be prefixed by catalog.
                tools
                    .all()
                    .await
                    .iter()
                    .map(|t| with_name(t, format!("{catalog_name}/{}", t.name()).as_str()))
                    .collect()
            }
        }
    }
}

impl From<Arc<dyn SpiceModelTool>> for Tooling {
    fn from(tool: Arc<dyn SpiceModelTool>) -> Self {
        Tooling::Tool(tool)
    }
}
