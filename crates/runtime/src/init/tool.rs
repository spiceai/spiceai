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

use std::{collections::HashMap, sync::Arc};

use crate::{
    get_params_with_secrets, metrics, status,
    tools::{self, factory::default_available_catalogs},
    Runtime, SpiceModelTool, SpiceToolCatalog, UnableToInitializeLlmToolSnafu,
};
use opentelemetry::KeyValue;
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::tool::Tool;

impl Runtime {
    #[allow(clippy::implicit_hasher)]
    pub(crate) async fn load_tools(&self) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for tool in &app.tools {
                self.load_tool(tool).await;
            }
        }

        // Load all built-in tools, regardless if they are in the spicepod.
        // This will enable loading each tool in the catalog, and the catalog as a whole. E.g:
        //   `tools: models, builtin`
        //   `tools: sql, load_memory`
        for ctlg in default_available_catalogs() {
            self.insert_tool_catalog(&ctlg).await;
            for tool in ctlg.all().await {
                self.insert_tool(&tool).await;
            }
        }
    }

    async fn insert_tool_catalog(&self, t: &Arc<dyn SpiceToolCatalog>) {
        let name = t.name().to_string();
        let mut tools_map = self.tools.write().await;

        tools_map.insert(name.clone(), Arc::clone(t).into());
        tracing::debug!("Tool catalog {} ready to use", name.clone());
        metrics::tools::COUNT.add(1, &[KeyValue::new("tool_catalog", name.clone())]);
        self.status
            .update_tool_catalog(&name, status::ComponentStatus::Ready);
    }

    async fn insert_tool(&self, t: &Arc<dyn SpiceModelTool>) {
        let name = t.name().to_string();
        let mut tools_map = self.tools.write().await;

        tools_map.insert(name.clone(), Arc::clone(t).into());
        tracing::debug!("Tool {} ready to use", name.clone());
        metrics::tools::COUNT.add(1, &[KeyValue::new("tool", name.clone())]);
        self.status
            .update_tool(&name, status::ComponentStatus::Ready);
    }

    async fn load_tool(&self, tool: &Tool) {
        self.status
            .update_tool(&tool.name, status::ComponentStatus::Initializing);
        let params_with_secrets: HashMap<String, SecretString> =
            get_params_with_secrets(self.secrets(), &tool.params).await;

        match tools::factory::forge(tool, params_with_secrets)
            .await
            .context(UnableToInitializeLlmToolSnafu)
        {
            Ok(t) => self.insert_tool(&t).await,
            Err(e) => {
                metrics::tools::LOAD_ERROR.add(1, &[]);
                self.status
                    .update_tool(&tool.name, status::ComponentStatus::Error);
                tracing::warn!(
                    "Unable to load tool from spicepod {}, error: {}",
                    tool.name,
                    e,
                );
            }
        }
    }
}
