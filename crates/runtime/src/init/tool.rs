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

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    Runtime, SpiceToolCatalog, UnableToInitializeLlmToolSnafu, metrics, status,
    tools::{self, Tooling, factory::default_available_catalogs},
};
use futures::future::join_all;
use opentelemetry::KeyValue;
use runtime_secrets::get_params_with_secrets;
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::tool::Tool;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

impl Runtime {
    pub(crate) async fn load_tools(self: Arc<Self>) {
        let app_lock = self.app.read().await;
        if let Some(app) = app_lock.as_ref() {
            for tool in &app.tools {
                tracing::debug!("Loading tool [{}] from {}...", tool.name, tool.from);
                Arc::clone(&self).load_tool(tool).await;
            }
        }

        let mut spawned_tasks = vec![];
        let cloned_self = Arc::clone(&self);

        // Load all built-in tools, regardless if they are in the spicepod.
        // This will enable loading each tool in the catalog, and the catalog as a whole. E.g:
        //   `tools: models, builtin`
        //   `tools: sql, load_memory`
        for ctlg in default_available_catalogs(Arc::clone(&self)) {
            self.insert_tool_catalog(&ctlg).await;
            for tool in ctlg.all().await {
                let cloned_self = Arc::clone(&cloned_self);
                let handle = tokio::spawn(async move {
                    cloned_self.insert_tool(tool.into()).await;
                });
                spawned_tasks.push(handle);
            }
        }

        let _ = join_all(spawned_tasks).await;
    }

    async fn insert_tool_catalog(&self, t: &Arc<dyn SpiceToolCatalog>) {
        let name = t.name().to_string();
        let mut tools_map = self.tools.write().await;

        tools_map.insert(name.clone(), Arc::clone(t).into());
        tracing::trace!("Tool catalog {} ready to use", name.clone());
        metrics::tools::COUNT.add(1, &[KeyValue::new("tool_catalog", name.clone())]);
        self.status
            .update_tool_catalog(&name, status::ComponentStatus::Ready);
    }

    async fn insert_tool(&self, t: Tooling) {
        let name = t.name().to_string();
        let mut tools_map = self.tools.write().await;

        tools_map.insert(name.clone(), t);
        tracing::trace!("Tool {} ready to use", name.clone());
        metrics::tools::COUNT.add(1, &[KeyValue::new("tool", name.clone())]);
        self.status
            .update_tool(&name, status::ComponentStatus::Ready);
    }

    async fn load_tool(self: Arc<Self>, tool: &Tool) {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(None)
            .max_duration(Some(Duration::from_secs(60)))
            .build();

        let _ = retry(retry_strategy, || async {
            self.status
                .update_tool(&tool.name, status::ComponentStatus::Initializing);
            let params_with_secrets: HashMap<String, SecretString> =
                get_params_with_secrets(self.secrets(), &tool.params).await;

            let env_with_secrets: HashMap<String, SecretString> =
                get_params_with_secrets(self.secrets(), &tool.env).await;

            match tools::factory::forge(
                tool,
                params_with_secrets,
                Arc::clone(&self),
                env_with_secrets,
            )
            .await
            .context(UnableToInitializeLlmToolSnafu)
            {
                Ok(t) => {
                    self.insert_tool(t).await;
                    Ok(())
                }
                Err(e) => {
                    metrics::tools::LOAD_ERROR.add(1, &[]);
                    self.status
                        .update_tool(&tool.name, status::ComponentStatus::Error);
                    tracing::warn!(
                        "Unable to load tool '{}' from spicepod. Error: {}",
                        tool.name,
                        e,
                    );
                    Err(RetryError::transient(e))
                }
            }
        })
        .await;
    }
}
