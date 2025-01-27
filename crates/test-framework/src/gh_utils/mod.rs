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

use octocrab::actions::ActionsHandler;
use serde_json::Value;

/// Represents a GitHub workflow to be dispatched
pub struct GitHubWorkflow {
    pub org: String,
    pub repo: String,
    pub workflow_file: String,
    pub r#ref: String,
}

impl GitHubWorkflow {
    #[must_use]
    pub fn new(org: &str, repo: &str, workflow_file: &str, r#ref: &str) -> Self {
        Self {
            org: org.to_string(),
            repo: repo.to_string(),
            workflow_file: workflow_file.to_string(),
            r#ref: r#ref.to_string(),
        }
    }

    /// Dispatches the GitHub workflow with the provided JSON input as workflow inputs
    /// Uses an ``ActionsHandler`` from ``octocrab`` to send the request
    pub async fn send(
        &self,
        handler: ActionsHandler<'_>,
        input: Option<Value>,
    ) -> anyhow::Result<()> {
        let action = handler.create_workflow_dispatch(
            self.org.clone(),
            self.repo.clone(),
            self.workflow_file.clone(),
            self.r#ref.clone(),
        );
        if let Some(input) = input {
            action.inputs(input)
        } else {
            action
        }
        .send()
        .await?;

        Ok(())
    }
}

#[must_use]
pub fn map_numbers_to_strings(mut payload: Value) -> Value {
    // GitHub Actions cannot be called with number types in arguments, so they must be converted to strings
    if let serde_json::Value::Object(ref mut map) = payload {
        map.values_mut().for_each(|v| {
            if let serde_json::Value::Number(n) = v {
                *v = serde_json::Value::String(n.to_string());
            }
        });
    }

    payload
}
