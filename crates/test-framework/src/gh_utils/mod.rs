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

use http_body_util::BodyExt;
use octocrab::{OctoBody, Octocrab, actions::ActionsHandler, map_github_error};
use serde::Deserialize;
use serde_json::Value;
use tonic::transport::Uri;

/// Represents a GitHub workflow to be dispatched
pub struct GitHubWorkflow {
    pub org: String,
    pub repo: String,
    pub workflow_file: String,
    pub r#ref: String,
    /// Run ID excluded from `active_runs_count`. A workflow that dispatches runs of its
    /// own workflow file must exclude its own run, or it always counts toward the
    /// concurrency limit and no slot ever frees up.
    pub excluded_run_id: Option<u64>,
}

impl GitHubWorkflow {
    #[must_use]
    pub fn new(org: &str, repo: &str, workflow_file: &str, r#ref: &str) -> Self {
        Self {
            org: org.to_string(),
            repo: repo.to_string(),
            workflow_file: workflow_file.to_string(),
            r#ref: r#ref.to_string(),
            excluded_run_id: None,
        }
    }

    #[must_use]
    pub fn with_excluded_run_id(mut self, run_id: Option<u64>) -> Self {
        self.excluded_run_id = run_id;
        self
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

    /// Dispatches the GitHub workflow and returns the workflow run URL.
    ///
    /// Requires `X-GitHub-Api-Version: 2026-03-10` which causes the dispatch
    /// endpoint to return a 201 response with `run_url` instead of 204 No Content.
    pub async fn run_workflow(
        &self,
        crab: &Octocrab,
        input: Option<Value>,
    ) -> anyhow::Result<String> {
        let body = serde_json::json!({
            "ref": &self.r#ref,
            "inputs": input.unwrap_or(Value::Null)
        });
        let uri: Uri = format!(
            "/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches",
            owner = self.org,
            repo = self.repo,
            workflow_id = self.workflow_file
        )
        .parse()?;

        // Only used for internal response decoding.
        #[expect(clippy::items_after_statements)]
        #[derive(Deserialize)]
        struct WorkflowDispatchResponse {
            html_url: String,
        }

        // Build the request manually to set X-GitHub-Api-Version: 2026-03-10.
        // octocrab's `build_request` bakes in 2022-11-28 via _SET_HEADERS_MAP, which
        // would produce a comma-joined invalid header. Auth/User-Agent are still injected
        // by the middleware in `execute`.
        let serialized = serde_json::to_string(&body)?;
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(uri)
            .header(http::header::CONTENT_TYPE, "application/json")
            .header("X-GitHub-Api-Version", "2026-03-10")
            .body(OctoBody::from(serialized))?;
        let response = map_github_error(crab.execute(request).await?).await?;
        let bytes = response.into_body().collect().await?.to_bytes();
        let WorkflowDispatchResponse { html_url } =
            serde_json::from_slice::<WorkflowDispatchResponse>(&bytes)?;
        Ok(html_url)
    }

    /// Returns the number of active workflow runs for this workflow.
    ///
    /// Active runs include workflows that are either queued or currently in progress.
    /// The run identified by `excluded_run_id` (if set) is not counted.
    ///
    /// Notes:
    /// - This method retrieves **only the first page** of results, with a maximum of **100 runs** (`per_page(100)` limit).
    pub async fn active_runs_count(&self, octo: &Octocrab) -> anyhow::Result<usize> {
        let page = octo
            .workflows(&self.org, &self.repo)
            .list_runs(&self.workflow_file)
            .per_page(100)
            .send()
            .await?;

        let active_runs = page
            .items
            .into_iter()
            .filter(|run| matches!(run.status.as_str(), "queued" | "in_progress" | "waiting"))
            .filter(|run| self.excluded_run_id.is_none_or(|id| run.id.0 != id))
            .count();

        Ok(active_runs)
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
