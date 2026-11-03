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

use serde::Deserialize;
use serde_json::Value;

const GITHUB_API: &str = "https://api.github.com";

/// An authenticated client for the GitHub REST API.
pub struct GitHubClient {
    http: reqwest::Client,
    token: String,
}

impl GitHubClient {
    /// A client that authenticates every request with `token`.
    ///
    /// # Errors
    ///
    /// When the HTTP client can't be built.
    pub fn new(token: impl Into<String>) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("spiceai-testoperator")
            .build()?;
        Ok(Self {
            http,
            token: token.into(),
        })
    }

    /// Sends `request` with the token and `api_version`, failing on a
    /// non-success status with GitHub's response body.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        api_version: &str,
    ) -> anyhow::Result<reqwest::Response> {
        let response = request
            .bearer_auth(&self.token)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", api_version)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("GitHub API request failed with {status}: {body}");
        }
        Ok(response)
    }
}

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

    fn workflow_url(&self) -> String {
        format!(
            "{GITHUB_API}/repos/{owner}/{repo}/actions/workflows/{workflow_id}",
            owner = self.org,
            repo = self.repo,
            workflow_id = self.workflow_file
        )
    }

    /// Dispatches the GitHub workflow and returns the workflow run URL.
    ///
    /// Requires `X-GitHub-Api-Version: 2026-03-10` which causes the dispatch
    /// endpoint to return a 201 response with `run_url` instead of 204 No Content.
    ///
    /// # Errors
    ///
    /// When the request fails or GitHub refuses the dispatch.
    pub async fn run_workflow(
        &self,
        client: &GitHubClient,
        input: Option<Value>,
    ) -> anyhow::Result<String> {
        // Only used for internal response decoding.
        #[derive(Deserialize)]
        struct WorkflowDispatchResponse {
            html_url: String,
        }

        let body = serde_json::json!({
            "ref": &self.r#ref,
            "inputs": input.unwrap_or(Value::Null)
        });
        let request = client
            .http
            .post(format!("{}/dispatches", self.workflow_url()))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&body)?);
        let bytes = client.send(request, "2026-03-10").await?.bytes().await?;
        let WorkflowDispatchResponse { html_url } =
            serde_json::from_slice::<WorkflowDispatchResponse>(&bytes)?;
        Ok(html_url)
    }

    /// Returns the number of active workflow runs for this workflow.
    ///
    /// Active runs include workflows that are either queued or currently in progress.
    ///
    /// Notes:
    /// - This method retrieves **only the first page** of results, with a maximum of **100 runs** (`per_page(100)` limit).
    ///
    /// # Errors
    ///
    /// When the request fails or the response isn't a list of runs.
    pub async fn active_runs_count(&self, client: &GitHubClient) -> anyhow::Result<usize> {
        #[derive(Deserialize)]
        struct Run {
            status: String,
        }
        #[derive(Deserialize)]
        struct Runs {
            workflow_runs: Vec<Run>,
        }

        let request = client
            .http
            .get(format!("{}/runs", self.workflow_url()))
            .query(&[("per_page", "100")]);
        let bytes = client.send(request, "2022-11-28").await?.bytes().await?;
        let runs: Runs = serde_json::from_slice(&bytes)?;

        let active_runs = runs
            .workflow_runs
            .into_iter()
            .filter(|run| matches!(run.status.as_str(), "queued" | "in_progress" | "waiting"))
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
