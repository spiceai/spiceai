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

use anyhow::{anyhow, Context, Result};
use async_openai::types::chat::{CreateChatCompletionRequest, CreateChatCompletionResponse};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spicepod::component::eval::Eval;

/// Client for communicating with a Spice runtime
pub struct SpiceClient {
    endpoint: String,
    client: Client,
}

impl SpiceClient {
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    /// List all available evaluations by reading from spicepod files
    pub async fn list_evals(&self) -> Result<Vec<Eval>> {
        // For now, use the existing /v1/evals endpoint to list available evals
        // In the future, this could read directly from spicepod files
        let url = format!("{}/v1/evals", self.endpoint);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to list evals")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "List evals failed with status {}: {}",
                status,
                text
            ));
        }

        #[derive(Deserialize)]
        struct ListEvalElement {
            pub name: String,
            pub description: Option<String>,
            pub dataset: String,
            pub scorers: Vec<String>,
        }

        let list: Vec<ListEvalElement> = response
            .json()
            .await
            .context("Failed to parse evals list")?;

        Ok(list
            .into_iter()
            .map(|e| Eval {
                name: e.name,
                description: e.description,
                dataset: e.dataset,
                scorers: e.scorers,
                depends_on: Vec::new(),
                metrics: None,
            })
            .collect())
    }

    /// Get a specific evaluation by name
    pub async fn get_eval(&self, name: &str) -> Result<Eval> {
        let evals = self.list_evals().await?;
        evals
            .into_iter()
            .find(|e| e.name == name)
            .ok_or_else(|| anyhow!("Evaluation '{}' not found", name))
    }

    /// Execute a SQL query and get JSON results
    pub async fn query_sql(&self, sql: &str) -> Result<Vec<Value>> {
        let url = format!("{}/v1/sql", self.endpoint);

        #[derive(Serialize)]
        struct QueryRequest {
            query: String,
        }

        let response = self
            .client
            .post(&url)
            .json(&QueryRequest {
                query: sql.to_string(),
            })
            .send()
            .await
            .context("Failed to send SQL query")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("SQL query failed with status {}: {}", status, text));
        }

        let data: Vec<Value> = response
            .json()
            .await
            .context("Failed to parse query response")?;

        Ok(data)
    }

    /// Execute a chat completion request
    pub async fn chat_completion(
        &self,
        request: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse> {
        let url = format!("{}/v1/chat/completions", self.endpoint);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send chat completion request")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Chat completion failed with status {}: {}",
                status,
                text
            ));
        }

        let result: CreateChatCompletionResponse = response
            .json()
            .await
            .context("Failed to parse chat completion response")?;

        Ok(result)
    }

    /// Write results back to the eval runs table
    pub async fn write_eval_run(
        &self,
        id: &str,
        dataset: &str,
        model: &str,
        scorers: &[String],
    ) -> Result<()> {
        let scorers_json = serde_json::to_string(scorers)?;
        let sql = format!(
            "INSERT INTO spice.evals.runs (id, created_at, dataset, model, status, scorers, metrics) \
             VALUES ('{}', CURRENT_TIMESTAMP, '{}', '{}', 'Waiting', '{}', '{{}}')",
            id, dataset, model, scorers_json
        );

        self.query_sql(&sql).await?;
        Ok(())
    }

    /// Update eval run status
    pub async fn update_eval_run_status(
        &self,
        id: &str,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<()> {
        let error_clause = if let Some(err) = error_message {
            format!(", error_message = '{}'", err.replace('\'', "''"))
        } else {
            String::new()
        };

        let sql = format!(
            "UPDATE spice.evals.runs SET status = '{}'{} WHERE id = '{}'",
            status, error_clause, id
        );

        self.query_sql(&sql).await?;
        Ok(())
    }

    /// Write eval results to the results table
    pub async fn write_eval_results(
        &self,
        run_id: &str,
        input: &str,
        actual: &str,
        expected: &str,
        scorer: &str,
        score: f32,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO spice.evals.results (run_id, timestamp, input, actual, expected, scorer, score) \
             VALUES ('{}', CURRENT_TIMESTAMP, '{}', '{}', '{}', '{}', {})",
            run_id,
            input.replace('\'', "''"),
            actual.replace('\'', "''"),
            expected.replace('\'', "''"),
            scorer,
            score
        );

        self.query_sql(&sql).await?;
        Ok(())
    }
}
