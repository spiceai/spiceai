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

use async_openai::{
    config::Config,
    error::OpenAIError,
    types::responses::{CreateResponse, CreateResponseArgs, Response, ResponseStream},
};
use async_trait::async_trait;
use snafu::ResultExt;
use tracing_futures::Instrument;

use crate::{
    openai::Openai,
    responses::{Error::HealthCheckError, FailedToLoadModelSnafu, Responses, Result},
};

#[async_trait]
impl<C: Config + Send + Sync + Clone> Responses for Openai<C> {
    async fn health(&self) -> Result<()> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");

        let mut req = CreateResponseArgs::default()
            .input("ping")
            .model(self.model.clone())
            .build()
            .boxed()
            .context(FailedToLoadModelSnafu)?;

        req.max_output_tokens = Some(150);

        let result = self.responses_request(req).instrument(span.clone()).await;
        tracing::debug!(
            "{} model responses API health check response: {:?}",
            self.model,
            result
        );
        if let Err(e) = result {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return Err(HealthCheckError { source: e.into() });
        }
        Ok(())
    }

    async fn responses_stream(&self, req: CreateResponse) -> Result<ResponseStream, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model = Some(self.model.clone());

        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let stream = self.client.responses().create_stream(inner_req).await?;

        drop(permit); // drop the permit after acquiring the stream, instead of after receiving the response

        Ok(Box::pin(stream))
    }

    async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model = Some(self.model.clone());

        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let resp = self.client.responses().create(inner_req).await?;

        drop(permit);

        Ok(resp)
    }
}
