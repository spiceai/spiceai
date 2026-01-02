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
    error::OpenAIError,
    types::responses::{CreateResponse, CreateResponseArgs, Response, ResponseStream},
};
use async_trait::async_trait;
use snafu::prelude::*;
use tracing_futures::Instrument;

use crate::chat::nsql::SqlGeneration;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to load the model: {source}"))]
    FailedToLoadModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Failed to invoke the model: {source}"))]
    FailedToRunModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display(
        "Failed to invoke the model: {source}. Verify the model configuration and try again."
    ))]
    HealthCheckError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Responses: Sync + Send {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
    async fn run(&self, prompt: String) -> Result<Option<String>> {
        let span = tracing::Span::current();

        async move {
            let req = CreateResponseArgs::default()
                .input(prompt)
                .build()
                .boxed()
                .context(FailedToLoadModelSnafu)?;

            let resp = self
                .responses_request(req)
                .await
                .boxed()
                .context(FailedToRunModelSnafu)?;

            Ok(resp.output_text())
        }
        .instrument(span)
        .await
    }

    async fn health(&self) -> Result<()>;
    async fn responses_stream(&self, req: CreateResponse) -> Result<ResponseStream, OpenAIError>;
    async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError>;
}
