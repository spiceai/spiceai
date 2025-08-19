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

use std::pin::Pin;

use crate::chat::nsql::SqlGeneration;
use async_openai::{
    error::OpenAIError,
    types::responses::{CreateResponse, Response, ResponseStream},
};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {}

type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Responses: Sync + Send {
    fn as_sql(&self) -> Option<&dyn SqlGeneration>;
    async fn run(&self, prompt: String) -> Result<Option<String>>;
    async fn health(&self) -> Result<()>;
    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Option<String>>> + Send>>> {
        let resp = self.run(prompt).await;
        Ok(Box::pin(stream! { yield resp }))
    }

    async fn responses_stream(
        &self,
        request: CreateResponse,
    ) -> Result<ResponseStream, OpenAIError>;
    async fn responses_request(&self, request: CreateResponse) -> Result<Response, OpenAIError>;
}
