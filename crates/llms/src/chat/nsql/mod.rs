/*
Copyright 2024 The Spice.ai OSS Authors
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

use snafu::Snafu;
use async_openai::types::{CreateChatCompletionRequest, CreateChatCompletionResponse};

/// Additional methods (beyond [`super::Chat`]), whereby a model can provide improved results for SQL code generation.
pub trait SqlGeneration: Sync + Send {
    fn create_request_for_query(&self, query: &str, create_table_statements: &[String]) -> Result<CreateChatCompletionRequest>;

    fn parse_response(&self, resp: CreateChatCompletionResponse) -> Result<Option<String>>;
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to generate SQL code from response: {}", source))]
    ResponseError { source: Box<dyn std::error::Error + Send + Sync> },
}