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
    types::{CreateChatCompletionRequest, CreateChatCompletionResponse},
};

pub mod default;
pub(crate) mod json;
pub(crate) mod structured_output;

/// Additional methods (beyond [`super::Chat`]), whereby a model can provide improved results for SQL code generation.
pub trait SqlGeneration: Sync + Send {
    fn create_request_for_query(
        &self,
        model_id: &str,
        query: &str,
    ) -> Result<CreateChatCompletionRequest, OpenAIError>;

    fn parse_response(
        &self,
        resp: CreateChatCompletionResponse,
    ) -> Result<Option<String>, OpenAIError>;
}

/// Default system prompt for SQL code generation.
#[must_use]
pub fn create_prompt(query: &str) -> String {
    format!(
        r#"Task: Write a SQL query to answer this question: _\"{query}\"_. Instruction: Return only valid SQL code, nothing additional, don't wrap it in ```. Columns with capitals must be quoted. For tables with schemas and catalogs '"catalog"."schema"."table"' not '"catalog.schema.table"'."#
    )
}

#[cfg(test)]
mod tests {

    use default::DefaultSqlGeneration;

    use super::*;
    static MODEL_ID: &str = "model_id";

    #[test]
    fn test_default_create_request_for_query() {
        let req = DefaultSqlGeneration {}
            .create_request_for_query(MODEL_ID, "SELECT * FROM table")
            .expect("failed to create request");
        let req_str = serde_json::to_string_pretty(&req).expect("failed to serialize");

        insta::assert_snapshot!("sql_gen_default", req_str);
    }

    #[test]
    fn test_json_create_request_for_query() {
        let req = json::JsonSchemaSqlGeneration {}
            .create_request_for_query(MODEL_ID, "SELECT * FROM table")
            .expect("failed to create request");
        let req_str = serde_json::to_string_pretty(&req).expect("failed to serialize");

        insta::assert_snapshot!("sql_gen_json", req_str);
    }

    #[test]
    fn test_structured_output_create_request_for_query() {
        let req = structured_output::StructuredOutputSqlGeneration {}
            .create_request_for_query(MODEL_ID, "SELECT * FROM table")
            .expect("failed to create request");
        let req_str = serde_json::to_string_pretty(&req).expect("failed to serialize");

        insta::assert_snapshot!("sql_gen_structured", req_str);
    }
}
