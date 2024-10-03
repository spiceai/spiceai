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

use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestSystemMessageArgs, CreateChatCompletionRequest,
        CreateChatCompletionRequestArgs, CreateChatCompletionResponse, ResponseFormat,
    },
};

use super::SqlGeneration;

/// Implementation for [`SqlGeneration`] for [`super::Chat`] models that support [`ResponseFormat::JsonObject`].
pub struct DefaultSqlGeneration;

impl SqlGeneration for DefaultSqlGeneration {
    fn create_request_for_query(
        &self,
        model_id: &str,
        query: &str,
        create_table_statements: &[String],
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let prompt = format!(
            "```SQL\n{table_create_schemas}\n-- Using valid postgres SQL, without comments, answer the following questions for the tables provided above.\n-- {query}",
            table_create_schemas=create_table_statements.join("\n")
        );

        CreateChatCompletionRequestArgs::default()
            .model(model_id)
            .response_format(ResponseFormat::Text)
            .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()?
                .into()])
            .build()
    }

    fn parse_response(
        &self,
        resp: CreateChatCompletionResponse,
    ) -> Result<Option<String>, OpenAIError> {
        Ok(resp.choices.iter().find_map(|c| c.message.content.clone()))
    }
}
