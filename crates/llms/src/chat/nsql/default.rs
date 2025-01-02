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
    types::{
        ChatCompletionRequestSystemMessageArgs, CreateChatCompletionRequest,
        CreateChatCompletionRequestArgs, CreateChatCompletionResponse, ResponseFormat,
    },
};

use super::{create_prompt, SqlGeneration};

pub struct DefaultSqlGeneration;

impl SqlGeneration for DefaultSqlGeneration {
    fn create_request_for_query(
        &self,
        model_id: &str,
        query: &str,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let prompt = create_prompt(query);

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
        // Often models prefix with '```sql', (since markdown syntax in request). If present, remove.
        let value = resp
            .choices
            .iter()
            .find_map(|c| c.message.content.clone())
            .map(|c| c.strip_prefix("```SQL").unwrap_or(c.as_str()).to_string());

        Ok(value)
    }
}
