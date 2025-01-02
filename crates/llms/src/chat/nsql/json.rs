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
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
        ResponseFormat,
    },
};

use super::{create_prompt, SqlGeneration};

/// Implementation for [`SqlGeneration`] for [`super::Chat`] models that support [`ResponseFormat::JsonObject`].
pub struct JsonSchemaSqlGeneration;
impl JsonSchemaSqlGeneration {
    /// Convert the Json object returned when using a `{ "type": "json_object" } ` response format.
    /// Expected format is `"content": "{\"arbitrary_key\": \"arbitrary_value\"}"`
    pub fn convert_json_object_to_sql(raw_json: &str) -> Result<Option<String>, serde_json::Error> {
        let result: serde_json::Value = serde_json::from_str(raw_json)?;
        Ok(result["sql"].as_str().map(std::string::ToString::to_string))
    }
}

impl SqlGeneration for JsonSchemaSqlGeneration {
    fn create_request_for_query(
        &self,
        model_id: &str,
        query: &str,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let prompt = create_prompt(query);

        let messages: Vec<ChatCompletionRequestMessage> = vec![
            ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()?
                .into(),
            ChatCompletionRequestSystemMessageArgs::default()
                .content("Response Format: JSON, with the postgres SQL under 'sql'.")
                .build()?
                .into(),
        ];

        CreateChatCompletionRequestArgs::default()
            .model(model_id)
            .response_format(ResponseFormat::JsonObject {})
            .messages(messages)
            .build()
    }

    fn parse_response(
        &self,
        resp: CreateChatCompletionResponse,
    ) -> Result<Option<String>, OpenAIError> {
        match resp.choices.iter().find_map(|c| c.message.content.clone()) {
            Some(json_resp) => {
                Self::convert_json_object_to_sql(&json_resp).map_err(OpenAIError::JSONDeserialize)
            }
            None => Ok(None),
        }
    }
}
