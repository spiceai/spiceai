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
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
        ResponseFormat, ResponseFormatJsonSchema,
    },
};
use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::SqlGeneration;

/// Implementation for [`SqlGeneration`] for [`super::Chat`] models that support [`ResponseFormat::JsonSchema`].
pub struct StructuredOutputSqlGeneration;

/// Use structured output schema to generate SQL code more reliably.
impl SqlGeneration for StructuredOutputSqlGeneration {
    fn create_request_for_query(
        &self,
        model_id: &str,
        query: &str,
        create_table_statements: &[String],
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let prompt = format!(
            "```SQL\n{table_create_schemas}\n-- Using valid postgres SQL, without comments, answer the following questions for the tables provided above.\n-- {user_query}",
            user_query=query,
            table_create_schemas=create_table_statements.join("\n")
        );
        let messages: Vec<ChatCompletionRequestMessage> = vec![
            ChatCompletionRequestSystemMessageArgs::default()
                .content("Return JSON, with the requested SQL under 'sql'.")
                .build()?
                .into(),
            ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()?
                .into(),
        ];

        let mut structured_output_schema = serde_json::to_value(schema_for!(StructuredNsqlOutput))
            .map_err(|e| {
                OpenAIError::InvalidArgument(format!(
                    "Failed to serialize structured output schema: {e}"
                ))
            })?;

        // [`schemars:schema_for`] does not include the `additionalProperties` field when false. Explictly required by some providers (e.g. OpenAI).
        structured_output_schema["additionalProperties"] = Value::Bool(false);

        tracing::debug!("Structured output schema: {structured_output_schema}");

        CreateChatCompletionRequestArgs::default()
            .model(model_id)
            .response_format(ResponseFormat::JsonSchema {
                json_schema: ResponseFormatJsonSchema {
                    name: "sql_mode".to_string(),
                    description: None,
                    strict: Some(true),
                    schema: Some(structured_output_schema),
                },
            })
            .messages(messages)
            .build()
    }

    fn parse_response(
        &self,
        resp: CreateChatCompletionResponse,
    ) -> Result<Option<String>, OpenAIError> {
        match resp
            .choices
            .first()
            .and_then(|c| c.message.content.as_ref())
        {
            Some(message) => match serde_json::from_str(message) {
                Ok(StructuredNsqlOutput { sql }) => Ok(Some(sql)),
                Err(e) => Err(OpenAIError::JSONDeserialize(e)),
            },
            None => Ok(None),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct StructuredNsqlOutput {
    pub sql: String,
}
