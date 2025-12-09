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

#![allow(clippy::expect_used)]

use google_genai::{
    Client,
    generate::GenerateContentRequest,
    types::{
        Content, FunctionCallingConfig, FunctionCallingMode, FunctionDeclaration, FunctionResponse,
        Part, Schema, Tool, ToolConfig,
    },
};
use std::collections::HashMap;

fn get_current_weather(location: &str, unit: Option<&str>) -> String {
    let unit = unit.unwrap_or("fahrenheit");
    format!(
        "{{\"location\": \"{location}\", \"temperature\": \"72\", \"unit\": \"{unit}\", \"forecast\": \"sunny\"}}"
    )
}

#[expect(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key =
        std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY environment variable not set");

    let client = Client::new(api_key)?;

    let properties = HashMap::from([
        (
            "location".to_string(),
            Schema {
                schema_type: "string".to_string(),
                description: Some("The city and state, e.g. San Francisco, CA".to_string()),
                ..Default::default()
            },
        ),
        (
            "unit".to_string(),
            Schema {
                schema_type: "string".to_string(),
                enum_values: Some(vec!["celsius".to_string(), "fahrenheit".to_string()]),
                ..Default::default()
            },
        ),
    ]);

    let get_weather_function = FunctionDeclaration {
        name: "get_current_weather".to_string(),
        description: "Get the current weather in a given location".to_string(),
        parameters: Some(Schema {
            schema_type: "object".to_string(),
            properties: Some(properties),
            required: Some(vec!["location".to_string()]),
            ..Default::default()
        }),
    };

    let tools = vec![Tool {
        function_declarations: Some(vec![get_weather_function]),
    }];

    let tool_config = ToolConfig {
        function_calling_config: Some(FunctionCallingConfig {
            mode: Some(FunctionCallingMode::Auto),
            allowed_function_names: None,
        }),
    };

    println!("=== Step 1: Initial request with function calling ===\n");

    let initial_request = GenerateContentRequest::new(vec![Content::user(
        "What's the weather like in San Francisco?",
    )])
    .with_tools(tools.clone())
    .with_tool_config(tool_config.clone());

    let initial_response = client
        .generate_content("gemini-2.0-flash", initial_request)
        .await?;

    let mut conversation_history = vec![Content::user(
        "What's the weather like in San Francisco?".to_string(),
    )];

    if let Some(candidate) = initial_response.candidates.first() {
        conversation_history.push(candidate.content.clone());

        for part in &candidate.content.parts {
            match part {
                Part::FunctionCall { function_call } => {
                    println!("Model requested function call:");
                    println!("  Function: {}", function_call.name);
                    println!("  Arguments: {:?}\n", function_call.args);

                    let location = function_call
                        .args
                        .get("location")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    let unit = function_call.args.get("unit").and_then(|v| v.as_str());

                    println!("Calling function: get_current_weather(\"{location}\", {unit:?})");
                    let function_result = get_current_weather(location, unit);
                    println!("Function result: {function_result}\n");

                    let mut response_map = HashMap::new();
                    response_map.insert("result".to_string(), serde_json::json!(function_result));

                    let function_response_part = Part::FunctionResponse {
                        function_response: FunctionResponse {
                            name: function_call.name.clone(),
                            response: response_map,
                        },
                    };

                    conversation_history.push(Content {
                        role: Some("function".to_string()),
                        parts: vec![function_response_part],
                    });
                }
                Part::Text { text } => {
                    println!("Model text response: {text}\n");
                }
                _ => {}
            }
        }
    }

    println!("=== Step 2: Send function response back to model ===\n");

    let followup_request = GenerateContentRequest::new(conversation_history)
        .with_tools(tools)
        .with_tool_config(tool_config);

    let final_response = client
        .generate_content("gemini-2.0-flash", followup_request)
        .await?;

    if let Some(candidate) = final_response.candidates.first() {
        for part in &candidate.content.parts {
            if let Part::Text { text } = part {
                println!("Final response from model:\n{text}\n");
            }
        }
    }

    if let Some(usage) = final_response.usage_metadata {
        println!("Total token usage:");
        println!("  Prompt tokens: {}", usage.prompt_token_count);
        if let Some(candidate_tokens) = usage.candidates_token_count {
            println!("  Candidate tokens: {candidate_tokens}");
        }
        println!("  Total tokens: {}", usage.total_token_count);
    }

    Ok(())
}
