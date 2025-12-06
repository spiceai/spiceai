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
        Content, FunctionCallingConfig, FunctionCallingMode, FunctionDeclaration, Schema, Tool,
        ToolConfig,
    },
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key =
        std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY environment variable not set");

    let client = Client::new(api_key)?;

    let search_function = FunctionDeclaration {
        name: "search_web".to_string(),
        description: "Search the web for information".to_string(),
        parameters: Some(Schema {
            schema_type: "object".to_string(),
            properties: Some(HashMap::from([(
                "query".to_string(),
                Schema {
                    schema_type: "string".to_string(),
                    description: Some("The search query".to_string()),
                    ..Default::default()
                },
            )])),
            required: Some(vec!["query".to_string()]),
            ..Default::default()
        }),
    };

    let calculate_function = FunctionDeclaration {
        name: "calculate".to_string(),
        description: "Perform mathematical calculations".to_string(),
        parameters: Some(Schema {
            schema_type: "object".to_string(),
            properties: Some(HashMap::from([(
                "expression".to_string(),
                Schema {
                    schema_type: "string".to_string(),
                    description: Some("The mathematical expression to evaluate".to_string()),
                    ..Default::default()
                },
            )])),
            required: Some(vec!["expression".to_string()]),
            ..Default::default()
        }),
    };

    let tools = vec![Tool {
        function_declarations: Some(vec![search_function, calculate_function]),
    }];

    println!("Testing different ToolConfig modes:\n");

    println!("1. AUTO mode - Model decides when to use functions:");
    let tool_config_auto = ToolConfig {
        function_calling_config: Some(FunctionCallingConfig {
            mode: Some(FunctionCallingMode::Auto),
            allowed_function_names: None,
        }),
    };

    let request = GenerateContentRequest::new(vec![Content::user("What is 2 + 2?")])
        .with_tools(tools.clone())
        .with_tool_config(tool_config_auto);

    match client.generate_content("gemini-2.0-flash", request).await {
        Ok(response) => {
            if let Some(candidate) = response.candidates.first() {
                println!("  Response: {:?}\n", candidate.content.parts);
            }
        }
        Err(e) => println!("  Error: {e}\n"),
    }

    println!("2. NONE mode - Function calling disabled:");
    let tool_config_none = ToolConfig {
        function_calling_config: Some(FunctionCallingConfig {
            mode: Some(FunctionCallingMode::None),
            allowed_function_names: None,
        }),
    };

    let request = GenerateContentRequest::new(vec![Content::user("What is 2 + 2?")])
        .with_tools(tools.clone())
        .with_tool_config(tool_config_none);

    match client.generate_content("gemini-2.0-flash", request).await {
        Ok(response) => {
            if let Some(candidate) = response.candidates.first() {
                println!("  Response: {:?}\n", candidate.content.parts);
            }
        }
        Err(e) => println!("  Error: {e:?}\n"),
    }

    println!("3. ANY mode with restricted functions - Only allow 'calculate':");
    let tool_config_restricted = ToolConfig {
        function_calling_config: Some(FunctionCallingConfig {
            mode: Some(FunctionCallingMode::Any),
            allowed_function_names: Some(vec!["calculate".to_string()]),
        }),
    };

    let request =
        GenerateContentRequest::new(vec![Content::user("Calculate 5 * 7 and search for cats")])
            .with_tools(tools)
            .with_tool_config(tool_config_restricted);

    match client.generate_content("gemini-2.0-flash", request).await {
        Ok(response) => {
            if let Some(candidate) = response.candidates.first() {
                println!("  Response: {:?}\n", candidate.content.parts);
            }
        }
        Err(e) => println!("  Error: {e:?}\n"),
    }

    Ok(())
}
