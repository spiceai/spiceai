# google-genai

A Rust client library for the Google Generative AI (Gemini) REST API.

## Features

- ✅ Text generation and chat
- ✅ Multi-turn conversations
- ✅ Streaming responses (SSE)
- ✅ Text embeddings with configurable dimensions
- ✅ Function calling (tools)
- ✅ Tool configuration (function calling modes)
- ✅ Thinking configuration for complex reasoning
- ✅ Cached content support
- ✅ Structured output (response schema)
- ✅ Safety settings
- ✅ Batch embeddings

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
google-genai = { path = "path/to/google-genai" }
```

## Usage

### Basic Text Generation

```rust
use google_genai::{Client, generate::GenerateContentRequest, types::Content};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    let request = GenerateContentRequest::new(vec![
        Content::user("Explain how AI works in a few words.")
    ]);
    
    let response = client.generate_content("gemini-2.0-flash", request).await?;
    
    if let Some(candidate) = response.candidates.first() {
        if let Some(part) = candidate.content.parts.first() {
            println!("Response: {:?}", part);
        }
    }
    
    Ok(())
}
```

### Multi-turn Conversation

```rust
use google_genai::{Client, generate::GenerateContentRequest, types::Content};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    let request = GenerateContentRequest::new(vec![
        Content::user("Hello! My name is Alice."),
        Content::model("Hi Alice! Nice to meet you."),
        Content::user("What's my name?"),
    ]);
    
    let response = client.generate_content("gemini-2.0-flash", request).await?;
    
    Ok(())
}
```

### Streaming Responses

```rust
use google_genai::{Client, generate::GenerateContentRequest, types::Content};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    let request = GenerateContentRequest::new(vec![
        Content::user("Write a story about a magic backpack.")
    ]);
    
    let mut stream = client.stream_generate_content("gemini-2.0-flash", request).await?;
    
    while let Some(result) = stream.next().await {
        match result {
            Ok(response) => {
                if let Some(candidate) = response.candidates.first() {
                    println!("Chunk: {:?}", candidate.content.parts);
                }
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }
    
    Ok(())
}
```

### Embeddings

```rust
use google_genai::{
    Client,
    embeddings::{EmbedContentRequest, EmbedParameters, TaskType},
    types::Content,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    let request = EmbedContentRequest::new(vec![
        Content::user("What is the meaning of life?")
    ]).with_parameters(EmbedParameters {
        output_dimensionality: Some(768),
        task_type: Some(TaskType::RetrievalQuery),
    });
    
    let response = client.embed_content("text-embedding-004", request).await?;
    
    for embedding in response.embeddings {
        println!("Embedding dimension: {}", embedding.values.len());
    }
    
    Ok(())
}
```

### Function Calling

Complete example with function execution and response:

```rust
use google_genai::{
    Client,
    generate::GenerateContentRequest,
    types::{Content, Tool, FunctionDeclaration, Schema, FunctionResponse, Part},
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    // Step 1: Define the function
    let tool = Tool {
        function_declarations: Some(vec![FunctionDeclaration {
            name: "get_weather".to_string(),
            description: "Get the weather for a city".to_string(),
            parameters: Some(Schema {
                schema_type: "object".to_string(),
                properties: Some(HashMap::from([(
                    "city".to_string(),
                    Schema {
                        schema_type: "string".to_string(),
                        ..Default::default()
                    }
                )])),
                required: Some(vec!["city".to_string()]),
                ..Default::default()
            }),
        }])
    };
    
    // Step 2: Initial request
    let request = GenerateContentRequest::new(vec![
        Content::user("What's the weather in San Francisco?")
    ]).with_tools(vec![tool.clone()]);
    
    let response = client.generate_content("gemini-2.0-flash", request).await?;
    
    // Step 3: Execute function if requested
    let mut conversation = vec![Content::user("What's the weather in San Francisco?".to_string())];
    
    if let Some(candidate) = response.candidates.first() {
        conversation.push(candidate.content.clone());
        
        for part in &candidate.content.parts {
            if let Part::FunctionCall { function_call } = part {
                // Execute your function here
                let weather_data = r#"{"temp": 72, "condition": "sunny"}"#;
                
                // Add function response
                let mut response_map = HashMap::new();
                response_map.insert("result".to_string(), serde_json::json!(weather_data));
                
                conversation.push(Content {
                    role: Some("function".to_string()),
                    parts: vec![Part::FunctionResponse {
                        function_response: FunctionResponse {
                            name: function_call.name.clone(),
                            response: response_map,
                        }
                    }],
                });
            }
        }
    }
    
    // Step 4: Send function response back
    let final_request = GenerateContentRequest::new(conversation).with_tools(vec![tool]);
    let final_response = client.generate_content("gemini-2.0-flash", final_request).await?;
    
    Ok(())
}
```

### Tool Configuration

Control function calling behavior with `ToolConfig`:

```rust
use google_genai::{
    Client,
    generate::GenerateContentRequest,
    types::{
        Content, Tool, FunctionDeclaration, ToolConfig,
        FunctionCallingConfig, FunctionCallingMode,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    // Define your tools
    let tools = vec![/* ... */];
    
    // Configure function calling mode
    // Note: allowed_function_names should only be used with ANY mode
    let tool_config = ToolConfig {
        function_calling_config: Some(FunctionCallingConfig {
            mode: Some(FunctionCallingMode::Auto), // AUTO, ANY, or NONE
            allowed_function_names: None, // Only use with ANY mode
        }),
    };
    
    let request = GenerateContentRequest::new(vec![
        Content::user("What's the weather?")
    ])
    .with_tools(tools)
    .with_tool_config(tool_config);
    
    let response = client.generate_content("gemini-2.0-flash", request).await?;
    
    Ok(())
}
```

### Cached Content

Use cached content for optimized repeated queries with large contexts:

```rust
use google_genai::{
    Client,
    generate::GenerateContentRequest,
    types::{Content, CachedContent},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    // Reference a previously created cache
    let cached_content = CachedContent {
        name: Some("cachedContents/your-cache-id".to_string()),
    };
    
    let request = GenerateContentRequest::new(vec![
        Content::user("Based on the cached context, summarize the key points.")
    ]).with_cached_content(cached_content);
    
    let response = client.generate_content("gemini-2.0-flash", request).await?;
    
    Ok(())
}
```

### Thinking Mode

Enable internal reasoning for complex tasks (requires thinking-capable models):

```rust
use google_genai::{
    Client,
    generate::GenerateContentRequest,
    types::{Content, GenerationConfig, ThinkingConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    let thinking_config = ThinkingConfig {
        include_thoughts: Some(true),
        thinking_budget: Some(5),
    };
    
    let generation_config = GenerationConfig {
        thinking_config: Some(thinking_config),
        ..Default::default()
    };
    
    let request = GenerateContentRequest::new(vec![
        Content::user("Solve this complex problem step by step...")
    ]).with_generation_config(generation_config);
    
    let response = client
        .generate_content("gemini-2.0-flash-thinking-exp-1219", request)
        .await?;
    
    Ok(())
}
```

### Structured Output

```rust
use google_genai::{
    Client,
    generate::GenerateContentRequest,
    types::{Content, GenerationConfig, Schema},
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    let mut properties = HashMap::new();
    properties.insert("recipe_name".to_string(), Schema {
        schema_type: "string".to_string(),
        ..Default::default()
    });
    
    let schema = Schema {
        schema_type: "object".to_string(),
        properties: Some(properties),
        required: Some(vec!["recipe_name".to_string()]),
        ..Default::default()
    };
    
    let config = GenerationConfig {
        response_mime_type: Some("application/json".to_string()),
        response_schema: Some(schema),
        ..Default::default()
    };
    
    let request = GenerateContentRequest::new(vec![
        Content::user("Give me a recipe for chocolate chip cookies.")
    ]).with_generation_config(config);
    
    let response = client.generate_content("gemini-2.0-flash", request).await?;
    
    Ok(())
}
```

## API Reference

See the [Google AI documentation](https://ai.google.dev/api) for detailed API specifications.

### Supported Models

- **Chat/Text Generation**: `gemini-2.0-flash`, `gemini-1.5-pro`, `gemini-1.5-flash`
- **Embeddings**: `text-embedding-004`

## Examples

The `examples/` directory contains complete working examples:

- **`simple_chat.rs`** - Basic text generation with token usage
- **`streaming.rs`** - Streaming responses with Server-Sent Events
- **`embeddings.rs`** - Generate text embeddings with multiple inputs
- **`function_calling.rs`** - Function calling with weather API example
- **`tool_config_modes.rs`** - Demonstrates different `ToolConfig` modes (AUTO, NONE, ANY with restrictions)
- **`thinking.rs`** - Using thinking mode for complex reasoning tasks
- **`cached_content.rs`** - Using cached content for optimized queries

Run examples with:
```bash
cargo run --example simple_chat
cargo run --example function_calling
cargo run --example thinking
```

## Authentication

Get an API key from [Google AI Studio](https://aistudio.google.com/app/apikey).

## License

Licensed under the Apache License, Version 2.0. See LICENSE for details.
