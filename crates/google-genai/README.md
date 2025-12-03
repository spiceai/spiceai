# google-genai

A Rust client library for the Google Generative AI (Gemini) REST API.

## Features

- ✅ Text generation and chat
- ✅ Multi-turn conversations
- ✅ Streaming responses (SSE)
- ✅ Text embeddings with configurable dimensions
- ✅ Function calling (tools)
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

```rust
use google_genai::{
    Client,
    generate::GenerateContentRequest,
    types::{Content, Tool, FunctionDeclaration, Schema, GenerationConfig},
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-api-key")?;
    
    let mut properties = HashMap::new();
    properties.insert("city".to_string(), Schema {
        schema_type: "string".to_string(),
        description: Some("The city name".to_string()),
        ..Default::default()
    });
    
    let tool = Tool {
        function_declarations: Some(vec![FunctionDeclaration {
            name: "get_weather".to_string(),
            description: "Get the weather for a city".to_string(),
            parameters: Some(Schema {
                schema_type: "object".to_string(),
                properties: Some(properties),
                required: Some(vec!["city".to_string()]),
                ..Default::default()
            }),
        }])
    };
    
    let request = GenerateContentRequest::new(vec![
        Content::user("What's the weather in San Francisco?")
    ]).with_tools(vec![tool]);
    
    let response = client.generate_content("gemini-2.0-flash", request).await?;
    
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

## Authentication

Get an API key from [Google AI Studio](https://aistudio.google.com/app/apikey).

## License

Licensed under the Apache License, Version 2.0. See LICENSE for details.
