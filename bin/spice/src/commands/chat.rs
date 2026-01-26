/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `spice chat` command - Chat with an LLM.

use crate::context::RuntimeContext;
use crate::error::{ConnectionFailedSnafu, InvalidResponseSnafu, Result};
use clap::Args;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::io::{self, BufRead, Write};
use std::time::Instant;

/// Arguments for the `chat` command.
#[derive(Args, Debug)]
pub struct ChatArgs {
    /// Model to use for chat
    #[arg(long, short)]
    pub model: Option<String>,

    /// Single message to send (non-interactive mode)
    pub message: Option<String>,

    /// Temperature for sampling (0.0 = deterministic, higher = more random)
    #[arg(long)]
    pub temperature: Option<f32>,
}

/// A chat message.
#[derive(Serialize, Deserialize, Clone)]
struct Message {
    role: String,
    content: String,
}

/// Request body for chat completions.
#[derive(Serialize)]
struct ChatRequest {
    messages: Vec<Message>,
    model: String,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<StreamOptions>,
}

/// Stream options for chat completions.
#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}

/// A streaming chunk from the chat completions API.
#[derive(Deserialize)]
struct ChatChunk {
    choices: Vec<ChunkChoice>,
    #[serde(default)]
    #[expect(dead_code)]
    usage: Option<Usage>,
}

/// A choice in a chat chunk.
#[derive(Deserialize)]
struct ChunkChoice {
    delta: Delta,
}

/// Delta content in a streaming response.
#[derive(Deserialize)]
struct Delta {
    #[serde(default)]
    content: Option<String>,
}

/// Token usage statistics.
#[derive(Deserialize, Default)]
#[expect(dead_code, clippy::struct_field_names)]
struct Usage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

/// Model information.
#[derive(Deserialize)]
struct Model {
    id: String,
}

/// Models response.
#[derive(Deserialize)]
struct ModelsResponse {
    data: Vec<Model>,
}

/// Execute the `chat` command.
///
/// # Errors
///
/// Returns an error if the API requests fail or input/output fails.
pub async fn execute(ctx: &RuntimeContext, args: &ChatArgs) -> Result<()> {
    // Get or select the model
    let model = match &args.model {
        Some(m) => m.clone(),
        None => select_model(ctx).await?,
    };

    // If a message was provided on command line, send it and exit
    if let Some(message) = &args.message {
        let messages = vec![Message {
            role: "user".to_string(),
            content: message.clone(),
        }];
        send_chat_streaming(ctx, &model, &messages, args.temperature, false).await?;
        println!();
        return Ok(());
    }

    // Interactive mode
    println!("Welcome to the Spice.ai Chat REPL!");
    println!("\nUsing model:\n {model}");
    println!("\nType your message and press Enter. Type 'exit' to quit.\n");

    run_repl(ctx, &model, args.temperature).await
}

/// Select a model from available models.
async fn select_model(ctx: &RuntimeContext) -> Result<String> {
    let url = format!("{}/v1/models?status=true", ctx.http_endpoint());

    let mut request = ctx.http_client().get(&url);
    for (key, value) in ctx.get_headers() {
        request = request.header(&key, &value);
    }

    let response = request
        .send()
        .await
        .context(ConnectionFailedSnafu { endpoint: &url })?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(InvalidResponseSnafu {
            message: format!("Failed to get models: {status} - {text}"),
        }
        .build());
    }

    let models: ModelsResponse = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse models response: {e}"),
        }
        .build()
    })?;

    if models.data.is_empty() {
        return Err(InvalidResponseSnafu {
            message: "No models found. Please configure a model in your Spicepod.".to_string(),
        }
        .build());
    }

    // If only one model, use it
    if models.data.len() == 1 {
        return Ok(models.data[0].id.clone());
    }

    // Let user select
    println!("\nAvailable models:");
    for (i, model) in models.data.iter().enumerate() {
        println!("  {}: {}", i + 1, model.id);
    }

    print!("Select model (1-{}): ", models.data.len());
    let _ = io::stdout().flush();

    let stdin = io::stdin();
    let mut input = String::new();
    stdin.lock().read_line(&mut input).map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to read input: {e}"),
        }
        .build()
    })?;

    let selection: usize = input.trim().parse().map_err(|_| {
        InvalidResponseSnafu {
            message: "Invalid selection".to_string(),
        }
        .build()
    })?;

    if selection == 0 || selection > models.data.len() {
        return Err(InvalidResponseSnafu {
            message: format!("Selection must be between 1 and {}", models.data.len()),
        }
        .build());
    }

    Ok(models.data[selection - 1].id.clone())
}

/// Run the REPL loop.
async fn run_repl(ctx: &RuntimeContext, model: &str, temperature: Option<f32>) -> Result<()> {
    let stdin = io::stdin();
    let mut messages: Vec<Message> = Vec::new();

    loop {
        print!("chat> ");
        let _ = io::stdout().flush();

        let mut input = String::new();
        match stdin.lock().read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error reading input: {e}");
                continue;
            }
        }

        let user_input = input.trim();
        if user_input.is_empty() {
            continue;
        }

        // Handle exit commands
        if user_input == "exit"
            || user_input == "quit"
            || user_input == ".exit"
            || user_input == ".quit"
        {
            break;
        }

        // Handle clear history
        if user_input.to_lowercase() == ".clear" {
            messages.clear();
            println!("Chat history cleared.");
            continue;
        }

        // Add user message
        messages.push(Message {
            role: "user".to_string(),
            content: user_input.to_string(),
        });

        // Send and stream response
        let start = Instant::now();
        match send_chat_streaming(ctx, model, &messages, temperature, true).await {
            Ok(response_content) => {
                // Add assistant response to history
                if !response_content.is_empty() {
                    messages.push(Message {
                        role: "assistant".to_string(),
                        content: response_content,
                    });
                }
                let elapsed = start.elapsed();
                println!("\n\n[{:.2}s]\n", elapsed.as_secs_f64());
            }
            Err(e) => {
                eprintln!("\x1b[31mError\x1b[0m {e}");
                // Remove the failed user message
                messages.pop();
            }
        }
    }

    Ok(())
}

/// Send a chat request with streaming response.
async fn send_chat_streaming(
    ctx: &RuntimeContext,
    model: &str,
    messages: &[Message],
    temperature: Option<f32>,
    _interactive: bool,
) -> Result<String> {
    let url = format!("{}/v1/chat/completions", ctx.http_endpoint());

    let body = ChatRequest {
        messages: messages.to_vec(),
        model: model.to_string(),
        stream: true,
        temperature,
        stream_options: Some(StreamOptions {
            include_usage: true,
        }),
    };

    let mut request = ctx
        .http_client()
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream")
        .json(&body);

    for (key, value) in ctx.get_headers() {
        request = request.header(&key, &value);
    }

    let response = request
        .send()
        .await
        .context(ConnectionFailedSnafu { endpoint: &url })?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(InvalidResponseSnafu {
            message: format!("Chat request failed: {status} - {text}"),
        }
        .build());
    }

    // Stream the response
    let mut full_response = String::new();
    let mut stream = response.bytes_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            InvalidResponseSnafu {
                message: format!("Failed to read stream: {e}"),
            }
            .build()
        })?;

        let text = String::from_utf8_lossy(&chunk);

        // Parse SSE events
        for line in text.lines() {
            if let Some(data) = line.strip_prefix("data: ") {
                if data == "[DONE]" {
                    continue;
                }

                // Parse the JSON chunk
                if let Ok(chat_chunk) = serde_json::from_str::<ChatChunk>(data) {
                    for choice in &chat_chunk.choices {
                        if let Some(content) = &choice.delta.content {
                            print!("{content}");
                            let _ = io::stdout().flush();
                            full_response.push_str(content);
                        }
                    }
                }
            }
        }
    }

    Ok(full_response)
}
