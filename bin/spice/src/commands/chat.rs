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
use crate::error::{
    ConnectionFailedSnafu, InvalidResponseSnafu, ModelNotFoundSnafu, NoModelsConfiguredSnafu,
    Result,
};
use clap::Args;
use futures::StreamExt;
use repl::util::{Spinner, create_editor_with_history, save_history};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::io::{self, Read, Write};
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

    /// Remote Spice instance HTTP endpoint (e.g., `http://localhost:8090`)
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Custom HTTP headers in format 'Key:Value' (can be specified multiple times)
    #[arg(long = "headers", value_name = "KEY:VALUE")]
    pub custom_headers: Vec<String>,
}

/// Configuration for chat operations.
struct ChatConfig<'a> {
    model: &'a str,
    temperature: Option<f32>,
    endpoint: Option<&'a str>,
    custom_headers: &'a [String],
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
#[derive(Deserialize, Default, Clone)]
#[expect(clippy::struct_field_names)]
struct Usage {
    prompt_tokens: u32,
    completion_tokens: u32,
    #[expect(dead_code)]
    total_tokens: u32,
}

/// Chat response with timing and usage statistics.
struct ChatResponse {
    content: String,
    total_duration: std::time::Duration,
    first_token_duration: Option<std::time::Duration>,
    usage: Option<Usage>,
}

impl ChatResponse {
    /// Format the stats output like the Go CLI:
    /// `Time: 3.36s (first token 0.45s). Tokens: 1652. Prompt: 1475. Completion: 177 (292.25/s).`
    fn format_stats(&self) -> String {
        let total_secs = self.total_duration.as_secs_f64();

        let first_token_part = self.first_token_duration.map_or(String::new(), |d| {
            format!(" (first token {:.2}s)", d.as_secs_f64())
        });

        if let Some(usage) = &self.usage {
            let total_tokens = usage.prompt_tokens + usage.completion_tokens;
            let completion_rate = if total_secs > 0.0 {
                let rate = f64::from(usage.completion_tokens) / total_secs;
                format!(" ({rate:.2}/s)")
            } else {
                String::new()
            };
            format!(
                "Time: {total_secs:.2}s{first_token_part}. Tokens: {total_tokens}. Prompt: {}. Completion: {}{completion_rate}.",
                usage.prompt_tokens, usage.completion_tokens
            )
        } else {
            format!("Time: {total_secs:.2}s{first_token_part}.")
        }
    }
}

/// Get or validate a model using the runtime context.
async fn get_or_select_model(
    ctx: &RuntimeContext,
    model: Option<&str>,
    endpoint: Option<&str>,
    custom_headers: &[String],
) -> Result<String> {
    let base_endpoint = endpoint.unwrap_or_else(|| ctx.http_endpoint());
    let mut headers: Vec<(String, String)> = ctx.get_headers().into_iter().collect();

    // Add custom headers from command line
    for header in custom_headers {
        if let Some((key, value)) = header.split_once(':') {
            headers.push((key.trim().to_string(), value.trim().to_string()));
        }
    }

    repl::util::get_or_select_model(ctx.http_client(), base_endpoint, &headers, model)
        .await
        .map_err(|e| match e {
            repl::util::UtilError::ModelNotFound { model, available } => {
                ModelNotFoundSnafu { model, available }.build()
            }
            repl::util::UtilError::NoModelsConfigured => NoModelsConfiguredSnafu.build(),
            repl::util::UtilError::ConnectionFailed { endpoint, source } => InvalidResponseSnafu {
                message: format!("Failed to connect to {endpoint}: {source}"),
            }
            .build(),
            repl::util::UtilError::InvalidResponse { message } => {
                InvalidResponseSnafu { message }.build()
            }
        })
}

/// Execute the `chat` command.
///
/// # Errors
///
/// Returns an error if the API requests fail or input/output fails.
pub async fn execute(ctx: &RuntimeContext, args: &ChatArgs) -> Result<()> {
    // Get or select the model
    let model = get_or_select_model(
        ctx,
        args.model.as_deref(),
        args.endpoint.as_deref(),
        &args.custom_headers,
    )
    .await?;

    // Check if running in a terminal (interactive) vs piped input
    let is_terminal = std::io::IsTerminal::is_terminal(&std::io::stdin());

    // Read piped stdin if available
    let stdin_input = if is_terminal {
        None
    } else {
        let mut input = String::new();
        std::io::stdin().read_to_string(&mut input).ok();
        let trimmed = input.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    };

    // Combine piped input with command line message
    let message = match (&args.message, stdin_input) {
        (Some(arg_msg), Some(stdin_msg)) => Some(format!("{stdin_msg}\n{arg_msg}")),
        (Some(arg_msg), None) => Some(arg_msg.clone()),
        (None, Some(stdin_msg)) => Some(stdin_msg),
        (None, None) => None,
    };

    // Create chat config
    let config = ChatConfig {
        model: &model,
        temperature: args.temperature,
        endpoint: args.endpoint.as_deref(),
        custom_headers: &args.custom_headers,
    };

    // If a message was provided (command line or piped), send it and exit
    if let Some(message) = message {
        let messages = vec![Message {
            role: "user".to_string(),
            content: message,
        }];
        let response = send_chat_streaming(ctx, &config, &messages, false).await?;
        // Only show stats if running in a terminal
        if is_terminal {
            println!("\n\n{}\n", response.format_stats());
        } else {
            println!();
        }
        return Ok(());
    }

    // Interactive mode
    println!("Welcome to the Spice.ai Chat REPL!");
    println!("\nUsing model:\n {model}");
    println!("\nType your message and press Enter. Type 'exit' to quit.\n");

    run_repl(ctx, &config).await
}

/// Run the REPL loop.
async fn run_repl(ctx: &RuntimeContext, config: &ChatConfig<'_>) -> Result<()> {
    let (mut rl, history_path) = create_editor_with_history("chat_history.txt").map_err(|e| {
        InvalidResponseSnafu {
            message: e.to_string(),
        }
        .build()
    })?;

    let mut messages: Vec<Message> = Vec::new();

    loop {
        let readline = rl.readline("chat> ");
        let user_input = match readline {
            Ok(line) => line,
            Err(
                rustyline::error::ReadlineError::Interrupted | rustyline::error::ReadlineError::Eof,
            ) => {
                break;
            }
            Err(e) => {
                eprintln!("Error reading input: {e}");
                continue;
            }
        };

        let user_input = user_input.trim();
        if user_input.is_empty() {
            continue;
        }

        // Add to history
        let _ = rl.add_history_entry(user_input);

        // Handle exit commands
        if user_input == "exit"
            || user_input == "quit"
            || user_input == ".exit"
            || user_input == ".quit"
        {
            break;
        }

        // Handle clear screen
        if user_input.to_lowercase() == ".clear" {
            print!("\x1b[H\x1b[2J");
            let _ = io::stdout().flush();
            continue;
        }

        // Handle clear history (both in-memory and persistent)
        if user_input.to_lowercase() == ".clear history" {
            messages.clear();
            let _ = rl.clear_history();
            // Clear persistent history file
            if let Some(path) = &history_path {
                if std::fs::remove_file(path).is_ok() {
                    println!("Chat history cleared.");
                } else {
                    println!("Chat history cleared (in-memory only).");
                }
            } else {
                println!("Chat history cleared.");
            }
            continue;
        }

        // Add user message
        messages.push(Message {
            role: "user".to_string(),
            content: user_input.to_string(),
        });

        // Send and stream response
        match send_chat_streaming(ctx, config, &messages, true).await {
            Ok(response) => {
                // Print stats first before consuming content
                println!("\n\n{}\n", response.format_stats());
                // Add assistant response to history
                if !response.content.is_empty() {
                    messages.push(Message {
                        role: "assistant".to_string(),
                        content: response.content,
                    });
                }
            }
            Err(e) => {
                eprintln!("\x1b[31mError\x1b[0m {e}");
                // Remove the failed user message
                messages.pop();
            }
        }
    }

    // Save history
    save_history(&mut rl, history_path.as_ref());

    Ok(())
}

/// Send a chat request with streaming response.
async fn send_chat_streaming(
    ctx: &RuntimeContext,
    config: &ChatConfig<'_>,
    messages: &[Message],
    interactive: bool,
) -> Result<ChatResponse> {
    let start_time = Instant::now();
    let base_endpoint = config.endpoint.unwrap_or_else(|| ctx.http_endpoint());
    let url = format!("{base_endpoint}/v1/chat/completions");

    let body = ChatRequest {
        messages: messages.to_vec(),
        model: config.model.to_string(),
        stream: true,
        temperature: config.temperature,
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

    // Add custom headers from command line
    for header in config.custom_headers {
        if let Some((key, value)) = header.split_once(':') {
            request = request.header(key.trim(), value.trim());
        }
    }

    // Start spinner in interactive mode
    let spinner = if interactive {
        Some(Spinner::start())
    } else {
        None
    };

    let response = request
        .send()
        .await
        .context(ConnectionFailedSnafu { endpoint: &url })?;

    if !response.status().is_success() {
        // Stop spinner on error
        if let Some(s) = spinner {
            s.stop().await;
        }
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
    let mut spinner = spinner;
    let mut first_token_time: Option<std::time::Duration> = None;
    let mut usage: Option<Usage> = None;

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
                    // Capture usage from the final chunk (if present)
                    if chat_chunk.usage.is_some() {
                        usage = chat_chunk.usage;
                    }

                    for choice in &chat_chunk.choices {
                        if let Some(content) = &choice.delta.content {
                            // Record first token time and stop spinner
                            if first_token_time.is_none() {
                                first_token_time = Some(start_time.elapsed());
                                if let Some(s) = spinner.take() {
                                    s.stop().await;
                                }
                            }
                            print!("{content}");
                            let _ = io::stdout().flush();
                            full_response.push_str(content);
                        }
                    }
                }
            }
        }
    }

    // Ensure spinner is stopped
    if let Some(s) = spinner {
        s.stop().await;
    }

    let total_duration = start_time.elapsed();

    Ok(ChatResponse {
        content: full_response,
        total_duration,
        first_token_duration: first_token_time,
        usage,
    })
}
