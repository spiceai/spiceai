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
use std::io::{self, Write};
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
    /// `Time: 16.09s (first token 0.53s). Tokens: 197.`
    /// `Prompt: 64. Completion: 133 (8.55/s).`
    fn format_stats(&self) -> String {
        let total_secs = self.total_duration.as_secs_f64();

        let first_token_part = self.first_token_duration.map_or(String::new(), |d| {
            format!(" (first token {:.2}s)", d.as_secs_f64())
        });

        let mut result = if let Some(usage) = &self.usage {
            let total_tokens = usage.prompt_tokens + usage.completion_tokens;
            format!("Time: {total_secs:.2}s{first_token_part}. Tokens: {total_tokens}.")
        } else {
            format!("Time: {total_secs:.2}s{first_token_part}.")
        };

        if let Some(usage) = &self.usage {
            let completion_rate = if total_secs > 0.0 {
                let rate = f64::from(usage.completion_tokens) / total_secs;
                format!(" ({rate:.2}/s)")
            } else {
                String::new()
            };
            result.push_str(&format!(
                "\nPrompt: {}. Completion: {}{completion_rate}.",
                usage.prompt_tokens, usage.completion_tokens
            ));
        }

        result
    }
}

/// Get or validate a model using the runtime context.
async fn get_or_select_model(ctx: &RuntimeContext, model: Option<&str>) -> Result<String> {
    let headers: Vec<(String, String)> = ctx.get_headers().into_iter().collect();
    repl::util::get_or_select_model(ctx.http_client(), ctx.http_endpoint(), &headers, model)
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
    let model = get_or_select_model(ctx, args.model.as_deref()).await?;

    // If a message was provided on command line, send it and exit
    if let Some(message) = &args.message {
        let messages = vec![Message {
            role: "user".to_string(),
            content: message.clone(),
        }];
        let response = send_chat_streaming(ctx, &model, &messages, args.temperature, false).await?;
        println!("\n\n{}\n", response.format_stats());
        return Ok(());
    }

    // Interactive mode
    println!("Welcome to the Spice.ai Chat REPL!");
    println!("\nUsing model:\n {model}");
    println!("\nType your message and press Enter. Type 'exit' to quit.\n");

    run_repl(ctx, &model, args.temperature).await
}

/// Run the REPL loop.
async fn run_repl(ctx: &RuntimeContext, model: &str, temperature: Option<f32>) -> Result<()> {
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
        match send_chat_streaming(ctx, model, &messages, temperature, true).await {
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
    model: &str,
    messages: &[Message],
    temperature: Option<f32>,
    interactive: bool,
) -> Result<ChatResponse> {
    let start_time = Instant::now();
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
