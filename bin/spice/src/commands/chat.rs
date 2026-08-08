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
    ConnectionFailedSnafu, InvalidArgumentSnafu, InvalidResponseSnafu, ModelNotFoundSnafu,
    NoModelsConfiguredSnafu, Result,
};
use crate::output::{OutputFormat, write_json};
use crate::sse::{SseDecoder, SseEvent};
use clap::Args;
use futures::StreamExt;
use repl::util::{Spinner, create_editor_with_history, save_history};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::io::{self, Read, Write};
use std::time::Instant;

/// Arguments for the `chat` command.
#[derive(Args, Debug)]
#[command(
    about = "Chat with a configured LLM through the runtime's OpenAI-compatible API",
    long_about = r#"Chat with an LLM hosted by the Spice runtime.

With no message, opens an interactive REPL. With a positional message,
streams a single response and exits (non-interactive mode). The model must be
registered in `spicepod.yaml` under `models:` and reported by `spice models`.

The `-p` and `-chat` forms are root-level shortcuts for one-shot prompts. Quote
multi-word prompts so the shell passes them as one argument.

EXAMPLES
    spice -chat "Summarize loaded datasets"       # One-shot prompt with the only configured model
    spice -p --model llm "Summarize TPC-H Q1"     # One-shot prompt with a specific model
  spice chat                                    # Interactive REPL (prompts to pick a model)
  spice chat --model llm                        # REPL with a specific model
  spice chat --model llm "Summarize TPC-H Q1"  # One-shot non-interactive query
  echo "What datasets are loaded?" | spice chat --model llm

Docs: https://spiceai.org/docs"#
)]
pub struct ChatArgs {
    /// Model id to use (must be registered under `models:` in `spicepod.yaml`).
    #[arg(long, short)]
    pub model: Option<String>,

    /// Require a prompt and auto-select only when exactly one model is configured.
    #[arg(long, hide = true)]
    pub direct_prompt: bool,

    /// Single message to send (skip the REPL and exit after streaming the response).
    pub message: Option<String>,

    /// Sampling temperature (0.0 = deterministic, higher = more random).
    #[arg(long)]
    pub temperature: Option<f32>,

    /// Override the runtime HTTP endpoint (e.g. `http://localhost:8090`).
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Custom HTTP headers in `Key:Value` form (repeatable).
    #[arg(long = "headers", value_name = "KEY:VALUE")]
    pub custom_headers: Vec<String>,

    /// Output format for one-shot responses
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

/// Configuration for chat operations.
struct ChatConfig<'a> {
    model: &'a str,
    temperature: Option<f32>,
    endpoint: Option<&'a str>,
    custom_headers: &'a [String],
}

enum ModelSelection {
    Interactive,
    SingleModelOrExplicit,
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

/// The payload of an error the server reports mid-stream.
///
/// The runtime sends its own as an `error`-named event carrying `{"type":…,"message":…}`;
/// an OpenAI-compatible server nests the same information under `error`. Either way the
/// stream is over, and whatever has been printed is a partial answer rather than a whole one.
#[derive(Deserialize)]
struct StreamErrorPayload {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    error: Option<NestedStreamError>,
}

#[derive(Deserialize)]
struct NestedStreamError {
    #[serde(default)]
    message: Option<String>,
}

impl StreamErrorPayload {
    /// The reason `data` gives for the stream failing, if it reads as a failure and gives one.
    fn reason_in(data: &str) -> Option<String> {
        let payload = serde_json::from_str::<Self>(data).ok()?;
        let message = payload
            .message
            .as_deref()
            .or_else(|| payload.error.as_ref()?.message.as_deref())?;

        Some(message.to_string())
    }
}

/// A choice in a chat chunk.
#[derive(Deserialize)]
struct ChunkChoice {
    /// Absent on a choice that carries no content. Azure `OpenAI`'s asynchronous content
    /// filtering emits annotation choices holding only `content_filter_results`, and those
    /// arrive on a passing stream too -- so a choice without a delta is an ordinary event
    /// with nothing to print, not an unreadable one.
    #[serde(default)]
    delta: Option<Delta>,
}

/// Delta content in a streaming response.
#[derive(Deserialize)]
struct Delta {
    #[serde(default)]
    content: Option<String>,
}

/// Token usage statistics.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[expect(clippy::struct_field_names)]
struct Usage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

/// Chat response with timing and usage statistics.
#[derive(Debug)]
struct ChatResponse {
    content: String,
    total_duration: std::time::Duration,
    first_token_duration: Option<std::time::Duration>,
    usage: Option<Usage>,
}

#[derive(Serialize)]
struct ChatJsonResponse<'a> {
    model: &'a str,
    content: &'a str,
    duration_ms: u128,
    first_token_ms: Option<u128>,
    usage: Option<&'a Usage>,
}

impl<'a> ChatJsonResponse<'a> {
    fn from_response(model: &'a str, response: &'a ChatResponse) -> Self {
        Self {
            model,
            content: &response.content,
            duration_ms: response.total_duration.as_millis(),
            first_token_ms: response
                .first_token_duration
                .map(|duration| duration.as_millis()),
            usage: response.usage.as_ref(),
        }
    }
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
    model_selection: ModelSelection,
) -> Result<String> {
    let base_endpoint = endpoint.unwrap_or_else(|| ctx.http_endpoint());
    let mut headers: Vec<(String, String)> = ctx.get_headers().into_iter().collect();

    // Add custom headers from command line
    for header in custom_headers {
        if let Some((key, value)) = header.split_once(':') {
            headers.push((key.trim().to_string(), value.trim().to_string()));
        }
    }

    match model_selection {
        ModelSelection::Interactive => {
            repl::util::get_or_select_model(ctx.http_client(), base_endpoint, &headers, model)
                .await
                .map_err(map_model_error)
        }
        ModelSelection::SingleModelOrExplicit => {
            get_or_require_explicit_model(ctx.http_client(), base_endpoint, &headers, model).await
        }
    }
}

async fn get_or_require_explicit_model(
    client: &reqwest::Client,
    base_endpoint: &str,
    headers: &[(String, String)],
    model: Option<&str>,
) -> Result<String> {
    if let Some(model_name) = model {
        repl::util::validate_model(client, base_endpoint, headers, model_name)
            .await
            .map_err(map_model_error)?;
        return Ok(model_name.to_string());
    }

    let models = repl::util::get_available_models(client, base_endpoint, headers)
        .await
        .map_err(map_model_error)?;
    select_single_available_model(&models)
}

fn select_single_available_model(models: &[String]) -> Result<String> {
    match models {
        [] => NoModelsConfiguredSnafu.fail(),
        [model] => Ok(model.clone()),
        _ => InvalidArgumentSnafu {
            message: format!(
                "Multiple models are configured: {}. Specify one with --model.",
                models.join(", ")
            ),
        }
        .fail(),
    }
}

fn map_model_error(error: repl::util::UtilError) -> crate::error::Error {
    match error {
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
    }
}

/// Execute the `chat` command.
///
/// # Errors
///
/// Returns an error if the API requests fail or input/output fails.
pub async fn execute(ctx: &RuntimeContext, args: &ChatArgs) -> Result<()> {
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

    if args.direct_prompt && message.is_none() {
        return InvalidArgumentSnafu {
            message: "A prompt is required. Pass one after -p or -chat, or pipe prompt text on stdin.",
        }
        .fail();
    }

    let model_selection = if args.direct_prompt {
        ModelSelection::SingleModelOrExplicit
    } else {
        ModelSelection::Interactive
    };

    // Get or select the model
    let model = get_or_select_model(
        ctx,
        args.model.as_deref(),
        args.endpoint.as_deref(),
        &args.custom_headers,
        model_selection,
    )
    .await?;

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
        let response = send_chat_streaming(
            ctx,
            &config,
            &messages,
            false,
            args.output != OutputFormat::Json,
        )
        .await?;
        if args.output == OutputFormat::Json {
            return write_json(&ChatJsonResponse::from_response(&model, &response));
        }
        // Only show stats if running in a terminal
        if is_terminal && !args.direct_prompt {
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
        match send_chat_streaming(ctx, config, &messages, true, true).await {
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
    emit_tokens: bool,
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

    // The streamed answer's duration is the model's, not the network's, so this must not
    // go out under the control-plane client's whole-request deadline.
    let mut request = ctx
        .inference_http_client()
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
    let mut stream = response.bytes_stream();
    let mut state = StreamState {
        response: String::new(),
        usage: None,
        first_token: None,
        spinner,
    };

    // An event can straddle two reads, so the bytes are reassembled into events before
    // anything is read out of them; a reader that took each read as whole lines would drop
    // whichever events happened to be split, without saying so.
    let mut decoder = SseDecoder::new();

    // The client's read deadline counts bytes, and the runtime keeps this stream alive with an
    // SSE comment every 30 seconds, so it can never fire here however long the model has been
    // stuck. What has to be bounded is the gap between *events*: a keep-alive says the
    // connection is up, not that anything is being produced.
    let progress_deadline = ctx.inference_deadline().duration();
    let mut last_event = Instant::now();

    'stream: loop {
        let remaining = progress_deadline.saturating_sub(last_event.elapsed());
        let Ok(next) = tokio::time::timeout(remaining, stream.next()).await else {
            state.stop_spinner().await;
            return Err(InvalidResponseSnafu {
                message: format!(
                    "The model at {url} sent nothing for {}s. The connection is still open, so the request is being held rather than refused -- check the runtime's logs for the model, then retry. See: https://spiceai.org/docs/components/models",
                    progress_deadline.as_secs()
                ),
            }
            .build());
        };
        let Some(chunk_result) = next else {
            // The stream ended. A server that closed without its final blank line still has
            // one event's worth of bytes buffered here, and dropping it would lose the tail
            // of the answer.
            decoder.close();
            drain_events(&mut decoder, &url, emit_tokens, start_time, &mut state).await?;
            break 'stream;
        };

        let chunk = chunk_result.map_err(|e| {
            InvalidResponseSnafu {
                message: format!("Failed to read stream: {e}"),
            }
            .build()
        })?;

        let data_before = decoder.data_fields_seen();
        decoder.push(&chunk);

        if drain_events(&mut decoder, &url, emit_tokens, start_time, &mut state).await?
            == EventOutcome::Stop
        {
            break 'stream;
        }

        // Progress is a `data` line arriving, not an event completing: a large event spread
        // over several reads is the model producing, even before its terminator lands. A
        // keep-alive comment carries no data field, so it still cannot hold the stream open.
        if decoder.data_fields_seen() != data_before {
            last_event = Instant::now();
        }

        // Asked after draining, so the cap bounds one event the stream never ends rather
        // than however many whole events happened to arrive in a single read.
        if let Some(buffered) = decoder.oversized_bytes() {
            state.stop_spinner().await;
            return Err(InvalidResponseSnafu {
                message: format!(
                    "The model at {url} sent {buffered} bytes of a single response event without ending it. Check the runtime's logs for the model, then retry. See: https://spiceai.org/docs/components/models"
                ),
            }
            .build());
        }
    }

    state.stop_spinner().await;

    Ok(ChatResponse {
        content: state.response,
        total_duration: start_time.elapsed(),
        first_token_duration: state.first_token,
        usage: state.usage,
    })
}

/// Read every event the decoder has ready, stopping early if one ends the stream.
async fn drain_events(
    decoder: &mut SseDecoder,
    url: &str,
    emit_tokens: bool,
    start_time: Instant,
    state: &mut StreamState,
) -> Result<EventOutcome> {
    while let Some(event) = decoder.next_event() {
        if apply_event(&event, url, emit_tokens, start_time, state).await? == EventOutcome::Stop {
            return Ok(EventOutcome::Stop);
        }
    }

    Ok(EventOutcome::Continue)
}

/// What the stream should do once an event has been read.
#[derive(PartialEq, Eq)]
enum EventOutcome {
    /// Keep reading.
    Continue,
    /// The stream is over: the server said so.
    Stop,
}

/// The answer as it accumulates across the stream's events.
struct StreamState {
    response: String,
    usage: Option<Usage>,
    first_token: Option<std::time::Duration>,
    spinner: Option<Spinner>,
}

impl StreamState {
    async fn stop_spinner(&mut self) {
        if let Some(spinner) = self.spinner.take() {
            spinner.stop().await;
        }
    }
}

/// Read one SSE event into the answer.
///
/// Every event the server sends is either content, the protocol's terminator, or a report
/// that the stream has failed. An event that is none of those cannot be read as content, and
/// discarding it would leave the user a truncated answer presented as a whole one — so it is
/// an error, not a skip.
async fn apply_event(
    event: &SseEvent,
    url: &str,
    emit_tokens: bool,
    start_time: Instant,
    state: &mut StreamState,
) -> Result<EventOutcome> {
    // The runtime reports a mid-stream failure as an `error` event. Read as content it
    // parses as nothing, which is how it used to disappear.
    if event.name.as_deref() == Some("error") {
        state.stop_spinner().await;
        let detail = StreamErrorPayload::reason_in(&event.data)
            .unwrap_or_else(|| "the server did not say why".to_string());

        return Err(InvalidResponseSnafu {
            message: format!(
                "The model at {url} failed part-way through its answer: {detail}. Any answer printed above is incomplete. Check the runtime's logs for the model, then retry. See: https://spiceai.org/docs/components/models"
            ),
        }
        .build());
    }

    // An event with no payload — a bare `event:` line, or a keep-alive a proxy reframed —
    // carries no content and no failure. It is progress, not something to read.
    if event.data.is_empty() {
        return Ok(EventOutcome::Continue);
    }

    if event.data == "[DONE]" {
        // The protocol's terminator. Reading on would wait for an EOF the server is under no
        // obligation to send promptly.
        return Ok(EventOutcome::Stop);
    }

    let Ok(chunk) = serde_json::from_str::<ChatChunk>(&event.data) else {
        state.stop_spinner().await;

        // An OpenAI-compatible server reports a failure in an unnamed event instead, so the
        // payload gets one more reading before this is called unintelligible.
        let detail = StreamErrorPayload::reason_in(&event.data).map_or_else(
            || "the response could not be read".to_string(),
            |message| format!("the model reported: {message}"),
        );

        return Err(InvalidResponseSnafu {
            message: format!(
                "The model at {url} sent a response event that is not a chat completion -- {detail}. Any answer printed above is incomplete. Check the runtime's logs for the model, then retry. See: https://spiceai.org/docs/components/models"
            ),
        }
        .build());
    };

    // Capture usage from the final chunk (if present)
    if chunk.usage.is_some() {
        state.usage = chunk.usage;
    }

    for choice in &chunk.choices {
        if let Some(content) = choice
            .delta
            .as_ref()
            .and_then(|delta| delta.content.as_ref())
        {
            // Record first token time and stop spinner
            if state.first_token.is_none() {
                state.first_token = Some(start_time.elapsed());
                state.stop_spinner().await;
            }
            if emit_tokens {
                print!("{content}");
                let _ = io::stdout().flush();
            }
            state.response.push_str(content);
        }
    }

    Ok(EventOutcome::Continue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Deadline;
    use crate::test_support::SlowServer;
    use std::time::Duration;

    /// The one-shot prompt every streaming test sends. The content is irrelevant to all of
    /// them — what is under test is how the response is read.
    fn one_shot_prompt() -> Vec<Message> {
        vec![Message {
            role: "user".to_string(),
            content: "hello".to_string(),
        }]
    }

    /// A config that leaves the endpoint to the context, so a test's `SlowServer` is what the
    /// request reaches.
    const fn test_chat_config() -> ChatConfig<'static> {
        ChatConfig {
            model: "test-model",
            temperature: None,
            endpoint: None,
            custom_headers: &[],
        }
    }

    /// One SSE event carrying `text` as a content delta — the shape of every chunk a chat
    /// completion stream sends.
    fn content_event(text: &str) -> String {
        format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\n")
    }

    /// One SSE event carrying `payload` verbatim, for the events that are not content.
    fn raw_event(payload: &str) -> String {
        format!("data: {payload}\n\n")
    }

    /// A context whose deadlines are generous enough that only the response itself ends the
    /// read — most of these tests are about what is read, not about when reading stops.
    fn unhurried_context(url: &str) -> RuntimeContext {
        RuntimeContext::with_deadlines_for_test(
            url,
            Deadline::Total(Duration::from_secs(30)),
            Deadline::Silence(Duration::from_secs(30)),
        )
    }

    async fn stream_from(server: &SlowServer) -> Result<ChatResponse> {
        let ctx = unhurried_context(server.url());
        send_chat_streaming(&ctx, &test_chat_config(), &one_shot_prompt(), false, false).await
    }

    /// A streamed answer that keeps arriving must not be cut off for taking longer than the
    /// control-plane deadline — the failure reported in
    /// <https://github.com/spiceai/spiceai/issues/12583>.
    ///
    /// Both deadlines are shrunk from their production values so the difference between them
    /// is observable in under a second; the production gap is 30 seconds.
    #[tokio::test]
    async fn a_streamed_answer_outlives_the_control_plane_deadline() {
        let control_plane = Duration::from_millis(400);
        let server = SlowServer::dribbling(
            std::iter::repeat_n(content_event("token "), 8).collect(),
            control_plane / 4,
        );

        let ctx = RuntimeContext::with_deadlines_for_test(
            server.url(),
            Deadline::Total(control_plane),
            Deadline::Silence(control_plane),
        );

        // Positive control: the same response, over the control-plane client, is the bug.
        // Without it a green test could mean the server simply answered quickly.
        let cut_off = ctx
            .http_client()
            .get(server.url())
            .send()
            .await
            .expect("the response head arrives promptly")
            .text()
            .await
            .expect_err("the control-plane deadline should cut this response off");
        assert!(
            cut_off.is_timeout(),
            "expected the control-plane client to time out, got: {cut_off}"
        );

        let config = test_chat_config();
        let messages = one_shot_prompt();

        let response = send_chat_streaming(&ctx, &config, &messages, false, false)
            .await
            .expect("a streamed answer that keeps arriving should be read to the end");

        assert_eq!(response.content, "token ".repeat(8));
        assert!(
            response.total_duration > control_plane,
            "the answer should have outlasted the control-plane deadline, took {:?}",
            response.total_duration
        );
        // The server answers every path alike, so without this the test would pass against any
        // route and would pin only the client, not the endpoint.
        assert!(
            server
                .targets()
                .iter()
                .any(|target| target == "/v1/chat/completions"),
            "expected a request to /v1/chat/completions, saw {:?}",
            server.targets()
        );
    }

    /// `data: [DONE]` is the protocol's terminator, so the answer is complete when it arrives.
    /// Reading on for an EOF the server is under no obligation to send promptly leaves the CLI
    /// looking hung after it already has the whole answer.
    #[tokio::test]
    async fn a_completed_answer_does_not_wait_for_the_server_to_hang_up() {
        let mut chunks: Vec<String> = std::iter::repeat_n(content_event("token "), 3).collect();
        chunks.push(raw_event("[DONE]"));

        // The connection stays open after `[DONE]`, so only the terminator can end the read.
        let server = SlowServer::dribbling_then_holding(chunks, Duration::from_millis(20));

        let ctx = unhurried_context(server.url());
        let config = test_chat_config();
        let messages = one_shot_prompt();

        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            send_chat_streaming(&ctx, &config, &messages, false, false),
        )
        .await;

        let Ok(result) = outcome else {
            panic!("the CLI kept reading after [DONE] instead of returning the finished answer");
        };
        let response = result.expect("the answer arrived in full before the terminator");
        assert_eq!(response.content, "token ".repeat(3));
    }

    /// The runtime keeps an SSE stream alive with a comment every 30 seconds
    /// (`KEEP_ALIVE_INTERVAL` in `crates/runtime/src/http/v1/chat.rs`), and each one resets the
    /// client's read deadline. So a model that stops producing but never closes would hold the
    /// CLI open forever if the only bound counted bytes; the bound has to count events.
    #[tokio::test]
    async fn a_stream_that_only_sends_keep_alives_is_not_waited_on_forever() {
        // Long enough that a descheduled runner cannot mistake it for a stall, short enough
        // that many of them arrive inside the progress deadline below.
        let keep_alive_gap = Duration::from_millis(50);
        let server = SlowServer::dribbling(
            std::iter::repeat_n(": keep-alive\n\n".to_string(), 200).collect(),
            keep_alive_gap,
        );

        let ctx = RuntimeContext::with_deadlines_for_test(
            server.url(),
            Deadline::Total(Duration::from_secs(30)),
            // The progress bound as well as the client's. A keep-alive resets the client's, so
            // if that were the only bound this would run until the server ran out of them.
            Deadline::Silence(Duration::from_millis(300)),
        );

        let config = test_chat_config();
        let messages = one_shot_prompt();

        let progress_deadline = Duration::from_millis(600);
        let started = Instant::now();
        let outcome = tokio::time::timeout(
            progress_deadline,
            send_chat_streaming(&ctx, &config, &messages, false, false),
        )
        .await;

        // The CLI must end this itself. Reaching the outer timeout means it did not.
        let Ok(result) = outcome else {
            panic!(
                "the CLI waited out {progress_deadline:?} of keep-alives without giving up; the progress bound is counting bytes, not events"
            );
        };
        let Err(error) = result else {
            panic!("a stream carrying no events should not report an answer");
        };
        assert!(
            error.to_string().contains("sent nothing"),
            "expected the no-progress error, got: {error}"
        );
        assert!(
            started.elapsed() < progress_deadline,
            "took {:?}",
            started.elapsed()
        );
    }

    /// An SSE event is not obliged to arrive in one read. Splitting one mid-payload used to
    /// lose it entirely — the leading fragment is truncated JSON and the trailing one has no
    /// `data:` prefix, so both were discarded without a word. That is
    /// <https://github.com/spiceai/spiceai/issues/12588>.
    #[tokio::test]
    async fn an_event_split_across_two_reads_is_not_dropped() {
        let event = content_event("whole");
        let split = event.len() / 2;
        let server = SlowServer::dribbling(
            vec![event[..split].to_string(), event[split..].to_string()],
            Duration::from_millis(10),
        );

        let response = stream_from(&server)
            .await
            .expect("an event split across two reads is still one event");

        assert_eq!(response.content, "whole");
    }

    /// The tokens either side of a split must survive too, and in order — a decoder that
    /// dropped only the split event would still pass the test above if it were the sole event.
    #[tokio::test]
    async fn tokens_either_side_of_a_split_event_keep_their_order() {
        let split_event = content_event("middle ");
        let split = split_event.len() / 2;
        let server = SlowServer::dribbling(
            vec![
                content_event("first "),
                split_event[..split].to_string(),
                split_event[split..].to_string(),
                content_event("last"),
            ],
            Duration::from_millis(5),
        );

        let response = stream_from(&server)
            .await
            .expect("every event should be read");

        assert_eq!(response.content, "first middle last");
    }

    /// A multi-byte character split across two reads must be reassembled. Decoding each read
    /// on its own replaces both halves with U+FFFD, corrupting the answer silently.
    #[tokio::test]
    async fn a_character_split_across_two_reads_is_not_corrupted() {
        let event = content_event("café");
        // Between the two bytes of `é`, which is the last character before the closing quote.
        let split = event
            .find('é')
            .expect("the payload contains the character being split")
            + 1;
        // The halves are carried as bytes: putting them through `String` would replace the
        // split character here, in the fixture, and the test would pass on any decoder.
        let server = SlowServer::dribbling_bytes(
            vec![
                event.as_bytes()[..split].to_vec(),
                event.as_bytes()[split..].to_vec(),
            ],
            Duration::from_millis(10),
        );

        let response = stream_from(&server)
            .await
            .expect("a character split across two reads is still one character");

        assert_eq!(response.content, "café");
    }

    /// The runtime reports a mid-stream failure as an `error` event
    /// (`to_openai_error_event` in `crates/runtime/src/http/v1/chat.rs`). Read as a chat
    /// completion it parses as nothing, so it used to be discarded — leaving the user a
    /// truncated answer, printed as though it were whole, with no error and no exit code.
    #[tokio::test]
    async fn an_error_event_ends_the_stream_rather_than_truncating_the_answer() {
        let server = SlowServer::dribbling(
            vec![
                content_event("partial"),
                "event: error\ndata: {\"type\":\"error\",\"message\":\"the model ran out of context\"}\n\n"
                    .to_string(),
            ],
            Duration::from_millis(5),
        );

        let error = stream_from(&server)
            .await
            .expect_err("a stream that failed part-way through has no complete answer");

        let message = error.to_string();
        assert!(
            message.contains("the model ran out of context"),
            "the server's reason should reach the user, got: {message}"
        );
        assert!(
            message.contains("incomplete"),
            "the user should be told the printed answer is partial, got: {message}"
        );
        // The wording the named branch produces, and the generic unreadable-event branch
        // does not. Without this the test passes with that branch removed: the payload
        // falls through to the generic path, which happens to quote the same reason.
        assert!(
            message.contains("failed part-way through its answer"),
            "an error event should be reported as a failed answer, not as an unreadable event: {message}"
        );
    }

    /// The same failure from an OpenAI-compatible server, which nests it under `error` in an
    /// unnamed event instead of naming the event.
    #[tokio::test]
    async fn an_unnamed_error_payload_is_reported_with_its_reason() {
        let server = SlowServer::dribbling(
            vec![raw_event(r#"{"error":{"message":"upstream rate limit"}}"#)],
            Duration::from_millis(5),
        );

        let error = stream_from(&server)
            .await
            .expect_err("an event that is not a chat completion is not an answer");

        assert!(
            error.to_string().contains("upstream rate limit"),
            "the server's reason should reach the user, got: {error}"
        );
    }

    /// An event whose data field is empty — the heartbeat some servers send in place of a
    /// comment — is neither content nor a failure, and must not be mistaken for either.
    /// (A frame carrying no data field at all is not dispatched at all; `sse` covers that.)
    #[tokio::test]
    async fn an_event_with_no_payload_does_not_end_the_stream() {
        let server = SlowServer::dribbling(
            vec!["data: \n\n".to_string(), content_event("answer")],
            Duration::from_millis(5),
        );

        let response = stream_from(&server)
            .await
            .expect("an empty event is not a failure");

        assert_eq!(response.content, "answer");
    }

    /// Azure `OpenAI`'s asynchronous content filtering emits annotation choices carrying only
    /// `content_filter_results` and no `delta`, on passing streams as well as filtered ones.
    /// Treating every payload that is not a plain content chunk as unreadable would stop the
    /// stream on a valid event and lose every token after it.
    #[tokio::test]
    async fn an_annotation_choice_without_a_delta_does_not_stop_the_stream() {
        let server = SlowServer::dribbling(
            vec![
                content_event("before "),
                raw_event(
                    r#"{"choices":[{"index":0,"finish_reason":null,"content_filter_results":{},"content_filter_offsets":{"check_offset":44,"start_offset":44,"end_offset":198}}],"usage":null}"#,
                ),
                content_event("after"),
            ],
            Duration::from_millis(5),
        );

        let response = stream_from(&server)
            .await
            .expect("an annotation choice is a valid event with nothing to print");

        assert_eq!(response.content, "before after");
    }

    /// A single event spread over several reads is the model producing, even before its
    /// terminator arrives. Recording progress only at dispatch times such a stream out.
    #[tokio::test]
    async fn a_data_line_keeps_the_stream_alive_before_its_event_ends() {
        let progress_bound = Duration::from_millis(500);
        // One multiline event, in three reads. Its data lands well inside the bound each
        // time, but the terminator does not arrive until after the bound has elapsed.
        let server = SlowServer::dribbling(
            vec![
                "data: {\"choices\":[{\"delta\":\n".to_string(),
                "data:  {\"content\":\"slow\"}}]}\n".to_string(),
                "\n".to_string(),
            ],
            Duration::from_millis(200),
        );

        let ctx = RuntimeContext::with_deadlines_for_test(
            server.url(),
            Deadline::Total(Duration::from_secs(30)),
            Deadline::Silence(progress_bound),
        );

        let response =
            send_chat_streaming(&ctx, &test_chat_config(), &one_shot_prompt(), false, false)
                .await
                .expect("an event still arriving is a stream that is producing");

        assert_eq!(response.content, "slow");
    }

    /// A server that hangs up without the blank line terminating its last event still sent
    /// that event, and its tokens belong in the answer.
    #[tokio::test]
    async fn a_final_event_without_its_terminator_is_still_read() {
        let server = SlowServer::dribbling(
            vec![content_event("tail").trim_end().to_string()],
            Duration::from_millis(5),
        );

        let response = stream_from(&server)
            .await
            .expect("the last event arrived, only its terminator did not");

        assert_eq!(response.content, "tail");
    }

    #[test]
    fn select_single_available_model_uses_only_model() {
        let models = vec!["llm".to_string()];

        let model = select_single_available_model(&models)
            .expect("single configured model should be selected");

        assert_eq!(model, "llm");
    }

    #[test]
    fn select_single_available_model_requires_model_when_ambiguous() {
        let models = vec!["llm-a".to_string(), "llm-b".to_string()];

        let error = select_single_available_model(&models)
            .expect_err("multiple configured models should require --model");

        assert_eq!(
            error.to_string(),
            "Invalid argument: Multiple models are configured: llm-a, llm-b. Specify one with --model."
        );
    }

    #[test]
    fn select_single_available_model_reports_no_models() {
        let models = Vec::new();

        let error = select_single_available_model(&models)
            .expect_err("empty model list should report no configured models");

        assert_eq!(
            error.to_string(),
            "No models found. Please configure a model in your Spicepod."
        );
    }
}
