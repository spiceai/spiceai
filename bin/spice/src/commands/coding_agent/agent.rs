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

//! What each coding agent needs to use a Spice runtime as its model backend.
//!
//! Every agent reaches its model through a different mechanism — environment
//! variables, `-c` config overrides, an inline JSON config, an extension file —
//! but all of them reduce to the same [`Launch`]: a program, arguments placed
//! before the user's own, environment variables to set or clear, and files to
//! write first. Building a [`Launch`] is pure, so what an agent is told is
//! asserted exactly in tests rather than observed through a child process.
//!
//! The mechanisms chosen here change nothing in the user's own configuration:
//! no agent's config file is edited, and anything an agent has to read from
//! disk is written to a directory that is removed when the agent exits.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use serde_json::json;

/// The token sent to a runtime that has no API key configured.
///
/// Every agent here refuses to start, or prompts for a login, without some
/// credential, and a runtime without `runtime.auth` ignores whatever it is
/// sent. A fixed, recognizable value keeps a user's real vendor key from being
/// the one that reaches Spice.
pub(crate) const PLACEHOLDER_TOKEN: &str = "spice";

/// The provider id the agents that take a provider table register Spice under.
const PROVIDER_ID: &str = "spice";

/// The display name of that provider.
const PROVIDER_NAME: &str = "Spice";

/// A coding agent `spice` can launch against a Spice runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agent {
    Antigravity,
    ClaudeCode,
    Codex,
    CopilotCli,
    Cursor,
    Grok,
    OpenCode,
    Pi,
}

/// How to find an agent on this machine and how to tell the user to install it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Install {
    /// Executable names, in the order they are searched for on `PATH`.
    pub binaries: &'static [&'static str],
    /// Install locations, relative to the home directory, that are searched
    /// when `PATH` has none — installers that write to a directory a fresh
    /// shell has not yet put on `PATH`.
    pub home_fallbacks: &'static [&'static str],
    /// The command that installs the agent.
    pub command: &'static str,
    /// The agent's install documentation.
    pub docs: &'static str,
}

impl Agent {
    /// Every agent, in the order the CLI lists them.
    pub const ALL: [Self; 8] = [
        Self::Antigravity,
        Self::ClaudeCode,
        Self::Codex,
        Self::CopilotCli,
        Self::Cursor,
        Self::Grok,
        Self::OpenCode,
        Self::Pi,
    ];

    /// The product name, as the agent's own documentation writes it.
    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Antigravity => "Antigravity CLI",
            Self::ClaudeCode => "Claude Code",
            Self::Codex => "Codex CLI",
            Self::CopilotCli => "GitHub Copilot CLI",
            Self::Cursor => "Cursor CLI",
            Self::Grok => "Grok Build",
            Self::OpenCode => "OpenCode",
            Self::Pi => "pi",
        }
    }

    /// The `spice` subcommand that launches the agent.
    #[must_use]
    pub const fn command(self) -> &'static str {
        match self {
            Self::Antigravity => "agy",
            Self::ClaudeCode => "claude",
            Self::Codex => "codex",
            Self::CopilotCli => "copilot",
            Self::Cursor => "cursor",
            Self::Grok => "grok",
            Self::OpenCode => "opencode",
            Self::Pi => "pi",
        }
    }

    pub(crate) const fn install(self) -> Install {
        match self {
            Self::Antigravity => Install {
                binaries: &["agy"],
                home_fallbacks: &[".local/bin/agy"],
                command: "curl -fsSL https://antigravity.google/cli/install.sh | bash",
                docs: "https://antigravity.google/docs/install/",
            },
            Self::ClaudeCode => Install {
                binaries: &["claude"],
                home_fallbacks: &[".local/bin/claude", ".claude/local/claude"],
                command: "curl -fsSL https://claude.ai/install.sh | bash",
                docs: "https://code.claude.com/docs/en/setup",
            },
            Self::Codex => Install {
                binaries: &["codex"],
                home_fallbacks: &[".local/bin/codex"],
                command: "npm install -g @openai/codex",
                docs: "https://developers.openai.com/codex/cli",
            },
            Self::CopilotCli => Install {
                binaries: &["copilot"],
                home_fallbacks: &[".local/bin/copilot"],
                command: "npm install -g @github/copilot",
                docs: "https://docs.github.com/en/copilot/how-tos/copilot-cli",
            },
            // Only Cursor's local-agent build accepts a model endpoint; the
            // public `cursor-agent` talks to Cursor's own backend and nothing
            // else. See [`Agent::unsupported_build`].
            Self::Cursor => Install {
                binaries: &["cursor-agent-local", "agent-local"],
                home_fallbacks: &[".local/bin/cursor-agent-local", ".local/bin/agent-local"],
                command: "curl https://cursor.com/install -fsS | bash",
                docs: "https://cursor.com/docs/cli/overview",
            },
            Self::Grok => Install {
                binaries: &["grok"],
                home_fallbacks: &[".grok/bin/grok"],
                command: "curl -fsSL https://x.ai/cli/install.sh | bash",
                docs: "https://docs.x.ai/build/overview",
            },
            Self::OpenCode => Install {
                binaries: &["opencode"],
                home_fallbacks: &[".opencode/bin/opencode"],
                command: "curl -fsSL https://opencode.ai/install | bash",
                docs: "https://opencode.ai/docs/",
            },
            Self::Pi => Install {
                binaries: &["pi"],
                home_fallbacks: &[".local/bin/pi"],
                command: "curl -fsSL https://pi.dev/install.sh | sh",
                docs: "https://pi.dev",
            },
        }
    }

    /// A build of the agent that is installed but cannot use a custom model
    /// endpoint, named so the user is told why instead of seeing it ignore Spice.
    ///
    /// Returns the binary names that identify such a build and the explanation.
    pub(crate) const fn unsupported_build(self) -> Option<(&'static [&'static str], &'static str)> {
        match self {
            Self::Cursor => Some((
                &["cursor-agent"],
                "The public Cursor CLI (`cursor-agent`) sends every request to Cursor's own servers and cannot use another model endpoint; only Cursor's local-agent build (`cursor-agent-local`) can. Install the local-agent build, or use another agent such as `spice claude` or `spice codex`.",
            )),
            _ => None,
        }
    }

    /// How the agent's own model option names a Spice model, for help text.
    pub(crate) const fn model_example(self) -> &'static str {
        match self {
            Self::OpenCode => "-m spice/my-model",
            Self::Pi => "--model spice/my-model",
            _ => "--model my-model",
        }
    }

    /// The agent's own command-line options that name its model. A user picks
    /// the Spice model with these, exactly as they would without Spice.
    const fn model_options(self) -> &'static [&'static str] {
        match self {
            Self::Antigravity | Self::ClaudeCode | Self::CopilotCli | Self::Pi => &["--model"],
            Self::Codex | Self::Cursor | Self::Grok | Self::OpenCode => &["--model", "-m"],
        }
    }

    /// The Spice model a user's `--model` value names, or `None` when it names
    /// something the agent resolves itself.
    fn spice_model_of(self, value: &str) -> Option<String> {
        match self {
            // `provider/model` (pi adds an optional `:<thinking>` level).
            Self::OpenCode | Self::Pi => {
                let model = value.strip_prefix(&format!("{PROVIDER_ID}/"))?;
                let model = match (self, model.rsplit_once(':')) {
                    (Self::Pi, Some((model, level))) if PI_THINKING_LEVELS.contains(&level) => {
                        model
                    }
                    _ => model,
                };
                Some(model.to_string())
            }
            // Claude Code's aliases resolve to the Spice model through the
            // `ANTHROPIC_DEFAULT_*_MODEL` variables, so they name no model of
            // their own.
            Self::ClaudeCode if is_claude_code_alias(value) => None,
            _ => Some(value.to_string()),
        }
    }
}

/// pi's thinking levels, which may follow a model as `provider/id:<level>`.
const PI_THINKING_LEVELS: &[&str] = &["off", "minimal", "low", "medium", "high", "xhigh"];

/// Claude Code's model aliases, with or without the `[1m]` context suffix.
fn is_claude_code_alias(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    let alias = value.strip_suffix("[1m]").unwrap_or(&value);
    matches!(
        alias,
        "default" | "best" | "fable" | "opus" | "sonnet" | "haiku" | "opusplan"
    )
}

/// What the user's arguments say about the agent's model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestedModel {
    /// The agent's model option is absent: `spice` chooses the model.
    Unspecified,
    /// The option names something the agent resolves itself — an alias, or
    /// another provider's model — so there is no Spice model to check, but no
    /// model option may be added either.
    AgentOwned,
    /// The option names this Spice model.
    Spice(String),
}

impl RequestedModel {
    /// The Spice model to wait on, if one was named.
    #[must_use]
    pub fn spice_model(&self) -> Option<&str> {
        match self {
            Self::Spice(model) => Some(model),
            Self::Unspecified | Self::AgentOwned => None,
        }
    }

    /// Whether the user named the model, so no model option is added.
    #[must_use]
    pub fn given(&self) -> bool {
        !matches!(self, Self::Unspecified)
    }
}

/// The model the user named with the agent's own option, if they did.
///
/// Scanning stops at `--`, after which the agent reads its arguments as
/// positional.
pub(crate) fn requested_model(agent: Agent, args: &[OsString]) -> RequestedModel {
    let options = agent.model_options();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let Some(arg) = arg.to_str() else { continue };
        if arg == "--" {
            break;
        }
        for option in options {
            let value = if arg == *option {
                args.next()
                    .and_then(|value| value.to_str())
                    .map(str::to_string)
            } else if let Some(value) = arg.strip_prefix(&format!("{option}=")) {
                Some(value.to_string())
            } else if option.len() == 2 && arg.len() > 2 && arg.starts_with(option) {
                // A short option with its value attached: `-mgpt`.
                Some(arg[2..].to_string())
            } else {
                continue;
            };
            return match value.and_then(|value| agent.spice_model_of(&value)) {
                Some(model) => RequestedModel::Spice(model),
                None => RequestedModel::AgentOwned,
            };
        }
    }
    RequestedModel::Unspecified
}

/// What a Spice runtime offers an agent: where it is, how to authenticate, and
/// which models it serves.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Backend<'a> {
    /// The runtime's HTTP endpoint, such as `http://127.0.0.1:8090`, without a
    /// trailing slash.
    pub endpoint: &'a str,
    /// The runtime's API key, when it has one.
    pub api_key: Option<&'a str>,
    /// The model the agent starts on.
    pub model: &'a str,
    /// Every model the runtime serves, including [`Self::model`], so an agent
    /// with a model picker can switch between them.
    pub models: &'a [String],
    /// Whether every one of [`Self::models`] answers the Messages API natively.
    pub native_messages: bool,
    /// Whether the user named the model with the agent's own option, in which
    /// case no model option is added: the agent would refuse it twice.
    pub model_given: bool,
}

impl Backend<'_> {
    fn token(&self) -> &str {
        self.api_key.unwrap_or(PLACEHOLDER_TOKEN)
    }

    /// The OpenAI-compatible base URL, under which `/chat/completions`,
    /// `/responses` and `/models` are resolved.
    fn openai_base_url(&self) -> String {
        format!("{}/v1", self.endpoint)
    }
}

/// A file an agent reads, written before it starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LaunchFile {
    pub path: PathBuf,
    pub contents: String,
    /// Whether the file must be executable.
    pub executable: bool,
}

/// Everything needed to start an agent against a [`Backend`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Launch {
    /// Arguments placed before the user's own.
    pub args: Vec<OsString>,
    /// Environment variables to set, overriding the inherited value.
    pub env: Vec<(&'static str, OsString)>,
    /// Inherited environment variables to clear, because they would route the
    /// agent somewhere other than Spice or hand Spice a vendor credential.
    pub env_remove: Vec<&'static str>,
    /// Files to write before the agent starts.
    pub files: Vec<LaunchFile>,
    /// Arguments of a run of the agent that has to complete before the real one
    /// starts, under the same environment.
    pub prepare: Option<Vec<OsString>>,
}

/// Environment variables that would send Claude Code to another provider, or a
/// vendor credential to Spice, if inherited.
const CLAUDE_CODE_ENV_REMOVE: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_SMALL_FAST_MODEL",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
    "CLAUDE_CODE_USE_MANTLE",
    "CLAUDE_CODE_USE_ANTHROPIC_AWS",
    "CLAUDE_CODE_USE_ANTHROPIC_GOOGLE_CLOUD",
    "CLAUDE_CODE_USE_GATEWAY",
];

/// Copilot provider settings a user may have exported for another provider,
/// which would otherwise be applied to Spice.
const COPILOT_ENV_REMOVE: &[&str] = &[
    "COPILOT_PROVIDER_BEARER_TOKEN",
    "COPILOT_PROVIDER_API_KEY_COMMAND",
    "COPILOT_PROVIDER_MODEL_ID",
    "COPILOT_PROVIDER_WIRE_MODEL",
    "COPILOT_PROVIDER_TRANSPORT",
    "COPILOT_PROVIDER_AZURE_API_VERSION",
];

/// Build the [`Launch`] that points `agent` at `backend`.
///
/// `scratch` is a private directory that outlives the agent's run; files the
/// agent reads are placed in it.
#[must_use]
pub(crate) fn launch(agent: Agent, backend: &Backend<'_>, scratch: &Path) -> Launch {
    match agent {
        Agent::ClaudeCode => claude_code(backend),
        Agent::Codex => codex(backend),
        Agent::CopilotCli => copilot(backend),
        Agent::Cursor => cursor(backend),
        Agent::Grok => grok(backend, scratch),
        Agent::OpenCode => opencode(backend),
        Agent::Pi => pi(backend, scratch),
        Agent::Antigravity => antigravity(backend),
    }
}

fn os(value: impl Into<OsString>) -> OsString {
    value.into()
}

/// Claude Code speaks the Anthropic Messages API, which the runtime serves on
/// `/v1/messages`. `ANTHROPIC_BASE_URL` takes the root; Claude Code appends
/// `/v1/...` itself.
fn claude_code(backend: &Backend<'_>) -> Launch {
    let model = backend.model;
    let mut env = vec![
        ("ANTHROPIC_BASE_URL", os(backend.endpoint)),
        // Sent as `Authorization: Bearer`, which the runtime accepts, and
        // takes precedence over a claude.ai login.
        ("ANTHROPIC_AUTH_TOKEN", os(backend.token())),
        ("ANTHROPIC_MODEL", os(model)),
        // Every alias Claude Code resolves on its own — the `/model`
        // tiers, background work, subagents — resolves to a model Spice
        // serves, never to a Claude model id Spice does not.
        ("ANTHROPIC_DEFAULT_FABLE_MODEL", os(model)),
        ("ANTHROPIC_DEFAULT_OPUS_MODEL", os(model)),
        ("ANTHROPIC_DEFAULT_SONNET_MODEL", os(model)),
        ("ANTHROPIC_DEFAULT_HAIKU_MODEL", os(model)),
        ("CLAUDE_CODE_SUBAGENT_MODEL", os(model)),
        // Lists the runtime's `/v1/models` in the `/model` picker.
        ("CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY", os("1")),
        // Claude Code otherwise opens every system prompt with a billing
        // line for Anthropic's API, which no other backend reads and which
        // differs between versions, so it would only cost a cache miss.
        ("CLAUDE_CODE_ATTRIBUTION_HEADER", os("0")),
    ];
    if !backend.native_messages {
        // Tool search defers tools with `defer_loading`, which a Messages
        // request translated into a chat completion cannot carry: the runtime
        // rejects the request, and Claude Code retries before it falls back to
        // loading every tool. Starting with every tool loaded skips the failures.
        env.push(("ENABLE_TOOL_SEARCH", os("false")));
    }
    Launch {
        args: Vec::new(),
        env,
        env_remove: CLAUDE_CODE_ENV_REMOVE.to_vec(),
        files: Vec::new(),
        prepare: None,
    }
}

/// Codex speaks only the Responses API (`wire_api = "chat"` was removed), so
/// it is pointed at `/v1/responses` through a provider defined with `-c`
/// overrides, which take precedence over `~/.codex/config.toml` without
/// editing it.
fn codex(backend: &Backend<'_>) -> Launch {
    let provider = format!("model_providers.{PROVIDER_ID}");
    let mut args = vec![
        os("-c"),
        os(format!("model_provider={}", toml_string(PROVIDER_ID))),
        os("-c"),
        os(format!("{provider}.name={}", toml_string(PROVIDER_NAME))),
        os("-c"),
        os(format!(
            "{provider}.base_url={}",
            toml_string(&backend.openai_base_url())
        )),
        os("-c"),
        os(format!("{provider}.wire_api={}", toml_string("responses"))),
    ];
    let mut env = Vec::new();
    if let Some(key) = backend.api_key {
        // `env_key` names the variable Codex reads the bearer token from, so
        // the key never appears in the process arguments.
        args.push(os("-c"));
        args.push(os(format!(
            "{provider}.env_key={}",
            toml_string(SPICE_API_KEY_ENV)
        )));
        env.push((SPICE_API_KEY_ENV, os(key)));
    }
    // The default model as config rather than `--model`, which the user's own
    // `--model` overrides instead of colliding with.
    args.push(os("-c"));
    args.push(os(format!("model={}", toml_string(backend.model))));
    Launch {
        args,
        env,
        env_remove: Vec::new(),
        files: Vec::new(),
        prepare: None,
    }
}

/// The variable the CLI reads the runtime API key from, reused to hand the key
/// to an agent that takes the name of a variable rather than its value.
const SPICE_API_KEY_ENV: &str = "SPICE_API_KEY";

/// GitHub Copilot CLI's bring-your-own-key mode, configured entirely through
/// `COPILOT_PROVIDER_*` variables. Chat completions is the wire API every
/// Spice model serves.
fn copilot(backend: &Backend<'_>) -> Launch {
    Launch {
        args: Vec::new(),
        env: vec![
            ("COPILOT_PROVIDER_BASE_URL", os(backend.openai_base_url())),
            ("COPILOT_PROVIDER_TYPE", os("openai")),
            ("COPILOT_PROVIDER_WIRE_API", os("completions")),
            ("COPILOT_PROVIDER_API_KEY", os(backend.token())),
            ("COPILOT_MODEL", os(backend.model)),
        ],
        env_remove: COPILOT_ENV_REMOVE.to_vec(),
        files: Vec::new(),
        prepare: None,
    }
}

/// Cursor's local-agent build: an OpenAI-compatible base URL, with the
/// authless mode that keeps it from contacting Cursor's servers.
fn cursor(backend: &Backend<'_>) -> Launch {
    Launch {
        args: model_option(backend, "--model"),
        env: vec![
            ("CURSOR_LOCAL_AGENT_BASE_URL", os(backend.openai_base_url())),
            ("CURSOR_LOCAL_AGENT_API_KEY", os(backend.token())),
            ("CURSOR_ENABLE_AUTHLESS", os("1")),
        ],
        env_remove: vec!["CURSOR_API_KEY"],
        files: Vec::new(),
        prepare: None,
    }
}

/// Grok Build fetches its model list from `GROK_MODELS_BASE_URL` and sends
/// chat completions there.
///
/// Grok authenticates those requests with its signed-in session when one
/// exists, which would hand the user's xAI token to Spice. So the session is
/// replaced for this run: `GROK_AUTH_PATH` moves the session file into the
/// scratch directory, and `GROK_AUTH_PROVIDER_COMMAND` mints the Spice token
/// into it during a `grok login` that runs before the real launch. The user's
/// own `~/.grok/auth.json` is never read or written.
fn grok(backend: &Backend<'_>, scratch: &Path) -> Launch {
    let dir = scratch.join("grok");
    let token_command = dir.join("token.sh");
    let auth_path = dir.join("auth.json");
    let model = backend.model;
    Launch {
        args: model_option(backend, "--model"),
        env: vec![
            ("GROK_MODELS_BASE_URL", os(backend.openai_base_url())),
            // Authenticates the model-list request, which never uses the session.
            ("XAI_API_KEY", os(backend.token())),
            ("GROK_AUTH_PATH", os(auth_path)),
            ("GROK_AUTH_PROVIDER_COMMAND", os(&token_command)),
            ("GROK_DEFAULT_MODEL", os(model)),
            // Helper requests — session titles and summaries — otherwise ask
            // for an xAI model the runtime does not serve.
            ("GROK_SESSION_SUMMARY_MODEL", os(model)),
        ],
        env_remove: vec!["GROK_MODELS_LIST_URL", "GROK_CLI_CHAT_PROXY_BASE_URL"],
        files: vec![LaunchFile {
            path: token_command,
            contents: format!("#!/bin/sh\nprintf '%s' {}\n", shell_quote(backend.token())),
            executable: true,
        }],
        prepare: Some(vec![os("login")]),
    }
}

/// `OpenCode` reads an inline JSON config from `OPENCODE_CONFIG_CONTENT`, which
/// is merged over the user's global and project configs.
fn opencode(backend: &Backend<'_>) -> Launch {
    let models: serde_json::Map<String, serde_json::Value> = backend
        .models
        .iter()
        .map(|model| (model.clone(), json!({ "name": model })))
        .collect();
    let default_model = format!("{PROVIDER_ID}/{}", backend.model);
    let config = json!({
        "$schema": "https://opencode.ai/config.json",
        "provider": {
            PROVIDER_ID: {
                "npm": "@ai-sdk/openai-compatible",
                "name": PROVIDER_NAME,
                "options": {
                    "baseURL": backend.openai_base_url(),
                    "apiKey": backend.token(),
                },
                "models": models,
            }
        },
        "model": default_model,
        // Title generation and other small tasks otherwise go to a model
        // from another provider.
        "small_model": default_model,
        // A user's `enabled_providers` would otherwise filter Spice out, and
        // every other provider would remain one keystroke away from the session.
        "enabled_providers": [PROVIDER_ID],
    });
    Launch {
        args: Vec::new(),
        env: vec![("OPENCODE_CONFIG_CONTENT", os(config.to_string()))],
        env_remove: Vec::new(),
        files: Vec::new(),
        prepare: None,
    }
}

/// pi has no base-URL flag or variable, but loads extensions given with `-e`,
/// and an extension can register a provider before pi picks its startup model.
fn pi(backend: &Backend<'_>, scratch: &Path) -> Launch {
    let extension = scratch.join("pi").join("spice.ts");
    let models: Vec<serde_json::Value> = backend
        .models
        .iter()
        .map(|model| {
            json!({
                "id": model,
                "name": model,
                "reasoning": false,
                "input": ["text"],
                "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
                // pi's own defaults for a model it has no catalog entry for.
                "contextWindow": 128_000,
                "maxTokens": 16_384,
            })
        })
        .collect();
    let provider = json!({
        "name": PROVIDER_NAME,
        "baseUrl": backend.openai_base_url(),
        "apiKey": backend.token(),
        "api": "openai-completions",
        "models": models,
    });
    let contents = format!(
        "// Generated by `spice pi`: registers the Spice runtime as a provider for this session.\nexport default function (pi) {{\n  pi.registerProvider({}, {});\n}}\n",
        serde_json::Value::String(PROVIDER_ID.to_string()),
        provider
    );
    let mut args = vec![os("-e"), os(&extension)];
    if !backend.model_given {
        args.push(os("--model"));
        args.push(os(format!("{PROVIDER_ID}/{}", backend.model)));
    }
    Launch {
        args,
        env: Vec::new(),
        env_remove: Vec::new(),
        files: vec![LaunchFile {
            path: extension,
            contents,
            executable: false,
        }],
        prepare: None,
    }
}

/// The Antigravity CLI reaches models only through the Gemini API, pointed at
/// another host with `GOOGLE_GEMINI_BASE_URL`.
fn antigravity(backend: &Backend<'_>) -> Launch {
    let args = if backend.model_given {
        Vec::new()
    } else {
        vec![os(format!("--model={}", backend.model))]
    };
    Launch {
        args,
        env: vec![
            ("GOOGLE_GEMINI_BASE_URL", os(backend.endpoint)),
            ("GEMINI_API_KEY", os(backend.token())),
        ],
        env_remove: vec!["GOOGLE_API_KEY"],
        files: Vec::new(),
        prepare: None,
    }
}

/// `option model`, unless the user named the model themselves.
fn model_option(backend: &Backend<'_>, option: &str) -> Vec<OsString> {
    if backend.model_given {
        Vec::new()
    } else {
        vec![os(option), os(backend.model)]
    }
}

/// A TOML basic string, for a `-c key=value` override.
fn toml_string(value: &str) -> String {
    // A JSON string is a valid TOML basic string: both escape `"`, `\` and
    // control characters the same way.
    serde_json::Value::String(value.to_string()).to_string()
}

/// A single-quoted POSIX shell word.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENDPOINT: &str = "http://127.0.0.1:8090";

    fn models() -> Vec<String> {
        vec!["qwen".to_string(), "gpt".to_string()]
    }

    fn backend<'a>(models: &'a [String], api_key: Option<&'a str>) -> Backend<'a> {
        Backend {
            endpoint: ENDPOINT,
            api_key,
            model: "qwen",
            models,
            native_messages: false,
            model_given: false,
        }
    }

    fn env_of(launch: &Launch) -> Vec<(&'static str, String)> {
        launch
            .env
            .iter()
            .map(|(key, value)| (*key, value.to_string_lossy().into_owned()))
            .collect()
    }

    fn args_of(launch: &Launch) -> Vec<String> {
        launch
            .args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn claude_code_reaches_the_messages_api_with_every_alias_on_the_spice_model() {
        let models = models();
        let launch = launch(
            Agent::ClaudeCode,
            &backend(&models, None),
            Path::new("/scratch"),
        );
        assert_eq!(
            env_of(&launch),
            vec![
                ("ANTHROPIC_BASE_URL", ENDPOINT.to_string()),
                ("ANTHROPIC_AUTH_TOKEN", "spice".to_string()),
                ("ANTHROPIC_MODEL", "qwen".to_string()),
                ("ANTHROPIC_DEFAULT_FABLE_MODEL", "qwen".to_string()),
                ("ANTHROPIC_DEFAULT_OPUS_MODEL", "qwen".to_string()),
                ("ANTHROPIC_DEFAULT_SONNET_MODEL", "qwen".to_string()),
                ("ANTHROPIC_DEFAULT_HAIKU_MODEL", "qwen".to_string()),
                ("CLAUDE_CODE_SUBAGENT_MODEL", "qwen".to_string()),
                (
                    "CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY",
                    "1".to_string()
                ),
                ("CLAUDE_CODE_ATTRIBUTION_HEADER", "0".to_string()),
                ("ENABLE_TOOL_SEARCH", "false".to_string()),
            ]
        );
        // A vendor key or provider switch in the user's shell must not survive.
        assert_eq!(launch.env_remove, CLAUDE_CODE_ENV_REMOVE);
        assert!(launch.args.is_empty() && launch.files.is_empty() && launch.prepare.is_none());
    }

    #[test]
    fn claude_code_keeps_tool_search_when_every_model_is_native_anthropic() {
        let models = models();
        let mut native = backend(&models, None);
        native.native_messages = true;
        let launch = launch(Agent::ClaudeCode, &native, Path::new("/scratch"));
        assert!(
            !env_of(&launch)
                .iter()
                .any(|(key, _)| *key == "ENABLE_TOOL_SEARCH"),
            "{:?}",
            env_of(&launch)
        );
    }

    #[test]
    fn claude_code_sends_the_runtime_key_when_there_is_one() {
        let models = models();
        let launch = launch(
            Agent::ClaudeCode,
            &backend(&models, Some("sk-runtime")),
            Path::new("/scratch"),
        );
        assert_eq!(
            env_of(&launch)[1],
            ("ANTHROPIC_AUTH_TOKEN", "sk-runtime".to_string())
        );
    }

    #[test]
    fn codex_gets_a_responses_provider_through_config_overrides() {
        let models = models();
        let launch = launch(Agent::Codex, &backend(&models, None), Path::new("/scratch"));
        assert_eq!(
            args_of(&launch),
            vec![
                "-c",
                r#"model_provider="spice""#,
                "-c",
                r#"model_providers.spice.name="Spice""#,
                "-c",
                r#"model_providers.spice.base_url="http://127.0.0.1:8090/v1""#,
                "-c",
                r#"model_providers.spice.wire_api="responses""#,
                "-c",
                r#"model="qwen""#,
            ]
        );
        // Without a runtime key Codex sends no credential at all.
        assert!(launch.env.is_empty());
    }

    #[test]
    fn codex_reads_the_runtime_key_from_the_environment_not_the_arguments() {
        let models = models();
        let launch = launch(
            Agent::Codex,
            &backend(&models, Some("sk-runtime")),
            Path::new("/scratch"),
        );
        let args = args_of(&launch);
        assert!(
            args.contains(&r#"model_providers.spice.env_key="SPICE_API_KEY""#.to_string()),
            "{args:?}"
        );
        assert!(
            !args.iter().any(|arg| arg.contains("sk-runtime")),
            "{args:?}"
        );
        assert_eq!(
            env_of(&launch),
            vec![("SPICE_API_KEY", "sk-runtime".to_string())]
        );
    }

    #[test]
    fn copilot_uses_its_bring_your_own_key_variables() {
        let models = models();
        let launch = launch(
            Agent::CopilotCli,
            &backend(&models, None),
            Path::new("/scratch"),
        );
        assert_eq!(
            env_of(&launch),
            vec![
                (
                    "COPILOT_PROVIDER_BASE_URL",
                    "http://127.0.0.1:8090/v1".to_string()
                ),
                ("COPILOT_PROVIDER_TYPE", "openai".to_string()),
                ("COPILOT_PROVIDER_WIRE_API", "completions".to_string()),
                ("COPILOT_PROVIDER_API_KEY", "spice".to_string()),
                ("COPILOT_MODEL", "qwen".to_string()),
            ]
        );
        assert_eq!(launch.env_remove, COPILOT_ENV_REMOVE);
    }

    #[test]
    fn cursor_runs_authless_against_the_runtime() {
        let models = models();
        let launch = launch(
            Agent::Cursor,
            &backend(&models, None),
            Path::new("/scratch"),
        );
        assert_eq!(args_of(&launch), vec!["--model", "qwen"]);
        assert_eq!(
            env_of(&launch),
            vec![
                (
                    "CURSOR_LOCAL_AGENT_BASE_URL",
                    "http://127.0.0.1:8090/v1".to_string()
                ),
                ("CURSOR_LOCAL_AGENT_API_KEY", "spice".to_string()),
                ("CURSOR_ENABLE_AUTHLESS", "1".to_string()),
            ]
        );
    }

    #[test]
    fn grok_signs_in_to_a_private_session_minted_from_the_runtime_key() {
        let models = models();
        let launch = launch(
            Agent::Grok,
            &backend(&models, Some("k'ey")),
            Path::new("/scratch"),
        );
        assert_eq!(args_of(&launch), vec!["--model", "qwen"]);
        assert_eq!(
            env_of(&launch),
            vec![
                (
                    "GROK_MODELS_BASE_URL",
                    "http://127.0.0.1:8090/v1".to_string()
                ),
                ("XAI_API_KEY", "k'ey".to_string()),
                ("GROK_AUTH_PATH", "/scratch/grok/auth.json".to_string()),
                (
                    "GROK_AUTH_PROVIDER_COMMAND",
                    "/scratch/grok/token.sh".to_string()
                ),
                ("GROK_DEFAULT_MODEL", "qwen".to_string()),
                ("GROK_SESSION_SUMMARY_MODEL", "qwen".to_string()),
            ]
        );
        assert_eq!(
            launch.files,
            vec![LaunchFile {
                path: PathBuf::from("/scratch/grok/token.sh"),
                // The quote in the key is escaped, so the script prints it verbatim.
                contents: "#!/bin/sh\nprintf '%s' 'k'\\''ey'\n".to_string(),
                executable: true,
            }]
        );
        assert_eq!(launch.prepare, Some(vec![OsString::from("login")]));
    }

    #[test]
    fn opencode_gets_one_provider_with_every_spice_model() {
        let models = models();
        let launch = launch(
            Agent::OpenCode,
            &backend(&models, None),
            Path::new("/scratch"),
        );
        let env = env_of(&launch);
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, "OPENCODE_CONFIG_CONTENT");
        let config: serde_json::Value =
            serde_json::from_str(&env[0].1).expect("the inline config is JSON");
        assert_eq!(
            config,
            json!({
                "$schema": "https://opencode.ai/config.json",
                "provider": {
                    "spice": {
                        "npm": "@ai-sdk/openai-compatible",
                        "name": "Spice",
                        "options": {
                            "baseURL": "http://127.0.0.1:8090/v1",
                            "apiKey": "spice",
                        },
                        "models": { "qwen": { "name": "qwen" }, "gpt": { "name": "gpt" } },
                    }
                },
                "model": "spice/qwen",
                "small_model": "spice/qwen",
                "enabled_providers": ["spice"],
            })
        );
    }

    #[test]
    fn pi_loads_an_extension_that_registers_the_runtime() {
        let models = models();
        let launch = launch(Agent::Pi, &backend(&models, None), Path::new("/scratch"));
        assert_eq!(
            args_of(&launch),
            vec!["-e", "/scratch/pi/spice.ts", "--model", "spice/qwen"]
        );
        assert_eq!(launch.files.len(), 1);
        let file = &launch.files[0];
        assert_eq!(file.path, PathBuf::from("/scratch/pi/spice.ts"));
        assert!(!file.executable);
        insta::assert_snapshot!("pi_extension", file.contents);
    }

    #[test]
    fn antigravity_reaches_the_gemini_api_at_the_runtime_root() {
        let models = models();
        let launch = launch(
            Agent::Antigravity,
            &backend(&models, None),
            Path::new("/scratch"),
        );
        assert_eq!(args_of(&launch), vec!["--model=qwen"]);
        assert_eq!(
            env_of(&launch),
            vec![
                ("GOOGLE_GEMINI_BASE_URL", ENDPOINT.to_string()),
                ("GEMINI_API_KEY", "spice".to_string()),
            ]
        );
    }

    fn requested(agent: Agent, args: &[&str]) -> RequestedModel {
        let args: Vec<OsString> = args.iter().map(OsString::from).collect();
        requested_model(agent, &args)
    }

    #[test]
    fn the_agents_own_model_option_names_the_spice_model() {
        let named = |model: &str| RequestedModel::Spice(model.to_string());
        assert_eq!(
            requested(Agent::Codex, &["--yolo", "-m", "gpt"]),
            named("gpt")
        );
        assert_eq!(requested(Agent::Codex, &["--model=gpt"]), named("gpt"));
        assert_eq!(requested(Agent::Codex, &["-mgpt"]), named("gpt"));
        assert_eq!(
            requested(Agent::Grok, &["-p", "hi", "--model", "gpt"]),
            named("gpt")
        );
        assert_eq!(
            requested(Agent::ClaudeCode, &["--model", "gpt"]),
            named("gpt")
        );
        // Claude Code has no `-m`, so it is the user's argument, not a model.
        assert_eq!(
            requested(Agent::ClaudeCode, &["-m", "gpt"]),
            RequestedModel::Unspecified
        );
        // After `--` everything is positional for the agent.
        assert_eq!(
            requested(Agent::Codex, &["--", "-m", "gpt"]),
            RequestedModel::Unspecified
        );
        assert_eq!(
            requested(Agent::Codex, &["--yolo"]),
            RequestedModel::Unspecified
        );
    }

    #[test]
    fn provider_qualified_models_are_read_back_to_the_spice_model() {
        let named = |model: &str| RequestedModel::Spice(model.to_string());
        assert_eq!(
            requested(Agent::OpenCode, &["-m", "spice/gpt"]),
            named("gpt")
        );
        assert_eq!(
            requested(Agent::Pi, &["--model", "spice/gpt:high"]),
            named("gpt")
        );
        // A colon that is not a thinking level is part of the model id.
        assert_eq!(
            requested(Agent::Pi, &["--model", "spice/qwen3:4b"]),
            named("qwen3:4b")
        );
        // Another provider's model is the agent's business: nothing to validate.
        assert_eq!(
            requested(Agent::OpenCode, &["-m", "anthropic/claude"]),
            RequestedModel::AgentOwned
        );
        assert_eq!(
            requested(Agent::ClaudeCode, &["--model", "opus[1m]"]),
            RequestedModel::AgentOwned
        );
    }

    #[test]
    fn a_model_the_user_named_is_not_named_again() {
        let models = models();
        let mut given = backend(&models, None);
        given.model_given = true;
        for agent in [Agent::Cursor, Agent::Grok, Agent::Antigravity] {
            let args = args_of(&launch(agent, &given, Path::new("/scratch")));
            assert!(
                !args.iter().any(|arg| arg.starts_with("--model")),
                "{agent:?}: {args:?}"
            );
        }
        assert_eq!(
            args_of(&launch(Agent::Pi, &given, Path::new("/scratch"))),
            vec!["-e", "/scratch/pi/spice.ts"]
        );
        // Codex sets its default as config, which the user's `--model` overrides.
        assert!(
            args_of(&launch(Agent::Codex, &given, Path::new("/scratch")))
                .contains(&r#"model="qwen""#.to_string())
        );
    }

    #[test]
    fn every_agent_has_a_distinct_command() {
        let mut commands: Vec<&str> = Agent::ALL.iter().map(|agent| agent.command()).collect();
        commands.sort_unstable();
        commands.dedup();
        assert_eq!(commands.len(), Agent::ALL.len());
    }

    #[test]
    fn toml_strings_escape_quotes_and_backslashes() {
        assert_eq!(toml_string(r#"a"b\c"#), r#""a\"b\\c""#);
    }
}
