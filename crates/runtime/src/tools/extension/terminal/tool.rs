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
use async_trait::async_trait;
use snafu::ResultExt;
use std::{io, sync::Arc};
use tracing::Span;

use crate::{
    tools::{utils::parameters, SpiceModelTool},
    Runtime,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing_futures::Instrument;

use super::manager::TerminalManager;

/// Parameters for the [`TerminalTool`].
///
/// - `command`: The command to execute in the terminal.
/// - `terminal_id`: (Optional) The ID of an existing terminal to send the command to.
///                 If omitted, a new terminal will be spawned.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct TerminalToolParams {
    /// The command to execute in the terminal.
    command: String,

    /// Optional ID of an existing terminal. If provided, the command will be sent to this terminal.
    /// Otherwise, a new terminal will be created for the command.
    #[serde(default)]
    terminal_id: Option<usize>,
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct TerminalToolResponse {
    /// The ID of the terminal that executed the command.
    terminal_id: usize,

    /// The output of the command.
    output: String,
}

/// The [`TerminalTool`] provides the ability to execute commands in terminals.
/// It can create new terminals or interact with existing ones via the [`TerminalManager`].
pub struct TerminalTool {
    name: String,
    description: Option<String>,
    terminal_manager: Arc<TerminalManager>,
}

impl TerminalTool {
    #[must_use]
    pub fn new(name: &str, description: Option<&str>) -> Self {
        Self {
            name: name.to_string(),
            description: description.map(String::from),
            terminal_manager: Arc::new(TerminalManager::default()),
        }
    }
}

impl Default for TerminalTool {
    fn default() -> Self {
        Self::new(
            "terminal",
            Some("Execute shell commands in managed terminal sessions.
Use 'command' to run in a new terminal or provide 'terminal_id' to send commands to an existing terminal.".to_string())
        )
    }
}

#[async_trait]
impl SpiceModelTool for TerminalTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<TerminalToolParams>()
    }

    async fn call(
        &self,
        arg: &str,
        _rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span: Span = tracing::span!(
            target: "task_history",
            tracing::Level::INFO,
            "tool_use::terminal",
            tool = self.name(),
            input = arg
        );

        let result = async {
            let params: TerminalToolParams = serde_json::from_str(arg).boxed()?;

            let terminal_id = match params.terminal_id {
                Some(id) => {
                    // Use existing terminal
                    if !self.terminal_manager.list_terminals().await.contains(&id) {
                        return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
                            "Terminal ID {id} not found"
                        )));
                    };
                    Some(id)
                }
                None => Some(
                    self.terminal_manager
                        .spawn_terminal()
                        .await
                        .map_err(box_io_err)?,
                ),
            };

            let Some(terminal_id) = terminal_id else {
                return Err(Box::<dyn std::error::Error + Send + Sync>::from(
                    "Failed to obtain terminal ID".to_string(),
                ));
            };

            self.terminal_manager
                .send_command(terminal_id, &params.command)
                .await
                .boxed()?;

            let output = self
                .terminal_manager
                .read_output(terminal_id)
                .await
                .boxed()?;

            let response = TerminalToolResponse {
                terminal_id,
                output,
            };
            serde_json::to_value(response).boxed()
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

fn box_io_err(err: io::Error) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(err)
}
