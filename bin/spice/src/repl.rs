/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Shared REPL utilities for CLI commands.

use crate::context::RuntimeContext;
use crate::error::{
    ConnectionFailedSnafu, InvalidResponseSnafu, ModelNotFoundSnafu, NoModelsConfiguredSnafu,
    Result,
};
use dialoguer::{Select, theme::ColorfulTheme};
use rustyline::DefaultEditor;
use serde::Deserialize;
use snafu::ResultExt;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Spinner animation frames.
const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Model information from the models endpoint.
#[derive(Deserialize)]
struct Model {
    id: String,
}

/// Response from the models endpoint.
#[derive(Deserialize)]
struct ModelsResponse {
    data: Vec<Model>,
}

/// A spinner that shows activity while waiting for an async operation.
pub struct Spinner {
    running: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Spinner {
    /// Start a new spinner.
    #[must_use]
    pub fn start() -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let mut idx = 0;
            while running_clone.load(Ordering::Relaxed) {
                let frame = SPINNER_FRAMES[idx % SPINNER_FRAMES.len()];
                print!("\r{frame} ");
                let _ = io::stdout().flush();
                idx += 1;
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
            // Clear spinner
            print!("\r  \r");
            let _ = io::stdout().flush();
        });

        Self {
            running,
            handle: Some(handle),
        }
    }

    /// Stop the spinner and wait for it to clear.
    pub async fn stop(mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// Stop the spinner synchronously (best effort, may not fully clear).
    pub fn stop_sync(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

/// Get the list of available models from the runtime.
pub async fn get_available_models(ctx: &RuntimeContext) -> Result<Vec<String>> {
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

    Ok(models.data.into_iter().map(|m| m.id).collect())
}

/// Validate that a model exists in the runtime.
pub async fn validate_model(ctx: &RuntimeContext, model: &str) -> Result<()> {
    let models = get_available_models(ctx).await?;

    if !models.iter().any(|m| m == model) {
        let available = if models.is_empty() {
            "none".to_string()
        } else {
            models.join(", ")
        };
        return Err(ModelNotFoundSnafu {
            model: model.to_string(),
            available,
        }
        .build());
    }

    Ok(())
}

/// Select a model from available models using an interactive picker.
pub async fn select_model(ctx: &RuntimeContext) -> Result<String> {
    let models = get_available_models(ctx).await?;

    snafu::ensure!(!models.is_empty(), NoModelsConfiguredSnafu);

    // If only one model, use it
    if models.len() == 1 {
        return Ok(models[0].clone());
    }

    // Let user select with arrow keys
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Select model")
        .items(&models)
        .default(0)
        .interact()
        .map_err(|e| {
            InvalidResponseSnafu {
                message: format!("Failed to read selection: {e}"),
            }
            .build()
        })?;

    Ok(models[selection].clone())
}

/// Get or validate a model - validates if specified, selects interactively if not.
pub async fn get_or_select_model(ctx: &RuntimeContext, model: Option<&str>) -> Result<String> {
    match model {
        Some(m) => {
            validate_model(ctx, m).await?;
            Ok(m.to_string())
        }
        None => select_model(ctx).await,
    }
}

/// Create a new rustyline editor with history loaded from the specified file.
pub fn create_editor_with_history(history_file: &str) -> Result<(DefaultEditor, Option<PathBuf>)> {
    let mut rl = DefaultEditor::new().map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to initialize line editor: {e}"),
        }
        .build()
    })?;

    let history_path = dirs::home_dir().map(|h| h.join(".spice").join(history_file));
    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    Ok((rl, history_path))
}

/// Save editor history to the specified path.
pub fn save_history(rl: &mut DefaultEditor, history_path: Option<&PathBuf>) {
    if let Some(path) = history_path {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = rl.save_history(path);
    }
}
