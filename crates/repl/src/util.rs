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

//! Shared REPL utilities for CLI commands.

use dialoguer::{Select, theme::ColorfulTheme};
use rustyline::DefaultEditor;
use serde::Deserialize;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Spinner animation frames.
pub const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

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

/// Error type for REPL utilities.
#[derive(Debug)]
pub enum UtilError {
    /// Connection failed
    ConnectionFailed { endpoint: String, source: String },
    /// Invalid response
    InvalidResponse { message: String },
    /// Model not found
    ModelNotFound { model: String, available: String },
    /// No models configured
    NoModelsConfigured,
}

impl std::fmt::Display for UtilError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionFailed { endpoint, source } => {
                write!(f, "Failed to connect to {endpoint}: {source}")
            }
            Self::InvalidResponse { message } => write!(f, "{message}"),
            Self::ModelNotFound { model, available } => {
                write!(
                    f,
                    "Model '{model}' not found. Available models: {available}"
                )
            }
            Self::NoModelsConfigured => {
                write!(f, "No models are configured in the runtime")
            }
        }
    }
}

impl std::error::Error for UtilError {}

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
///
/// # Errors
///
/// Returns an error if the connection fails or the response is invalid.
pub async fn get_available_models(
    client: &reqwest::Client,
    http_endpoint: &str,
    headers: &[(String, String)],
) -> Result<Vec<String>, UtilError> {
    let url = format!("{http_endpoint}/v1/models?status=true");

    let mut request = client.get(&url);
    for (key, value) in headers {
        request = request.header(key, value);
    }

    let response = request
        .send()
        .await
        .map_err(|e| UtilError::ConnectionFailed {
            endpoint: url.clone(),
            source: e.to_string(),
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(UtilError::InvalidResponse {
            message: format!("Failed to get models: {status} - {text}"),
        });
    }

    let models: ModelsResponse = response
        .json()
        .await
        .map_err(|e| UtilError::InvalidResponse {
            message: format!("Failed to parse models response: {e}"),
        })?;

    Ok(models.data.into_iter().map(|m| m.id).collect())
}

/// Validate that a model exists in the runtime.
///
/// # Errors
///
/// Returns an error if the model is not found or the connection fails.
pub async fn validate_model(
    client: &reqwest::Client,
    http_endpoint: &str,
    headers: &[(String, String)],
    model: &str,
) -> Result<(), UtilError> {
    let models = get_available_models(client, http_endpoint, headers).await?;

    if !models.iter().any(|m| m == model) {
        let available = if models.is_empty() {
            "none".to_string()
        } else {
            models.join(", ")
        };
        return Err(UtilError::ModelNotFound {
            model: model.to_string(),
            available,
        });
    }

    Ok(())
}

/// Select a model from available models using an interactive picker.
///
/// # Errors
///
/// Returns an error if no models are configured or user selection fails.
pub async fn select_model(
    client: &reqwest::Client,
    http_endpoint: &str,
    headers: &[(String, String)],
) -> Result<String, UtilError> {
    let models = get_available_models(client, http_endpoint, headers).await?;

    if models.is_empty() {
        return Err(UtilError::NoModelsConfigured);
    }

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
        .map_err(|e| UtilError::InvalidResponse {
            message: format!("Failed to read selection: {e}"),
        })?;

    Ok(models[selection].clone())
}

/// Get or validate a model - validates if specified, selects interactively if not.
///
/// # Errors
///
/// Returns an error if the model is not found, no models are configured,
/// or the connection fails.
pub async fn get_or_select_model(
    client: &reqwest::Client,
    http_endpoint: &str,
    headers: &[(String, String)],
    model: Option<&str>,
) -> Result<String, UtilError> {
    match model {
        Some(m) => {
            validate_model(client, http_endpoint, headers, m).await?;
            Ok(m.to_string())
        }
        None => select_model(client, http_endpoint, headers).await,
    }
}

/// Create a new rustyline editor with history loaded from the specified file.
///
/// # Errors
///
/// Returns an error if the editor fails to initialize.
pub fn create_editor_with_history(
    history_file: &str,
) -> Result<(DefaultEditor, Option<PathBuf>), UtilError> {
    let mut rl = DefaultEditor::new().map_err(|e| UtilError::InvalidResponse {
        message: format!("Failed to initialize line editor: {e}"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_util_error_display_connection_failed() {
        let err = UtilError::ConnectionFailed {
            endpoint: "http://localhost:8090".to_string(),
            source: "connection refused".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Failed to connect to http://localhost:8090: connection refused"
        );
    }

    #[test]
    fn test_util_error_display_invalid_response() {
        let err = UtilError::InvalidResponse {
            message: "Bad JSON".to_string(),
        };
        assert_eq!(err.to_string(), "Bad JSON");
    }

    #[test]
    fn test_util_error_display_model_not_found() {
        let err = UtilError::ModelNotFound {
            model: "gpt-5".to_string(),
            available: "gpt-3, gpt-4".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Model 'gpt-5' not found. Available models: gpt-3, gpt-4"
        );
    }

    #[test]
    fn test_util_error_display_no_models_configured() {
        let err = UtilError::NoModelsConfigured;
        assert_eq!(err.to_string(), "No models are configured in the runtime");
    }

    #[test]
    fn test_spinner_frames_constant() {
        // Verify spinner frames are defined correctly
        assert_eq!(SPINNER_FRAMES.len(), 10);
        assert!(SPINNER_FRAMES.iter().all(|f| !f.is_empty()));
    }

    #[tokio::test]
    async fn test_spinner_start_and_stop() {
        // Test that spinner can be started and stopped without panic
        let spinner = Spinner::start();
        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Stop should complete without error
        spinner.stop().await;
    }

    #[tokio::test]
    async fn test_spinner_drop_stops_animation() {
        // Test that dropping the spinner stops the animation
        let spinner = Spinner::start();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(spinner);
        // Just verify no panic occurs
    }

    #[test]
    fn test_create_editor_with_history() {
        // Test that we can create an editor (may fail in some CI environments without a terminal)
        let result = create_editor_with_history("test_history.txt");
        // This should succeed if rustyline can initialize
        if let Ok((editor, history_path)) = result {
            // Verify history path is constructed correctly
            if let Some(path) = history_path {
                assert!(path.ends_with(".spice/test_history.txt"));
            }
            drop(editor);
        }
        // If it fails (e.g., no terminal in CI), that's acceptable for this test
    }

    #[test]
    fn test_save_history_creates_directory() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let history_path = temp_dir.path().join("nested").join("history.txt");

        // Create editor
        if let Ok((mut editor, _)) = create_editor_with_history("test.txt") {
            // Add some history entries
            let _ = editor.add_history_entry("SELECT * FROM table1");
            let _ = editor.add_history_entry("SELECT * FROM table2");

            // Save to the nested path
            save_history(&mut editor, Some(&history_path));

            // Verify parent directory was created
            assert!(history_path.parent().is_some_and(std::path::Path::exists));
        }
    }

    #[test]
    fn test_save_history_with_none_path() {
        // Test that save_history handles None path gracefully
        if let Ok((mut editor, _)) = create_editor_with_history("test.txt") {
            // This should not panic
            save_history(&mut editor, None);
        }
    }

    // Note: The following functions require network calls and are tested via integration tests:
    // - get_available_models
    // - validate_model
    // - select_model (also requires TTY for interactive selection)
    // - get_or_select_model
    //
    // See test/spicepods/ for integration tests that exercise these functions
    // with a running Spice runtime.

    #[test]
    fn test_models_response_deserialization() {
        // Test that ModelsResponse can be deserialized correctly
        let json = r#"{"data": [{"id": "model1"}, {"id": "model2"}]}"#;
        let response: ModelsResponse =
            serde_json::from_str(json).expect("Failed to parse ModelsResponse");
        assert_eq!(response.data.len(), 2);
        assert_eq!(response.data[0].id, "model1");
        assert_eq!(response.data[1].id, "model2");
    }

    #[test]
    fn test_models_response_empty() {
        // Test empty models list
        let json = r#"{"data": []}"#;
        let response: ModelsResponse =
            serde_json::from_str(json).expect("Failed to parse empty ModelsResponse");
        assert!(response.data.is_empty());
    }
}
