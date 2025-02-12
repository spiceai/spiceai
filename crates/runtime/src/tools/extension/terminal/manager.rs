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
use std::collections::HashMap;
use std::env;
use std::io::{self};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

pub struct TerminalManager {
    terminals: Arc<Mutex<HashMap<usize, Child>>>,
    next_id: Arc<Mutex<usize>>,
}

pub static END_OF_COMMAND_MARKER: &str = "\x1e__CMD_END__\x1f";

impl Default for TerminalManager {
    fn default() -> Self {
        TerminalManager {
            terminals: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
        }
    }
}

impl TerminalManager {
    fn get_shell_command() -> Command {
        let default_shell = if cfg!(target_os = "windows") {
            env::var("COMSPEC").unwrap_or("cmd.exe".to_string())
        } else {
            env::var("SHELL").unwrap_or("bash".to_string())
        };
        Command::new(default_shell)
    }

    /// Spawns a new terminal session and adds it to the manager.
    ///
    /// # Returns
    ///
    /// * `io::Result<usize>` - The unique ID of the new terminal or an I/O error.
    pub async fn spawn_terminal(&self) -> io::Result<usize> {
        // Generate a unique ID for the new terminal
        let mut id_lock = self.next_id.lock().await;
        let id = *id_lock;
        *id_lock += 1;
        drop(id_lock);

        let child = Self::get_shell_command()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        self.terminals.lock().await.insert(id, child);
        Ok(id)
    }

    /// Sends a command to the specified terminal's stdin.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier of the terminal.
    /// * `command` - The command string to send.
    ///
    /// # Returns
    ///
    /// * `io::Result<()>` - Ok if successful, or an I/O error.
    pub async fn send_command(&self, id: usize, command: &str) -> io::Result<()> {
        let mut terminals = self.terminals.lock().await;
        if let Some(child) = terminals.get_mut(&id) {
            if let Some(stdin) = child.stdin.as_mut() {
                let full_command = format!("{command}\necho {END_OF_COMMAND_MARKER}\n");
                stdin.write_all(full_command.as_bytes()).await?;
                stdin.flush().await
            } else {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stdin not available",
                ))
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Terminal ID not found",
            ))
        }
    }

    /// Reads a single line of output from the specified terminal's stdout.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier of the terminal.
    ///
    /// # Returns
    ///
    /// * `io::Result<String>` - The output string or an I/O error.
    pub async fn read_output(&self, id: usize) -> io::Result<String> {
        let mut terminals = self.terminals.lock().await;
        if let Some(child) = terminals.get_mut(&id) {
            if let Some(stdout) = child.stdout.as_mut() {
                let mut reader = BufReader::new(stdout);
                let mut buffer = String::new();

                loop {
                    if reader.read_line(&mut buffer).await? == 0 {
                        // EOF reached without finding the marker
                        break;
                    }
                    if buffer.contains(END_OF_COMMAND_MARKER) {
                        buffer = buffer.replace(END_OF_COMMAND_MARKER, "");
                        break;
                    }
                }

                Ok(buffer.trim().to_string())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stdout not available",
                ))
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Terminal ID not found",
            ))
        }
    }

    /// Lists all active terminal IDs.
    ///
    /// # Returns
    ///
    /// * `Vec<usize>` - A vector of active terminal IDs.
    pub async fn list_terminals(&self) -> Vec<usize> {
        let terminals = self.terminals.lock().await;
        terminals.keys().copied().collect()
    }
}
