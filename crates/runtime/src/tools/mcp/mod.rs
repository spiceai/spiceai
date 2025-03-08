/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

pub mod catalog;
pub mod factory;
pub mod tool;

use std::{collections::HashMap, str::FromStr};

use mcp_client::{transport::Error as TransportError, Error as McpError};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid MCP directive 'from: mcp:{}'", id))]
    InvalidMCPDirective { id: String },

    #[snafu(display("Could not construct tool `{}`. Error: {}", name, e))]
    CouldNotConstructTool { name: String, e: String },

    #[snafu(display("Invalid params 'mcp_args': '{}'.", args_str))]
    InvalidMcpArgs { args_str: String },

    #[snafu(display(
        "Error occured in underlying communication to MCP tool. Error: {}",
        source
    ))]
    UnderlyingTransportError { source: TransportError },

    #[snafu(display(
        "Error occured in initialization client connection with underlying MCP server. Error: {}",
        source
    ))]
    UnderlyingInitilizationError { source: McpError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MCPType {
    /// Connect to an MCP server over HTTP(s) SSE protocol.
    Https(url::Url),

    /// Uses stdio to communicate with an MCP server. The string is the command to run.
    Stdio(String),
}

impl FromStr for MCPType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match serde_json::from_str(format!("\"{s}\"").as_str()) {
            Ok(mcp_type) => Ok(mcp_type),
            Err(_) => Err(Error::InvalidMCPDirective { id: s.to_string() }),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum MCPConfig {
    Stdio {
        command: String,
        args: Option<Vec<String>>,
    },
    Https {
        url: url::Url,
    },
}
impl MCPConfig {
    fn from_type(mcp_type: &MCPType, params: &HashMap<String, SecretString>) -> Self {
        match mcp_type {
            MCPType::Stdio(command) => match params.get("mcp_args") {
                Some(args) => {
                    let args = ExposeSecret::expose_secret(args);
                    Self::Stdio {
                        command: command.clone(),
                        args: Some(args.split_whitespace().map(|s| s.to_string()).collect()),
                    }
                }
                None => Self::Stdio {
                    command: command.clone(),
                    args: None,
                },
            },
            MCPType::Https(url) => Self::Https { url: url.clone() },
        }
    }
}
