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
pub mod server;
pub mod tool;

use std::{collections::HashMap, str::FromStr};

use rmcp::ErrorData as McpError;
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
    UnderlyingTransportError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
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
    /// Connect to an MCP server over Streamable HTTP (HTTPS in production; HTTP is permitted for localhost).
    StreamableHttp(url::Url),

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
        args: Vec<String>,
        env: HashMap<String, String>,
    },
    StreamableHttp {
        url: url::Url,
    },
}
impl MCPConfig {
    fn from_type(
        mcp_type: &MCPType,
        params: &HashMap<String, SecretString>,
        env: &HashMap<String, SecretString>,
    ) -> Self {
        match mcp_type.clone() {
            MCPType::Stdio(command) => {
                let args = params
                    .get("mcp_args")
                    .map(ExposeSecret::expose_secret)
                    .unwrap_or_default()
                    .split_whitespace()
                    .map(ToString::to_string)
                    .collect();

                let env = env
                    .iter()
                    .map(|(k, v)| (k.clone(), ExposeSecret::expose_secret(v).to_string()))
                    .collect();

                Self::Stdio { command, args, env }
            }
            MCPType::StreamableHttp(url) => Self::StreamableHttp { url },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_type_parses_stdio_command() {
        let t = MCPType::from_str("docker").expect("valid stdio MCP directive");
        match t {
            MCPType::Stdio(cmd) => assert_eq!(cmd, "docker"),
            MCPType::StreamableHttp(_) => panic!("expected stdio variant"),
        }
    }

    #[test]
    fn mcp_type_parses_https_url() {
        let t = MCPType::from_str("https://example.com/v1/mcp").expect("valid https MCP directive");
        match t {
            MCPType::StreamableHttp(url) => {
                assert_eq!(url.scheme(), "https");
                assert_eq!(url.host_str(), Some("example.com"));
                assert_eq!(url.path(), "/v1/mcp");
            }
            MCPType::Stdio(_) => panic!("expected https variant"),
        }
    }

    #[test]
    fn mcp_type_parses_http_url_for_localhost() {
        let t = MCPType::from_str("http://127.0.0.1:8090/v1/mcp")
            .expect("valid http MCP directive for localhost");
        match t {
            MCPType::StreamableHttp(url) => {
                assert_eq!(url.scheme(), "http");
                assert_eq!(url.path(), "/v1/mcp");
            }
            MCPType::Stdio(_) => panic!("expected https variant"),
        }
    }

    #[test]
    fn mcp_config_from_stdio_collects_args_and_env() {
        let mcp_type = MCPType::Stdio("docker".to_string());
        let mut params = HashMap::new();
        params.insert(
            "mcp_args".to_string(),
            SecretString::from("run -i --rm mcp/fetch"),
        );
        let mut env = HashMap::new();
        env.insert("FOO".to_string(), SecretString::from("bar"));

        let cfg = MCPConfig::from_type(&mcp_type, &params, &env);
        match cfg {
            MCPConfig::Stdio { command, args, env } => {
                assert_eq!(command, "docker");
                assert_eq!(args, vec!["run", "-i", "--rm", "mcp/fetch"]);
                assert_eq!(env.get("FOO").map(String::as_str), Some("bar"));
            }
            MCPConfig::StreamableHttp { .. } => panic!("expected stdio config"),
        }
    }

    #[test]
    fn mcp_config_from_https_preserves_url() {
        let url = url::Url::parse("https://example.com/v1/mcp").expect("valid url");
        let mcp_type = MCPType::StreamableHttp(url.clone());
        let cfg = MCPConfig::from_type(&mcp_type, &HashMap::new(), &HashMap::new());
        match cfg {
            MCPConfig::StreamableHttp { url: u } => assert_eq!(u, url),
            MCPConfig::Stdio { .. } => panic!("expected https config"),
        }
    }
}
