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

//! MCP client and server wrappers.
//!
//! Spice is dual-era: the server prefers [`rmcp::model::ProtocolVersion::V_2026_07_28`]
//! (`server/discover`, sessionless Streamable HTTP) and still answers legacy
//! `initialize` so existing clients keep working until that handshake is dropped.

pub mod catalog;
pub mod factory;
mod http_client;
pub mod server;
pub mod tool;

pub use catalog::attach_mcp_schema_snapshot;

use std::{collections::HashMap, str::FromStr, time::Duration};

use http::{HeaderName, HeaderValue, header::AUTHORIZATION};
use rmcp::ErrorData as McpError;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    /// A URL directive is shown without its credentials; the message is logged and kept as
    /// the tool's status.
    #[snafu(display(
        "Invalid MCP directive 'from: mcp:{}'",
        redact::redact_url_credentials(id)
    ))]
    InvalidMCPDirective { id: String },

    #[snafu(display("Could not construct tool `{}`. Error: {}", name, e))]
    CouldNotConstructTool { name: String, e: String },

    #[snafu(display(
        "Invalid param 'mcp_args': {source}. Quote arguments with spaces, e.g. `--flag \"a b\"`."
    ))]
    InvalidMcpArgs { source: shell_words::ParseError },

    #[snafu(display(
        "Failed to load tool '{tool}': MCP server URL '{url}' uses plaintext HTTP to a host that is not loopback, which would send the tool's credentials and results unencrypted. Use https://, or set `mcp_transport_security: allow_plaintext` on the tool if the network already encrypts this connection. See: {MCP_DOCS}"
    ))]
    PlaintextToRemoteHost { tool: String, url: String },

    #[snafu(display(
        "Failed to load tool '{tool}': MCP server URL '{url}' uses plaintext HTTP to a host that is not loopback ({wildcard} is not a loopback address), which would send the tool's credentials and results unencrypted. Use {local_url} for a local server, https:// for a remote one, or set `mcp_transport_security: allow_plaintext` on the tool if the network already encrypts this connection. See: {MCP_DOCS}"
    ))]
    PlaintextToWildcard {
        tool: String,
        url: String,
        wildcard: String,
        local_url: String,
    },

    #[snafu(display(
        "Failed to load tool '{tool}': `mcp_transport_security` applies only to an MCP server reached over HTTP (`mcp:http://` or `mcp:https://`), and tool '{tool}' runs a local program. Remove `mcp_transport_security` from tool '{tool}'. See: {MCP_DOCS}"
    ))]
    TransportSecurityOnStdio { tool: String },

    #[snafu(display(
        "Failed to load tool '{tool}': `mcp_transport_security` is '{value}', and it takes `require_tls` or `allow_plaintext`. See: {MCP_DOCS}"
    ))]
    InvalidTransportSecurity { tool: String, value: String },

    #[snafu(display(
        "Error occurred in underlying communication to MCP tool. Error: {}",
        source
    ))]
    UnderlyingTransportError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display(
        "Error occurred in initialization client connection with underlying MCP server. Error: {}",
        source
    ))]
    UnderlyingInitilizationError { source: McpError },
}

impl Error {
    /// Returns `true` when the error is a mistake in the tool's Spicepod configuration, which
    /// no retry can clear.
    #[must_use]
    pub fn is_configuration_error(&self) -> bool {
        matches!(
            self,
            Error::InvalidMCPDirective { .. }
                | Error::InvalidMcpArgs { .. }
                | Error::PlaintextToRemoteHost { .. }
                | Error::PlaintextToWildcard { .. }
                | Error::TransportSecurityOnStdio { .. }
                | Error::InvalidTransportSecurity { .. }
        )
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Prefix every tool-use task name carries.
///
/// `spice trace` and the task-history exporter both match on this prefix, but
/// neither can share this constant — `bin/spice` depends on neither this crate
/// nor `tools` — so each spells it out itself.
const TOOL_USE_PREFIX: &str = "tool_use::";

/// `runtime.task_history.task` for a call on the tool exposed as `exposed_name`.
///
/// A proxied tool is reachable through two entry points — the `/v1/mcp` gateway
/// and [`McpToolWrapper::call`](tool::McpToolWrapper::call), which a
/// `POST /v1/tools/{name}` request or a model-driven call enters directly. Both
/// label the task through this one function, from the name the tool is exposed
/// under, so one tool cannot be recorded under two spellings: grouping
/// `runtime.task_history` by `task` would then split it across two rows, and
/// `spice trace <task>` would show only the half matching the spelling it was
/// given.
pub(crate) fn task_name_for_exposed_tool(exposed_name: &str) -> String {
    format!("{TOOL_USE_PREFIX}{exposed_name}")
}

/// Longest wait for an MCP server to finish its handshake. Model loading waits for tools, so a
/// server that never answers must not hold it forever. A stdio server started through
/// `npx`/`uvx` may download its package first, so this allows more than a request.
pub const MCP_HANDSHAKE_TIMEOUT: Duration = Duration::from_mins(2);

/// Longest wait for a `tools/list` or `ping` answer.
pub const MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Longest wait for one `tools/call`. Tools do real work, so this is longer than
/// [`MCP_REQUEST_TIMEOUT`]; it exists so that a stalled server cannot hold a caller forever.
pub const MCP_CALL_TOOL_TIMEOUT: Duration = Duration::from_mins(10);

/// Largest MCP response, in bytes, Spice accepts from an MCP server over
/// Streamable HTTP, and the largest serialized tool result it passes on.
///
/// A JSON body is capped as a whole. A POST that comes back as SSE is capped
/// as a whole too: progress notifications and the final result count together,
/// so that stream is rejected even when every individual event is under the
/// limit. A GET SSE stream keeps rmcp's per-event cap, set to this same value.
/// 16 MiB is rmcp's own default event limit, and it is far above real tool
/// output: a whole 1M-token model context is about 4 MiB of text.
///
/// The bound exists because one tool result is copied several times on its
/// way to the caller (decoded, re-serialized, captured to task history, and
/// written to the HTTP response or model context), so its size multiplies
/// into peak memory.
const MAX_TOOL_RESULT_SIZE: usize = crate::utils::MAX_TOOL_RESULT_BYTES;

/// Documentation for MCP tools, named in their errors.
const MCP_DOCS: &str = "https://spiceai.org/docs/components/tools/mcp";

const MCP_AUTH_TOKEN_PARAM: &str = "mcp_auth_token";

/// Whether a Streamable HTTP MCP tool may use plaintext HTTP to a host that is not loopback.
const MCP_TRANSPORT_SECURITY_PARAM: &str = "mcp_transport_security";

/// The values of [`MCP_TRANSPORT_SECURITY_PARAM`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum TransportSecurity {
    /// `http://` only to a loopback host; any other host needs `https://`.
    #[default]
    RequireTls,
    /// `http://` to any host: the network below HTTP already protects the connection.
    AllowPlaintext,
}

impl TransportSecurity {
    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "require_tls" => Some(Self::RequireTls),
            "allow_plaintext" => Some(Self::AllowPlaintext),
            _ => None,
        }
    }
}

/// Refuses an `http://` MCP server URL to a host that is not loopback, unless `security`
/// permits plaintext. Loopback is decided from the host as written, never from DNS.
fn check_transport_security(tool: &str, url: &url::Url, security: TransportSecurity) -> Result<()> {
    if url.scheme() != "http" || security == TransportSecurity::AllowPlaintext {
        return Ok(());
    }
    let host = url.host_str().unwrap_or_default();
    if util::net::is_loopback_host(host) {
        return Ok(());
    }
    // The URL is shown in the error, which is logged: never with credentials written into it.
    let mut url = url.clone();
    let _ = url.set_username("");
    let _ = url.set_password(None);
    let wildcard = match url.host() {
        Some(url::Host::Ipv4(ip)) if ip.is_unspecified() => Some(("0.0.0.0", "127.0.0.1")),
        Some(url::Host::Ipv6(ip)) if ip.is_unspecified() => Some(("::", "[::1]")),
        _ => None,
    };
    if let Some((wildcard, loopback)) = wildcard {
        let mut local_url = url.clone();
        if local_url.set_host(Some(loopback)).is_ok() {
            return Err(Error::PlaintextToWildcard {
                tool: tool.to_string(),
                url: url.to_string(),
                wildcard: wildcard.to_string(),
                local_url: local_url.to_string(),
            });
        }
    }
    Err(Error::PlaintextToRemoteHost {
        tool: tool.to_string(),
        url: url.to_string(),
    })
}

/// Custom HTTP headers, formatted `Name: Value` and delimited like the HTTP
/// connector's `http_headers`. If the value contains `;`, only `;` separates
/// headers (so values may contain `,`); otherwise `,` separates them.
const MCP_HEADERS_PARAM: &str = "mcp_headers";

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MCPType {
    /// Connect to an MCP server over Streamable HTTP: HTTPS, or HTTP to a loopback host, or HTTP
    /// to any host with `mcp_transport_security: allow_plaintext`.
    StreamableHttp(url::Url),

    /// Uses stdio to communicate with an MCP server. The string is the command to run.
    Stdio(String),
}

impl FromStr for MCPType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let invalid = || Error::InvalidMCPDirective { id: s.to_string() };
        let mcp_type: Self =
            serde_json::from_str(format!("\"{s}\"").as_str()).map_err(|_| invalid())?;
        // A Spicepod dependency's tool whose target starts no program is loaded
        // without trust (`spicepod::host_program`). Such a target that is not a
        // valid URL is refused here, never run as a command, so the trust check
        // holds.
        if matches!(mcp_type, Self::Stdio(_))
            && spicepod::host_program::mcp_target_starts_no_program(s)
        {
            return Err(invalid());
        }
        Ok(mcp_type)
    }
}

#[derive(Clone)]
pub(crate) enum MCPConfig {
    Stdio {
        command: String,
        args: Vec<String>,
        env: HashMap<String, String>,
    },
    StreamableHttp {
        url: url::Url,
        auth_token: Option<SecretString>,
        headers: HashMap<HeaderName, HeaderValue>,
    },
}
impl MCPConfig {
    fn from_type(
        tool: &str,
        mcp_type: &MCPType,
        params: &HashMap<String, SecretString>,
        env: &HashMap<String, SecretString>,
    ) -> Result<Self> {
        let transport_security = params.get(MCP_TRANSPORT_SECURITY_PARAM);
        Ok(match mcp_type.clone() {
            MCPType::Stdio(_) if transport_security.is_some() => {
                return Err(Error::TransportSecurityOnStdio {
                    tool: tool.to_string(),
                });
            }
            MCPType::Stdio(command) => {
                // Shell-word rules: quotes and backslash escapes group arguments.
                let args = shell_words::split(
                    params
                        .get("mcp_args")
                        .map(ExposeSecret::expose_secret)
                        .unwrap_or_default(),
                )
                .context(InvalidMcpArgsSnafu)?;

                let env = env
                    .iter()
                    .map(|(k, v)| (k.clone(), ExposeSecret::expose_secret(v).to_string()))
                    .collect();

                Self::Stdio { command, args, env }
            }
            MCPType::StreamableHttp(url) => {
                let security = match transport_security {
                    None => TransportSecurity::default(),
                    Some(value) => {
                        TransportSecurity::parse(value.expose_secret()).ok_or_else(|| {
                            Error::InvalidTransportSecurity {
                                tool: tool.to_string(),
                                value: value.expose_secret().to_string(),
                            }
                        })?
                    }
                };
                check_transport_security(tool, &url, security)?;
                let auth_token = params.get(MCP_AUTH_TOKEN_PARAM).cloned();
                let mut headers = parse_custom_headers(params);
                if auth_token.is_some() && headers.remove(&AUTHORIZATION).is_some() {
                    tracing::warn!(
                        "Ignoring 'authorization' header from MCP custom headers because '{MCP_AUTH_TOKEN_PARAM}' is configured"
                    );
                }
                Self::StreamableHttp {
                    url,
                    auth_token,
                    headers,
                }
            }
        })
    }
}

fn parse_custom_headers(
    params: &HashMap<String, SecretString>,
) -> HashMap<HeaderName, HeaderValue> {
    let mut custom_headers = HashMap::new();
    let Some(headers) = params.get(MCP_HEADERS_PARAM) else {
        return custom_headers;
    };
    let param_name = MCP_HEADERS_PARAM;

    // Same UX as the HTTP connector's `http_headers` parameter:
    // `Header1: Value1, Header2: Value2` (or semicolon-delimited). Any `;`
    // makes `;` the only delimiter.
    let headers_str = headers.expose_secret();
    let delimiter = if headers_str.contains(';') { ';' } else { ',' };
    for header in headers_str.split(delimiter) {
        let Some((name, value)) = header.split_once(':') else {
            tracing::warn!(
                "Malformed MCP HTTP header in '{param_name}'. Expected format 'Name: Value'. Skipping this header."
            );
            continue;
        };

        let name = name.trim();
        let value = value.trim();
        let Ok(header_name) = HeaderName::try_from(name) else {
            tracing::warn!(
                "Invalid MCP HTTP header name in '{param_name}': '{name}'. Skipping this header."
            );
            continue;
        };
        let Ok(mut header_value) = HeaderValue::from_str(value) else {
            tracing::warn!(
                "Invalid MCP HTTP header value for '{name}' in '{param_name}'. Skipping this header."
            );
            continue;
        };
        header_value.set_sensitive(true);
        custom_headers.insert(header_name, header_value);
    }

    custom_headers
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The dependency gate (`spicepod::host_program`) and this parser decide
    /// the same thing about a `from: mcp:<target>` tool. A target the gate
    /// admits from an untrusted dependency because it starts no program is
    /// connected to over HTTP or refused, and never started as a program: an
    /// HTTP-looking target that is not a valid URL is refused, not run.
    #[test]
    fn a_tool_the_dependency_gate_admits_as_remote_never_starts_a_program() {
        use spicepod::component::tool::Tool;
        use spicepod::host_program::{ComponentRef, host_program};

        // (target, whether the gate sees a program, how the runtime connects:
        // "stdio", "http", or `None` for a refused directive)
        let cases: &[(&str, bool, Option<&str>)] = &[
            ("docker", true, Some("stdio")),
            ("./bin/server", true, Some("stdio")),
            ("https://mcp.example.com/mcp", false, Some("http")),
            ("HTTPS://mcp.example.com/mcp", false, Some("http")),
            (" https://mcp.example.com/mcp", false, Some("http")),
            ("http://localhost:3000/mcp", false, Some("http")),
            ("https://exa mple.com/mcp", false, None),
            ("https://%zz/server", false, None),
            ("http://[::1/mcp", false, None),
            ("https://", false, None),
            ("", false, None),
            ("   ", false, None),
        ];
        for &(target, gate_sees_program, connects) in cases {
            let tool: Tool = serde_json::from_value(serde_json::json!({
                "from": format!("mcp:{target}"),
                "name": "t",
            }))
            .expect("a tool");
            assert_eq!(
                host_program(ComponentRef::Tool(&tool)).is_some(),
                gate_sees_program,
                "the gate's decision for 'mcp:{target}'"
            );
            let parsed = match MCPType::from_str(target) {
                Ok(MCPType::Stdio(_)) => Some("stdio"),
                Ok(MCPType::StreamableHttp(_)) => Some("http"),
                Err(_) => None,
            };
            assert_eq!(
                parsed, connects,
                "how the runtime connects to 'mcp:{target}'"
            );
        }
    }

    /// A directive that does not parse is named without the credentials its URL carries.
    #[test]
    fn invalid_directive_message_leaves_out_url_credentials() {
        let error = MCPType::from_str(r#"https://alice:pa"ss@tools.example.com/sse?token=abc123"#)
            .err()
            .expect("a quote does not parse");
        assert_eq!(
            error.to_string(),
            "Invalid MCP directive 'from: mcp:https://tools.example.com/sse?token=***'"
        );
    }

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

        let cfg =
            MCPConfig::from_type("test_tool", &mcp_type, &params, &env).expect("valid MCP config");
        match cfg {
            MCPConfig::Stdio { command, args, env } => {
                assert_eq!(command, "docker");
                assert_eq!(args, vec!["run", "-i", "--rm", "mcp/fetch"]);
                assert_eq!(env.get("FOO").map(String::as_str), Some("bar"));
            }
            MCPConfig::StreamableHttp { .. } => panic!("expected stdio config"),
        }
    }

    fn http_config(url: &str, params: &[(&str, &str)]) -> Result<MCPConfig> {
        let mcp_type = MCPType::StreamableHttp(url::Url::parse(url).expect("valid url"));
        let params = params
            .iter()
            .map(|(key, value)| ((*key).to_string(), SecretString::from(*value)))
            .collect();
        MCPConfig::from_type("remote_tools", &mcp_type, &params, &HashMap::new())
    }

    /// The signed-off rule (spiceai/spiceai#16479): `http://` only to a loopback host, decided
    /// from the host as written, unless the tool permits plaintext.
    #[test]
    fn plaintext_mcp_is_allowed_only_to_loopback_or_with_permission() {
        for url in [
            "http://localhost:8090/v1/mcp",
            "http://tools.localhost:8090/v1/mcp",
            "http://127.0.0.1:8090/v1/mcp",
            "http://127.1.2.3/v1/mcp",
            "http://[::1]:8090/v1/mcp",
            "https://tools.example.com/mcp",
        ] {
            http_config(url, &[]).unwrap_or_else(|e| panic!("{url} is allowed: {e}"));
        }
        http_config(
            "https://tools.example.com/mcp",
            &[("mcp_transport_security", "allow_plaintext")],
        )
        .expect("a permission an https tool never uses is accepted");
        http_config(
            "http://tools.default.svc:8080/mcp",
            &[("mcp_transport_security", "allow_plaintext")],
        )
        .expect("plaintext is allowed when the tool permits it");

        let message = http_config("http://user:s3cr3t@tools.example.com/mcp", &[])
            .err()
            .expect("credentials in the URL do not make it loopback")
            .to_string();
        assert!(
            message.contains("MCP server URL 'http://tools.example.com/mcp'")
                && !message.contains("s3cr3t")
                && !message.contains("user@"),
            "{message}"
        );

        for url in [
            "http://tools.example.com/mcp",
            "http://127.0.0.1.nip.io/mcp",
        ] {
            let message = http_config(url, &[("mcp_transport_security", "require_tls")])
                .err()
                .unwrap_or_else(|| panic!("{url} is refused"))
                .to_string();
            assert_eq!(
                message,
                format!(
                    "Failed to load tool 'remote_tools': MCP server URL '{url}' uses plaintext HTTP to a host that is not loopback, which would send the tool's credentials and results unencrypted. Use https://, or set `mcp_transport_security: allow_plaintext` on the tool if the network already encrypts this connection. See: https://spiceai.org/docs/components/tools/mcp"
                )
            );
        }
    }

    #[test]
    fn plaintext_mcp_to_a_wildcard_address_names_the_loopback_one() {
        let message = http_config("http://0.0.0.0:8090/v1/mcp", &[])
            .err()
            .expect("the wildcard address is not loopback")
            .to_string();
        assert_eq!(
            message,
            "Failed to load tool 'remote_tools': MCP server URL 'http://0.0.0.0:8090/v1/mcp' uses plaintext HTTP to a host that is not loopback (0.0.0.0 is not a loopback address), which would send the tool's credentials and results unencrypted. Use http://127.0.0.1:8090/v1/mcp for a local server, https:// for a remote one, or set `mcp_transport_security: allow_plaintext` on the tool if the network already encrypts this connection. See: https://spiceai.org/docs/components/tools/mcp"
        );
        let message = http_config("http://[::]:8090/v1/mcp", &[])
            .err()
            .expect("the IPv6 wildcard address is not loopback")
            .to_string();
        assert!(
            message.contains("(:: is not a loopback address)")
                && message.contains("Use http://[::1]:8090/v1/mcp for a local server"),
            "{message}"
        );
    }

    #[test]
    fn transport_security_is_refused_on_a_stdio_tool_and_checked_for_its_values() {
        let mcp_type = MCPType::Stdio("npx".to_string());
        let params = HashMap::from([(
            "mcp_transport_security".to_string(),
            SecretString::from("allow_plaintext"),
        )]);
        let message = MCPConfig::from_type("maps", &mcp_type, &params, &HashMap::new())
            .err()
            .expect("a stdio tool takes no transport security")
            .to_string();
        assert_eq!(
            message,
            "Failed to load tool 'maps': `mcp_transport_security` applies only to an MCP server reached over HTTP (`mcp:http://` or `mcp:https://`), and tool 'maps' runs a local program. Remove `mcp_transport_security` from tool 'maps'. See: https://spiceai.org/docs/components/tools/mcp"
        );

        let error = http_config(
            "https://tools.example.com/mcp",
            &[("mcp_transport_security", "plaintext")],
        )
        .err()
        .expect("an unknown value is refused");
        assert!(error.is_configuration_error());
        assert_eq!(
            error.to_string(),
            "Failed to load tool 'remote_tools': `mcp_transport_security` is 'plaintext', and it takes `require_tls` or `allow_plaintext`. See: https://spiceai.org/docs/components/tools/mcp"
        );
    }

    #[test]
    fn mcp_config_from_https_preserves_url() {
        let url = url::Url::parse("https://example.com/v1/mcp").expect("valid url");
        let mcp_type = MCPType::StreamableHttp(url.clone());
        let cfg = MCPConfig::from_type("test_tool", &mcp_type, &HashMap::new(), &HashMap::new())
            .expect("valid MCP config");
        match cfg {
            MCPConfig::StreamableHttp {
                url: u,
                auth_token,
                headers,
            } => {
                assert_eq!(u, url);
                assert!(auth_token.is_none());
                assert!(headers.is_empty());
            }
            MCPConfig::Stdio { .. } => panic!("expected https config"),
        }
    }

    #[test]
    fn mcp_config_from_https_collects_auth_token() {
        let url = url::Url::parse("https://example.com/v1/mcp").expect("valid url");
        let mcp_type = MCPType::StreamableHttp(url);
        let mut params = HashMap::new();
        params.insert(
            "mcp_auth_token".to_string(),
            SecretString::from("test-api-key"),
        );

        let cfg = MCPConfig::from_type("test_tool", &mcp_type, &params, &HashMap::new())
            .expect("valid MCP config");
        match cfg {
            MCPConfig::StreamableHttp {
                auth_token,
                headers,
                ..
            } => {
                assert_eq!(
                    auth_token.as_ref().map(ExposeSecret::expose_secret),
                    Some("test-api-key")
                );
                assert!(headers.is_empty());
            }
            MCPConfig::Stdio { .. } => panic!("expected https config"),
        }
    }

    #[test]
    fn mcp_config_from_https_collects_custom_headers() {
        let url = url::Url::parse("https://example.com/v1/mcp").expect("valid url");
        let mcp_type = MCPType::StreamableHttp(url);
        let mut params = HashMap::new();
        params.insert(
            MCP_HEADERS_PARAM.to_string(),
            SecretString::from("X-API-Key: test-api-key, X-Tenant: acme"),
        );

        let cfg = MCPConfig::from_type("test_tool", &mcp_type, &params, &HashMap::new())
            .expect("valid MCP config");
        match cfg {
            MCPConfig::StreamableHttp { headers, .. } => {
                assert_eq!(
                    headers
                        .get(&HeaderName::from_static("x-api-key"))
                        .and_then(|value| value.to_str().ok()),
                    Some("test-api-key")
                );
                assert_eq!(
                    headers
                        .get(&HeaderName::from_static("x-tenant"))
                        .and_then(|value| value.to_str().ok()),
                    Some("acme")
                );
            }
            MCPConfig::Stdio { .. } => panic!("expected https config"),
        }
    }

    #[test]
    fn mcp_auth_token_removes_custom_authorization_header() {
        let url = url::Url::parse("https://example.com/v1/mcp").expect("valid url");
        let mcp_type = MCPType::StreamableHttp(url);
        let mut params = HashMap::new();
        params.insert(
            MCP_AUTH_TOKEN_PARAM.to_string(),
            SecretString::from("test-api-key"),
        );
        params.insert(
            MCP_HEADERS_PARAM.to_string(),
            SecretString::from("Authorization: Basic abc, X-Tenant: acme"),
        );

        let cfg = MCPConfig::from_type("test_tool", &mcp_type, &params, &HashMap::new())
            .expect("valid MCP config");
        match cfg {
            MCPConfig::StreamableHttp { headers, .. } => {
                assert!(!headers.contains_key(&AUTHORIZATION));
                assert_eq!(
                    headers
                        .get(&HeaderName::from_static("x-tenant"))
                        .and_then(|value| value.to_str().ok()),
                    Some("acme")
                );
            }
            MCPConfig::Stdio { .. } => panic!("expected https config"),
        }
    }
}
