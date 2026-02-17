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

use std::io::{self, BufRead, Write};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use test_framework::anyhow;

use crate::args::StdioArgs;

const JSONRPC_VERSION: &str = "2.0";

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;
const COMMAND_FAILED: i64 = -32001;

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Option<Value>,
    #[serde(default)]
    id: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

pub fn run_stdio_server(args: &StdioArgs) -> anyhow::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout());

    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        if args.verbose {
            eprintln!("[stdio] <= {line}");
        }

        let parsed = serde_json::from_str::<JsonRpcRequest>(line);
        let request = match parsed {
            Ok(request) => request,
            Err(error) => {
                let response = JsonRpcResponse {
                    jsonrpc: JSONRPC_VERSION,
                    id: Value::Null,
                    result: None,
                    error: Some(JsonRpcError {
                        code: PARSE_ERROR,
                        message: "Parse error".to_string(),
                        data: Some(json!({"details": error.to_string()})),
                    }),
                };
                write_response(&mut stdout, &response)?;
                continue;
            }
        };

        let is_notification = request.id.is_none();
        let response = handle_request(request, args.verbose);

        if is_notification {
            continue;
        }

        write_response(&mut stdout, &response)?;
    }

    Ok(())
}

fn write_response(
    stdout: &mut io::BufWriter<io::Stdout>,
    response: &JsonRpcResponse,
) -> anyhow::Result<()> {
    let line = serde_json::to_string(response)?;
    writeln!(stdout, "{line}")?;
    stdout.flush()?;
    Ok(())
}

fn handle_request(request: JsonRpcRequest, verbose: bool) -> JsonRpcResponse {
    let id = request.id.clone().unwrap_or(Value::Null);

    if request.jsonrpc != JSONRPC_VERSION {
        return error_response(id, INVALID_REQUEST, "Invalid Request", None);
    }

    if request.method == "rpc.methods" {
        return ok_response(id, json!({ "methods": supported_methods() }));
    }

    let method_args = match command_prefix(&request.method) {
        Some(command) => command,
        None => return error_response(id, METHOD_NOT_FOUND, "Method not found", None),
    };

    let rpc_args = match parse_args(&request.params) {
        Ok(args) => args,
        Err(message) => {
            return error_response(
                id,
                INVALID_PARAMS,
                "Invalid params",
                Some(json!({"details": message})),
            );
        }
    };

    if verbose {
        eprintln!(
            "[stdio] executing: spidapter {} {}",
            method_args.join(" "),
            rpc_args.join(" ")
        );
    }

    match execute_child(&method_args, &rpc_args) {
        Ok(output) => ok_response(id, output),
        Err(CommandExecutionError::Failed {
            exit_code,
            stdout,
            stderr,
        }) => error_response(
            id,
            COMMAND_FAILED,
            "Command failed",
            Some(json!({
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "command": method_args,
                "args": rpc_args,
            })),
        ),
        Err(CommandExecutionError::System(error)) => error_response(
            id,
            INTERNAL_ERROR,
            "Internal error",
            Some(json!({"details": error.to_string()})),
        ),
    }
}

fn ok_response(id: Value, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION,
        id,
        result: Some(result),
        error: None,
    }
}

fn error_response(id: Value, code: i64, message: &str, data: Option<Value>) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION,
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.to_string(),
            data,
        }),
    }
}

fn parse_args(params: &Option<Value>) -> Result<Vec<String>, String> {
    let Some(params) = params else {
        return Ok(Vec::new());
    };

    match params {
        Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(ToString::to_string)
                    .ok_or_else(|| "params array must contain only strings".to_string())
            })
            .collect(),
        Value::Object(object) => {
            let Some(args) = object.get("args") else {
                return Ok(Vec::new());
            };

            let Value::Array(values) = args else {
                return Err("params.args must be an array of strings".to_string());
            };

            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(ToString::to_string)
                        .ok_or_else(|| "params.args must contain only strings".to_string())
                })
                .collect()
        }
        _ => {
            Err("params must be an object with optional args array or a raw args array".to_string())
        }
    }
}

fn command_prefix(method: &str) -> Option<Vec<String>> {
    let command = match method {
        "dispatch" => vec!["dispatch"],

        "run.throughput" => vec!["run", "throughput"],
        "run.load" => vec!["run", "load"],
        "run.bench" => vec!["run", "bench"],
        "run.data_consistency" => vec!["run", "data-consistency"],
        "run.evals" => vec!["run", "evals"],
        "run.search" => vec!["run", "search"],
        "run.query" => vec!["run", "query"],
        "run.text_to_sql" => vec!["run", "text-to-sql"],
        "run.streaming_dynamodb" => vec!["run", "streaming-dynamodb"],
        "run.streaming_dynamodb_dispatch" => vec!["run", "streaming-dynamodb-dispatch"],

        "export.throughput" => vec!["export", "throughput"],
        "export.load" => vec!["export", "load"],
        "export.bench" => vec!["export", "bench"],
        "export.data_consistency" => vec!["export", "data-consistency"],
        "export.evals" => vec!["export", "evals"],
        "export.search" => vec!["export", "search"],
        "export.text_to_sql" => vec!["export", "text-to-sql"],

        #[cfg(feature = "append")]
        "run.append" => vec!["run", "append"],
        #[cfg(feature = "append")]
        "export.append" => vec!["export", "append"],

        _ => return None,
    };

    Some(command.into_iter().map(ToString::to_string).collect())
}

fn supported_methods() -> Vec<&'static str> {
    let mut methods = vec![
        "rpc.methods",
        "dispatch",
        "run.throughput",
        "run.load",
        "run.bench",
        "run.data_consistency",
        "run.evals",
        "run.search",
        "run.query",
        "run.text_to_sql",
        "run.streaming_dynamodb",
        "run.streaming_dynamodb_dispatch",
        "export.throughput",
        "export.load",
        "export.bench",
        "export.data_consistency",
        "export.evals",
        "export.search",
        "export.text_to_sql",
    ];

    #[cfg(feature = "append")]
    {
        methods.push("run.append");
        methods.push("export.append");
    }

    methods
}

enum CommandExecutionError {
    Failed {
        exit_code: i32,
        stdout: String,
        stderr: String,
    },
    System(anyhow::Error),
}

fn execute_child(command: &[String], args: &[String]) -> Result<Value, CommandExecutionError> {
    let current_exe =
        std::env::current_exe().map_err(|e| CommandExecutionError::System(e.into()))?;

    let output = Command::new(current_exe)
        .args(command)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| CommandExecutionError::System(e.into()))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if output.status.success() {
        return Ok(json!({
            "exit_code": output.status.code().unwrap_or(0),
            "stdout": stdout,
            "stderr": stderr,
            "success": true,
        }));
    }

    Err(CommandExecutionError::Failed {
        exit_code: output.status.code().unwrap_or(-1),
        stdout,
        stderr,
    })
}
