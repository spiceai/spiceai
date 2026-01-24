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

//! `spice trace` command - Return traces for operations in Spice.

use crate::RuntimeContext;
use crate::error::{InvalidArgumentSnafu, InvalidResponseSnafu, Result};
use crate::output::TableOutput;
use chrono::{DateTime, Utc};
use clap::{Args, ValueEnum};
use serde::Deserialize;
use snafu::ensure;
use std::collections::HashMap;

/// Supported trace task types.
const SUPPORTED_TRACE_TASKS: &[&str] = &[
    "ai",
    "ai_chat",
    "accelerated_refresh",
    "ai_completion",
    "eval_run",
    "nsql",
    "sql_query",
    "tool_use::search",
    "tool_use::list_datasets",
    "tool_use::load_memory",
    "tool_use::sample_data",
    "tool_use::sql",
    "tool_use::store_memory",
    "tool_use::table_schema",
    "search",
    "scheduled_worker",
    "text_embed",
];

/// Output format for trace command.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum OutputFormat {
    /// Display results as a table (default)
    #[default]
    Table,
    /// Output the SQL query that would be executed
    Sql,
}

/// Arguments for the `trace` command.
#[derive(Args, Debug)]
#[command(
    about = "Return a user friendly trace into an operation that occurred in Spice",
    long_about = format!(
        "Return a user friendly trace into an operation that occurred in Spice\n\n\
        Available operations:\n  {}",
        SUPPORTED_TRACE_TASKS.join(", ")
    )
)]
pub struct TraceArgs {
    /// The task type to trace
    pub task: String,

    /// Return the trace with the given id
    #[arg(long)]
    pub id: Option<String>,

    /// Return the trace with the given trace id
    #[arg(long)]
    pub trace_id: Option<String>,

    /// Include input data in the trace
    #[arg(long)]
    pub include_input: bool,

    /// Include output data in the trace
    #[arg(long)]
    pub include_output: bool,

    /// Truncate input/output data (default 80 when set without value)
    #[arg(long, default_missing_value = "80")]
    pub truncate: Option<usize>,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub output: OutputFormat,
}

/// Task history record from the runtime.
#[derive(Debug, Deserialize)]
struct TaskHistory {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    task: String,
    input: String,
    captured_output: Option<String>,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    execution_duration_ms: f64,
    error_message: Option<String>,
    labels: HashMap<String, String>,
}

/// Tree node for trace hierarchy.
struct TreeNode {
    task_history: TaskHistory,
    children: Vec<TreeNode>,
}

/// Row for display with tree prefix.
struct TaskHistoryRow {
    tree: String,
    task: TaskHistory,
}

/// Execute the `trace` command.
///
/// # Errors
///
/// Returns an error if the task type is invalid or the API request fails.
pub async fn execute(ctx: &RuntimeContext, args: &TraceArgs) -> Result<()> {
    // Validate task type
    ensure!(
        is_valid_trace_task(&args.task),
        InvalidArgumentSnafu {
            message: format!(
                "invalid trace type '{}'. Available: {}",
                args.task,
                SUPPORTED_TRACE_TASKS.join(", ")
            )
        }
    );

    // Build the SQL query filter
    let filter = get_trace_filter(&args.task, args.id.as_deref(), args.trace_id.as_deref());
    let sql_query =
        format!("SELECT * FROM runtime.task_history WHERE {filter} ORDER BY start_time asc");

    // If SQL output requested, just print the query
    if matches!(args.output, OutputFormat::Sql) {
        println!("{sql_query}");
        return Ok(());
    }

    // Execute SQL query
    let traces = sql_request_to_traces(ctx, &sql_query).await?;

    if traces.is_empty() {
        eprintln!("Error: No events found");
        return Ok(());
    }

    // Build tree and display
    let rows = tree_rows_from_traces(traces);
    display_trace_table(
        &rows,
        args.include_input,
        args.include_output,
        args.truncate,
    );

    Ok(())
}

fn is_valid_trace_task(task: &str) -> bool {
    SUPPORTED_TRACE_TASKS.contains(&task)
}

/// Quote a SQL string value (escape single quotes).
fn quote_sql_string(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

/// Build the SQL filter based on provided arguments.
fn get_trace_filter(task: &str, id: Option<&str>, trace_id: Option<&str>) -> String {
    if let Some(id) = id {
        format!(
            "trace_id=(SELECT trace_id from runtime.task_history where labels.id={})",
            quote_sql_string(id)
        )
    } else if let Some(trace_id) = trace_id {
        format!("trace_id={}", quote_sql_string(trace_id))
    } else {
        // Use last trace by default
        format!(
            "trace_id=(SELECT trace_id from runtime.task_history where task={} order by start_time desc limit 1)",
            quote_sql_string(task)
        )
    }
}

/// Execute SQL query and parse results into `TaskHistory` records.
///
/// # Errors
///
/// Returns an error if the HTTP request fails or the response cannot be parsed.
async fn sql_request_to_traces(ctx: &RuntimeContext, sql: &str) -> Result<Vec<TaskHistory>> {
    let url = format!("{}/v1/sql", ctx.http_endpoint());

    let mut request = ctx
        .http_client()
        .post(&url)
        .header("Content-Type", "text/plain")
        .header("Accept", "application/json")
        .body(sql.to_string());

    for (key, value) in ctx.get_headers() {
        request = request.header(&key, &value);
    }

    let response = request
        .send()
        .await
        .map_err(|e| crate::error::Error::ConnectionFailed {
            endpoint: url.clone(),
            source: e,
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        // Special case for task_history disabled
        if text.contains("table 'spice.runtime.task_history' not found") {
            eprintln!(
                "Trace functionality requires task history, which is disabled. Set `runtime.task_history: true` in the Spicepod YAML file and retry. Details: https://spiceai.org/docs/reference/spicepod/runtime#runtimetask_history"
            );
            return Ok(vec![]);
        }

        return Err(InvalidResponseSnafu {
            message: format!("SQL request failed with status {status}: {text}"),
        }
        .build());
    }

    let text = response.text().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to read response: {e}"),
        }
        .build()
    })?;

    if text.is_empty() {
        return Ok(vec![]);
    }

    serde_json::from_str(&text).map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse response: {e}"),
        }
        .build()
    })
}

/// Recursively build a tree node from the node map.
fn build_tree_node(
    span_id: &str,
    node_map: &mut HashMap<String, TreeNode>,
    children_by_parent: &HashMap<String, Vec<String>>,
) -> Option<TreeNode> {
    let mut node = node_map.remove(span_id)?;

    if let Some(child_ids) = children_by_parent.get(span_id) {
        let mut children: Vec<TreeNode> = child_ids
            .iter()
            .filter_map(|child_id| build_tree_node(child_id, node_map, children_by_parent))
            .collect();

        // Sort children by start time
        children.sort_by(|a, b| a.task_history.start_time.cmp(&b.task_history.start_time));

        node.children = children;
    }

    Some(node)
}

/// Build a hierarchical tree from `TaskHistory` entries.
fn build_trace_tree(tasks: Vec<TaskHistory>) -> Option<TreeNode> {
    if tasks.is_empty() {
        return None;
    }

    // Create a lookup map for SpanID -> Node
    let mut node_map: HashMap<String, TreeNode> = tasks
        .into_iter()
        .map(|task| {
            let span_id = task.span_id.clone();
            (
                span_id,
                TreeNode {
                    task_history: task,
                    children: Vec::new(),
                },
            )
        })
        .collect();

    // Find root and build parent-child relationships
    let mut root_span_id: Option<String> = None;
    let parent_child_pairs: Vec<(String, String)> = node_map
        .values()
        .filter_map(|node| {
            if let Some(parent_id) = &node.task_history.parent_span_id {
                Some((parent_id.clone(), node.task_history.span_id.clone()))
            } else {
                root_span_id = Some(node.task_history.span_id.clone());
                None
            }
        })
        .collect();

    // Group children by parent
    let mut children_by_parent: HashMap<String, Vec<String>> = HashMap::new();
    for (parent_id, child_id) in parent_child_pairs {
        children_by_parent
            .entry(parent_id)
            .or_default()
            .push(child_id);
    }

    root_span_id.and_then(|root_id| build_tree_node(&root_id, &mut node_map, &children_by_parent))
}

/// Convert trace tree to display rows with tree formatting.
fn tree_rows_from_traces(traces: Vec<TaskHistory>) -> Vec<TaskHistoryRow> {
    let Some(tree) = build_trace_tree(traces) else {
        return vec![];
    };

    let mut rows = Vec::new();
    recurse_through_tree(&mut rows, &tree, "", true);
    rows
}

/// Recursively traverse tree and build formatted rows.
fn recurse_through_tree(
    rows: &mut Vec<TaskHistoryRow>,
    node: &TreeNode,
    indent: &str,
    is_last: bool,
) {
    let connector = if indent.is_empty() {
        ""
    } else if is_last {
        "└── "
    } else {
        "├── "
    };

    rows.push(TaskHistoryRow {
        tree: format!("{indent}{connector}{}", node.task_history.task),
        task: TaskHistory {
            trace_id: node.task_history.trace_id.clone(),
            span_id: node.task_history.span_id.clone(),
            parent_span_id: node.task_history.parent_span_id.clone(),
            task: node.task_history.task.clone(),
            input: node.task_history.input.clone(),
            captured_output: node.task_history.captured_output.clone(),
            start_time: node.task_history.start_time,
            end_time: node.task_history.end_time,
            execution_duration_ms: node.task_history.execution_duration_ms,
            error_message: node.task_history.error_message.clone(),
            labels: node.task_history.labels.clone(),
        },
    });

    let new_indent = if indent.is_empty() {
        String::new()
    } else if is_last {
        format!("{indent}  ")
    } else {
        format!("{indent}│ ")
    };

    for (i, child) in node.children.iter().enumerate() {
        let child_is_last = i == node.children.len() - 1;
        recurse_through_tree(rows, child, &new_indent, child_is_last);
    }
}

/// Display trace results as a table.
fn display_trace_table(
    rows: &[TaskHistoryRow],
    include_input: bool,
    include_output: bool,
    truncate: Option<usize>,
) {
    let mut headers = vec!["Tree", "Status", "Duration", "Span ID"];

    if include_input {
        headers.push("Input");
    }
    if include_output {
        headers.push("Output");
    }

    let mut table = TableOutput::new(headers);

    for row in rows {
        let status = if row.task.error_message.is_none()
            || row
                .task
                .error_message
                .as_ref()
                .is_some_and(String::is_empty)
        {
            "OK".to_string()
        } else {
            "ERR".to_string()
        };

        let duration = format!("{:>8.2}ms", row.task.execution_duration_ms);

        let mut values = vec![row.tree.clone(), status, duration, row.task.span_id.clone()];

        if include_input {
            let input = truncate_string(&row.task.input, truncate);
            values.push(input);
        }

        if include_output {
            let output = row
                .task
                .captured_output
                .as_ref()
                .map_or_else(|| "<empty>".to_string(), |o| truncate_string(o, truncate));
            values.push(output);
        }

        table.add_row(values);
    }

    println!("{table}");
}

/// Truncate a string with an indicator of how many characters were omitted.
fn truncate_string(s: &str, max_len: Option<usize>) -> String {
    if s.is_empty() {
        return "<empty>".to_string();
    }

    match max_len {
        Some(max) if s.len() > max => {
            let omitted = s.len() - max;
            format!("{}... ({omitted} characters omitted)", &s[..max])
        }
        _ => s.to_string(),
    }
}
