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

//! Acceleration command implementation - manage dataset acceleration features.

use crate::context::RuntimeContext;
use crate::error::{self, InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use crate::output::{OutputFormat, TableRow, write_json, write_table};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};

/// Arguments for the acceleration command.
#[derive(Args, Debug)]
#[command(
    about = "Manage dataset acceleration features",
    long_about = r#"Commands for managing accelerated datasets including snapshots.

Use subcommands to list snapshots, view snapshot details, pin a dataset to
a snapshot, and unpin it.

Examples:
  # List all snapshots for a dataset
  spice acceleration snapshots taxi_trips

  # Get details of a specific snapshot
  spice acceleration snapshot taxi_trips 3

  # Pin the dataset to snapshot 41: every instance serves it
  spice acceleration pin taxi_trips 41

  # Unpin the dataset: every instance serves the newest snapshot again
  spice acceleration unpin taxi_trips"#
)]
pub struct AccelerationArgs {
    #[command(subcommand)]
    pub command: AccelerationCommand,
}

#[derive(Subcommand, Debug)]
pub enum AccelerationCommand {
    /// List all acceleration snapshots for a dataset
    Snapshots(SnapshotsArgs),

    /// Get details of a specific acceleration snapshot
    Snapshot(SnapshotArgs),

    /// Pin the dataset to a snapshot: every instance serves it, and newer
    /// snapshots are published but not served, until the dataset is unpinned
    Pin(PinArgs),

    /// Unpin the dataset: every instance serves the newest snapshot again
    Unpin(UnpinArgs),

    /// Deprecated: use `pin`. Pins the dataset to a snapshot, as `pin` does
    SetSnapshot(PinArgs),
}

/// Arguments for the snapshots subcommand.
#[derive(Args, Debug)]
pub struct SnapshotsArgs {
    /// The dataset name
    pub dataset: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,

    /// Maximum number of snapshots to return (most recent first)
    #[arg(long, default_value = "10")]
    pub limit: usize,
}

/// Arguments for the snapshot subcommand.
#[derive(Args, Debug)]
pub struct SnapshotArgs {
    /// The dataset name
    pub dataset: String,

    /// The snapshot ID
    pub snapshot_id: u64,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

/// Arguments for the pin and set-snapshot subcommands.
#[derive(Args, Debug)]
pub struct PinArgs {
    /// The dataset name
    pub dataset: String,

    /// The snapshot ID to pin the dataset to
    pub snapshot_id: u64,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

/// Arguments for the unpin subcommand.
#[derive(Args, Debug)]
pub struct UnpinArgs {
    /// The dataset name
    pub dataset: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

/// Snapshot information from the API.
#[derive(Debug, Deserialize, Serialize)]
pub struct SnapshotInfo {
    pub snapshot_id: u64,
    pub timestamp_ms: i64,
    pub location: String,
    pub checksum: String,
    pub checksum_algorithm: String,
    pub size_bytes: u64,
    pub engine: Option<String>,
    pub row_count: Option<u64>,
    pub is_current: bool,
    pub status: String,
}

/// Snapshot summary from the API.
#[derive(Debug, Deserialize, Serialize)]
pub struct SnapshotSummary {
    pub dataset_name: String,
    pub location: String,
    pub last_updated_ms: i64,
    pub engine: Option<String>,
    pub current_snapshot_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_snapshot_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newer_snapshots: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instances: Vec<InstanceStatus>,
    pub snapshots: Vec<SnapshotInfo>,
}

/// What one instance serves of the dataset's pin, from the API.
#[derive(Debug, Deserialize, Serialize)]
pub struct InstanceStatus {
    pub instance_id: String,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serves_snapshot_id: Option<u64>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl TableRow for InstanceStatus {
    fn headers() -> Vec<&'static str> {
        vec!["INSTANCE", "SERVES", "STATUS"]
    }

    fn values(&self) -> Vec<String> {
        let status = match (self.status.as_str(), &self.reason) {
            ("pinned", _) => "pinned".to_string(),
            (_, Some(reason)) => format!("not pinned: {reason}"),
            (_, None) => "not pinned".to_string(),
        };
        vec![
            format!("{} ({})", self.instance_id, self.role),
            self.serves_snapshot_id
                .map_or_else(|| "live".to_string(), |id| id.to_string()),
            status,
        ]
    }
}

/// Request to pin a dataset to a snapshot.
#[derive(Debug, Serialize)]
struct PinSnapshotRequest {
    snapshot_id: u64,
}

/// Generic message response.
#[derive(Debug, Deserialize, Serialize)]
struct MessageResponse {
    message: String,
}

/// Row for snapshot table display.
struct SnapshotTableRow {
    id: u64,
    timestamp: String,
    size: String,
    rows: String,
    checksum: String,
    current: String,
    status: String,
}

impl TableRow for SnapshotTableRow {
    fn headers() -> Vec<&'static str> {
        vec![
            "ID",
            "TIMESTAMP",
            "SIZE",
            "ROWS",
            "CHECKSUM",
            "CURRENT",
            "STATUS",
        ]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.timestamp.clone(),
            self.size.clone(),
            self.rows.clone(),
            self.checksum.clone(),
            self.current.clone(),
            self.status.clone(),
        ]
    }
}

/// Execute the acceleration command.
pub async fn execute(ctx: &RuntimeContext, args: &AccelerationArgs) -> Result<()> {
    if ctx.is_cloud() {
        tracing::error!("`spice acceleration` does not support `--cloud`.");
        return Ok(());
    }

    match &args.command {
        AccelerationCommand::Snapshots(snap_args) => execute_snapshots(ctx, snap_args).await,
        AccelerationCommand::Snapshot(snap_args) => execute_snapshot(ctx, snap_args).await,
        AccelerationCommand::Pin(pin_args) => execute_pin(ctx, pin_args).await,
        AccelerationCommand::Unpin(unpin_args) => execute_unpin(ctx, unpin_args).await,
        AccelerationCommand::SetSnapshot(pin_args) => execute_set_snapshot(ctx, pin_args).await,
    }
}

/// Execute the snapshots subcommand.
async fn execute_snapshots(ctx: &RuntimeContext, args: &SnapshotsArgs) -> Result<()> {
    let url = format!(
        "/v1/datasets/{}/acceleration/snapshots?limit={}",
        args.dataset, args.limit
    );

    let response = ctx.get(&url).await.map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    // Check for authentication errors before other status handling
    let response = error::check_response(response, ctx.http_endpoint()).await?;

    let summary: SnapshotSummary = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse snapshots response: {e}"),
        }
        .build()
    })?;

    if matches!(args.output, OutputFormat::Json) {
        return write_json(&summary);
    }

    if summary.snapshots.is_empty() {
        println!("No snapshots found for dataset {}", args.dataset);
        println!("Location: {}", summary.location);
        return Ok(());
    }

    println!("Dataset: {}", summary.dataset_name);
    println!("Location: {}", summary.location);
    if let Some(ref engine) = summary.engine {
        println!("Engine: {engine}");
    }
    let last_updated = format_timestamp_ms(summary.last_updated_ms);
    println!("Last Updated: {last_updated}");
    if let Some(current_id) = summary.current_snapshot_id {
        println!("Current Snapshot ID: {current_id}");
    }
    for line in pin_lines(&summary) {
        println!("{line}");
    }
    write_table(&summary.instances);
    println!();

    // Convert to table rows
    let rows: Vec<SnapshotTableRow> = summary
        .snapshots
        .iter()
        .map(|s| SnapshotTableRow {
            id: s.snapshot_id,
            timestamp: format_timestamp_ms(s.timestamp_ms),
            size: format_bytes(s.size_bytes),
            rows: s.row_count.map_or("-".to_string(), format_number),
            checksum: truncate_checksum(&s.checksum),
            current: if s.is_current {
                "✓".to_string()
            } else {
                String::new()
            },
            status: s.status.clone(),
        })
        .collect();

    write_table(&rows);

    Ok(())
}

/// Execute the snapshot subcommand.
async fn execute_snapshot(ctx: &RuntimeContext, args: &SnapshotArgs) -> Result<()> {
    let url = format!(
        "/v1/datasets/{}/acceleration/snapshots/{}",
        args.dataset, args.snapshot_id
    );

    let response = ctx.get(&url).await.map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    let response = error::check_response(response, ctx.http_endpoint()).await?;

    let snapshot: SnapshotInfo = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse snapshot response: {e}"),
        }
        .build()
    })?;

    if matches!(args.output, OutputFormat::Json) {
        return write_json(&snapshot);
    }

    println!("Snapshot ID: {}", snapshot.snapshot_id);
    println!("Timestamp: {}", format_timestamp_ms(snapshot.timestamp_ms));
    println!("Location: {}", snapshot.location);
    if let Some(ref engine) = snapshot.engine {
        println!("Engine: {engine}");
    }
    println!(
        "Size: {} ({} bytes)",
        format_bytes(snapshot.size_bytes),
        snapshot.size_bytes
    );
    if let Some(row_count) = snapshot.row_count {
        println!("Rows: {}", format_number(row_count));
    }
    println!(
        "Checksum ({}): {}",
        snapshot.checksum_algorithm, snapshot.checksum
    );
    println!("Is Current: {}", snapshot.is_current);

    Ok(())
}

/// Execute the pin subcommand.
async fn execute_pin(ctx: &RuntimeContext, args: &PinArgs) -> Result<()> {
    let url = format!("/v1/datasets/{}/acceleration/snapshots/pin", args.dataset);
    let request = PinSnapshotRequest {
        snapshot_id: args.snapshot_id,
    };
    print_message(ctx, ctx.put_json(&url, &request).await, args.output).await
}

/// Execute the deprecated set-snapshot subcommand.
async fn execute_set_snapshot(ctx: &RuntimeContext, args: &PinArgs) -> Result<()> {
    tracing::warn!(
        "`spice acceleration set-snapshot` is deprecated: use `spice acceleration pin {} {}`.",
        args.dataset,
        args.snapshot_id
    );
    let url = format!(
        "/v1/datasets/{}/acceleration/snapshots/current",
        args.dataset
    );
    let request = PinSnapshotRequest {
        snapshot_id: args.snapshot_id,
    };
    print_message(ctx, ctx.post_json(&url, &request).await, args.output).await
}

/// Execute the unpin subcommand.
async fn execute_unpin(ctx: &RuntimeContext, args: &UnpinArgs) -> Result<()> {
    let url = format!("/v1/datasets/{}/acceleration/snapshots/pin", args.dataset);
    print_message(ctx, ctx.delete(&url).await, args.output).await
}

/// Prints the message the runtime answered `response` with.
async fn print_message(
    ctx: &RuntimeContext,
    response: Result<reqwest::Response>,
    output: OutputFormat,
) -> Result<()> {
    let response = response.map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    let response = error::check_response(response, ctx.http_endpoint()).await?;

    let reply: serde_json::Value = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse response: {e}"),
        }
        .build()
    })?;

    if matches!(output, OutputFormat::Json) {
        return write_json(&reply);
    }

    let result: MessageResponse = serde_json::from_value(reply).map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse response: {e}"),
        }
        .build()
    })?;
    tracing::info!("{}", result.message);

    Ok(())
}

/// The lines that describe the dataset's pin, when it is pinned.
fn pin_lines(summary: &SnapshotSummary) -> Vec<String> {
    let Some(pinned_id) = summary.pinned_snapshot_id else {
        return Vec::new();
    };
    let since = summary
        .pinned_at
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|at| format!(", since {}", at.format("%Y-%m-%d %H:%M UTC")))
        .unwrap_or_default();
    let mut lines = vec![format!(
        "Pinned Snapshot ID: {pinned_id}{since} (unpin with `spice acceleration unpin`)"
    )];
    if let Some(newer) = summary.newer_snapshots {
        lines.push(format!("Newer snapshots not served: {newer}"));
    }
    lines
}

/// Format a timestamp in milliseconds as an RFC3339 string.
fn format_timestamp_ms(ms: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    let duration = Duration::from_millis(ms as u64);
    let datetime = UNIX_EPOCH + duration;

    // Format as RFC3339-like string
    let secs = datetime
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let naive =
        chrono::DateTime::from_timestamp(secs as i64, 0).unwrap_or(chrono::DateTime::UNIX_EPOCH);
    naive.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Format bytes as a human-readable string.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Format a number with comma separators.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}

/// Truncate a checksum for display.
fn truncate_checksum(checksum: &str) -> String {
    if checksum.len() > 16 {
        format!("{}...", &checksum[..16])
    } else {
        checksum.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(pinned: Option<u64>, pinned_at: Option<i64>, newer: Option<usize>) -> SnapshotSummary {
        SnapshotSummary {
            dataset_name: "taxi_trips".to_string(),
            location: "s3://my-bucket/spice/snapshots".to_string(),
            last_updated_ms: 0,
            engine: None,
            current_snapshot_id: Some(41),
            pinned_snapshot_id: pinned,
            pinned_at,
            newer_snapshots: newer,
            instances: Vec::new(),
            snapshots: Vec::new(),
        }
    }

    #[test]
    fn a_pinned_dataset_shows_when_it_was_pinned_and_the_snapshots_not_served() {
        // 2026-10-16 10:05 UTC.
        let lines = pin_lines(&summary(Some(41), Some(1_792_145_100_000), Some(3)));
        assert_eq!(
            lines,
            vec![
                "Pinned Snapshot ID: 41, since 2026-10-16 10:05 UTC (unpin with `spice acceleration unpin`)",
                "Newer snapshots not served: 3",
            ]
        );
    }

    #[test]
    fn an_instance_shows_the_snapshot_it_serves_and_why_it_does_not_serve_the_pin() {
        let instance = |serves, status: &str, reason: Option<&str>| InstanceStatus {
            instance_id: "reader-1".to_string(),
            role: "this".to_string(),
            serves_snapshot_id: serves,
            status: status.to_string(),
            reason: reason.map(ToString::to_string),
        };
        assert_eq!(
            instance(Some(41), "pinned", None).values(),
            vec!["reader-1 (this)", "41", "pinned"]
        );
        assert_eq!(
            instance(
                Some(44),
                "not_pinned",
                Some("not enough disk for snapshot 41 (2.1 GiB needed, 1.4 GiB free)")
            )
            .values(),
            vec![
                "reader-1 (this)",
                "44",
                "not pinned: not enough disk for snapshot 41 (2.1 GiB needed, 1.4 GiB free)"
            ]
        );
        assert_eq!(
            instance(None, "not_pinned", None).values(),
            vec!["reader-1 (this)", "live", "not pinned"]
        );
    }

    #[test]
    fn a_dataset_that_is_not_pinned_shows_no_pin() {
        assert!(pin_lines(&summary(None, None, None)).is_empty());
    }
}
