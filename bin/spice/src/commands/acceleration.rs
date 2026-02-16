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
use crate::error::{InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use crate::output::{TableRow, write_table};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};

/// Arguments for the acceleration command.
#[derive(Args, Debug)]
#[command(
    about = "Manage dataset acceleration features",
    long_about = r#"Commands for managing accelerated datasets including snapshots.

Use subcommands to list snapshots, view snapshot details, and perform
rollback operations.

Examples:
  # List all snapshots for a dataset
  spice acceleration snapshots taxi_trips

  # Get details of a specific snapshot
  spice acceleration snapshot taxi_trips 3

  # Set the current snapshot for rollback
  spice acceleration set-snapshot taxi_trips 2"#
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

    /// Set the current snapshot for rollback
    SetSnapshot(SetSnapshotArgs),
}

/// Arguments for the snapshots subcommand.
#[derive(Args, Debug)]
pub struct SnapshotsArgs {
    /// The dataset name
    pub dataset: String,
}

/// Arguments for the snapshot subcommand.
#[derive(Args, Debug)]
pub struct SnapshotArgs {
    /// The dataset name
    pub dataset: String,

    /// The snapshot ID
    pub snapshot_id: u64,
}

/// Arguments for the set-snapshot subcommand.
#[derive(Args, Debug)]
pub struct SetSnapshotArgs {
    /// The dataset name
    pub dataset: String,

    /// The snapshot ID to set as current
    pub snapshot_id: u64,
}

/// Snapshot information from the API.
#[derive(Debug, Deserialize)]
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
}

/// Snapshot summary from the API.
#[derive(Debug, Deserialize)]
pub struct SnapshotSummary {
    pub dataset_name: String,
    pub location: String,
    pub last_updated_ms: i64,
    pub engine: Option<String>,
    pub current_snapshot_id: Option<u64>,
    pub snapshots: Vec<SnapshotInfo>,
}

/// Request to set the current snapshot.
#[derive(Debug, Serialize)]
struct SetCurrentSnapshotRequest {
    snapshot_id: u64,
}

/// Generic message response.
#[derive(Debug, Deserialize)]
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
}

impl TableRow for SnapshotTableRow {
    fn headers() -> Vec<&'static str> {
        vec!["ID", "TIMESTAMP", "SIZE", "ROWS", "CHECKSUM", "CURRENT"]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.timestamp.clone(),
            self.size.clone(),
            self.rows.clone(),
            self.checksum.clone(),
            self.current.clone(),
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
        AccelerationCommand::SetSnapshot(snap_args) => execute_set_snapshot(ctx, snap_args).await,
    }
}

/// Execute the snapshots subcommand.
async fn execute_snapshots(ctx: &RuntimeContext, args: &SnapshotsArgs) -> Result<()> {
    let url = format!("/v1/datasets/{}/acceleration/snapshots", args.dataset);

    let response = ctx.get(&url).await.map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        tracing::error!("Failed to list snapshots: {} - {}", status, body);
        return Ok(());
    }

    let summary: SnapshotSummary = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse snapshots response: {e}"),
        }
        .build()
    })?;

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
    if let Some(current_id) = summary.current_snapshot_id {
        println!("Current Snapshot ID: {current_id}");
    }
    let last_updated = format_timestamp_ms(summary.last_updated_ms);
    println!("Last Updated: {last_updated}");
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

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        tracing::error!("Failed to get snapshot: {} - {}", status, body);
        return Ok(());
    }

    let snapshot: SnapshotInfo = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse snapshot response: {e}"),
        }
        .build()
    })?;

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

/// Execute the set-snapshot subcommand.
async fn execute_set_snapshot(ctx: &RuntimeContext, args: &SetSnapshotArgs) -> Result<()> {
    let url = format!(
        "/v1/datasets/{}/acceleration/snapshots/current",
        args.dataset
    );

    let request = SetCurrentSnapshotRequest {
        snapshot_id: args.snapshot_id,
    };

    let response = ctx.post_json(&url, &request).await.map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        tracing::error!("Failed to set current snapshot: {} - {}", status, body);
        return Ok(());
    }

    let result: MessageResponse = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse response: {e}"),
        }
        .build()
    })?;

    tracing::info!("{}", result.message);

    Ok(())
}

/// Format a timestamp in milliseconds as an RFC3339 string.
fn format_timestamp_ms(ms: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    let duration = Duration::from_millis(ms as u64);
    let datetime = UNIX_EPOCH + duration;

    // Format as RFC3339-like string
    let secs = datetime
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
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
