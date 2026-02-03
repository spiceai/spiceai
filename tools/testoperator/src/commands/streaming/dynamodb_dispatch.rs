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

//! DynamoDB Streams dispatch for multi-config benchmarks.
//!
//! This module orchestrates benchmarks across multiple spicepod configurations,
//! ingesting data once and running each configuration sequentially.
//!
//! ## Flow
//! 1. Create tables, insert first record (for schema inference)
//! 2. For each config: capture checkpoint snapshot (sequential)
//! 3. Insert remaining data
//! 4. For each config: trigger GitHub workflow OR run benchmark locally (sequential)
//! 5. Report results
//!
//! ## Modes
//!
//! ### GitHub Workflow Mode (`--workflow`)
//! When `--workflow` is specified, dispatch triggers GitHub Actions workflows
//! for each config instead of running benchmarks locally. This is useful for
//! running benchmarks in CI/CD environments.
//!
//! ### Local Mode (default)
//! When `--workflow` is not specified, benchmarks run locally (sequential).

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use futures::future::try_join_all;
use test_framework::anyhow::{self, Result};
use test_framework::spiced::{SpicedInstance, StartRequest};

use super::datasets::DatasetType;
use super::dynamodb_runner::build_snapshot_config;
use super::mutations;
use super::sources::{DynamoDbConfig, DynamoDbStreamsSource};
use super::traits::{DynamoDBStreamingSource, SnapshotConfig, StreamingDataset, StreamingSource};
use super::utils::{
    DatasetInfo, generate_run_id, load_spicepod_definition, poll_for_all_snapshots, skip_rows,
    write_temp_spicepod,
};
use crate::args::StreamingDynamodbDispatchArgs;

/// Run the DynamoDB streaming dispatch (multi-config benchmarks).
///
/// This ingests data once and runs benchmarks for multiple configurations.
pub async fn run_dispatch(args: &StreamingDynamodbDispatchArgs) -> Result<()> {
    let spicepod_paths = args.all_spicepod_paths();
    let datasets = args.queryset.get_datasets();

    if spicepod_paths.len() < 2 {
        println!("Warning: dispatch-dynamodb is designed for multiple configs.");
        println!("Consider using streaming-dynamodb for single config benchmarks.");
    }

    println!("Starting DynamoDB streaming dispatch");
    println!("Query set: {}", args.queryset);
    println!("Configs: {}", spicepod_paths.len());
    println!(
        "Datasets: {}",
        datasets
            .iter()
            .map(|d| d.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Scale factor: {}", args.scale_factor);

    // Generate unique run ID for table isolation
    let run_id = generate_run_id();
    println!("Generated run ID: {run_id}");

    // Create DynamoDB source from environment variables
    let config = DynamoDbConfig::from_env()?;
    let mut source = DynamoDbStreamsSource::new(config);
    source.set_table_prefix(run_id.clone());
    source.set_scale_factor(args.scale_factor);

    // Check if snapshots are configured (required for DynamoDB)
    let snapshot_config = build_snapshot_config().ok_or_else(|| {
        anyhow::anyhow!("DynamoDB benchmarks require SNAPSHOT_S3_LOCATION environment variable")
    })?;

    // Phase 1: Prepare source and create tables
    println!("Phase 1: Preparing streaming source");
    source.prepare().await?;

    let source: Arc<dyn DynamoDBStreamingSource> = Arc::from(source);

    println!("Phase 2: Creating tables for all datasets (parallel)");
    let table_creation_futures: Vec<_> = datasets
        .iter()
        .map(|dataset| {
            let source = Arc::clone(&source);
            let dataset_type = dataset.dataset_type();
            async move { source.create_table(dataset_type).await }
        })
        .collect();

    try_join_all(table_creation_futures).await?;
    println!("All tables created");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Phase 3: Generate data for all datasets
    println!("Phase 3: Generating data for all datasets");
    let mut dataset_infos = Vec::new();

    for dataset in datasets {
        let dataset_type = dataset.dataset_type();

        println!("Generating data for {dataset_type}");
        let records = dataset.generate(args.scale_factor)?;
        let record_count: usize = records.iter().map(RecordBatch::num_rows).sum();
        println!("Generated {record_count} records for {dataset_type}");

        let marker = dataset.marker_record()?;
        dataset_infos.push(DatasetInfo {
            dataset,
            marker,
            record_count,
            generated_data: records,
        });
    }

    // Phase 4: Insert initial records per dataset (for schema inference)
    println!(
        "Phase 4: Inserting {} records per dataset (for schema)",
        args.checkpoint_records
    );
    for info in &dataset_infos {
        let table_name = source.get_table_name(info.dataset.table_name());
        let mut rows_inserted = 0;
        let rows_to_insert = args.checkpoint_records;

        for batch in &info.generated_data {
            if rows_inserted >= rows_to_insert {
                break;
            }
            let remaining = rows_to_insert - rows_inserted;
            let take = remaining.min(batch.num_rows());
            if take > 0 {
                let slice = batch.slice(0, take);
                source.insert(&table_name, &[slice]).await?;
                rows_inserted += take;
            }
        }
        println!(
            "  Inserted {rows_inserted} initial records for {}",
            info.dataset.dataset_type()
        );
    }

    // Phase 5: Capture checkpoints for ALL configs (sequential due to fixed port)
    let config_names: Vec<String> = spicepod_paths
        .iter()
        .map(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string()
        })
        .collect();

    // Collect dataset names for snapshot polling
    let dataset_names: Vec<&str> = dataset_infos
        .iter()
        .map(|info| info.dataset.table_name())
        .collect();

    println!("Phase 5: Capturing checkpoints for all configs (sequential)");

    for (path, config_name) in spicepod_paths.iter().zip(config_names.iter()) {
        capture_checkpoint_snapshot(
            &source,
            path,
            &run_id,
            config_name,
            &snapshot_config,
            args,
            &dataset_names,
        )
        .await?;
    }

    println!("All checkpoint snapshots captured");

    // Phase 6: Insert remaining data
    println!("Phase 6: Inserting remaining data");
    let _data_insertion_start = Instant::now();
    let mut total_insert_duration = Duration::ZERO;

    if args.mutation_ratio > 0.0 {
        println!("  Executing mutation sequences for CDC testing");
        println!(
            "  Seed: {}, Mutation ratio: {:.1}%",
            args.mutation_seed,
            args.mutation_ratio * 100.0
        );

        let config = mutations::MutationConfig {
            seed: args.mutation_seed,
            mutation_ratio: args.mutation_ratio,
        };

        let datasets_for_mutation: Vec<Box<dyn StreamingDataset>> = dataset_infos
            .iter()
            .map(|info| info.dataset.dataset_type().create_dataset())
            .collect();

        // Skip checkpoint records (already inserted)
        let original_data: Vec<(DatasetType, Vec<RecordBatch>)> = dataset_infos
            .iter()
            .map(|info| {
                let batches = skip_rows(&info.generated_data, args.checkpoint_records);
                (info.dataset.dataset_type(), batches)
            })
            .collect();

        let insert_start = Instant::now();
        let summary = mutations::execute_mutation_sequences(
            source.as_ref(),
            &datasets_for_mutation,
            &original_data,
            config,
        )
        .await?;
        total_insert_duration = insert_start.elapsed();
        summary.print();
    } else {
        for info in &dataset_infos {
            let dataset_type = info.dataset.dataset_type();
            let table_name = source.get_table_name(info.dataset.table_name());
            println!("  Inserting data for {dataset_type}");

            // Skip checkpoint records (already inserted)
            let remaining_data = skip_rows(&info.generated_data, args.checkpoint_records);

            let insert_start = Instant::now();
            source.insert(&table_name, &remaining_data).await?;
            total_insert_duration += insert_start.elapsed();
        }
    };

    println!("Data insertion completed in {total_insert_duration:?}");

    // Phase 7: Trigger benchmarks for ALL configs (sequential)
    if let Some(ref workflow) = args.workflow {
        // GitHub workflow dispatch mode
        println!("Phase 7: Triggering GitHub workflows for all configs (sequential)");

        for (path, config_name) in spicepod_paths.iter().zip(config_names.iter()) {
            trigger_workflow(
                workflow,
                args.repo.as_deref(),
                args.git_ref.as_deref(),
                &run_id,
                config_name,
                path,
                args,
            )?;

            if args.wait_for_workflows {
                // Wait for workflow to complete before triggering next
                wait_for_workflow_completion(workflow, args.repo.as_deref())?;
            }
        }

        println!("\nPhase 8: Workflow dispatch complete");
        println!("Run ID: {run_id}");
        println!("Configs dispatched: {}", config_names.join(", "));
        println!(
            "\nMonitor workflows at: https://github.com/{}/actions",
            args.repo.as_deref().unwrap_or("spiceai/spiceai")
        );

        // Don't cleanup - tables need to remain for workflows to use
        println!("\nNote: DynamoDB tables preserved for workflow execution");
        println!("Run cleanup manually after workflows complete");
    }

    Ok(())
}

/// Trigger a GitHub workflow for a specific config.
fn trigger_workflow(
    workflow: &str,
    repo: Option<&str>,
    git_ref: Option<&str>,
    run_id: &str,
    config_name: &str,
    spicepod_path: &Path,
    args: &StreamingDynamodbDispatchArgs,
) -> Result<()> {
    println!("  Triggering workflow for {config_name}");

    let mut cmd = Command::new("gh");
    cmd.args(["workflow", "run", workflow]);

    if let Some(repo) = repo {
        cmd.args(["--repo", repo]);
    }

    if let Some(git_ref) = git_ref {
        cmd.args(["--ref", git_ref]);
    }

    // Pass workflow inputs
    cmd.args(["-f", &format!("run_id={run_id}")]);
    cmd.args(["-f", &format!("config_name={config_name}")]);
    cmd.args(["-f", &format!("spicepod_path={}", spicepod_path.display())]);
    cmd.args(["-f", &format!("queryset={}", args.queryset)]);
    cmd.args(["-f", &format!("scale_factor={}", args.scale_factor)]);
    cmd.args([
        "-f",
        &format!("ingestion_timeout={}", args.ingestion_timeout),
    ]);

    if args.verify {
        cmd.args(["-f", "verify=true"]);
    }

    let output = cmd.output().map_err(|e| {
        anyhow::anyhow!("Failed to run gh workflow command: {e}. Is GitHub CLI installed?")
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "Failed to trigger workflow for {config_name}: {stderr}"
        ));
    }

    println!("  Workflow triggered for {config_name}");
    Ok(())
}

/// Wait for a workflow run to complete.
fn wait_for_workflow_completion(workflow: &str, repo: Option<&str>) -> Result<()> {
    println!("  Waiting for workflow to complete...");

    let mut cmd = Command::new("gh");
    cmd.args(["run", "watch", "--exit-status"]);

    if let Some(repo) = repo {
        cmd.args(["--repo", repo]);
    }

    // Get the most recent run of this workflow
    let mut list_cmd = Command::new("gh");
    list_cmd.args([
        "run",
        "list",
        "--workflow",
        workflow,
        "--limit",
        "1",
        "--json",
        "databaseId",
        "--jq",
        ".[0].databaseId",
    ]);

    if let Some(repo) = repo {
        list_cmd.args(["--repo", repo]);
    }

    let list_output = list_cmd.output()?;
    if !list_output.status.success() {
        return Err(anyhow::anyhow!("Failed to get workflow run ID"));
    }

    let run_id = String::from_utf8_lossy(&list_output.stdout)
        .trim()
        .to_string();
    if run_id.is_empty() {
        return Err(anyhow::anyhow!("No workflow runs found"));
    }

    cmd.arg(&run_id);

    let status = cmd.status()?;
    if !status.success() {
        return Err(anyhow::anyhow!("Workflow failed"));
    }

    println!("  Workflow completed successfully");
    Ok(())
}

/// Capture a checkpoint snapshot for a single configuration.
#[expect(clippy::too_many_arguments)]
async fn capture_checkpoint_snapshot(
    source: &Arc<dyn DynamoDBStreamingSource>,
    spicepod_path: &Path,
    run_id: &str,
    config_name: &str,
    snapshot_config: &SnapshotConfig,
    args: &StreamingDynamodbDispatchArgs,
    dataset_names: &[&str],
) -> Result<()> {
    println!("  Capturing checkpoint for {config_name}");

    // Load and transform spicepod
    let spicepod_def = load_spicepod_definition(spicepod_path)?;
    let transformed =
        source.prepare_checkpoint_spicepod(spicepod_def, run_id, config_name, snapshot_config);

    // Write transformed spicepod to temp file
    let temp_path = write_temp_spicepod(&transformed, run_id, config_name, "checkpoint")?;
    println!("  Wrote transformed spicepod to capture checkpoints to {temp_path:?}");

    // Start temp Spice
    let mut start_request = StartRequest::new(args.spiced_path_buf(), transformed)?;

    if let Some(ref data_dir) = args.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.ready_wait))
        .await?;

    // Poll for snapshots on all datasets
    poll_for_all_snapshots(dataset_names, Duration::from_secs(args.ready_wait)).await?;

    // Stop temp Spice
    spiced_instance.stop()?;

    // Cleanup temp file
    let _ = std::fs::remove_file(&temp_path);

    println!("  Checkpoint captured for {config_name}");
    Ok(())
}
