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

//! `DynamoDB` Streams dispatch for multi-config benchmarks.
//!
//! This module orchestrates benchmarks across multiple spicepod configurations,
//! using a single set of shared tables for all configs.
//!
//! ## Flow
//! 1. Create tables with unique prefix (shared across all configs)
//! 2. Insert initial records (for schema inference)
//! 3. Capture checkpoint snapshot FOR EACH config (sequential)
//! 4. Insert remaining data (shared)
//! 5. Trigger GitHub workflow FOR EACH config OR run benchmark locally
//!
//! ## Modes
//!
//! ### GitHub Workflow Mode (`--workflow`)
//! When `--workflow` is specified, dispatch triggers GitHub Actions workflows
//! for each config instead of running benchmarks locally.
//!
//! ### Local Mode (default)
//! When `--workflow` is not specified, benchmarks run locally (sequential).

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use futures::future::try_join_all;
use test_framework::anyhow::{self, Result};
use test_framework::gh_utils::GitHubWorkflow;
use test_framework::octocrab::{self, Octocrab};
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

/// Run the `DynamoDB` streaming dispatch (multi-config benchmarks).
///
/// Creates a single set of shared tables, then processes each spicepod configuration
/// with its own checkpoint snapshot and workflow trigger.
pub async fn run_dispatch(args: &StreamingDynamodbDispatchArgs) -> Result<()> {
    let spicepod_paths = args.all_spicepod_paths()?;
    let datasets = args.queryset.get_datasets();

    println!("Starting DynamoDB streaming dispatch");
    println!("Config directory: {}", args.path.display());
    println!("Query set: {}", args.queryset);
    println!("Scale factor: {}", args.scale_factor);
    println!("Configs found: {}", spicepod_paths.len());
    for path in &spicepod_paths {
        println!("  - {}", path.display());
    }

    // Check if snapshots are configured (required for DynamoDB)
    let snapshot_config = build_snapshot_config().ok_or_else(|| {
        anyhow::anyhow!("DynamoDB benchmarks require SNAPSHOT_S3_LOCATION environment variable")
    })?;

    // Generate unique run ID for table isolation (shared across all configs)
    let run_id = generate_run_id();
    println!("Generated run ID: {run_id}");

    // Create DynamoDB source from environment variables
    let config = DynamoDbConfig::from_env()?;
    let mut source = DynamoDbStreamsSource::new(config);
    source.set_table_prefix(run_id.clone());
    source.set_scale_factor(args.scale_factor);

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

        println!("  Generating data for {dataset_type}");
        let records = dataset.generate(args.scale_factor)?;
        let record_count: usize = records.iter().map(RecordBatch::num_rows).sum();
        println!("  Generated {record_count} records for {dataset_type}");

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

    // Phase 5: Capture checkpoint snapshot FOR EACH config
    let dataset_names: Vec<&str> = dataset_infos
        .iter()
        .map(|info| info.dataset.table_name())
        .collect();

    println!("Phase 5: Capturing checkpoint snapshots for each config");
    for (idx, spicepod_path) in spicepod_paths.iter().enumerate() {
        let config_name = spicepod_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        println!(
            "  Capturing checkpoint {}/{}: {config_name}",
            idx + 1,
            spicepod_paths.len()
        );

        capture_checkpoint_snapshot(
            &source,
            spicepod_path,
            &run_id,
            &config_name,
            &snapshot_config,
            args,
            &dataset_names,
        )
        .await?;
    }

    // Phase 6: Insert remaining data
    println!("Phase 6: Inserting remaining data");
    let mut total_insert_duration = Duration::ZERO;

    if args.mutation_ratio > 0.0 {
        println!("  Executing mutation sequences for CDC testing");
        println!(
            "  Seed: {}, Mutation ratio: {:.1}%",
            args.mutation_seed,
            args.mutation_ratio * 100.0
        );

        let mutation_config = mutations::MutationConfig {
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
            mutation_config,
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
    }

    println!("Data insertion completed in {total_insert_duration:?}");

    // Phase 7: Trigger workflows FOR EACH config
    if let Some(ref workflow) = args.workflow {
        println!("Phase 7: Triggering GitHub workflows for each config");

        let gh_token = std::env::var("GH_TOKEN").map_err(|_| {
            anyhow::anyhow!("GH_TOKEN environment variable is required for workflow dispatch")
        })?;
        let octo_client = octocrab::instance().user_access_token(gh_token)?;
        let (org, repo_name) = parse_repo(args.repo.as_deref());
        let git_ref = args
            .git_ref
            .clone()
            .unwrap_or_else(|| std::env::var("GITHUB_REF").unwrap_or_else(|_| "trunk".to_string()));

        for (idx, spicepod_path) in spicepod_paths.iter().enumerate() {
            let config_name = spicepod_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();

            println!(
                "  Triggering workflow {}/{}: {config_name}",
                idx + 1,
                spicepod_paths.len()
            );

            trigger_workflow(
                &octo_client,
                workflow,
                org,
                repo_name,
                &git_ref,
                &run_id,
                &config_name,
                spicepod_path,
                args,
            )
            .await?;

            if args.wait_for_workflows {
                let gh_workflow = GitHubWorkflow::new(org, repo_name, workflow, &git_ref);
                wait_for_workflow_completion(&octo_client, &gh_workflow).await?;
            }
        }

        println!("Run ID: {run_id}");
        println!("Monitor workflows at: https://github.com/{org}/{repo_name}/actions",);
        println!("Note: DynamoDB tables preserved for workflow execution");
    }

    println!(
        "\n{}\nAll configs processed successfully\n{}",
        "=".repeat(60),
        "=".repeat(60)
    );
    Ok(())
}

/// Parse a repo string like "spiceai/spiceai" into (org, repo).
/// Defaults to ("spiceai", "spiceai") if not provided.
fn parse_repo(repo: Option<&str>) -> (&str, &str) {
    repo.and_then(|r| r.split_once('/'))
        .unwrap_or(("spiceai", "spiceai"))
}

/// Trigger a GitHub workflow for a specific config using Octocrab.
#[expect(clippy::too_many_arguments)]
async fn trigger_workflow(
    octo: &Octocrab,
    workflow: &str,
    org: &str,
    repo: &str,
    git_ref: &str,
    run_id: &str,
    config_name: &str,
    spicepod_path: &Path,
    args: &StreamingDynamodbDispatchArgs,
) -> Result<()> {
    let inputs = serde_json::json!({
        "run_id": run_id,
        "config_name": config_name,
        "spicepod_path": spicepod_path.display().to_string(),
        "queryset": args.queryset.to_string(),
        "scale_factor": args.scale_factor.to_string(),
        "ready_wait": args.ready_wait.to_string(),
        "verify": args.verify.to_string(),
    });

    let gh_workflow = GitHubWorkflow::new(org, repo, workflow, git_ref);
    gh_workflow.send(octo.actions(), Some(inputs)).await?;

    println!("    Workflow triggered for {config_name}");
    Ok(())
}

/// Wait for the most recent workflow run to complete using Octocrab.
async fn wait_for_workflow_completion(octo: &Octocrab, workflow: &GitHubWorkflow) -> Result<()> {
    println!("    Waiting for workflow to complete...");

    let timeout = Duration::from_secs(7200); // 2 hours
    let start = Instant::now();

    loop {
        let page = octo
            .workflows(&workflow.org, &workflow.repo)
            .list_runs(&workflow.workflow_file)
            .per_page(1)
            .send()
            .await?;

        if let Some(run) = page.items.first() {
            match run.status.as_str() {
                "completed" => {
                    if run.conclusion.as_deref() == Some("success") {
                        println!("    Workflow completed successfully");
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(
                        "Workflow failed with conclusion: {}",
                        run.conclusion.as_deref().unwrap_or("unknown")
                    ));
                }
                status => {
                    if start.elapsed() >= timeout {
                        return Err(anyhow::anyhow!(
                            "Timeout waiting for workflow (status: {status})"
                        ));
                    }
                    println!(
                        "    Workflow status: {status}, waiting... ({:.0}s elapsed)",
                        start.elapsed().as_secs_f64()
                    );
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        } else {
            return Err(anyhow::anyhow!("No workflow runs found"));
        }
    }
}

/// Capture a checkpoint snapshot for a single configuration.
async fn capture_checkpoint_snapshot(
    source: &Arc<dyn DynamoDBStreamingSource>,
    spicepod_path: &Path,
    run_id: &str,
    config_name: &str,
    snapshot_config: &SnapshotConfig,
    args: &StreamingDynamodbDispatchArgs,
    dataset_names: &[&str],
) -> Result<()> {
    // Load and transform spicepod
    let spicepod_def = load_spicepod_definition(spicepod_path)?;
    let transformed =
        source.prepare_checkpoint_spicepod(spicepod_def, run_id, config_name, snapshot_config);

    // Write transformed spicepod to temp file
    let temp_path = write_temp_spicepod(&transformed, run_id, config_name, "checkpoint")?;
    println!("    Wrote transformed spicepod to {}", temp_path.display());

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

    println!("    Checkpoint captured for {config_name}");
    Ok(())
}
