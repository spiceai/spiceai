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

//! Multi-round CDC data correctness test.
//!
//! This module runs multiple rounds of TPC-H data ingestion with CDC mutations,
//! verifying correctness after each round. Each round uses a different mutation
//! seed to exercise different mutation patterns.
//!
//! ## Flow
//!
//! 1. Creates `DynamoDB` tables and inserts initial records (schema inference)
//! 2. Starts Spice (no snapshots)
//! 3. For each round:
//!    a. Executes mutation sequences (INSERT wrong -> UPDATE/DELETE -> INSERT correct)
//!    b. Inserts markers and waits for ingestion
//!    c. Runs TPC-H query verification
//! 4. Reports per-round and overall results

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::RecordBatch;
use futures::future::try_join_all;
use test_framework::anyhow::{self, Result};
use test_framework::git;
use test_framework::opentelemetry::KeyValue;
use test_framework::opentelemetry_sdk::Resource;
use test_framework::spiced::{SpicedInstance, StartRequest};

use super::datasets::DatasetType;
use super::mutations::{self, MutationConfig};
use super::sources::{DynamoDbConfig, DynamoDbStreamsSource, transform_spicepod};
use super::traits::{StreamingDataset, StreamingSource};
use super::utils::{
    generate_run_id, load_spicepod_definition, poll_for_all_markers, wait_for_all_marker_deletions,
    write_temp_spicepod,
};
use super::{utils, verification};
use crate::args::StreamingDynamodbCorrectnessArgs;
use crate::commands::create_telemetry_with_resource;

/// Run the multi-round CDC data correctness test.
///
/// Creates tables once, starts Spice, then runs multiple rounds of CDC mutations
/// with verification after each round. Each round uses a different mutation seed
/// (`args.mutation_seed + round_index`) to exercise different mutation patterns.
///
/// Because `DynamoDB` has upsert semantics, each round overwrites the previous data.
/// The final state after each round is always correct TPC-H data, so the same
/// verification queries work every round.
pub async fn run_correctness(args: &StreamingDynamodbCorrectnessArgs) -> Result<()> {
    let run_id = generate_run_id();
    let datasets = args.queryset.get_datasets();

    // Print header
    println!("{}", "=".repeat(60));
    println!("DynamoDB Streaming CDC Correctness Test");
    println!("{}", "=".repeat(60));
    println!("Rounds:         {}", args.rounds);
    println!("Scale factor:   {}", args.scale_factor);
    println!("Mutation ratio: {:.1}%", args.mutation_ratio * 100.0);
    println!("Base seed:      {}", args.mutation_seed);
    println!("Run ID:         {run_id}");
    println!(
        "Datasets:       {}",
        datasets
            .iter()
            .map(|d| d.dataset_type().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("{}", "=".repeat(60));

    // Create DynamoDB source
    let config = DynamoDbConfig::from_env()?;
    let mut source = DynamoDbStreamsSource::new(config);
    source.set_table_prefix(run_id.clone());
    source.set_scale_factor(args.scale_factor);

    // Prepare source (connects to DynamoDB)
    source.prepare().await?;

    // Create tables in parallel
    println!("\nCreating tables...");
    let table_creation_futures: Vec<_> = datasets
        .iter()
        .map(|dataset| {
            let dataset_type = dataset.dataset_type();
            source.create_table(dataset_type)
        })
        .collect();

    try_join_all(table_creation_futures).await?;
    println!("All tables created");

    // Wait for table propagation
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Generate TPC-H data for all datasets (once, reused across rounds)
    println!("\nGenerating TPC-H data...");
    let mut generated_data: Vec<(DatasetType, Vec<RecordBatch>)> = Vec::new();
    for dataset in &datasets {
        let dataset_type = dataset.dataset_type();
        let records = dataset.generate(args.scale_factor)?;
        let record_count: usize = records.iter().map(RecordBatch::num_rows).sum();
        println!("  {dataset_type}: {record_count} records");
        generated_data.push((dataset_type, records));
    }

    // Insert initial records for schema inference
    println!(
        "\nInserting {} initial records per dataset...",
        args.initial_records
    );
    for (dataset_type, batches) in &generated_data {
        let table_name = source.get_table_name(dataset_type.table_name());
        let mut rows_inserted = 0;
        let rows_to_insert = args.initial_records;

        for batch in batches {
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
        println!("  Inserted {rows_inserted} initial records for {dataset_type}");
    }

    // Load and transform spicepod (no snapshots for correctness test)
    let spicepod_def = load_spicepod_definition(&args.common.spicepod_path)?;
    let transformed = transform_spicepod(spicepod_def, &run_id, "correctness", false);

    let config_name = args
        .common
        .spicepod_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    // Write transformed spicepod to temp file
    let temp_path = write_temp_spicepod(&transformed, &run_id, &config_name, "correctness")?;

    // Start Spice with metrics enabled
    let mut start_request = StartRequest::new(args.common.spiced_path_buf(), transformed)?
        .with_additional_args(vec!["--metrics".to_string(), "0.0.0.0:9090".to_string()]);

    if let Some(ref data_dir) = args.common.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // Build telemetry resource
    let spiced_version = spiced_instance.version().to_string();
    let testoperator_commit_sha = git::get_commit_sha();
    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    let branch_name = git::get_branch_name();

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "testoperator"),
            KeyValue::new("type", "streaming_correctness"),
            KeyValue::new("config_name", config_name.clone()),
            KeyValue::new("run_id", run_id.clone()),
            KeyValue::new("queryset", args.queryset.to_string()),
            KeyValue::new("scale_factor", args.scale_factor.to_string()),
            KeyValue::new("rounds", args.rounds.to_string()),
            KeyValue::new("mutation_ratio", args.mutation_ratio.to_string()),
            KeyValue::new("testoperator_commit_sha", testoperator_commit_sha),
            KeyValue::new("spiced_commit_sha", spiced_commit_sha),
            KeyValue::new("spiced_version", spiced_version),
            KeyValue::new("branch_name", branch_name),
        ])
        .build();

    let telemetry = create_telemetry_with_resource(&args.common, resource);

    println!("\nSpice is ready, starting correctness rounds\n");

    // Track results per round
    let mut round_results: Vec<(usize, bool)> = Vec::new();

    for round in 0..args.rounds {
        let seed = args.mutation_seed + round as u64;

        println!("{}", "-".repeat(60));
        println!("Round {}/{} (seed: {seed})", round + 1, args.rounds);
        println!("{}", "-".repeat(60));

        // Create mutation config with round-specific seed
        let mutation_config = MutationConfig {
            seed,
            mutation_ratio: args.mutation_ratio,
        };

        // Create datasets for mutation
        let datasets_for_mutation: Vec<Box<dyn StreamingDataset>> = generated_data
            .iter()
            .map(|(dt, _)| dt.create_dataset())
            .collect();

        // Create original data references
        let original_data: Vec<(DatasetType, Vec<RecordBatch>)> = generated_data
            .iter()
            .map(|(dt, batches)| (*dt, batches.clone()))
            .collect();

        // Execute mutation sequences
        let summary = mutations::execute_mutation_sequences(
            &source,
            &datasets_for_mutation,
            &original_data,
            mutation_config,
        )
        .await?;
        summary.print();

        // Generate markers for each dataset and insert into DynamoDB
        let mut dataset_markers = Vec::new();
        for dataset in &datasets {
            let marker = dataset.marker_record()?;
            dataset_markers.push((dataset.dataset_type(), marker));
        }

        for (dataset_type, marker) in &dataset_markers {
            let table_name = source.get_table_name(dataset_type.table_name());
            source
                .insert(&table_name, std::slice::from_ref(marker))
                .await?;
        }

        // Build marker queries and counts
        let marker_queries: HashMap<DatasetType, String> = dataset_markers
            .iter()
            .map(|(dt, _)| (*dt, dt.create_dataset().marker_detection_query()))
            .collect();

        let marker_counts: HashMap<DatasetType, usize> = dataset_markers
            .iter()
            .map(|(dt, _)| (*dt, dt.create_dataset().marker_count()))
            .collect();

        // Poll for markers
        let timeout = Duration::from_secs(args.common.ready_wait);
        let all_markers_detected =
            poll_for_all_markers(&spiced_instance, &marker_queries, &marker_counts, timeout)
                .await?;

        if !all_markers_detected {
            spiced_instance.stop()?;
            source.cleanup().await?;
            let _ = std::fs::remove_file(&temp_path);
            return Err(anyhow::anyhow!(
                "Round {}: markers not detected within timeout",
                round + 1
            ));
        }

        // Delete markers and wait for deletions
        for (dataset_type, _) in &dataset_markers {
            source.delete_marker(*dataset_type).await?;
        }

        wait_for_all_marker_deletions(&spiced_instance, &marker_queries, Duration::from_secs(30))
            .await?;

        // Run verification
        let verification_result = verification::run_verification(
            spiced_instance,
            &config_name,
            1,
            args.scale_factor,
            false,
        )
        .await?;

        // Get spiced_instance back from result
        spiced_instance = verification_result.spiced_instance;

        let passed = verification_result.all_passed;
        round_results.push((round + 1, passed));

        println!(
            "\nRound {}/{}: {}",
            round + 1,
            args.rounds,
            if passed { "PASS" } else { "FAIL" }
        );
    }

    // Compute results
    let all_passed = round_results.iter().all(|(_, passed)| *passed);
    let pass_count = round_results.iter().filter(|(_, p)| *p).count();
    let fail_count = round_results.len() - pass_count;

    // Fetch DynamoDB metrics (requires spiced running)
    let dynamodb_metrics = match utils::get_dynamodb_metrics().await {
        Ok(metrics) => {
            println!(
                "\nDynamoDB records consumed: {}",
                metrics.records_consumed_total
            );
            if metrics.errors_transient_total > 0 {
                println!(
                    "DynamoDB transient errors: {}",
                    metrics.errors_transient_total
                );
            }
            metrics
        }
        Err(e) => {
            println!("\nWarning: Failed to fetch DynamoDB metrics: {e}");
            utils::DynamoDbMetrics::default()
        }
    };

    // Record metrics
    crate::metrics::RECORD_COUNT.record(dynamodb_metrics.records_consumed_total, &[]);
    crate::metrics::DYNAMODB_TRANSIENT_ERRORS.record(dynamodb_metrics.errors_transient_total, &[]);
    crate::metrics::CORRECTNESS_ROUNDS_TOTAL.record(round_results.len() as u64, &[]);
    crate::metrics::CORRECTNESS_ROUNDS_PASSED.record(pass_count as u64, &[]);
    crate::metrics::CORRECTNESS_ROUNDS_FAILED.record(fail_count as u64, &[]);

    // Emit telemetry
    telemetry.emit().await?;

    // Stop Spice
    spiced_instance.stop()?;

    // Cleanup temp file
    let _ = std::fs::remove_file(&temp_path);

    // Cleanup DynamoDB tables
    source.cleanup().await?;

    // Print summary
    println!("\n{}", "=".repeat(60));
    println!("Correctness Test Summary");
    println!("{}", "=".repeat(60));
    for (round, passed) in &round_results {
        println!("  Round {round}: {}", if *passed { "PASS" } else { "FAIL" });
    }

    println!(
        "\nResult: {pass_count}/{} rounds passed",
        round_results.len()
    );
    println!("{}", "=".repeat(60));

    if !all_passed {
        return Err(anyhow::anyhow!(
            "Correctness test failed: {fail_count} of {} rounds failed",
            round_results.len()
        ));
    }

    Ok(())
}
