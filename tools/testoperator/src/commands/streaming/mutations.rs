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

//! Reproducible mutations for CDC (Change Data Capture) testing.
//!
//! This module provides functionality to generate reproducible mutation sequences
//! for testing CDC behavior. Each row goes through X mutations before reaching
//! its final TPC-H state.
//!
//! The mutation sequence for each row:
//! 1. INSERT with mutated values (different from final)
//! 2. UPDATE 1..N-1 with intermediate mutated values
//! 3. UPDATE N with final TPC-H values
//!
//! Mutations are batched by round for efficiency:
//! - Round 1: All INSERTs (batched 25 at a time)
//! - Round 2: All first UPDATEs (batched 25 at a time)
//! - Round N: All final UPDATEs with correct values (batched 25 at a time)

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Schema};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_framework::anyhow::{Context, Result};

use super::datasets::DatasetType;
use super::traits::{StreamingDataset, StreamingSource};

/// Configuration for mutation generation.
#[derive(Debug, Clone)]
pub struct MutationConfig {
    /// Random seed for reproducibility.
    pub seed: u64,
    /// Number of mutations per row (including initial insert and final update).
    /// Must be at least 2 (insert + final update).
    pub mutations_per_row: usize,
    /// Maximum number of rows to mutate per dataset.
    /// If None, mutate all rows.
    pub max_rows_per_dataset: Option<usize>,
}

impl Default for MutationConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            mutations_per_row: 3,            // INSERT -> UPDATE -> final UPDATE
            max_rows_per_dataset: Some(100), // Limit for testing
        }
    }
}

/// Summary of executed mutations.
#[derive(Debug, Default)]
pub struct MutationSummary {
    pub total_rows: usize,
    pub total_mutations: usize,
    pub successful_mutations: usize,
    pub failed_mutations: usize,
    pub datasets_processed: usize,
}

impl MutationSummary {
    pub fn print(&self) {
        println!("\nMutation Summary:");
        println!("  Datasets Processed: {}", self.datasets_processed);
        println!("  Total Rows:         {}", self.total_rows);
        println!("  Total Mutations:    {}", self.total_mutations);
        println!("  Successful:         {}", self.successful_mutations);
        println!("  Failed:             {}", self.failed_mutations);
    }
}

/// Generator for reproducible mutation sequences.
pub struct MutationGenerator {
    rng: StdRng,
    config: MutationConfig,
}

impl MutationGenerator {
    /// Create a new mutation generator with the given configuration.
    #[must_use]
    pub fn new(config: MutationConfig) -> Self {
        let rng = StdRng::seed_from_u64(config.seed);
        Self { rng, config }
    }

    /// Generate a mutated version of a value.
    fn mutate_value(&mut self, original: &dyn Array, row: usize, is_final: bool) -> Arc<dyn Array> {
        if is_final {
            // Return the original value for the final mutation
            return original.slice(row, 1);
        }

        // Generate a mutated value based on the data type
        match original.data_type() {
            DataType::Int64 => {
                let Some(arr) = original.as_any().downcast_ref::<Int64Array>() else {
                    return original.slice(row, 1);
                };
                let original_val = arr.value(row);
                // Add some random offset for mutation
                let mutated = original_val.wrapping_add(self.rng.random_range(-100..100));
                Arc::new(Int64Array::from(vec![mutated]))
            }
            DataType::Float64 => {
                let Some(arr) = original.as_any().downcast_ref::<Float64Array>() else {
                    return original.slice(row, 1);
                };
                let original_val = arr.value(row);
                // Add some random offset for mutation
                let mutated = original_val + self.rng.random_range(-10.0..10.0);
                Arc::new(Float64Array::from(vec![mutated]))
            }
            DataType::Utf8 => {
                let Some(arr) = original.as_any().downcast_ref::<StringArray>() else {
                    return original.slice(row, 1);
                };
                let original_val = arr.value(row);
                // Append mutation marker
                let mutated = format!("{}_MUT{}", original_val, self.rng.random_range(0..1000));
                Arc::new(StringArray::from(vec![mutated.as_str()]))
            }
            _ => {
                // For other types, just return the original
                original.slice(row, 1)
            }
        }
    }

    /// Generate a single row record batch with mutated values.
    fn generate_mutated_row(
        &mut self,
        schema: &Schema,
        original_batch: &RecordBatch,
        row: usize,
        primary_key_columns: &[&str],
        is_final: bool,
    ) -> Result<RecordBatch> {
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let original_col = original_batch.column(col_idx);

            // Keep primary key columns unchanged
            if primary_key_columns.contains(&field.name().as_str()) {
                columns.push(original_col.slice(row, 1));
            } else {
                columns.push(self.mutate_value(original_col.as_ref(), row, is_final));
            }
        }

        RecordBatch::try_new(Arc::new(schema.clone()), columns)
            .context("Failed to create mutated row batch")
    }

    /// Generate all mutations for a dataset, organized by round.
    ///
    /// Returns a Vec where each element is a round of mutations.
    /// Round 0 = all INSERTs, Round 1..N-1 = UPDATEs, Round N = final UPDATEs.
    fn generate_all_mutations(
        &mut self,
        dataset: &dyn StreamingDataset,
        batches: &[RecordBatch],
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let schema = dataset.schema();
        let pk_columns = dataset.primary_key_columns();
        let max_rows = self.config.max_rows_per_dataset.unwrap_or(usize::MAX);

        // Each round is a vector of single-row batches
        let mut rounds: Vec<Vec<RecordBatch>> =
            vec![Vec::new(); self.config.mutations_per_row];

        let mut rows_processed = 0;

        'batch_loop: for batch in batches {
            for row in 0..batch.num_rows() {
                if rows_processed >= max_rows {
                    break 'batch_loop;
                }

                // Generate mutations for each round
                for (round_idx, round) in rounds.iter_mut().enumerate() {
                    let is_final = round_idx == self.config.mutations_per_row - 1;
                    let mutated_row =
                        self.generate_mutated_row(&schema, batch, row, &pk_columns, is_final)?;
                    round.push(mutated_row);
                }

                rows_processed += 1;
            }
        }

        Ok(rounds)
    }
}

/// Concatenate multiple single-row batches into larger batches.
fn concatenate_batches(single_row_batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    if single_row_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Group into batches of ~1000 rows for efficient processing
    const BATCH_SIZE: usize = 1000;

    let schema = single_row_batches[0].schema();
    let mut result = Vec::new();

    for chunk in single_row_batches.chunks(BATCH_SIZE) {
        let batch = arrow::compute::concat_batches(&schema, chunk)
            .context("Failed to concatenate batches")?;
        result.push(batch);
    }

    Ok(result)
}

/// Execute mutation sequences for all datasets using batched operations.
///
/// For each dataset, this will:
/// 1. Generate all mutations organized by round
/// 2. Execute each round as batched operations:
///    - Round 0: Batch INSERT all mutated rows
///    - Round 1..N-1: Batch UPDATE with intermediate values
///    - Round N: Batch UPDATE with final TPC-H values
///
/// This tests that CDC correctly processes all mutations and the final state
/// matches the expected TPC-H data.
pub async fn execute_mutation_sequences(
    source: &dyn StreamingSource,
    datasets: &[Box<dyn StreamingDataset>],
    original_data: &[(DatasetType, Vec<RecordBatch>)],
    config: MutationConfig,
) -> Result<MutationSummary> {
    let mut summary = MutationSummary::default();
    let mut generator = MutationGenerator::new(config.clone());

    for dataset in datasets {
        let dataset_type = dataset.dataset_type();
        let table_name = dataset.table_name();

        // Find the original data for this dataset
        let Some((_, batches)) = original_data.iter().find(|(dt, _)| *dt == dataset_type) else {
            println!("No original data found for {dataset_type}");
            continue;
        };

        println!("\nGenerating mutations for {dataset_type}...");

        // Generate all mutations organized by round
        let rounds = generator.generate_all_mutations(dataset.as_ref(), batches)?;

        let total_rows = if rounds.is_empty() {
            0
        } else {
            rounds[0].len()
        };

        summary.datasets_processed += 1;
        summary.total_rows += total_rows;
        summary.total_mutations += total_rows * config.mutations_per_row;

        println!(
            "Generated {} rows × {} mutations = {} total mutations",
            total_rows,
            config.mutations_per_row,
            total_rows * config.mutations_per_row
        );

        // Execute each round
        for (round_idx, round_batches) in rounds.into_iter().enumerate() {
            let is_insert = round_idx == 0;
            let is_final = round_idx == config.mutations_per_row - 1;

            let operation = if is_insert {
                "INSERT"
            } else if is_final {
                "UPDATE (final)"
            } else {
                "UPDATE"
            };

            println!(
                "  Round {}/{}: {} {} rows...",
                round_idx + 1,
                config.mutations_per_row,
                operation,
                round_batches.len()
            );

            // Concatenate single-row batches for efficient insertion
            let batches = concatenate_batches(round_batches)?;

            // Execute the batch operation
            let result = if is_insert {
                source.insert(table_name, &batches).await
            } else {
                source.update(table_name, &batches).await
            };

            match result {
                Ok(()) => {
                    summary.successful_mutations += total_rows;
                    println!("    Completed {} for {} rows", operation, total_rows);
                }
                Err(e) => {
                    summary.failed_mutations += total_rows;
                    eprintln!("    Failed {} for {} rows: {}", operation, total_rows, e);
                }
            }
        }

        println!(
            "Completed mutations for {}: {} rows processed",
            dataset_type, total_rows
        );
    }

    Ok(summary)
}
