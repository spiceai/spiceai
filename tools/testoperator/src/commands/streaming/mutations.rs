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

//! Reproducible mutations for CDC (Change Data Capture) testing.
//!
//! This module provides functionality to generate reproducible mutation sequences
//! for testing CDC behavior. Each row goes through X mutations before reaching
//! its final TPC-H state.
//!
//! The mutation sequence for each row:
//! 1. INSERT with initial values (different from final)
//! 2. UPDATE 1..N-1 with intermediate values
//! 3. UPDATE N with final TPC-H values
//!
//! This allows testing that CDC correctly propagates all changes and the final
//! state matches the expected TPC-H data.

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

    /// Execute mutation sequence for a single row.
    async fn execute_row_mutations(
        &mut self,
        source: &dyn StreamingSource,
        dataset: &dyn StreamingDataset,
        original_batch: &RecordBatch,
        row: usize,
    ) -> Result<usize> {
        let table_name = dataset.table_name();
        let schema = dataset.schema();
        let pk_columns = dataset.primary_key_columns();
        let mut successful = 0;

        for mutation_idx in 0..self.config.mutations_per_row {
            let is_final = mutation_idx == self.config.mutations_per_row - 1;
            let mutated_row =
                self.generate_mutated_row(&schema, original_batch, row, &pk_columns, is_final)?;

            // First mutation is INSERT, rest are UPDATEs
            let result = if mutation_idx == 0 {
                source.insert(table_name, &[mutated_row]).await
            } else {
                source.update(table_name, &[mutated_row]).await
            };

            if result.is_ok() {
                successful += 1;
            }
        }

        Ok(successful)
    }
}

/// Execute mutation sequences for all datasets.
///
/// For each dataset, this will:
/// 1. Take the generated data
/// 2. For each row (up to `max_rows_per_dataset)`:
///    a. INSERT with mutated values
///    b. Apply mutations_per_row-2 UPDATE operations with intermediate values
///    c. Apply final UPDATE with the original TPC-H values
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

        // Find the original data for this dataset
        let Some((_, batches)) = original_data.iter().find(|(dt, _)| *dt == dataset_type) else {
            println!("No original data found for {dataset_type}");
            continue;
        };

        summary.datasets_processed += 1;

        // Process rows up to the limit
        let mut rows_processed = 0;
        let max_rows = config.max_rows_per_dataset.unwrap_or(usize::MAX);

        'batch_loop: for batch in batches {
            for row in 0..batch.num_rows() {
                if rows_processed >= max_rows {
                    break 'batch_loop;
                }

                summary.total_rows += 1;
                summary.total_mutations += config.mutations_per_row;

                match generator
                    .execute_row_mutations(source, dataset.as_ref(), batch, row)
                    .await
                {
                    Ok(successful) => {
                        summary.successful_mutations += successful;
                        summary.failed_mutations += config.mutations_per_row - successful;
                    }
                    Err(e) => {
                        eprintln!("Error executing mutations for row {row}: {e}");
                        summary.failed_mutations += config.mutations_per_row;
                    }
                }

                rows_processed += 1;

                if rows_processed % 10 == 0 {
                    println!("Processed {rows_processed}/{max_rows} rows for {dataset_type}");
                }
            }
        }

        println!(
            "Completed mutations for {}: {} rows, {} mutations",
            dataset_type,
            rows_processed,
            rows_processed * config.mutations_per_row
        );
    }

    Ok(summary)
}
