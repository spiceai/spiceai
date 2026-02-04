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
//! for testing CDC behavior. A configurable ratio of rows go through mutations
//! before reaching their final TPC-H state, while the rest are inserted directly.
//!
//! Mutated rows are split 50/50 into two paths:
//! - **Update path**: INSERT (wrong) → UPDATE (correct)
//! - **Delete path**: INSERT (wrong) → DELETE → INSERT (correct)
//!
//! Flow:
//! 1. INSERT wrong values for all mutated rows
//! 2. UPDATE correct values for update-path rows
//! 3. DELETE delete-path rows
//! 4. INSERT correct values for delete-path rows
//! 5. Direct INSERT remaining rows with final values

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Schema};
use rand::rngs::StdRng;
use rand::seq::index::sample;
use rand::{Rng, SeedableRng};
use test_framework::anyhow::{Context, Result};

use super::datasets::DatasetType;
use super::traits::{StreamingDataset, StreamingSource};

/// Configuration for mutation generation.
#[derive(Debug, Clone)]
pub struct MutationConfig {
    /// Random seed for reproducibility.
    pub seed: u64,
    /// Ratio of rows to mutate (0.0-1.0).
    /// Selected rows are split 50/50 between update path and delete path.
    pub mutation_ratio: f64,
}

impl Default for MutationConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            mutation_ratio: 0.1, // 10% of rows mutated
        }
    }
}

/// Summary of executed mutations.
#[derive(Debug, Default)]
pub struct MutationSummary {
    pub total_rows: usize,
    pub update_path_rows: usize,
    pub delete_path_rows: usize,
    pub direct_insert_rows: usize,
    pub datasets_processed: usize,
    pub failed_operations: usize,
}

impl MutationSummary {
    pub fn print(&self) {
        println!("\nMutation Summary:");
        println!("  Datasets Processed: {}", self.datasets_processed);
        println!("  Total Rows:         {}", self.total_rows);
        println!(
            "  Mutated Rows:       {} ({} update path, {} delete path)",
            self.update_path_rows + self.delete_path_rows,
            self.update_path_rows,
            self.delete_path_rows
        );
        println!("  Direct Inserts:     {}", self.direct_insert_rows);
        if self.failed_operations > 0 {
            println!("  Failed Operations:  {}", self.failed_operations);
        }
    }
}

/// Data for mutation execution, organized by operation type.
struct MutationData {
    /// Rows for update path: INSERT wrong → UPDATE correct
    /// (`wrong_values`, `correct_values`)
    update_path: (Vec<RecordBatch>, Vec<RecordBatch>),
    /// Rows for delete path: INSERT wrong → DELETE → INSERT correct
    /// (`wrong_values`, `keys_for_delete`, `correct_values`)
    delete_path: (Vec<RecordBatch>, Vec<RecordBatch>, Vec<RecordBatch>),
    /// Rows to insert directly with correct values
    direct_inserts: Vec<RecordBatch>,
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
    fn mutate_value(&mut self, original: &dyn Array, row: usize) -> Arc<dyn Array> {
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

    /// Generate a single row record batch with mutated (wrong) values.
    fn generate_wrong_row(
        &mut self,
        schema: &Schema,
        original_batch: &RecordBatch,
        row: usize,
        primary_key_columns: &[&str],
    ) -> Result<RecordBatch> {
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let original_col = original_batch.column(col_idx);

            // Keep primary key columns unchanged
            if primary_key_columns.contains(&field.name().as_str()) {
                columns.push(original_col.slice(row, 1));
            } else {
                columns.push(self.mutate_value(original_col.as_ref(), row));
            }
        }

        RecordBatch::try_new(Arc::new(schema.clone()), columns)
            .context("Failed to create mutated row batch")
    }

    /// Extract a single row from a batch as a new batch (correct values).
    fn extract_row(batch: &RecordBatch, row: usize) -> Result<RecordBatch> {
        let columns: Vec<Arc<dyn Array>> = batch
            .columns()
            .iter()
            .map(|col| col.slice(row, 1))
            .collect();

        RecordBatch::try_new(batch.schema(), columns).context("Failed to extract row")
    }

    /// Extract only primary key columns from a row (for DELETE operations).
    fn extract_key_row(
        batch: &RecordBatch,
        row: usize,
        primary_key_columns: &[&str],
    ) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut key_columns: Vec<Arc<dyn Array>> = Vec::new();
        let mut key_fields: Vec<arrow::datatypes::FieldRef> = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            if primary_key_columns.contains(&field.name().as_str()) {
                key_columns.push(batch.column(col_idx).slice(row, 1));
                key_fields.push(Arc::clone(field));
            }
        }

        let key_schema = Arc::new(Schema::new(key_fields));
        RecordBatch::try_new(key_schema, key_columns).context("Failed to extract key row")
    }

    /// Select which row indices to mutate based on the ratio.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    fn select_rows_to_mutate(&mut self, total_rows: usize) -> HashSet<usize> {
        let num_to_mutate =
            ((total_rows as f64 * self.config.mutation_ratio).round() as usize).min(total_rows);

        if num_to_mutate == 0 {
            return HashSet::new();
        }

        // Use reservoir sampling for reproducible random selection
        let indices = sample(&mut self.rng, total_rows, num_to_mutate);
        indices.into_iter().collect()
    }

    /// Generate mutation data for all rows.
    fn generate_mutation_data(
        &mut self,
        dataset: &dyn StreamingDataset,
        batches: &[RecordBatch],
    ) -> Result<MutationData> {
        let schema = dataset.schema();
        let pk_columns = dataset.primary_key_columns();

        // Count total rows
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

        // Select which rows to mutate
        let rows_to_mutate: Vec<usize> = {
            let set = self.select_rows_to_mutate(total_rows);
            let mut vec: Vec<usize> = set.into_iter().collect();
            vec.sort_unstable();
            vec
        };

        // Split mutated rows 50/50 between update path and delete path
        let midpoint = rows_to_mutate.len() / 2;
        let update_path_indices: HashSet<usize> =
            rows_to_mutate[..midpoint].iter().copied().collect();
        let delete_path_indices: HashSet<usize> =
            rows_to_mutate[midpoint..].iter().copied().collect();

        // Collect data for each path
        let mut update_path_wrong: Vec<RecordBatch> = Vec::new();
        let mut update_path_correct: Vec<RecordBatch> = Vec::new();
        let mut delete_path_wrong: Vec<RecordBatch> = Vec::new();
        let mut delete_path_keys: Vec<RecordBatch> = Vec::new();
        let mut delete_path_correct: Vec<RecordBatch> = Vec::new();
        let mut direct_inserts: Vec<RecordBatch> = Vec::new();

        let mut global_row_idx = 0;

        for batch in batches {
            for row in 0..batch.num_rows() {
                if update_path_indices.contains(&global_row_idx) {
                    // Update path: INSERT wrong → UPDATE correct
                    let wrong_row = self.generate_wrong_row(&schema, batch, row, &pk_columns)?;
                    let correct_row = Self::extract_row(batch, row)?;
                    update_path_wrong.push(wrong_row);
                    update_path_correct.push(correct_row);
                } else if delete_path_indices.contains(&global_row_idx) {
                    // Delete path: INSERT wrong → DELETE → INSERT correct
                    let wrong_row = self.generate_wrong_row(&schema, batch, row, &pk_columns)?;
                    let key_row = Self::extract_key_row(batch, row, &pk_columns)?;
                    let correct_row = Self::extract_row(batch, row)?;
                    delete_path_wrong.push(wrong_row);
                    delete_path_keys.push(key_row);
                    delete_path_correct.push(correct_row);
                } else {
                    // Direct insert with correct values
                    let row_batch = Self::extract_row(batch, row)?;
                    direct_inserts.push(row_batch);
                }

                global_row_idx += 1;
            }
        }

        Ok(MutationData {
            update_path: (update_path_wrong, update_path_correct),
            delete_path: (delete_path_wrong, delete_path_keys, delete_path_correct),
            direct_inserts,
        })
    }
}

/// Batch size for concatenating single-row batches.
const CONCATENATE_BATCH_SIZE: usize = 1000;

/// Concatenate multiple single-row batches into larger batches.
fn concatenate_batches(single_row_batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    if single_row_batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = single_row_batches[0].schema();
    let mut result = Vec::new();

    for chunk in single_row_batches.chunks(CONCATENATE_BATCH_SIZE) {
        let batch = arrow::compute::concat_batches(&schema, chunk)
            .context("Failed to concatenate batches")?;
        result.push(batch);
    }

    Ok(result)
}

/// Execute mutation sequences for all datasets using batched operations.
///
/// For each dataset, this will:
/// 1. Select rows to mutate based on `mutation_ratio`
/// 2. Split mutated rows 50/50 between update path and delete path
/// 3. Execute operations:
///    - INSERT wrong values for all mutated rows
///    - UPDATE correct values for update-path rows
///    - DELETE delete-path rows
///    - INSERT correct values for delete-path rows
///    - Direct INSERT remaining rows
///
/// This tests that CDC correctly processes UPDATEs and DELETEs, and the final
/// state matches the expected TPC-H data.
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
        let table_name = source.get_table_name(dataset.table_name());

        // Find the original data for this dataset
        let Some((_, batches)) = original_data.iter().find(|(dt, _)| *dt == dataset_type) else {
            println!("No original data found for {dataset_type}");
            continue;
        };

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        println!(
            "\nProcessing {dataset_type} ({total_rows} total rows, {:.1}% will be mutated)...",
            config.mutation_ratio * 100.0
        );

        // Generate mutation data
        let data = generator.generate_mutation_data(dataset.as_ref(), batches)?;

        let update_path_count = data.update_path.0.len();
        let delete_path_count = data.delete_path.0.len();
        let direct_insert_count = data.direct_inserts.len();

        summary.datasets_processed += 1;
        summary.total_rows += total_rows;
        summary.update_path_rows += update_path_count;
        summary.delete_path_rows += delete_path_count;
        summary.direct_insert_rows += direct_insert_count;

        println!(
            "  {update_path_count} update-path + {delete_path_count} delete-path + {direct_insert_count} direct = {total_rows} total"
        );

        // Step 1: INSERT wrong values for all mutated rows
        let all_wrong: Vec<RecordBatch> = data
            .update_path
            .0
            .into_iter()
            .chain(data.delete_path.0)
            .collect();

        if !all_wrong.is_empty() {
            println!(
                "  Step 1: INSERT {} rows with wrong values...",
                all_wrong.len()
            );
            let batches = concatenate_batches(&all_wrong)?;
            if let Err(e) = source.insert(&table_name, &batches).await {
                eprintln!("    Failed: {e}");
                summary.failed_operations += 1;
            } else {
                println!("    Done");
            }
        }

        // Step 2: UPDATE correct values for update-path rows
        if !data.update_path.1.is_empty() {
            println!(
                "  Step 2: UPDATE {} rows with correct values...",
                data.update_path.1.len()
            );
            let batches = concatenate_batches(&data.update_path.1)?;
            if let Err(e) = source.update(&table_name, &batches).await {
                eprintln!("    Failed: {e}");
                summary.failed_operations += 1;
            } else {
                println!("    Done");
            }
        }

        // Step 3: DELETE delete-path rows
        if !data.delete_path.1.is_empty() {
            println!("  Step 3: DELETE {} rows...", data.delete_path.1.len());
            let batches = concatenate_batches(&data.delete_path.1)?;
            if let Err(e) = source.delete(&table_name, &batches).await {
                eprintln!("    Failed: {e}");
                summary.failed_operations += 1;
            } else {
                println!("    Done");
            }
        }

        // Step 4: INSERT correct values for delete-path rows
        if !data.delete_path.2.is_empty() {
            println!(
                "  Step 4: INSERT {} rows with correct values (delete-path)...",
                data.delete_path.2.len()
            );
            let batches = concatenate_batches(&data.delete_path.2)?;
            if let Err(e) = source.insert(&table_name, &batches).await {
                eprintln!("    Failed: {e}");
                summary.failed_operations += 1;
            } else {
                println!("    Done");
            }
        }

        // Step 5: Direct INSERT remaining rows
        if !data.direct_inserts.is_empty() {
            println!(
                "  Step 5: Direct INSERT {} rows with correct values...",
                data.direct_inserts.len()
            );
            let batches = concatenate_batches(&data.direct_inserts)?;
            if let Err(e) = source.insert(&table_name, &batches).await {
                eprintln!("    Failed: {e}");
                summary.failed_operations += 1;
            } else {
                println!("    Done");
            }
        }

        println!(
            "Completed {dataset_type}: {update_path_count} update-path + {delete_path_count} delete-path + {direct_insert_count} direct = {total_rows} total"
        );
    }

    Ok(summary)
}
