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
//! This module provides functionality to generate reproducible INSERT, UPDATE, and DELETE
//! operations using a seeded random number generator. This allows for deterministic
//! testing of CDC behavior across runs with the same seed.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_framework::anyhow::{Context, Result};

use super::datasets::DatasetType;
use super::traits::StreamingSource;

/// Configuration for mutation generation.
#[derive(Debug, Clone)]
pub struct MutationConfig {
    /// Random seed for reproducibility.
    pub seed: u64,
    /// Total number of mutations to generate.
    pub count: usize,
    /// Ratio of INSERT operations (0.0 - 1.0).
    pub insert_ratio: f64,
    /// Ratio of UPDATE operations (0.0 - 1.0).
    pub update_ratio: f64,
    /// Ratio of DELETE operations (0.0 - 1.0).
    pub delete_ratio: f64,
}

impl Default for MutationConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            count: 100,
            insert_ratio: 0.5,
            update_ratio: 0.3,
            delete_ratio: 0.2,
        }
    }
}

impl MutationConfig {
    /// Normalize ratios to sum to 1.0.
    #[must_use]
    pub fn normalized(&self) -> Self {
        let total = self.insert_ratio + self.update_ratio + self.delete_ratio;
        if total == 0.0 {
            return Self::default();
        }

        Self {
            seed: self.seed,
            count: self.count,
            insert_ratio: self.insert_ratio / total,
            update_ratio: self.update_ratio / total,
            delete_ratio: self.delete_ratio / total,
        }
    }
}

/// Type of mutation operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationType {
    Insert,
    Update,
    Delete,
}

/// A single mutation record.
#[derive(Debug, Clone)]
pub struct Mutation {
    pub mutation_type: MutationType,
    pub dataset_type: DatasetType,
    pub record: RecordBatch,
}

/// Summary of executed mutations.
#[derive(Debug, Default)]
pub struct MutationSummary {
    pub inserts: usize,
    pub updates: usize,
    pub deletes: usize,
    pub errors: usize,
}

impl MutationSummary {
    pub fn total(&self) -> usize {
        self.inserts + self.updates + self.deletes
    }

    pub fn print(&self) {
        println!("\nMutation Summary:");
        println!("  Inserts: {}", self.inserts);
        println!("  Updates: {}", self.updates);
        println!("  Deletes: {}", self.deletes);
        println!("  Errors:  {}", self.errors);
        println!("  Total:   {}", self.total());
    }
}

/// Generator for reproducible mutations.
pub struct MutationGenerator {
    rng: StdRng,
    config: MutationConfig,
    /// Counter for generating unique IDs for new records.
    insert_counter: i64,
}

impl MutationGenerator {
    /// Create a new mutation generator with the given configuration.
    #[must_use]
    pub fn new(config: MutationConfig) -> Self {
        let config = config.normalized();
        let rng = StdRng::seed_from_u64(config.seed);

        Self {
            rng,
            config,
            // Use a very high negative number for inserts to avoid collisions
            insert_counter: -1_000_000,
        }
    }

    /// Determine the mutation type based on configured ratios.
    fn next_mutation_type(&mut self) -> MutationType {
        let roll: f64 = self.rng.random();
        let config = &self.config;

        if roll < config.insert_ratio {
            MutationType::Insert
        } else if roll < config.insert_ratio + config.update_ratio {
            MutationType::Update
        } else {
            MutationType::Delete
        }
    }

    /// Generate a simple test record for the lineitem table.
    ///
    /// For simplicity, we only generate mutations for the lineitem table since it has
    /// a composite primary key and is the most common table for CDC testing.
    fn generate_lineitem_record(&mut self, mutation_type: MutationType) -> Result<RecordBatch> {
        let (orderkey, linenumber) = match mutation_type {
            MutationType::Insert => {
                // Generate new unique keys for inserts
                let key = self.insert_counter;
                self.insert_counter -= 1;
                (key, 1i64)
            }
            MutationType::Update | MutationType::Delete => {
                // Use existing keys for updates and deletes
                // Pick a random order key from a reasonable range (1-1000 for small scale factors)
                let orderkey = self.rng.random_range(1..=1000) as i64;
                let linenumber = self.rng.random_range(1..=7) as i64;
                (orderkey, linenumber)
            }
        };

        // For deletes, we only need the key columns
        if mutation_type == MutationType::Delete {
            let schema = Schema::new(vec![
                Field::new("l_orderkey", DataType::Int64, false),
                Field::new("l_linenumber", DataType::Int64, false),
            ]);

            return RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(Int64Array::from(vec![orderkey])),
                    Arc::new(Int64Array::from(vec![linenumber])),
                ],
            )
            .context("Failed to create delete key batch");
        }

        // For inserts and updates, we need the full record
        // Use a simplified schema for mutation testing
        let schema = Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int64, false),
            Field::new("l_quantity", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Utf8, false),
            Field::new("l_discount", DataType::Utf8, false),
            Field::new("l_tax", DataType::Utf8, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Utf8, false),
            Field::new("l_commitdate", DataType::Utf8, false),
            Field::new("l_receiptdate", DataType::Utf8, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
        ]);

        let quantity = self.rng.random_range(1..=50) as i64;
        let price = format!("{:.2}", self.rng.random_range(100.0..10000.0));
        let discount = format!("{:.2}", self.rng.random_range(0.0..0.1));
        let tax = format!("{:.2}", self.rng.random_range(0.0..0.08));

        let return_flags = ["A", "N", "R"];
        let return_flag = return_flags[self.rng.random_range(0..3)];
        let line_status = if mutation_type == MutationType::Insert {
            "O"
        } else {
            "F"
        };

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![orderkey])),
                Arc::new(Int64Array::from(vec![linenumber])),
                Arc::new(Int64Array::from(vec![quantity])),
                Arc::new(StringArray::from(vec![price.as_str()])),
                Arc::new(StringArray::from(vec![discount.as_str()])),
                Arc::new(StringArray::from(vec![tax.as_str()])),
                Arc::new(StringArray::from(vec![return_flag])),
                Arc::new(StringArray::from(vec![line_status])),
                Arc::new(StringArray::from(vec!["2024-01-15"])),
                Arc::new(StringArray::from(vec!["2024-01-20"])),
                Arc::new(StringArray::from(vec!["2024-01-25"])),
                Arc::new(StringArray::from(vec!["DELIVER IN PERSON"])),
                Arc::new(StringArray::from(vec!["TRUCK"])),
                Arc::new(StringArray::from(vec!["MUTATION_TEST_RECORD"])),
                Arc::new(Int64Array::from(vec![self.rng.random_range(1..=200000) as i64])),
                Arc::new(Int64Array::from(vec![self.rng.random_range(1..=10000) as i64])),
            ],
        )
        .context("Failed to create mutation record batch")
    }

    /// Generate all mutations according to the configuration.
    pub fn generate_mutations(&mut self) -> Result<Vec<Mutation>> {
        let mut mutations = Vec::with_capacity(self.config.count);

        for _ in 0..self.config.count {
            let mutation_type = self.next_mutation_type();
            let record = self.generate_lineitem_record(mutation_type)?;

            mutations.push(Mutation {
                mutation_type,
                dataset_type: DatasetType::Lineitem,
                record,
            });
        }

        Ok(mutations)
    }
}

/// Execute mutations against a streaming source.
pub async fn execute_mutations(
    source: &dyn StreamingSource,
    mutations: &[Mutation],
) -> Result<MutationSummary> {
    let mut summary = MutationSummary::default();

    for (i, mutation) in mutations.iter().enumerate() {
        let table_name = mutation.dataset_type.table_name();

        let result = match mutation.mutation_type {
            MutationType::Insert => {
                source.insert(table_name, &[mutation.record.clone()]).await
            }
            MutationType::Update => {
                source.update(table_name, &[mutation.record.clone()]).await
            }
            MutationType::Delete => {
                source.delete(table_name, &[mutation.record.clone()]).await
            }
        };

        match result {
            Ok(()) => match mutation.mutation_type {
                MutationType::Insert => summary.inserts += 1,
                MutationType::Update => summary.updates += 1,
                MutationType::Delete => summary.deletes += 1,
            },
            Err(e) => {
                println!("Mutation {} failed: {}", i, e);
                summary.errors += 1;
            }
        }

        // Progress update every 10 mutations
        if (i + 1) % 10 == 0 {
            println!("Executed {}/{} mutations", i + 1, mutations.len());
        }
    }

    Ok(summary)
}
