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

//! Partsupp dataset for streaming benchmarks.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb_oracle::Connection;
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// Partsupp dataset (TPCH).
///
/// Generates partsupp data using `DuckDB`'s TPCH extension.
/// Contains 800,000 rows at SF=1.
/// Has composite primary key: (`ps_partkey`, `ps_suppkey`).
pub struct PartsuppDataset;

impl PartsuppDataset {
    /// Get the Arrow schema for the partsupp table.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int64, false),
            Field::new("ps_supplycost", DataType::Float64, false),
            Field::new("ps_comment", DataType::Utf8, false),
        ])
    }
}

impl StreamingDataset for PartsuppDataset {
    fn table_name(&self) -> &'static str {
        "partsupp"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Partsupp
    }

    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        println!("Generating TPCH partsupp data with scale factor {scale_factor}");

        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        conn.execute_batch("INSTALL tpch; LOAD tpch;")
            .context("Failed to load TPCH extension")?;

        conn.execute_batch(&format!("CALL dbgen(sf={scale_factor});"))
            .context("Failed to generate TPCH data")?;

        let batches = conn
            .query(
                "SELECT ps_partkey, ps_suppkey, ps_availqty,
                        CAST(ps_supplycost AS DOUBLE) as ps_supplycost,
                        ps_comment
                FROM partsupp
                ORDER BY ps_partkey, ps_suppkey",
            )
            .context("Failed to query partsupp query")?;

        let batches = super::with_dataset_schema(batches, Self::schema())?;
        let record_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        println!("Generated {record_count} records");
        Ok(batches)
    }

    fn marker_record(&self) -> Result<RecordBatch> {
        // Use negative values for both parts of the composite key
        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(vec![-1i64])),
                Arc::new(Int64Array::from(vec![-1i64])),
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(Float64Array::from(vec![0.0])),
                Arc::new(StringArray::from(vec!["BENCHMARK_MARKER"])),
            ],
        )
        .context("Failed to create marker RecordBatch")?;

        Ok(batch)
    }

    fn marker_detection_query(&self) -> String {
        format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE ps_partkey = -1 AND ps_suppkey = -1",
            self.table_name()
        )
    }

    fn schema(&self) -> arrow::datatypes::Schema {
        Self::schema()
    }

    fn primary_key_columns(&self) -> Vec<&'static str> {
        vec!["ps_partkey", "ps_suppkey"]
    }
}
