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

//! Region dataset for streaming benchmarks.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::Connection;
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// Region dataset (TPCH).
///
/// Generates region data using `DuckDB`'s TPCH extension.
/// Contains 5 rows at any scale factor.
pub struct RegionDataset;

impl RegionDataset {
    /// Get the Arrow schema for the region table.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ])
    }
}

impl StreamingDataset for RegionDataset {
    fn table_name(&self) -> &'static str {
        "region"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Region
    }

    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        println!("Generating TPCH region data with scale factor {scale_factor}");

        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        conn.execute_batch("INSTALL tpch; LOAD tpch;")
            .context("Failed to load TPCH extension")?;

        conn.execute_batch(&format!("CALL dbgen(sf={scale_factor});"))
            .context("Failed to generate TPCH data")?;

        let mut stmt = conn
            .prepare(
                "SELECT r_regionkey, r_name, r_comment
                FROM region
                ORDER BY r_regionkey",
            )
            .context("Failed to prepare region query")?;

        let mut r_regionkey = Vec::new();
        let mut r_name = Vec::new();
        let mut r_comment = Vec::new();

        let mut rows = stmt.query([]).context("Failed to query region data")?;
        while let Some(row) = rows.next().context("Failed to read row")? {
            r_regionkey.push(row.get::<_, i64>(0)?);
            r_name.push(row.get::<_, String>(1)?);
            r_comment.push(row.get::<_, String>(2)?);
        }

        let record_count = r_regionkey.len();
        println!("Generated {record_count} region records");

        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(r_regionkey)),
                Arc::new(StringArray::from(r_name)),
                Arc::new(StringArray::from(r_comment)),
            ],
        )
        .context("Failed to create Arrow RecordBatch")?;

        Ok(vec![batch])
    }

    fn marker_record(&self) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(vec![-1i64])),
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(StringArray::from(vec!["BENCHMARK_MARKER"])),
            ],
        )
        .context("Failed to create marker RecordBatch")?;

        Ok(batch)
    }

    fn marker_detection_query(&self) -> String {
        format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE r_regionkey = -1",
            self.table_name()
        )
    }

    fn schema(&self) -> arrow::datatypes::Schema {
        Self::schema()
    }

    fn primary_key_columns(&self) -> Vec<&'static str> {
        vec!["r_regionkey"]
    }
}
