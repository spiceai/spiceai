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

//! Supplier dataset for streaming benchmarks.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::Connection;
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// Supplier dataset (TPCH).
///
/// Generates supplier data using `DuckDB`'s TPCH extension.
/// Contains 10,000 rows at SF=1.
pub struct SupplierDataset;

impl SupplierDataset {
    /// Get the Arrow schema for the supplier table.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Float64, false),
            Field::new("s_comment", DataType::Utf8, false),
        ])
    }
}

impl StreamingDataset for SupplierDataset {
    fn table_name(&self) -> &'static str {
        "supplier"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Supplier
    }

    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        println!("Generating TPCH supplier data with scale factor {scale_factor}");

        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        conn.execute_batch("INSTALL tpch; LOAD tpch;")
            .context("Failed to load TPCH extension")?;

        conn.execute_batch(&format!("CALL dbgen(sf={scale_factor});"))
            .context("Failed to generate TPCH data")?;

        let mut stmt = conn
            .prepare(
                "SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, CAST(s_acctbal AS DOUBLE) as s_acctbal, s_comment
                FROM supplier
                ORDER BY s_suppkey",
            )
            .context("Failed to prepare supplier query")?;

        let mut s_suppkey = Vec::new();
        let mut s_name = Vec::new();
        let mut s_address = Vec::new();
        let mut s_nationkey = Vec::new();
        let mut s_phone = Vec::new();
        let mut s_acctbal = Vec::new();
        let mut s_comment = Vec::new();

        let mut rows = stmt.query([]).context("Failed to query supplier data")?;
        while let Some(row) = rows.next().context("Failed to read row")? {
            s_suppkey.push(row.get::<_, i64>(0)?);
            s_name.push(row.get::<_, String>(1)?);
            s_address.push(row.get::<_, String>(2)?);
            s_nationkey.push(row.get::<_, i64>(3)?);
            s_phone.push(row.get::<_, String>(4)?);
            s_acctbal.push(row.get::<_, f64>(5)?);
            s_comment.push(row.get::<_, String>(6)?);
        }

        let record_count = s_suppkey.len();
        println!("Generated {record_count} supplier records");

        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(s_suppkey)),
                Arc::new(StringArray::from(s_name)),
                Arc::new(StringArray::from(s_address)),
                Arc::new(Int64Array::from(s_nationkey)),
                Arc::new(StringArray::from(s_phone)),
                Arc::new(Float64Array::from(s_acctbal)),
                Arc::new(StringArray::from(s_comment)),
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
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(StringArray::from(vec!["000-000-0000"])),
                Arc::new(Float64Array::from(vec![0.0])),
                Arc::new(StringArray::from(vec!["BENCHMARK_MARKER"])),
            ],
        )
        .context("Failed to create marker RecordBatch")?;

        Ok(batch)
    }

    fn marker_detection_query(&self) -> String {
        format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE s_suppkey = -1",
            self.table_name()
        )
    }

    fn schema(&self) -> arrow::datatypes::Schema {
        Self::schema()
    }

    fn primary_key_columns(&self) -> Vec<&'static str> {
        vec!["s_suppkey"]
    }
}
