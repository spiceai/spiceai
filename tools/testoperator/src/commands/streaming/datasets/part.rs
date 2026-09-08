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

//! Part dataset for streaming benchmarks.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::Connection;
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// Part dataset (TPCH).
///
/// Generates part data using `DuckDB`'s TPCH extension.
/// Contains 200,000 rows at SF=1.
pub struct PartDataset;

impl PartDataset {
    /// Get the Arrow schema for the part table.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int64, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Float64, false),
            Field::new("p_comment", DataType::Utf8, false),
        ])
    }
}

impl StreamingDataset for PartDataset {
    fn table_name(&self) -> &'static str {
        "part"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Part
    }

    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        println!("Generating TPCH part data with scale factor {scale_factor}");

        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        conn.execute_batch("INSTALL tpch; LOAD tpch;")
            .context("Failed to load TPCH extension")?;

        conn.execute_batch(&format!("CALL dbgen(sf={scale_factor});"))
            .context("Failed to generate TPCH data")?;

        let mut stmt = conn
            .prepare(
                "SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container,
                        CAST(p_retailprice AS DOUBLE) as p_retailprice,
                        p_comment
                FROM part
                ORDER BY p_partkey",
            )
            .context("Failed to prepare part query")?;

        let mut p_partkey = Vec::new();
        let mut p_name = Vec::new();
        let mut p_mfgr = Vec::new();
        let mut p_brand = Vec::new();
        let mut p_type = Vec::new();
        let mut p_size = Vec::new();
        let mut p_container = Vec::new();
        let mut p_retailprice = Vec::new();
        let mut p_comment = Vec::new();

        let mut rows = stmt.query([]).context("Failed to query part data")?;
        while let Some(row) = rows.next().context("Failed to read row")? {
            p_partkey.push(row.get::<_, i64>(0)?);
            p_name.push(row.get::<_, String>(1)?);
            p_mfgr.push(row.get::<_, String>(2)?);
            p_brand.push(row.get::<_, String>(3)?);
            p_type.push(row.get::<_, String>(4)?);
            p_size.push(row.get::<_, i64>(5)?);
            p_container.push(row.get::<_, String>(6)?);
            p_retailprice.push(row.get::<_, f64>(7)?);
            p_comment.push(row.get::<_, String>(8)?);
        }

        let record_count = p_partkey.len();
        println!("Generated {record_count} part records");

        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(p_partkey)),
                Arc::new(StringArray::from(p_name)),
                Arc::new(StringArray::from(p_mfgr)),
                Arc::new(StringArray::from(p_brand)),
                Arc::new(StringArray::from(p_type)),
                Arc::new(Int64Array::from(p_size)),
                Arc::new(StringArray::from(p_container)),
                Arc::new(Float64Array::from(p_retailprice)),
                Arc::new(StringArray::from(p_comment)),
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
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(Float64Array::from(vec![0.0])),
                Arc::new(StringArray::from(vec!["BENCHMARK_MARKER"])),
            ],
        )
        .context("Failed to create marker RecordBatch")?;

        Ok(batch)
    }

    fn marker_detection_query(&self) -> String {
        format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE p_partkey = -1",
            self.table_name()
        )
    }

    fn schema(&self) -> arrow::datatypes::Schema {
        Self::schema()
    }

    fn primary_key_columns(&self) -> Vec<&'static str> {
        vec!["p_partkey"]
    }
}
