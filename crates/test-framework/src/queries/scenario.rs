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

use std::{collections::HashMap, path::Path, sync::Arc};

use arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use super::Query;

/// A scenario query definition that can be loaded from a YAML file
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ScenarioQueryDefinition {
    /// Unique name for the query
    pub name: String,

    /// SQL query to execute
    pub sql: String,

    /// Optional expected results for validation
    /// Can be specified as a path to a CSV file or inline data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_results: Option<ExpectedResults>,
}

/// Expected results for query validation
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum ExpectedResults {
    /// Path to a CSV file containing expected results
    FilePath(String),

    /// Inline structured data with columns and rows (comma-delimited strings)
    InlineData { columns: String, rows: Vec<String> },

    /// Inline CSV data as a string (legacy support)
    InlineCSV(String),

    /// Expected row count only (for cases where you just want to verify row count)
    RowCount { row_count: usize },
}

/// A collection of scenario queries loaded from a file
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ScenarioQuerySet {
    /// Optional name for the query set
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// List of queries in this set
    pub queries: Vec<ScenarioQueryDefinition>,
}

impl ScenarioQuerySet {
    /// Load a scenario query set from a YAML file
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())?;
        let query_set: ScenarioQuerySet = serde_yaml::from_str(&content)?;
        Ok(query_set)
    }

    /// Convert scenario query definitions into Query objects
    #[must_use]
    pub fn into_queries(self) -> Vec<Query> {
        self.queries
            .into_iter()
            .map(|def| Query::new(def.name.into(), def.sql.into(), false))
            .collect()
    }

    /// Get expected results for validation (as `RecordBatches`)
    /// Returns a map of query name to expected results
    pub fn get_expected_results(
        &self,
        base_path: Option<&Path>,
    ) -> anyhow::Result<HashMap<Arc<str>, Vec<RecordBatch>>> {
        let mut results = HashMap::new();

        for query in &self.queries {
            if let Some(expected) = &query.expected_results {
                let batches = match expected {
                    ExpectedResults::FilePath(path) => {
                        let full_path = if let Some(base) = base_path {
                            base.join(path)
                        } else {
                            Path::new(path).to_path_buf()
                        };
                        load_csv_as_batches(&full_path)?
                    }
                    ExpectedResults::InlineData { columns, rows } => {
                        load_from_structured_data(columns, rows)?
                    }
                    ExpectedResults::InlineCSV(csv_data) => load_csv_from_string(csv_data)?,
                    ExpectedResults::RowCount { .. } => {
                        // Row count validation is handled separately
                        continue;
                    }
                };

                results.insert(Arc::from(query.name.as_str()), batches);
            }
        }

        Ok(results)
    }

    /// Get expected row counts for queries that only specify row count validation
    #[must_use]
    pub fn get_expected_row_counts(&self) -> HashMap<Arc<str>, usize> {
        let mut counts = HashMap::new();

        for query in &self.queries {
            if let Some(ExpectedResults::RowCount { row_count }) = &query.expected_results {
                counts.insert(Arc::from(query.name.as_str()), *row_count);
            }
        }

        counts
    }
}

/// Load CSV file as Arrow `RecordBatches`
fn load_csv_as_batches(path: &Path) -> anyhow::Result<Vec<RecordBatch>> {
    use arrow::csv::ReaderBuilder;
    use arrow::csv::reader::Format;
    use std::io::Seek;

    let mut file = std::fs::File::open(path)?;
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut file, None)?;
    file.rewind()?;

    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_format(format)
        .build(file)?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }

    Ok(batches)
}

/// Load CSV from string as Arrow `RecordBatches`
fn load_csv_from_string(csv_data: &str) -> anyhow::Result<Vec<RecordBatch>> {
    use arrow::csv::ReaderBuilder;
    use arrow::csv::reader::Format;
    use std::io::{Cursor, Seek};

    let mut cursor = Cursor::new(csv_data.as_bytes());
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut cursor, None)?;
    cursor.rewind()?;

    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_format(format)
        .build(cursor)?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }

    Ok(batches)
}

/// Load structured data (columns + rows) as Arrow `RecordBatches`
/// Columns and rows are comma-delimited strings
fn load_from_structured_data(
    columns_str: &str,
    rows_str: &[String],
) -> anyhow::Result<Vec<RecordBatch>> {
    use arrow::csv::ReaderBuilder;
    use arrow::csv::reader::Format;
    use std::io::{Cursor, Seek};

    // Parse column names
    let columns: Vec<&str> = columns_str.split(',').map(str::trim).collect();

    if columns.is_empty() {
        return Ok(vec![]);
    }

    // Build CSV string from columns and rows
    let mut csv_data = columns.join(",");
    csv_data.push('\n');

    for row in rows_str {
        csv_data.push_str(row);
        csv_data.push('\n');
    }

    // Parse CSV data
    let mut cursor = Cursor::new(csv_data.as_bytes());
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut cursor, None)?;
    cursor.rewind()?;

    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_format(format)
        .build(cursor)?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_scenario_query_set() {
        let yaml = r#"
name: test_queries
queries:
  - name: simple_select
    sql: SELECT * FROM test_table
  - name: count_query
    sql: SELECT COUNT(*) FROM test_table
    expected_results:
      row_count: 1
  - name: with_structured_data
    sql: SELECT id, name FROM users
    expected_results:
      columns: "id, name"
      rows:
        - "1, Alice"
        - "2, Bob"
"#;

        let query_set: ScenarioQuerySet = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
        assert_eq!(query_set.name, Some("test_queries".to_string()));
        assert_eq!(query_set.queries.len(), 3);
        assert_eq!(query_set.queries[0].name, "simple_select");
        assert!(query_set.queries[0].expected_results.is_none());
        assert!(query_set.queries[1].expected_results.is_some());

        // Test structured data parsing
        if let Some(ExpectedResults::InlineData { columns, rows }) =
            &query_set.queries[2].expected_results
        {
            assert_eq!(columns, "id, name");
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], "1, Alice");
            assert_eq!(rows[1], "2, Bob");
        } else {
            panic!("Expected InlineData variant");
        }
    }
}
