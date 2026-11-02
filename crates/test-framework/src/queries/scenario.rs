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

use arrow::{
    array::{Array, AsArray, RecordBatch},
    datatypes::{DataType, Float64Type, Int64Type},
};
use serde::{Deserialize, Serialize};
use spiceai::Client as SpiceClient;

use super::{ParameterRows, Query, parameterized::ParameterValue};
use crate::flight::query_to_batches;

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

    /// SQL that returns one row of parameters for each query to run. When set, `sql` is a
    /// template: each row becomes one query, with `$1`, `$2`, ... in `sql` replaced by the row's
    /// columns, in order, as SQL literals. All the queries share `name`, so their latencies are
    /// reported together. The SQL runs against `spiced` when the test starts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters_from: Option<String>,
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
        let query_set: ScenarioQuerySet = yaml::from_str(&content)?;
        query_set.validate()?;
        Ok(query_set)
    }

    fn validate(&self) -> anyhow::Result<()> {
        for query in &self.queries {
            if query.parameters_from.is_some() && query.expected_results.is_some() {
                anyhow::bail!(
                    "Scenario query '{}' sets both `parameters_from` and `expected_results`. \
                    The queries of a `parameters_from` template return different results, so \
                    one expected result cannot apply to all of them.",
                    query.name
                );
            }
        }
        Ok(())
    }

    /// Convert scenario query definitions into Query objects. A definition with
    /// `parameters_from` becomes one unexpanded template; see [`expand_parameter_rows`].
    #[must_use]
    pub fn into_queries(self) -> Vec<Query> {
        self.queries
            .into_iter()
            .map(|def| {
                let mut query = Query::new(def.name.into(), def.sql.into(), false);
                if let Some(parameters_from) = def.parameters_from {
                    query.parameter_rows = ParameterRows::Unexpanded(parameters_from.into());
                }
                query
            })
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

/// Replaces each `parameters_from` template in `queries` with one query per row that its SQL
/// returns from `spiced`. Other queries are kept as they are, in order.
///
/// The parameters are inlined as SQL literals, not sent as prepared-statement parameters, because
/// some table functions (for example `text_search`) do not accept a placeholder argument.
///
/// # Errors
///
/// - If the parameter SQL fails, returns no rows, or returns a NULL or a column type that cannot
///   be a parameter.
/// - If the template has a placeholder with no matching column.
pub async fn expand_parameter_rows(
    queries: Vec<Query>,
    spice_client: &Arc<SpiceClient>,
) -> anyhow::Result<Vec<Query>> {
    let mut expanded = Vec::with_capacity(queries.len());
    for query in queries {
        let parameters_sql = match &query.parameter_rows {
            ParameterRows::Unexpanded(sql) => Some(Arc::clone(sql)),
            ParameterRows::None | ParameterRows::Row(_) => None,
        };
        let Some(parameters_sql) = parameters_sql else {
            expanded.push(query);
            continue;
        };

        let batches = query_to_batches(Arc::clone(spice_client), &parameters_sql, None)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to get the parameters of scenario query '{}': {e}",
                    query.name
                )
            })?;
        let rows = parameter_rows(&batches).map_err(|e| {
            anyhow::anyhow!(
                "Failed to read the parameters of scenario query '{}': {e}",
                query.name
            )
        })?;
        if rows.is_empty() {
            anyhow::bail!(
                "The `parameters_from` SQL of scenario query '{}' returned no rows",
                query.name
            );
        }

        println!(
            "Scenario query '{}': {} queries from `parameters_from`",
            query.name,
            rows.len()
        );
        for (row, parameters) in rows.iter().enumerate() {
            let sql = inline_parameters(&query.sql, parameters).map_err(|e| {
                anyhow::anyhow!("Scenario query '{}', parameter row {row}: {e}", query.name)
            })?;
            let mut row_query = Query::new(Arc::clone(&query.name), sql.into(), query.overridden);
            row_query.parameter_rows = ParameterRows::Row(row);
            expanded.push(row_query);
        }
    }
    Ok(expanded)
}

/// Reads every row of `batches` as a list of parameters, one per column.
fn parameter_rows(batches: &[RecordBatch]) -> anyhow::Result<Vec<Vec<ParameterValue>>> {
    let mut rows = Vec::new();
    for batch in batches {
        let columns = batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .map(|(column, field)| parameter_column(column, field.name()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            rows.push(columns.iter().map(|column| column[row].clone()).collect());
        }
    }
    Ok(rows)
}

/// Converts one column to parameter values: text, integers, or floats. NULL is an error.
fn parameter_column(column: &dyn Array, name: &str) -> anyhow::Result<Vec<ParameterValue>> {
    if column.null_count() > 0 {
        anyhow::bail!("column '{name}' has NULL values");
    }
    let values = match column.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let text = arrow::compute::cast(column, &DataType::Utf8)?;
            text.as_string::<i32>()
                .iter()
                .map(|value| ParameterValue::String(value.unwrap_or_default().into()))
                .collect()
        }
        data_type if data_type.is_integer() => {
            let options = arrow::compute::CastOptions {
                safe: false,
                ..Default::default()
            };
            let ints = arrow::compute::cast_with_options(column, &DataType::Int64, &options)?;
            ints.as_primitive::<Int64Type>()
                .values()
                .iter()
                .map(|value| ParameterValue::Number(*value))
                .collect()
        }
        data_type if data_type.is_floating() => {
            let floats = arrow::compute::cast(column, &DataType::Float64)?;
            floats
                .as_primitive::<Float64Type>()
                .values()
                .iter()
                .map(|value| ParameterValue::Float(*value))
                .collect()
        }
        other => anyhow::bail!(
            "column '{name}' has type {other}; a parameter must be text, an integer, or a float"
        ),
    };
    Ok(values)
}

/// Replaces each `$n` placeholder in `template` with `parameters[n - 1]` as a SQL literal, in one
/// pass, so that a placeholder-like string inside a parameter value is not replaced again.
fn inline_parameters(template: &str, parameters: &[ParameterValue]) -> anyhow::Result<String> {
    let mut sql = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(dollar) = rest.find('$') {
        sql.push_str(&rest[..dollar]);
        let after = &rest[dollar + 1..];
        let digits = after.len() - after.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        if digits == 0 {
            sql.push('$');
            rest = after;
            continue;
        }
        let index: usize = after[..digits].parse()?;
        let parameter = index
            .checked_sub(1)
            .and_then(|i| parameters.get(i))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "placeholder ${index} has no parameter; the row has {} column(s)",
                    parameters.len()
                )
            })?;
        sql.push_str(&parameter.to_sql_literal());
        rest = &after[digits..];
    }
    sql.push_str(rest);
    Ok(sql)
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

        let query_set: ScenarioQuerySet = yaml::from_str(yaml).expect("Failed to parse YAML");
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

    #[test]
    fn parameters_from_makes_an_unexpanded_template() {
        let yaml = r"
queries:
  - name: phrase
    sql: SELECT _id FROM text_search(corpus, $1, 10)
    parameters_from: SELECT text FROM test_queries WHERE query_type = 'phrase'
  - name: plain
    sql: SELECT 1
";
        let query_set: ScenarioQuerySet = yaml::from_str(yaml).expect("Failed to parse YAML");
        query_set.validate().expect("valid scenario");
        let queries = query_set.into_queries();
        assert_eq!(
            queries[0].parameter_rows,
            ParameterRows::Unexpanded(
                "SELECT text FROM test_queries WHERE query_type = 'phrase'".into()
            )
        );
        assert_eq!(queries[1].parameter_rows, ParameterRows::None);
    }

    #[test]
    fn parameters_from_rejects_expected_results() {
        let yaml = r"
queries:
  - name: phrase
    sql: SELECT _id FROM text_search(corpus, $1, 10)
    parameters_from: SELECT text FROM test_queries
    expected_results:
      row_count: 10
";
        let query_set: ScenarioQuerySet = yaml::from_str(yaml).expect("Failed to parse YAML");
        let err = query_set.validate().expect_err("both fields are set");
        assert!(err.to_string().contains("phrase"), "{err}");
    }

    #[test]
    fn inline_parameters_replaces_each_placeholder_once() {
        let parameters = [
            ParameterValue::String("it's $2".into()),
            ParameterValue::Number(10),
        ];
        // `$2` inside the first value must stay as text; `$` alone is not a placeholder.
        let sql = inline_parameters("SELECT $1 AS q, $2 AS k, '$' AS d", &parameters)
            .expect("all placeholders have parameters");
        assert_eq!(sql, "SELECT 'it''s $2' AS q, 10 AS k, '$' AS d");

        let err = inline_parameters("SELECT $3", &parameters).expect_err("no third parameter");
        assert!(err.to_string().contains("$3"), "{err}");
        inline_parameters("SELECT $0", &parameters).expect_err("placeholders start at $1");
    }

    #[test]
    fn parameter_rows_read_text_integer_and_float_columns() {
        use arrow::array::{Float32Array, LargeStringArray, UInt8Array};

        let batch = RecordBatch::try_from_iter(vec![
            (
                "text",
                Arc::new(LargeStringArray::from(vec!["a", "b"])) as Arc<dyn Array>,
            ),
            ("k", Arc::new(UInt8Array::from(vec![1_u8, 2])) as _),
            ("w", Arc::new(Float32Array::from(vec![0.5_f32, 1.5])) as _),
        ])
        .expect("valid batch");

        let rows = parameter_rows(&[batch]).expect("supported types");
        assert_eq!(
            rows,
            vec![
                vec![
                    ParameterValue::String("a".into()),
                    ParameterValue::Number(1),
                    ParameterValue::Float(0.5),
                ],
                vec![
                    ParameterValue::String("b".into()),
                    ParameterValue::Number(2),
                    ParameterValue::Float(1.5),
                ],
            ]
        );
    }

    #[test]
    fn parameter_rows_reject_nulls_and_unsupported_types() {
        use arrow::array::{BooleanArray, StringArray};

        let nulls = RecordBatch::try_from_iter(vec![(
            "text",
            Arc::new(StringArray::from(vec![Some("a"), None])) as Arc<dyn Array>,
        )])
        .expect("valid batch");
        let err = parameter_rows(&[nulls]).expect_err("NULL is not a parameter");
        assert!(err.to_string().contains("NULL"), "{err}");

        let booleans = RecordBatch::try_from_iter(vec![(
            "flag",
            Arc::new(BooleanArray::from(vec![true])) as Arc<dyn Array>,
        )])
        .expect("valid batch");
        let err = parameter_rows(&[booleans]).expect_err("booleans are not supported");
        assert!(err.to_string().contains("flag"), "{err}");
    }
}
