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

use arrow::datatypes::Schema;
use snafu::prelude::*;

/// Sort direction for on-refresh sorting operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl std::fmt::Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortDirection::Asc => write!(f, "ASC"),
            SortDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// A parsed sort column specification with column name and direction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortColumn {
    pub column: String,
    pub direction: SortDirection,
}

impl SortColumn {
    #[must_use]
    pub fn new(column: String, direction: SortDirection) -> Self {
        Self { column, direction }
    }

    #[must_use]
    pub fn asc(column: impl Into<String>) -> Self {
        Self::new(column.into(), SortDirection::Asc)
    }

    #[must_use]
    pub fn desc(column: impl Into<String>) -> Self {
        Self::new(column.into(), SortDirection::Desc)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid sort column specification '{specification}', expected 'column [ASC|DESC]'"
    ))]
    InvalidSpecification { specification: String },

    #[snafu(display(
        "Invalid sort direction '{direction}' for column '{column}', expected 'ASC' or 'DESC'"
    ))]
    InvalidDirection { column: String, direction: String },

    #[snafu(display(
        "Sort column '{column}' does not exist in schema. Available columns: {available}"
    ))]
    ColumnNotFound { column: String, available: String },

    #[snafu(display("No valid sort columns found in '{input}'"))]
    NoColumnsFound { input: String },
}

/// Parses and validates the `on_refresh_sort_columns` parameter against the schema.
///
/// # Format
/// `column1 ASC, column2 DESC` or `column1, column2` (defaults to ASC)
///
/// # Errors
/// Returns an error if:
/// - The input contains invalid sort column specifications
/// - A specified sort direction is not 'ASC' or 'DESC'
/// - A specified column does not exist in the schema
/// - No valid columns are found in the input
pub fn parse_sort_columns(sort_columns_str: &str, schema: &Schema) -> Result<Vec<SortColumn>> {
    let mut result = Vec::new();

    for part in sort_columns_str.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        let sort_column = match parts.as_slice() {
            [column] => SortColumn::asc(*column),
            [column, dir] => {
                let direction = match dir.to_uppercase().as_str() {
                    "ASC" => SortDirection::Asc,
                    "DESC" => SortDirection::Desc,
                    _ => {
                        return InvalidDirectionSnafu {
                            column: (*column).to_string(),
                            direction: (*dir).to_string(),
                        }
                        .fail();
                    }
                };
                SortColumn::new((*column).to_string(), direction)
            }
            _ => {
                return InvalidSpecificationSnafu {
                    specification: trimmed.to_string(),
                }
                .fail();
            }
        };

        // Validate that the column exists in the schema
        if schema.field_with_name(&sort_column.column).is_err() {
            return ColumnNotFoundSnafu {
                column: sort_column.column,
                available: schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            }
            .fail();
        }

        result.push(sort_column);
    }

    if result.is_empty() {
        return NoColumnsFoundSnafu {
            input: sort_columns_str.to_string(),
        }
        .fail();
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ])
    }

    #[test]
    fn test_parse_single_column_default_asc() {
        let schema = test_schema();
        let result = parse_sort_columns("id", &schema).expect("should parse");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column, "id");
        assert_eq!(result[0].direction, SortDirection::Asc);
    }

    #[test]
    fn test_parse_single_column_explicit_asc() {
        let schema = test_schema();
        let result = parse_sort_columns("id ASC", &schema).expect("should parse");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column, "id");
        assert_eq!(result[0].direction, SortDirection::Asc);
    }

    #[test]
    fn test_parse_single_column_desc() {
        let schema = test_schema();
        let result = parse_sort_columns("timestamp DESC", &schema).expect("should parse");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column, "timestamp");
        assert_eq!(result[0].direction, SortDirection::Desc);
    }

    #[test]
    fn test_parse_multiple_columns() {
        let schema = test_schema();
        let result = parse_sort_columns("timestamp DESC, id ASC", &schema).expect("should parse");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].column, "timestamp");
        assert_eq!(result[0].direction, SortDirection::Desc);
        assert_eq!(result[1].column, "id");
        assert_eq!(result[1].direction, SortDirection::Asc);
    }

    #[test]
    fn test_parse_case_insensitive_direction() {
        let schema = test_schema();
        let result = parse_sort_columns("id asc, timestamp desc", &schema).expect("should parse");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].direction, SortDirection::Asc);
        assert_eq!(result[1].direction, SortDirection::Desc);
    }

    #[test]
    fn test_parse_with_extra_whitespace() {
        let schema = test_schema();
        let result =
            parse_sort_columns("  id  ,  timestamp  DESC  ", &schema).expect("should parse");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_parse_empty_parts_ignored() {
        let schema = test_schema();
        let result = parse_sort_columns("id, , timestamp", &schema).expect("should parse");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_error_invalid_direction() {
        let schema = test_schema();
        parse_sort_columns("id INVALID", &schema).expect_err("should fail on invalid direction");
    }

    #[test]
    fn test_error_column_not_found() {
        let schema = test_schema();
        parse_sort_columns("nonexistent DESC", &schema)
            .expect_err("should fail on nonexistent column");
    }

    #[test]
    fn test_error_empty_input() {
        let schema = test_schema();
        parse_sort_columns("", &schema).expect_err("should fail on empty input");
    }

    #[test]
    fn test_error_only_whitespace() {
        let schema = test_schema();
        parse_sort_columns("   ,   ", &schema).expect_err("should fail on whitespace-only input");
    }

    #[test]
    fn test_error_too_many_parts() {
        let schema = test_schema();
        parse_sort_columns("id ASC extra", &schema).expect_err("should fail on too many parts");
    }

    #[test]
    fn test_sort_direction_display() {
        assert_eq!(SortDirection::Asc.to_string(), "ASC");
        assert_eq!(SortDirection::Desc.to_string(), "DESC");
    }
}
