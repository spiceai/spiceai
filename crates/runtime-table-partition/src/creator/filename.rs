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

use std::{
    num::ParseIntError,
    path::{Path, PathBuf},
    str::ParseBoolError,
};

use arrow_schema::{DataType, TimeUnit};
use datafusion::{
    common::{ExprSchema, HashMap},
    logical_expr::{ExprSchemable, expr::Alias},
    prelude::Expr,
    scalar::ScalarValue,
};
use snafu::prelude::*;

use crate::expression::PartitionedBy;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported scalar value type: {data_type}"))]
    UnsupportedType { data_type: DataType },

    #[snafu(display("Unsupported partition key: {value:?}"))]
    UnsupportedPartitionKey { value: ScalarValue },

    #[snafu(display("Failed to parse partition key: {source}"))]
    Parsing {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed while discovering partitions: {source}"))]
    Discovering {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Create a hive style partitioned directory.
///
/// # Errors
/// Returns an error for unsupported [`ScalarValue`] partition keys
pub fn to_hive_partition_dir(pairings: &[(PartitionedBy, ScalarValue)]) -> Result<PathBuf, Error> {
    let mut path = PathBuf::new();
    for (partitioned_by, key) in pairings {
        let name = &partitioned_by.name;
        let key = encode_key(key)?;
        let part = format!("{name}={key}");
        path = path.join(part);
    }

    Ok(path)
}

/// Encodes a [`ScalarValue`] partition key into a Hive-style string representation.
///
/// NULL values are encoded as `"none"` following Hive partitioning conventions.
///
/// # Errors
/// Returns [`Error::UnsupportedPartitionKey`] if the scalar value type is not supported
/// for partitioning.
pub fn encode_key(key: &ScalarValue) -> Result<String, Error> {
    let key = match key {
        ScalarValue::Boolean(v) => v.map(|v| format!("{v}")),
        ScalarValue::Int8(v) => v.map(|v| format!("{v}")),
        ScalarValue::Int16(v) => v.map(|v| format!("{v}")),
        ScalarValue::Int32(v) => v.map(|v| format!("{v}")),
        ScalarValue::Int64(v) => v.map(|v| format!("{v}")),
        ScalarValue::UInt8(v) => v.map(|v| format!("{v}")),
        ScalarValue::UInt16(v) => v.map(|v| format!("{v}")),
        ScalarValue::UInt32(v) => v.map(|v| format!("{v}")),
        ScalarValue::UInt64(v) => v.map(|v| format!("{v}")),
        ScalarValue::TimestampSecond(v, _) => v.map(|v| format!("{v}")),
        ScalarValue::TimestampMillisecond(v, _) => v.map(|v| format!("{v}")),
        ScalarValue::TimestampMicrosecond(v, _) => v.map(|v| format!("{v}")),
        ScalarValue::TimestampNanosecond(v, _) => v.map(|v| format!("{v}")),
        ScalarValue::Utf8(v) => v.as_ref().map(std::string::ToString::to_string),
        value => {
            return Err(Error::UnsupportedPartitionKey {
                value: value.clone(),
            });
        }
    };

    Ok(key.unwrap_or("none".to_string()))
}

/// Discover hive style partitions in the `base_dir` recursively.
///
/// # Errors
/// Returns an error if the directory structure cannot be read or parsing
/// expected [`ScalarValue`]s fails.
pub fn discover_hive_partitions(
    schema: &dyn ExprSchema,
    base_dir: &Path,
    partitioned_by: &[PartitionedBy],
) -> Result<Vec<(Vec<ScalarValue>, PathBuf)>, Error> {
    let mut results = Vec::new();

    let partition_map: HashMap<String, &PartitionedBy> =
        partitioned_by.iter().map(|p| (p.name.clone(), p)).collect();

    discover_partitions_recursive(schema, base_dir, &partition_map, &mut results, &[])?;

    Ok(results)
}

fn discover_partitions_recursive(
    schema: &dyn ExprSchema,
    current_dir: &Path,
    partition_map: &HashMap<String, &PartitionedBy>,
    results: &mut Vec<(Vec<ScalarValue>, PathBuf)>,
    current_partitions: &[ScalarValue],
) -> Result<(), Error> {
    let Some(dir_name) = current_dir.file_name().and_then(|n| n.to_str()) else {
        return Ok(());
    };

    let mut new_partitions = current_partitions.to_vec();

    if let Some((partition_name, value_str)) = dir_name.split_once('=')
        && let Some(partition_def) = partition_map.get(partition_name)
    {
        let parsed_value = parse_partition_value(schema, partition_def, value_str)?;
        new_partitions.push(parsed_value);
    }

    let entries =
        std::fs::read_dir(current_dir).map_err(|e| Error::Discovering { source: e.into() })?;

    let mut has_files = false;
    let mut subdirs = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| Error::Discovering { source: e.into() })?;

        let metadata = entry
            .metadata()
            .map_err(|e| Error::Discovering { source: e.into() })?;

        if metadata.is_dir() {
            subdirs.push(entry.path());
        } else {
            has_files = true;
        }
    }

    if has_files {
        let file_paths = std::fs::read_dir(current_dir)
            .map_err(|e| Error::Discovering { source: e.into() })?
            .filter_map(|entry| {
                let Ok(entry) = entry else {
                    return None;
                };

                let Ok(metadata) = entry.metadata() else {
                    return None;
                };

                if metadata.is_file() {
                    Some(entry.path())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for file_path in file_paths {
            results.push((new_partitions.clone(), file_path));
        }
    }

    if !subdirs.is_empty() {
        for subdir in subdirs {
            discover_partitions_recursive(
                schema,
                &subdir,
                partition_map,
                results,
                &new_partitions,
            )?;
        }
    }

    Ok(())
}

macro_rules! parse_numeric_scalar {
    ($value_str:expr, $scalar_type:ident, $parse_type:ty) => {
        if $value_str == "none" || $value_str == "NULL" {
            ScalarValue::$scalar_type(None)
        } else {
            let parsed: $parse_type = $value_str.parse()?;
            ScalarValue::$scalar_type(Some(parsed))
        }
    };
}

/// Converts string values from hive-style partition directories (e.g., "year=2025") into
/// strongly-typed [`ScalarValue`].
///
/// # Arguments
/// * `schema` - Schema context for type resolution
/// * `partition_by` - Partition definition containing the expression and name
/// * `value_str` - String value to parse (e.g., "2025", "october", "none")
///
/// # Returns
/// A [`ScalarValue`] of the appropriate type matching the partition's data type
///
/// # Errors
/// Returns `Error::Parsing` if the string cannot be parsed to the expected type,
/// or `Error::UnsupportedType` if the partition's data type is not supported.
pub fn parse_partition_value(
    schema: &dyn ExprSchema,
    partition_by: &PartitionedBy,
    value_str: &str,
) -> Result<ScalarValue, Error> {
    let alias = Expr::Alias(Alias::new(
        partition_by.expression.clone(),
        Option::<String>::None,
        &partition_by.name,
    ));
    let data_type = alias
        .get_type(schema)
        .map_err(|e| Error::Parsing { source: e.into() })?;
    let scalar_value = match data_type {
        DataType::Boolean => {
            if value_str == "none" || value_str == "NULL" {
                ScalarValue::Boolean(None)
            } else {
                let b = value_str.parse()?;
                ScalarValue::Boolean(Some(b))
            }
        }
        DataType::Int8 => parse_numeric_scalar!(value_str, Int8, i8),
        DataType::Int16 => parse_numeric_scalar!(value_str, Int16, i16),
        DataType::Int32 => parse_numeric_scalar!(value_str, Int32, i32),
        DataType::Int64 => parse_numeric_scalar!(value_str, Int64, i64),
        DataType::UInt8 => parse_numeric_scalar!(value_str, UInt8, u8),
        DataType::UInt16 => parse_numeric_scalar!(value_str, UInt16, u16),
        DataType::UInt32 => parse_numeric_scalar!(value_str, UInt32, u32),
        DataType::UInt64 => parse_numeric_scalar!(value_str, UInt64, u64),
        DataType::Timestamp(t, _) => match t {
            TimeUnit::Second => ScalarValue::TimestampSecond(Some(value_str.parse()?), None),
            TimeUnit::Millisecond => {
                ScalarValue::TimestampMillisecond(Some(value_str.parse()?), None)
            }
            TimeUnit::Microsecond => {
                ScalarValue::TimestampMicrosecond(Some(value_str.parse()?), None)
            }
            TimeUnit::Nanosecond => {
                ScalarValue::TimestampNanosecond(Some(value_str.parse()?), None)
            }
        },
        DataType::Utf8 => {
            if value_str == "none" || value_str == "NULL" {
                ScalarValue::Utf8(None)
            } else {
                ScalarValue::Utf8(Some(value_str.to_string()))
            }
        }
        data_type => return Err(Error::UnsupportedType { data_type }),
    };

    Ok(scalar_value)
}

impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Self::Parsing {
            source: value.into(),
        }
    }
}

impl From<ParseBoolError> for Error {
    fn from(value: ParseBoolError) -> Self {
        Self::Parsing {
            source: value.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::Write,
    };

    use arrow_schema::{Field, Schema};
    use datafusion::{
        common::DFSchema,
        prelude::{col, lit},
    };
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_hive_partition_name() -> Result<(), Error> {
        let partitioned_by = vec![
            PartitionedBy {
                name: "year".to_string(),
                expression: col("year"),
            },
            PartitionedBy {
                name: "month".to_string(),
                expression: col("month"),
            },
            PartitionedBy {
                name: "day".to_string(),
                expression: col("day"),
            },
        ];

        let keys = vec![
            ScalarValue::Int32(Some(2025)),
            ScalarValue::Int32(Some(10)),
            ScalarValue::Int32(Some(15)),
        ];

        let pairings = partitioned_by.into_iter().zip(keys).collect::<Vec<_>>();
        let path = to_hive_partition_dir(&pairings)?;

        let parts = path.iter().collect::<Vec<_>>();
        for (want, got) in ["year=2025", "month=10", "day=15"].iter().zip(parts) {
            assert_eq!(*want, got.to_str().expect("to_str"));
        }

        Ok(())
    }

    #[test]
    fn test_encode_key_with_nulls() -> Result<(), Error> {
        // Test NULL values are encoded as "none"
        assert_eq!(encode_key(&ScalarValue::Int32(None))?, "none");
        assert_eq!(encode_key(&ScalarValue::Int64(None))?, "none");
        assert_eq!(encode_key(&ScalarValue::Utf8(None))?, "none");
        assert_eq!(encode_key(&ScalarValue::Boolean(None))?, "none");
        assert_eq!(encode_key(&ScalarValue::UInt32(None))?, "none");

        // Test non-NULL values are encoded correctly
        assert_eq!(encode_key(&ScalarValue::Int32(Some(42)))?, "42");
        assert_eq!(encode_key(&ScalarValue::Int64(Some(-100)))?, "-100");
        assert_eq!(
            encode_key(&ScalarValue::Utf8(Some("test".to_string())))?,
            "test"
        );
        assert_eq!(encode_key(&ScalarValue::Boolean(Some(true)))?, "true");
        assert_eq!(encode_key(&ScalarValue::UInt32(Some(99)))?, "99");

        Ok(())
    }

    #[test]
    fn test_parse_partition_value_with_none() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![
            Field::new("int_col", DataType::Int32, true),
            Field::new("str_col", DataType::Utf8, true),
            Field::new("bool_col", DataType::Boolean, true),
        ]);
        let df_schema = DFSchema::try_from(schema)?;

        // Test parsing "none" as NULL
        let partition_by_int = PartitionedBy {
            name: "int_col".to_string(),
            expression: col("int_col"),
        };
        let result = parse_partition_value(&df_schema, &partition_by_int, "none")?;
        assert_eq!(result, ScalarValue::Int32(None));

        let partition_by_str = PartitionedBy {
            name: "str_col".to_string(),
            expression: col("str_col"),
        };
        let result = parse_partition_value(&df_schema, &partition_by_str, "none")?;
        assert_eq!(result, ScalarValue::Utf8(None));

        let partition_by_bool = PartitionedBy {
            name: "bool_col".to_string(),
            expression: col("bool_col"),
        };
        let result = parse_partition_value(&df_schema, &partition_by_bool, "none")?;
        assert_eq!(result, ScalarValue::Boolean(None));

        // Test parsing actual values
        let result = parse_partition_value(&df_schema, &partition_by_int, "42")?;
        assert_eq!(result, ScalarValue::Int32(Some(42)));

        let result = parse_partition_value(&df_schema, &partition_by_str, "hello")?;
        assert_eq!(result, ScalarValue::Utf8(Some("hello".to_string())));

        let result = parse_partition_value(&df_schema, &partition_by_bool, "true")?;
        assert_eq!(result, ScalarValue::Boolean(Some(true)));

        Ok(())
    }

    #[test]
    fn test_roundtrip_null_values() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("bucket_col", DataType::Int32, true)]);
        let df_schema = DFSchema::try_from(schema)?;

        let partition_by = PartitionedBy {
            name: "bucket_col".to_string(),
            expression: col("bucket_col"),
        };

        // Test roundtrip: ScalarValue::Int32(None) -> "none" -> ScalarValue::Int32(None)
        let null_value = ScalarValue::Int32(None);
        let encoded = encode_key(&null_value)?;
        assert_eq!(encoded, "none");

        let decoded = parse_partition_value(&df_schema, &partition_by, &encoded)?;
        assert_eq!(decoded, null_value);

        // Test roundtrip with actual value
        let value = ScalarValue::Int32(Some(5));
        let encoded = encode_key(&value)?;
        assert_eq!(encoded, "5");

        let decoded = parse_partition_value(&df_schema, &partition_by, &encoded)?;
        assert_eq!(decoded, value);

        Ok(())
    }

    #[test]
    fn test_discover_hive_partitions_with_multiple_files() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = TempDir::new()?;
        let base_path = temp_dir.path();

        let year_dir = base_path.join("year=2025");
        fs::create_dir_all(&year_dir)?;

        let month_dir = year_dir.join("month=october");
        fs::create_dir_all(&month_dir)?;

        let file1_path = month_dir.join("file1.parquet");
        let mut file1 = File::create(&file1_path)?;
        writeln!(file1, "test data 1")?;

        let file2_path = month_dir.join("file2.parquet");
        let mut file2 = File::create(&file2_path)?;
        writeln!(file2, "test data 2")?;

        let partitioned_by = vec![
            PartitionedBy {
                name: "year".to_string(),
                expression: lit(2025u64),
            },
            PartitionedBy {
                name: "month".to_string(),
                expression: lit("october"),
            },
        ];

        let arrow_schema = Schema::new(vec![
            Field::new("year", DataType::UInt64, false),
            Field::new("month", DataType::Utf8, false),
        ]);

        let df_schema = DFSchema::try_from(arrow_schema)?;

        let results = discover_hive_partitions(&df_schema, base_path, &partitioned_by)?;

        assert_eq!(results.len(), 2);
        for (partitions, path) in results {
            for key in partitions {
                match key {
                    ScalarValue::UInt64(Some(2025u64)) => {}
                    ScalarValue::Utf8(Some(val)) if val == "october" => {}
                    key => panic!("expected 2025u64 or 'october', got {key:?}"),
                }
            }
            assert!(path == file1_path || path == file2_path);
        }

        Ok(())
    }
}
