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

const PARTITION_KEY_CODEC_PREFIX: &str = "v1";
const NULL_COMPONENT: &str = "n";
const VALUE_COMPONENT_PREFIX: &str = "v";

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

    #[snafu(display(
        "Invalid partition name '{name}': names must contain only ASCII letters, digits, and underscores"
    ))]
    InvalidPartitionName { name: String },
}

/// Create a hive style partitioned directory.
///
/// # Errors
/// Returns an error for unsupported [`ScalarValue`] partition keys
pub fn to_hive_partition_dir(pairings: &[(PartitionedBy, ScalarValue)]) -> Result<PathBuf, Error> {
    let mut path = PathBuf::new();
    for (partitioned_by, key) in pairings {
        let name = &partitioned_by.name;
        validate_partition_name(name)?;
        let key = encode_key(key)?;
        let part = format!("{name}={key}");
        path = path.join(part);
    }

    Ok(path)
}

fn validate_partition_name(name: &str) -> Result<(), Error> {
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        InvalidPartitionNameSnafu { name }
    );
    Ok(())
}

/// Encodes a [`ScalarValue`] partition key into a versioned, path-safe representation.
///
/// NULL values and non-NULL values have distinct encodings, and the scalar type is
/// included so values such as `Int32(1)` and `Utf8("1")` cannot collide. The value
/// payload uses uppercase hexadecimal, making every encoded key a single safe path
/// component even when a string contains `/`, `=`, `..`, or platform separators.
///
/// # Errors
/// Returns [`Error::UnsupportedPartitionKey`] if the scalar value type is not supported
/// for partitioning.
pub fn encode_key(key: &ScalarValue) -> Result<String, Error> {
    let (type_tag, value) = match key {
        ScalarValue::Boolean(v) => ("bool", v.map(|v| v.to_string())),
        ScalarValue::Int8(v) => ("i8", v.map(|v| v.to_string())),
        ScalarValue::Int16(v) => ("i16", v.map(|v| v.to_string())),
        ScalarValue::Int32(v) => ("i32", v.map(|v| v.to_string())),
        ScalarValue::Int64(v) => ("i64", v.map(|v| v.to_string())),
        ScalarValue::UInt8(v) => ("u8", v.map(|v| v.to_string())),
        ScalarValue::UInt16(v) => ("u16", v.map(|v| v.to_string())),
        ScalarValue::UInt32(v) => ("u32", v.map(|v| v.to_string())),
        ScalarValue::UInt64(v) => ("u64", v.map(|v| v.to_string())),
        ScalarValue::TimestampSecond(v, _) => ("ts_s", v.map(|v| v.to_string())),
        ScalarValue::TimestampMillisecond(v, _) => ("ts_ms", v.map(|v| v.to_string())),
        ScalarValue::TimestampMicrosecond(v, _) => ("ts_us", v.map(|v| v.to_string())),
        ScalarValue::TimestampNanosecond(v, _) => ("ts_ns", v.map(|v| v.to_string())),
        ScalarValue::Utf8(v) => ("utf8", v.clone()),
        ScalarValue::LargeUtf8(v) => ("large_utf8", v.clone()),
        ScalarValue::Utf8View(v) => ("utf8_view", v.clone()),
        value => {
            return Err(Error::UnsupportedPartitionKey {
                value: value.clone(),
            });
        }
    };

    Ok(value.map_or_else(
        || format!("{PARTITION_KEY_CODEC_PREFIX}.{type_tag}.{NULL_COMPONENT}"),
        |value| {
            format!(
                "{PARTITION_KEY_CODEC_PREFIX}.{type_tag}.{VALUE_COMPONENT_PREFIX}{}",
                encode_hex(value.as_bytes())
            )
        },
    ))
}

/// Encodes multiple [`ScalarValue`] partition keys into a composite key string.
///
/// This is used for hierarchical partitions where the partition is identified
/// by multiple values (e.g., year=2025 and month=10).
///
/// The composite key is encoded by length-prefixing each versioned component.
/// This keeps tuple boundaries unambiguous without introducing path separators.
///
/// Note: This function produces a compact key format (e.g., "2025/10") without
/// column names. For Hive-style partition directories with column names
/// (e.g., "year=2025/month=10"), see [`to_hive_partition_dir`].
///
/// # Errors
/// Returns [`Error::UnsupportedPartitionKey`] if any scalar value type is not supported.
pub fn encode_composite_key(keys: &[ScalarValue]) -> Result<String, Error> {
    let mut composite = format!("{PARTITION_KEY_CODEC_PREFIX}:");
    for key in keys {
        let encoded = encode_key(key)?;
        composite.push_str(&encoded.len().to_string());
        composite.push(':');
        composite.push_str(&encoded);
    }
    Ok(composite)
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02X}");
    }
    encoded
}

fn decode_hex(encoded: &str) -> Result<String, Error> {
    if !encoded.len().is_multiple_of(2) {
        return Err(Error::Parsing {
            source: "partition value has an odd-length hexadecimal payload".into(),
        });
    }

    let bytes = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair)?;
            Ok(u8::from_str_radix(pair, 16)?)
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()
        .map_err(|source| Error::Parsing { source })?;
    String::from_utf8(bytes).map_err(|source| Error::Parsing {
        source: Box::new(source),
    })
}

fn decode_versioned_partition_value(
    value: &str,
    expected_type: &DataType,
) -> Result<Option<String>, Error> {
    let Some(remainder) = value.strip_prefix("v1.") else {
        return Ok(Some(value.to_string()));
    };
    let Some((type_tag, payload)) = remainder.split_once('.') else {
        return legacy_string_or_error(
            value,
            expected_type,
            "versioned partition value is missing its payload",
        );
    };
    let expected_tag = data_type_tag(expected_type).ok_or_else(|| Error::UnsupportedType {
        data_type: expected_type.clone(),
    })?;
    if type_tag != expected_tag {
        return legacy_string_or_error(
            value,
            expected_type,
            format!(
                "partition value type tag '{type_tag}' does not match expected type '{expected_tag}'"
            ),
        );
    }
    if payload == NULL_COMPONENT {
        return Ok(None);
    }
    let Some(encoded) = payload.strip_prefix(VALUE_COMPONENT_PREFIX) else {
        return legacy_string_or_error(
            value,
            expected_type,
            "versioned partition value has an invalid payload marker",
        );
    };
    match decode_hex(encoded) {
        Ok(decoded) => Ok(Some(decoded)),
        Err(error) => {
            if is_string_type(expected_type) {
                Ok(Some(value.to_string()))
            } else {
                Err(error)
            }
        }
    }
}

fn legacy_string_or_error(
    value: &str,
    expected_type: &DataType,
    message: impl Into<String>,
) -> Result<Option<String>, Error> {
    if is_string_type(expected_type) {
        Ok(Some(value.to_string()))
    } else {
        Err(Error::Parsing {
            source: message.into().into(),
        })
    }
}

const fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn data_type_tag(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Boolean => Some("bool"),
        DataType::Int8 => Some("i8"),
        DataType::Int16 => Some("i16"),
        DataType::Int32 => Some("i32"),
        DataType::Int64 => Some("i64"),
        DataType::UInt8 => Some("u8"),
        DataType::UInt16 => Some("u16"),
        DataType::UInt32 => Some("u32"),
        DataType::UInt64 => Some("u64"),
        DataType::Timestamp(TimeUnit::Second, _) => Some("ts_s"),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some("ts_ms"),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some("ts_us"),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some("ts_ns"),
        DataType::Utf8 => Some("utf8"),
        DataType::LargeUtf8 => Some("large_utf8"),
        DataType::Utf8View => Some("utf8_view"),
        _ => None,
    }
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
    ($value:expr, $scalar_type:ident, $parse_type:ty) => {
        if let Some(value_str) = $value {
            let parsed: $parse_type = value_str.parse()?;
            ScalarValue::$scalar_type(Some(parsed))
        } else {
            ScalarValue::$scalar_type(None)
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
    let legacy_null = value_str == "none" || value_str == "NULL";
    let decoded = decode_versioned_partition_value(value_str, &data_type)?;
    let decoded = if value_str.starts_with("v1.") || !legacy_null {
        decoded
    } else {
        None
    };
    let scalar_value = match data_type {
        DataType::Boolean => {
            if let Some(value_str) = decoded {
                let b = value_str.parse()?;
                ScalarValue::Boolean(Some(b))
            } else {
                ScalarValue::Boolean(None)
            }
        }
        DataType::Int8 => parse_numeric_scalar!(decoded, Int8, i8),
        DataType::Int16 => parse_numeric_scalar!(decoded, Int16, i16),
        DataType::Int32 => parse_numeric_scalar!(decoded, Int32, i32),
        DataType::Int64 => parse_numeric_scalar!(decoded, Int64, i64),
        DataType::UInt8 => parse_numeric_scalar!(decoded, UInt8, u8),
        DataType::UInt16 => parse_numeric_scalar!(decoded, UInt16, u16),
        DataType::UInt32 => parse_numeric_scalar!(decoded, UInt32, u32),
        DataType::UInt64 => parse_numeric_scalar!(decoded, UInt64, u64),
        DataType::Timestamp(t, timezone) => match t {
            TimeUnit::Second => ScalarValue::TimestampSecond(
                decoded.map(|value| value.parse()).transpose()?,
                timezone,
            ),
            TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(
                decoded.map(|value| value.parse()).transpose()?,
                timezone,
            ),
            TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(
                decoded.map(|value| value.parse()).transpose()?,
                timezone,
            ),
            TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(
                decoded.map(|value| value.parse()).transpose()?,
                timezone,
            ),
        },
        DataType::Utf8 => ScalarValue::Utf8(decoded),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(decoded),
        DataType::Utf8View => ScalarValue::Utf8View(decoded),
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
        sync::Arc,
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
        for (want, got) in [
            "year=v1.i32.v32303235",
            "month=v1.i32.v3130",
            "day=v1.i32.v3135",
        ]
        .iter()
        .zip(parts)
        {
            assert_eq!(*want, got.to_str().expect("to_str"));
        }

        Ok(())
    }

    #[test]
    fn test_encode_key_with_nulls() -> Result<(), Error> {
        assert_eq!(encode_key(&ScalarValue::Int32(None))?, "v1.i32.n");
        assert_eq!(encode_key(&ScalarValue::Int64(None))?, "v1.i64.n");
        assert_eq!(encode_key(&ScalarValue::Utf8(None))?, "v1.utf8.n");
        assert_eq!(encode_key(&ScalarValue::Boolean(None))?, "v1.bool.n");
        assert_eq!(encode_key(&ScalarValue::UInt32(None))?, "v1.u32.n");

        // Test non-NULL values are encoded correctly
        assert_eq!(encode_key(&ScalarValue::Int32(Some(42)))?, "v1.i32.v3432");
        assert_eq!(encode_key(&ScalarValue::Int64(Some(-100)))?, "v1.i64.v2D313030");
        assert_eq!(
            encode_key(&ScalarValue::Utf8(Some("test".to_string())))?,
            "v1.utf8.v74657374"
        );
        assert_eq!(encode_key(&ScalarValue::Boolean(Some(true)))?, "v1.bool.v74727565");
        assert_eq!(encode_key(&ScalarValue::UInt32(Some(99)))?, "v1.u32.v3939");

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
    fn test_parse_partition_value_large_utf8() -> Result<(), Box<dyn std::error::Error>> {
        // Cayenne represents strings as LargeUtf8, so inferring existing partitions
        // reads the partition value back as LargeUtf8. partition_key_to_string already
        // handles all three string types; this is the matching string -> scalar
        // direction.
        let schema = Schema::new(vec![Field::new("str_col", DataType::LargeUtf8, true)]);
        let df_schema = DFSchema::try_from(schema)?;
        let partition_by = PartitionedBy {
            name: "str_col".to_string(),
            expression: col("str_col"),
        };

        // NULL sentinels
        assert_eq!(
            parse_partition_value(&df_schema, &partition_by, "none")?,
            ScalarValue::LargeUtf8(None)
        );
        assert_eq!(
            parse_partition_value(&df_schema, &partition_by, "NULL")?,
            ScalarValue::LargeUtf8(None)
        );

        // Actual value
        assert_eq!(
            parse_partition_value(&df_schema, &partition_by, "MONTH")?,
            ScalarValue::LargeUtf8(Some("MONTH".to_string()))
        );

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

        // Test roundtrip through the versioned representation.
        let null_value = ScalarValue::Int32(None);
        let encoded = encode_key(&null_value)?;
        assert_eq!(encoded, "v1.i32.n");

        let decoded = parse_partition_value(&df_schema, &partition_by, &encoded)?;
        assert_eq!(decoded, null_value);

        // Test roundtrip with actual value
        let value = ScalarValue::Int32(Some(5));
        let encoded = encode_key(&value)?;
        assert_eq!(encoded, "v1.i32.v35");

        let decoded = parse_partition_value(&df_schema, &partition_by, &encoded)?;
        assert_eq!(decoded, value);

        Ok(())
    }

    #[test]
    fn test_partition_codec_is_injective_and_path_safe() -> Result<(), Box<dyn std::error::Error>> {
        let null = encode_key(&ScalarValue::Utf8(None))?;
        let literal_none = encode_key(&ScalarValue::Utf8(Some("none".to_string())))?;
        let literal_null = encode_key(&ScalarValue::Utf8(Some("NULL".to_string())))?;
        assert_ne!(null, literal_none);
        assert_ne!(null, literal_null);
        assert_ne!(
            encode_key(&ScalarValue::Int32(Some(1)))?,
            encode_key(&ScalarValue::Utf8(Some("1".to_string())))?
        );

        let slash_tuple = encode_composite_key(&[
            ScalarValue::Utf8(Some("a/b".to_string())),
            ScalarValue::Utf8(Some("c".to_string())),
        ])?;
        let split_tuple = encode_composite_key(&[
            ScalarValue::Utf8(Some("a".to_string())),
            ScalarValue::Utf8(Some("b/c".to_string())),
        ])?;
        assert_ne!(slash_tuple, split_tuple);

        let path = to_hive_partition_dir(&[(
            PartitionedBy {
                name: "value".to_string(),
                expression: col("value"),
            },
            ScalarValue::Utf8(Some("../a/b=c\\d".to_string())),
        )])?;
        assert_eq!(path.components().count(), 1);
        assert!(!path.to_string_lossy().contains(".."));

        Ok(())
    }

    #[test]
    fn test_versioned_string_roundtrip_preserves_legacy_null_sentinels_as_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("str_col", DataType::Utf8, true)]);
        let df_schema = DFSchema::try_from(schema)?;
        let partition_by = PartitionedBy {
            name: "str_col".to_string(),
            expression: col("str_col"),
        };

        for value in ["none", "NULL", "a/b", "", "東京"] {
            let scalar = ScalarValue::Utf8(Some(value.to_string()));
            let encoded = encode_key(&scalar)?;
            assert_eq!(
                parse_partition_value(&df_schema, &partition_by, &encoded)?,
                scalar
            );
        }

        // Existing deployments remain readable with the legacy sentinel format.
        assert_eq!(
            parse_partition_value(&df_schema, &partition_by, "none")?,
            ScalarValue::Utf8(None)
        );
        assert_eq!(
            parse_partition_value(&df_schema, &partition_by, "legacy")?,
            ScalarValue::Utf8(Some("legacy".to_string()))
        );

        Ok(())
    }

    #[test]
    fn test_legacy_string_starting_with_codec_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("str_col", DataType::Utf8, true)]);
        let df_schema = DFSchema::try_from(schema)?;
        let partition_by = PartitionedBy {
            name: "str_col".to_string(),
            expression: col("str_col"),
        };

        for legacy in ["v1.foo", "v1.i32.n", "v1.utf8.vNOT_HEX"] {
            assert_eq!(
                parse_partition_value(&df_schema, &partition_by, legacy)?,
                ScalarValue::Utf8(Some(legacy.to_string()))
            );
        }
        Ok(())
    }

    #[test]
    fn test_timestamp_roundtrip_preserves_timezone() -> Result<(), Box<dyn std::error::Error>> {
        let timezone: Arc<str> = Arc::from("America/Los_Angeles");
        let schema = Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::clone(&timezone))),
            false,
        )]);
        let df_schema = DFSchema::try_from(schema)?;
        let partition_by = PartitionedBy {
            name: "event_time".to_string(),
            expression: col("event_time"),
        };
        let value = ScalarValue::TimestampMillisecond(Some(1_234), Some(timezone));
        let encoded = encode_key(&value)?;

        assert_eq!(
            parse_partition_value(&df_schema, &partition_by, &encoded)?,
            value
        );
        Ok(())
    }

    #[test]
    fn test_hive_partition_rejects_unsafe_name() {
        let result = to_hive_partition_dir(&[(
            PartitionedBy {
                name: "../escape".to_string(),
                expression: col("value"),
            },
            ScalarValue::Utf8(Some("safe".to_string())),
        )]);

        assert!(matches!(result, Err(Error::InvalidPartitionName { .. })));
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
