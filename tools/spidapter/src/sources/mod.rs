// Copyright 2026 Spice AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub(super) mod cayenne;
pub(super) mod dynamodb;
pub(super) mod mongodb;
pub(super) mod postgres_cdc;
pub(super) mod postgres_debezium;

/// Format a list of column names as a `ColumnReference` string accepted by spicepod:
/// - single column → `"col"`
/// - composite key → `"(col1, col2)"`
/// - empty → `None`
pub(super) fn composite_key_str(cols: &[String]) -> Option<String> {
    match cols {
        [] => None,
        [single] => Some(single.clone()),
        many => Some(format!("({})", many.join(", "))),
    }
}

/// Type mapping for `DynamoDB` spicepod column declarations.
///
/// Maps Arrow types to the types the Spice acceleration engine will see when reading
/// back from `DynamoDB`. Must stay in sync with how `DynamoDbSink` encodes values:
/// - `Decimal128/256` → stored as `N` (number string) → declared as `Float64`
/// - `Timestamp` → stored as `S` (ISO 8601 string) → declared as `Utf8`
pub(super) fn dynamodb_arrow_type_to_spicepod_str(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "Int64".to_string(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "Float64".to_string(),
        DataType::Timestamp(_, _) => "Utf8".to_string(),
        other => arrow_type_to_spicepod_str(other),
    }
}

/// Convert an Arrow `DataType` to a spicepod type string for MongoDB datasets.
///
/// MongoDB stores all numeric values natively; the only special case is
/// `Decimal128/256` which the `MongoDbSink` encodes as BSON Double (Float64).
pub(super) fn mongodb_arrow_type_to_spicepod_str(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "Float64".to_string(),
        other => arrow_type_to_spicepod_str(other),
    }
}

/// Convert an Arrow `DataType` to a string the spicepod `parse_declared_type` parser accepts.
///
/// Arrow's `Display` uses abbreviated time-unit forms (`µs`, `ns`) that the parser does not
/// recognise; this function emits the full names (`Microsecond`, `Nanosecond`) instead.
pub(super) fn arrow_type_to_spicepod_str(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::{DataType, TimeUnit};
    let time_unit_str = |u: &TimeUnit| match u {
        TimeUnit::Second => "Second",
        TimeUnit::Millisecond => "Millisecond",
        TimeUnit::Microsecond => "Microsecond",
        TimeUnit::Nanosecond => "Nanosecond",
    };
    match dt {
        DataType::Timestamp(unit, None) => format!("Timestamp({}, None)", time_unit_str(unit)),
        DataType::Timestamp(unit, Some(tz)) => {
            format!("Timestamp({}, {})", time_unit_str(unit), tz)
        }
        DataType::Time32(unit) => format!("Time32({})", time_unit_str(unit)),
        DataType::Time64(unit) => format!("Time64({})", time_unit_str(unit)),
        DataType::Duration(unit) => format!("Duration({})", time_unit_str(unit)),
        DataType::Utf8View => "Utf8".to_string(),
        other => format!("{other}"),
    }
}
