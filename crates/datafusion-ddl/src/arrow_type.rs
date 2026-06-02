/*
Copyright 2026, Spice AI, Inc.

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

use arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};

/// Maps an Arrow [`DataType`] to a SQL type string suitable for DDL forwarding.
///
/// Returns a SQL type that `DataFusion`'s SQL parser can understand in a
/// `CREATE TABLE` statement. `DataFusion` SQL does not support specifying
/// timestamp time units, so those are always `Nanosecond` after a roundtrip.
/// Timezone presence is preserved via `TIMESTAMP WITH TIME ZONE`.
/// # Errors
///
/// Returns an error if `dt` has no direct SQL equivalent (e.g. nested or
/// complex Arrow types not representable as a SQL column type).
pub fn arrow_datatype_to_sql(dt: &DataType) -> DFResult<String> {
    match dt {
        DataType::Boolean => Ok("BOOLEAN".to_string()),
        DataType::Int8 => Ok("TINYINT".to_string()),
        DataType::Int16 => Ok("SMALLINT".to_string()),
        DataType::Int32 => Ok("INT".to_string()),
        DataType::Int64 => Ok("BIGINT".to_string()),
        DataType::UInt8 => Ok("TINYINT UNSIGNED".to_string()),
        DataType::UInt16 => Ok("SMALLINT UNSIGNED".to_string()),
        DataType::UInt32 => Ok("INT UNSIGNED".to_string()),
        DataType::UInt64 => Ok("BIGINT UNSIGNED".to_string()),
        DataType::Float16 | DataType::Float32 => Ok("FLOAT".to_string()),
        DataType::Float64 => Ok("DOUBLE".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("VARCHAR".to_string()),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok("BYTEA".to_string()),
        DataType::Date32 | DataType::Date64 => Ok("DATE".to_string()),
        DataType::Time32(_) | DataType::Time64(_) => Ok("TIME".to_string()),
        DataType::Timestamp(_, Some(_)) => Ok("TIMESTAMP WITH TIME ZONE".to_string()),
        DataType::Timestamp(_, None) => Ok("TIMESTAMP".to_string()),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Ok(format!("DECIMAL({p},{s})")),
        DataType::Dictionary(_, value_type) => arrow_datatype_to_sql(value_type.as_ref()),
        other => Err(DataFusionError::Execution(format!(
            "Unsupported Arrow type for forwarded DDL: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::arrow_datatype_to_sql;

    #[test]
    fn timestamp_without_tz_maps_to_timestamp() {
        let dt = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert_eq!(arrow_datatype_to_sql(&dt).expect("TIMESTAMP"), "TIMESTAMP");
    }

    #[test]
    fn timestamp_with_tz_maps_to_timestamptz() {
        let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            arrow_datatype_to_sql(&dt).expect("TIMESTAMP WITH TIME ZONE"),
            "TIMESTAMP WITH TIME ZONE"
        );
    }

    #[test]
    fn decimal_preserves_precision_and_scale() {
        let dt = DataType::Decimal128(18, 6);
        assert_eq!(
            arrow_datatype_to_sql(&dt).expect("DECIMAL(18,6)"),
            "DECIMAL(18,6)"
        );
    }

    #[test]
    fn dictionary_unwraps_to_value_type() {
        let dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(arrow_datatype_to_sql(&dt).expect("VARCHAR"), "VARCHAR");
    }

    #[test]
    fn unsupported_types_return_error() {
        let dt = DataType::Duration(TimeUnit::Second);
        arrow_datatype_to_sql(&dt).expect_err("Duration should be unsupported");
    }
}
