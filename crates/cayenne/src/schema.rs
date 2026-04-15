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

//! Arrow schema transformations for Vortex compatibility.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion_table_providers::UnsupportedTypeAction;

fn is_vortex_supported_type(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Interval(_) | DataType::Duration(_) | DataType::FixedSizeBinary(_)
    )
}

/// Transform an Arrow schema for Vortex compatibility.
///
/// Always applies:
/// - `Float16` → `Float32`
/// - Non-microsecond `Timestamp` → `Timestamp(Microsecond, tz)`
///
/// Truly unsupported types (`Interval`, `Duration`, `FixedSizeBinary`) are
/// handled according to `unsupported_type_action`.
///
/// # Errors
///
/// Returns an error when `unsupported_type_action` is [`UnsupportedTypeAction::Error`]
/// and the schema contains unsupported types.
pub fn transform_schema_for_vortex(
    schema: &Schema,
    unsupported_type_action: UnsupportedTypeAction,
) -> DFResult<Schema> {
    let mut unsupported_fields = Vec::new();
    let mut transformed_fields = Vec::new();

    for field in schema.fields() {
        let data_type = field.data_type();

        if matches!(data_type, DataType::Float16) {
            tracing::debug!(
                "Converting Float16 field '{}' to Float32 for Vortex compatibility",
                field.name()
            );
            transformed_fields.push(std::sync::Arc::new(Field::new(
                field.name(),
                DataType::Float32,
                field.is_nullable(),
            )));
            continue;
        }

        if let DataType::Timestamp(unit, tz) = data_type
            && !matches!(unit, TimeUnit::Microsecond)
        {
            tracing::debug!(
                "Converting timestamp field '{}' from {:?} to Microsecond for Vortex compatibility",
                field.name(),
                unit
            );
            transformed_fields.push(std::sync::Arc::new(Field::new(
                field.name(),
                DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                field.is_nullable(),
            )));
            continue;
        }

        if is_vortex_supported_type(data_type) {
            transformed_fields.push(std::sync::Arc::clone(field));
        } else {
            match unsupported_type_action {
                UnsupportedTypeAction::String => {
                    tracing::warn!(
                        "Converting unsupported type {:?} for field '{}' to Utf8.",
                        data_type,
                        field.name()
                    );
                    transformed_fields.push(std::sync::Arc::new(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    )));
                }
                UnsupportedTypeAction::Error => {
                    unsupported_fields.push(format!("'{}' (type: {:?})", field.name(), data_type));
                }
                UnsupportedTypeAction::Ignore => {
                    tracing::warn!(
                        "Ignoring unsupported type {:?} for field '{}'",
                        data_type,
                        field.name()
                    );
                }
                UnsupportedTypeAction::Warn => {
                    tracing::warn!(
                        "Including unsupported type {:?} for field '{}' — insertion may fail",
                        data_type,
                        field.name()
                    );
                    transformed_fields.push(std::sync::Arc::clone(field));
                }
            }
        }
    }

    if !unsupported_fields.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "Unsupported data type(s) in schema: {}. By default, unsupported types cause an \
             error. To convert unsupported types to strings, set 'unsupported_type_action: string'; \
             otherwise, remove the unsupported columns.",
            unsupported_fields.join(", ")
        )));
    }

    Ok(Schema::new(transformed_fields))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_table_providers::UnsupportedTypeAction;

    use super::transform_schema_for_vortex;

    #[test]
    fn float16_converts_to_float32() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float16, false)]);
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");
        assert_eq!(out.field(0).data_type(), &DataType::Float32);
    }

    #[test]
    fn non_microsecond_timestamp_converted() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");
        assert_eq!(
            out.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn unsupported_type_errors_when_action_is_error() {
        let schema = Schema::new(vec![Field::new(
            "d",
            DataType::Duration(TimeUnit::Second),
            false,
        )]);
        transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect_err("Duration should be unsupported");
    }
}
