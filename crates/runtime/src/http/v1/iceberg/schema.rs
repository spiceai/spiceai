/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{str::FromStr, sync::Arc};

use arrow::{
    array::timezone::Tz,
    datatypes::{DataType, Schema as ArrowSchema, TimeUnit},
    error::ArrowError,
};
use chrono::{Offset, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported Arrow type: {datatype:?}"))]
    UnsupportedType { datatype: DataType },

    #[snafu(display("Invalid map type structure: expected struct with key and value fields"))]
    InvalidMapStructure,

    #[snafu(display("Invalid map type: expected struct type for map entries"))]
    InvalidMapType,

    #[snafu(display("Invalid time zone {zone}: {source}"))]
    InvalidTimeZone { source: ArrowError, zone: Arc<str> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimitiveType(String);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Type {
    #[serde(rename = "primitive")]
    Primitive(PrimitiveType),
    #[serde(rename = "struct")]
    Struct(Box<StructType>),
    #[serde(rename = "list")]
    List(Box<ListType>),
    #[serde(rename = "map")]
    Map(Box<MapType>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapType {
    #[serde(rename = "key-id")]
    key_id: usize,
    key: Box<Type>,
    #[serde(rename = "value-id")]
    value_id: usize,
    value: Box<Type>,
    #[serde(rename = "value-required")]
    value_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListType {
    #[serde(rename = "element-id")]
    element_id: usize,
    element: Box<Type>,
    #[serde(rename = "element-required")]
    element_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructField {
    id: usize,
    name: String,
    #[serde(rename = "type")]
    field_type: Type,
    required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructType {
    fields: Vec<StructField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
pub struct Schema {
    #[serde(flatten)]
    struct_type: StructType,
    #[serde(rename = "schema-id", skip_serializing_if = "Option::is_none")]
    schema_id: Option<i32>,
    #[serde(
        rename = "identifier-field-ids",
        skip_serializing_if = "Option::is_none"
    )]
    identifier_field_ids: Option<Vec<i32>>,
}

impl TryFrom<ArrowSchema> for Schema {
    type Error = Error;

    #[allow(clippy::cast_possible_truncation)]
    fn try_from(arrow_schema: ArrowSchema) -> Result<Self, Self::Error> {
        let fields = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                Ok(StructField {
                    id: idx,
                    name: field.name().clone(),
                    field_type: arrow_datatype_to_type(field.data_type())?,
                    required: !field.is_nullable(),
                    doc: field.metadata().get("doc").map(ToString::to_string),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(Schema {
            struct_type: StructType { fields },
            schema_id: None,
            identifier_field_ids: None,
        })
    }
}

#[allow(clippy::cast_possible_truncation)]
fn arrow_datatype_to_type(dt: &DataType) -> Result<Type, Error> {
    match dt {
        // Primitive types
        DataType::Int64 => Ok(Type::Primitive(PrimitiveType("long".to_string()))),
        DataType::Int32 => Ok(Type::Primitive(PrimitiveType("int".to_string()))),
        DataType::Float64 => Ok(Type::Primitive(PrimitiveType("double".to_string()))),
        DataType::Float32 => Ok(Type::Primitive(PrimitiveType("float".to_string()))),
        DataType::Boolean => Ok(Type::Primitive(PrimitiveType("boolean".to_string()))),
        DataType::Utf8 => Ok(Type::Primitive(PrimitiveType("string".to_string()))),
        DataType::Binary => Ok(Type::Primitive(PrimitiveType("binary".to_string()))),
        DataType::FixedSizeBinary(size) => {
            Ok(Type::Primitive(PrimitiveType(format!("fixed[{size}]"))))
        }
        DataType::Decimal128(precision, scale) => Ok(Type::Primitive(PrimitiveType(format!(
            "decimal({precision}, {scale})",
        )))),
        DataType::Date32 => Ok(Type::Primitive(PrimitiveType("date".to_string()))),
        DataType::Time64(unit) if *unit == TimeUnit::Microsecond => {
            Ok(Type::Primitive(PrimitiveType("time".to_string())))
        }
        DataType::Timestamp(unit, Some(zone)) => {
            let tz = Tz::from_str(zone).context(InvalidTimeZoneSnafu {
                zone: Arc::clone(zone),
            })?;

            let tz_offset = tz.offset_from_utc_datetime(&Utc::now().naive_utc()).fix();
            let is_utc = tz_offset.local_minus_utc() == 0;

            let timestamp_type = if is_utc { "timestamptz" } else { "timestamp" };

            match *unit {
                TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond => {
                    Ok(Type::Primitive(PrimitiveType(timestamp_type.to_string())))
                }
                _ => UnsupportedTypeSnafu {
                    datatype: DataType::Timestamp(*unit, Some(Arc::clone(zone))),
                }
                .fail(),
            }
        }
        DataType::Timestamp(unit, None) => match *unit {
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond => {
                Ok(Type::Primitive(PrimitiveType("timestamp".to_string())))
            }
            _ => UnsupportedTypeSnafu {
                datatype: DataType::Timestamp(*unit, None),
            }
            .fail(),
        },

        // List type
        DataType::List(field) => Ok(Type::List(Box::new(ListType {
            element_id: 0,
            element: Box::new(arrow_datatype_to_type(field.data_type())?),
            element_required: !field.is_nullable(),
        }))),

        // Map type
        DataType::Map(field, _sorted) => match field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => Ok(Type::Map(Box::new(MapType {
                key_id: 0,
                key: Box::new(arrow_datatype_to_type(fields[0].data_type())?),
                value_id: 1,
                value: Box::new(arrow_datatype_to_type(fields[1].data_type())?),
                value_required: !fields[1].is_nullable(),
            }))),
            DataType::Struct(_) => InvalidMapStructureSnafu.fail(),
            _ => InvalidMapTypeSnafu.fail(),
        },

        // Struct type
        DataType::Struct(fields) => {
            let struct_fields = fields
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    Ok(StructField {
                        id: idx,
                        name: field.name().clone(),
                        field_type: arrow_datatype_to_type(field.data_type())?,
                        required: !field.is_nullable(),
                        doc: field.metadata().get("doc").map(ToString::to_string),
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            Ok(Type::Struct(Box::new(StructType {
                fields: struct_fields,
            })))
        }

        // Unsupported types
        other => UnsupportedTypeSnafu {
            datatype: other.clone(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_simple_schema_conversion() -> Result<(), Error> {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let schema = Schema::try_from(arrow_schema)?;
        assert_eq!(schema.struct_type.fields.len(), 2);
        Ok(())
    }

    #[test]
    fn test_unsupported_type() {
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "time",
            DataType::Time64(arrow::datatypes::TimeUnit::Second),
            false,
        )]);

        let result = Schema::try_from(arrow_schema);
        assert!(matches!(result, Err(Error::UnsupportedType { .. })));
    }

    #[test]
    fn test_primitive_types() -> Result<(), Error> {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("int32_field", DataType::Int32, false),
            Field::new("int64_field", DataType::Int64, false),
            Field::new("float32_field", DataType::Float32, false),
            Field::new("float64_field", DataType::Float64, false),
            Field::new("boolean_field", DataType::Boolean, false),
            Field::new("string_field", DataType::Utf8, false),
            Field::new("binary_field", DataType::Binary, false),
            Field::new("fixed_binary_field", DataType::FixedSizeBinary(16), false),
            Field::new("decimal_field", DataType::Decimal128(10, 2), false),
        ]);

        let schema = Schema::try_from(arrow_schema)?;
        let fields = &schema.struct_type.fields;

        assert_eq!(fields.len(), 9);

        // Check field types
        if let Type::Primitive(p) = &fields[0].field_type {
            assert_eq!(p.0, "int");
        } else {
            panic!("Expected primitive type");
        }

        if let Type::Primitive(p) = &fields[7].field_type {
            assert_eq!(p.0, "fixed[16]");
        } else {
            panic!("Expected fixed binary type");
        }

        if let Type::Primitive(p) = &fields[8].field_type {
            assert_eq!(p.0, "decimal(10,2)");
        } else {
            panic!("Expected decimal type");
        }

        Ok(())
    }

    #[test]
    fn test_list_type() -> Result<(), Error> {
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "list_field",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        )]);

        let schema = Schema::try_from(arrow_schema)?;
        let fields = &schema.struct_type.fields;

        assert_eq!(fields.len(), 1);

        if let Type::List(list_type) = &fields[0].field_type {
            assert!(!list_type.element_required);
            if let Type::Primitive(p) = list_type.element.as_ref() {
                assert_eq!(p.0, "long");
            } else {
                panic!("Expected primitive type inside list");
            }
        } else {
            panic!("Expected list type");
        }

        Ok(())
    }

    #[test]
    fn test_map_type() -> Result<(), Error> {
        let key_value_struct = DataType::Struct(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]);
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "map_field",
            DataType::Map(
                Arc::new(Field::new("entries", key_value_struct, false)),
                false,
            ),
            false,
        )]);

        let schema = Schema::try_from(arrow_schema)?;
        let fields = &schema.struct_type.fields;

        assert_eq!(fields.len(), 1);

        if let Type::Map(map_type) = &fields[0].field_type {
            if let Type::Primitive(key) = map_type.key.as_ref() {
                assert_eq!(key.0, "string");
            } else {
                panic!("Expected primitive type for key");
            }
            if let Type::Primitive(value) = map_type.value.as_ref() {
                assert_eq!(value.0, "long");
            } else {
                panic!("Expected primitive type for value");
            }
            assert!(!map_type.value_required);
        } else {
            panic!("Expected map type");
        }

        Ok(())
    }

    #[test]
    fn test_nested_struct() -> Result<(), Error> {
        let nested_struct = DataType::Struct(vec![
            Field::new("inner_int", DataType::Int32, false),
            Field::new("inner_string", DataType::Utf8, true),
        ]);
        let arrow_schema = ArrowSchema::new(vec![Field::new("struct_field", nested_struct, false)]);

        let schema = Schema::try_from(arrow_schema)?;
        let fields = &schema.struct_type.fields;

        assert_eq!(fields.len(), 1);

        if let Type::Struct(struct_type) = &fields[0].field_type {
            assert_eq!(struct_type.fields.len(), 2);
            assert_eq!(struct_type.fields[0].name, "inner_int");
            assert_eq!(struct_type.fields[1].name, "inner_string");
            assert!(struct_type.fields[0].required);
            assert!(!struct_type.fields[1].required);
        } else {
            panic!("Expected struct type");
        }

        Ok(())
    }

    #[test]
    fn test_invalid_map_structure() {
        // Test with a map that has an invalid structure (not exactly 2 fields)
        let invalid_struct = DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
                Field::new("extra", DataType::Boolean, true),
            ]
            .into(),
        );
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "map_field",
            DataType::Map(
                Arc::new(Field::new("entries", invalid_struct, false)),
                false,
            ),
            false,
        )]);

        let result = Schema::try_from(arrow_schema);
        assert!(matches!(result, Err(Error::InvalidMapStructure)));
    }
}
