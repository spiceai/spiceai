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
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
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

    #[snafu(display("Failed to parse fixed size binary length: {source}"))]
    InvalidFixedSize { source: std::num::ParseIntError },

    #[snafu(display("Failed to parse decimal precision/scale: {source}"))]
    InvalidDecimalFormat { source: std::num::ParseIntError },

    #[snafu(display("Invalid decimal format, expected 'decimal(precision, scale)'"))]
    MalformedDecimal,

    #[snafu(display("Unknown primitive type: {type_str}"))]
    UnknownPrimitiveType { type_str: String },
}

#[derive(Debug, Clone)]
pub struct PrimitiveType(String);

impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(PrimitiveType)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    Primitive(PrimitiveType),
    Complex(ComplexType),
}

impl Type {
    #[must_use]
    pub fn primitive(value: impl Into<String>) -> Self {
        Type::Primitive(PrimitiveType(value.into()))
    }

    #[must_use]
    pub fn complex(complex_type: ComplexType) -> Self {
        Type::Complex(complex_type)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ComplexType {
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
    schema_id: Option<usize>,
    #[serde(
        rename = "identifier-field-ids",
        skip_serializing_if = "Option::is_none"
    )]
    identifier_field_ids: Option<Vec<usize>>,
}

impl TryFrom<&ArrowSchema> for Schema {
    type Error = Error;

    fn try_from(arrow_schema: &ArrowSchema) -> Result<Self, Self::Error> {
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

impl TryFrom<&Schema> for ArrowSchema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        let fields = schema
            .struct_type
            .fields
            .iter()
            .map(|field| {
                Ok(Field::new(
                    &field.name,
                    type_to_arrow_datatype(&field.field_type)?,
                    !field.required,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(ArrowSchema::new(fields))
    }
}

fn type_to_arrow_datatype(type_: &Type) -> Result<DataType, Error> {
    match type_ {
        Type::Primitive(p) => match p.0.as_str() {
            "boolean" => Ok(DataType::Boolean),
            "int" => Ok(DataType::Int32),
            "long" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "date" => Ok(DataType::Date32),
            "time" => Ok(DataType::Time64(TimeUnit::Microsecond)),
            "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamptz" => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            "string" => Ok(DataType::Utf8),
            "binary" => Ok(DataType::Binary),
            s if s.starts_with("fixed[") => {
                let size = s
                    .trim_start_matches("fixed[")
                    .trim_end_matches(']')
                    .parse()
                    .context(InvalidFixedSizeSnafu)?;
                Ok(DataType::FixedSizeBinary(size))
            }
            s if s.starts_with("decimal(") => {
                let parts: Vec<&str> = s
                    .trim_start_matches("decimal(")
                    .trim_end_matches(')')
                    .split(',')
                    .collect();
                if parts.len() != 2 {
                    return MalformedDecimalSnafu.fail();
                }
                let precision = parts[0].trim().parse().context(InvalidDecimalFormatSnafu)?;
                let scale = parts[1].trim().parse().context(InvalidDecimalFormatSnafu)?;
                Ok(DataType::Decimal128(precision, scale))
            }
            unknown => UnknownPrimitiveTypeSnafu {
                type_str: unknown.to_string(),
            }
            .fail(),
        },
        Type::Complex(complex) => match complex {
            ComplexType::List(list) => Ok(DataType::List(Arc::new(Field::new(
                "item",
                type_to_arrow_datatype(&list.element)?,
                !list.element_required,
            )))),
            ComplexType::Map(map) => {
                let struct_fields = vec![
                    Field::new("key", type_to_arrow_datatype(&map.key)?, false),
                    Field::new(
                        "value",
                        type_to_arrow_datatype(&map.value)?,
                        !map.value_required,
                    ),
                ];
                Ok(DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(struct_fields.into()),
                        false,
                    )),
                    false,
                ))
            }
            ComplexType::Struct(struct_type) => {
                let fields = struct_type
                    .fields
                    .iter()
                    .map(|f| {
                        Ok(Field::new(
                            &f.name,
                            type_to_arrow_datatype(&f.field_type)?,
                            !f.required,
                        ))
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                Ok(DataType::Struct(fields.into()))
            }
        },
    }
}

#[allow(clippy::cast_possible_truncation)]
fn arrow_datatype_to_type(dt: &DataType) -> Result<Type, Error> {
    match dt {
        // Primitive types
        DataType::Int64 => Ok(Type::primitive("long")),
        DataType::Int32 => Ok(Type::primitive("int")),
        DataType::Float64 => Ok(Type::primitive("double")),
        DataType::Float32 => Ok(Type::primitive("float")),
        DataType::Boolean => Ok(Type::primitive("boolean")),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(Type::primitive("string")),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Ok(Type::primitive("binary"))
        }
        DataType::FixedSizeBinary(size) => Ok(Type::primitive(format!("fixed[{size}]"))),
        DataType::Decimal128(precision, scale) => {
            Ok(Type::primitive(format!("decimal({precision}, {scale})",)))
        }
        DataType::Date32 => Ok(Type::primitive("date")),
        DataType::Time64(unit) if *unit == TimeUnit::Microsecond => Ok(Type::primitive("time")),
        DataType::Timestamp(unit, Some(zone)) => {
            let tz = Tz::from_str(zone).context(InvalidTimeZoneSnafu {
                zone: Arc::clone(zone),
            })?;

            let tz_offset = tz.offset_from_utc_datetime(&Utc::now().naive_utc()).fix();
            let is_utc = tz_offset.local_minus_utc() == 0;

            let timestamp_type = if is_utc { "timestamptz" } else { "timestamp" };

            match *unit {
                TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond => {
                    Ok(Type::primitive(timestamp_type))
                }
                TimeUnit::Nanosecond => UnsupportedTypeSnafu {
                    datatype: DataType::Timestamp(*unit, Some(Arc::clone(zone))),
                }
                .fail(),
            }
        }
        DataType::Timestamp(unit, None) => match *unit {
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond => {
                Ok(Type::primitive("timestamp"))
            }
            TimeUnit::Nanosecond => UnsupportedTypeSnafu {
                datatype: DataType::Timestamp(*unit, None),
            }
            .fail(),
        },

        // List type
        DataType::List(field) => Ok(Type::complex(ComplexType::List(Box::new(ListType {
            element_id: 0,
            element: Box::new(arrow_datatype_to_type(field.data_type())?),
            element_required: !field.is_nullable(),
        })))),

        // Map type
        DataType::Map(field, _sorted) => match field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                Ok(Type::complex(ComplexType::Map(Box::new(MapType {
                    key_id: 0,
                    key: Box::new(arrow_datatype_to_type(fields[0].data_type())?),
                    value_id: 1,
                    value: Box::new(arrow_datatype_to_type(fields[1].data_type())?),
                    value_required: !fields[1].is_nullable(),
                }))))
            }
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

            Ok(Type::complex(ComplexType::Struct(Box::new(StructType {
                fields: struct_fields,
            }))))
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

        let schema = Schema::try_from(&arrow_schema)?;
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

        let result = Schema::try_from(&arrow_schema);
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

        let schema = Schema::try_from(&arrow_schema)?;
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
            assert_eq!(p.0, "decimal(10, 2)");
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

        let schema = Schema::try_from(&arrow_schema)?;
        let fields = &schema.struct_type.fields;

        assert_eq!(fields.len(), 1);

        if let Type::Complex(ComplexType::List(list_type)) = &fields[0].field_type {
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
        let key_value_struct = DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ]
            .into(),
        );
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "map_field",
            DataType::Map(
                Arc::new(Field::new("entries", key_value_struct, false)),
                false,
            ),
            false,
        )]);

        let schema = Schema::try_from(&arrow_schema)?;
        let fields = &schema.struct_type.fields;

        assert_eq!(fields.len(), 1);

        if let Type::Complex(ComplexType::Map(map_type)) = &fields[0].field_type {
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
        let nested_struct = DataType::Struct(
            vec![
                Field::new("inner_int", DataType::Int32, false),
                Field::new("inner_string", DataType::Utf8, true),
            ]
            .into(),
        );
        let arrow_schema = ArrowSchema::new(vec![Field::new("struct_field", nested_struct, false)]);

        let schema = Schema::try_from(&arrow_schema)?;
        let fields = &schema.struct_type.fields;

        assert_eq!(fields.len(), 1);

        if let Type::Complex(ComplexType::Struct(struct_type)) = &fields[0].field_type {
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

        let result = Schema::try_from(&arrow_schema);
        assert!(matches!(result, Err(Error::InvalidMapStructure)));
    }

    #[test]
    fn test_roundtrip_conversion() -> Result<(), Error> {
        let original_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
        ]);

        let iceberg_schema = Schema::try_from(&original_schema)?;
        let converted_schema = ArrowSchema::try_from(&iceberg_schema)?;

        assert_eq!(
            original_schema.fields().len(),
            converted_schema.fields().len()
        );

        for (orig, conv) in original_schema
            .fields()
            .iter()
            .zip(converted_schema.fields())
        {
            assert_eq!(orig.name(), conv.name());
            assert_eq!(orig.data_type(), conv.data_type());
            assert_eq!(orig.is_nullable(), conv.is_nullable());
        }
        Ok(())
    }

    #[test]
    fn test_complex_type_conversion() -> Result<(), Error> {
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let original_schema = ArrowSchema::new(vec![
            Field::new("map_field", map_type, false),
            Field::new(
                "struct_field",
                DataType::Struct(
                    vec![
                        Field::new("inner_int", DataType::Int32, false),
                        Field::new("inner_str", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]);

        let iceberg_schema = Schema::try_from(&original_schema)?;
        let converted_schema = ArrowSchema::try_from(&iceberg_schema)?;

        assert_eq!(
            original_schema.fields().len(),
            converted_schema.fields().len()
        );

        for (orig, conv) in original_schema
            .fields()
            .iter()
            .zip(converted_schema.fields())
        {
            assert_eq!(orig.name(), conv.name());
            assert_eq!(orig.data_type(), conv.data_type());
            assert_eq!(orig.is_nullable(), conv.is_nullable());
        }
        Ok(())
    }

    #[test]
    fn test_invalid_primitive_type() {
        let schema = Schema {
            struct_type: StructType {
                fields: vec![StructField {
                    id: 0,
                    name: "test".to_string(),
                    field_type: Type::Primitive(PrimitiveType("invalid".to_string())),
                    required: true,
                    doc: None,
                }],
            },
            schema_id: None,
            identifier_field_ids: None,
        };

        let result = ArrowSchema::try_from(&schema);
        assert!(matches!(result, Err(Error::UnknownPrimitiveType { .. })));
    }

    #[test]
    fn test_invalid_decimal_format() {
        let schema = Schema {
            struct_type: StructType {
                fields: vec![StructField {
                    id: 0,
                    name: "test".to_string(),
                    field_type: Type::Primitive(PrimitiveType("decimal(10)".to_string())),
                    required: true,
                    doc: None,
                }],
            },
            schema_id: None,
            identifier_field_ids: None,
        };

        let result = ArrowSchema::try_from(&schema);
        assert!(matches!(result, Err(Error::MalformedDecimal)));
    }
}
