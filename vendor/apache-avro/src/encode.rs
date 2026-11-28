// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{
    AvroResult,
    bigdecimal::serialize_big_decimal,
    error::Details,
    schema::{
        DecimalSchema, EnumSchema, FixedSchema, Name, Namespace, RecordSchema, ResolvedSchema,
        Schema, SchemaKind, UnionSchema,
    },
    types::{Value, ValueKind},
    util::{zig_i32, zig_i64},
};
use log::error;
use std::{borrow::Borrow, collections::HashMap, io::Write};

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode<W: Write>(value: &Value, schema: &Schema, writer: &mut W) -> AvroResult<usize> {
    let rs = ResolvedSchema::try_from(schema)?;
    encode_internal(value, schema, rs.get_names(), &None, writer)
}

pub(crate) fn encode_bytes<B: AsRef<[u8]> + ?Sized, W: Write>(
    s: &B,
    mut writer: W,
) -> AvroResult<usize> {
    let bytes = s.as_ref();
    encode_long(bytes.len() as i64, &mut writer)?;
    writer
        .write(bytes)
        .map_err(|e| Details::WriteBytes(e).into())
}

pub(crate) fn encode_long<W: Write>(i: i64, writer: W) -> AvroResult<usize> {
    zig_i64(i, writer)
}

pub(crate) fn encode_int<W: Write>(i: i32, writer: W) -> AvroResult<usize> {
    zig_i32(i, writer)
}

pub(crate) fn encode_internal<W: Write, S: Borrow<Schema>>(
    value: &Value,
    schema: &Schema,
    names: &HashMap<Name, S>,
    enclosing_namespace: &Namespace,
    writer: &mut W,
) -> AvroResult<usize> {
    if let Schema::Ref { name } = schema {
        let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
        let resolved = names
            .get(&fully_qualified_name)
            .ok_or(Details::SchemaResolutionError(fully_qualified_name))?;
        return encode_internal(value, resolved.borrow(), names, enclosing_namespace, writer);
    }

    match value {
        Value::Null => {
            if let Schema::Union(union) = schema {
                match union.schemas.iter().position(|sch| *sch == Schema::Null) {
                    None => Err(Details::EncodeValueAsSchemaError {
                        value_kind: ValueKind::Null,
                        supported_schema: vec![SchemaKind::Null, SchemaKind::Union],
                    }
                    .into()),
                    Some(p) => encode_long(p as i64, writer),
                }
            } else {
                Ok(0)
            }
        }
        Value::Boolean(b) => writer
            .write(&[u8::from(*b)])
            .map_err(|e| Details::WriteBytes(e).into()),
        // Pattern | Pattern here to signify that these _must_ have the same encoding.
        Value::Int(i) | Value::Date(i) | Value::TimeMillis(i) => encode_int(*i, writer),
        Value::Long(i)
        | Value::TimestampMillis(i)
        | Value::TimestampMicros(i)
        | Value::TimestampNanos(i)
        | Value::LocalTimestampMillis(i)
        | Value::LocalTimestampMicros(i)
        | Value::LocalTimestampNanos(i)
        | Value::TimeMicros(i) => encode_long(*i, writer),
        Value::Float(x) => writer
            .write(&x.to_le_bytes())
            .map_err(|e| Details::WriteBytes(e).into()),
        Value::Double(x) => writer
            .write(&x.to_le_bytes())
            .map_err(|e| Details::WriteBytes(e).into()),
        Value::Decimal(decimal) => match schema {
            Schema::Decimal(DecimalSchema { inner, .. }) => match *inner.clone() {
                Schema::Fixed(FixedSchema { size, .. }) => {
                    let bytes = decimal.to_sign_extended_bytes_with_len(size).unwrap();
                    let num_bytes = bytes.len();
                    if num_bytes != size {
                        return Err(Details::EncodeDecimalAsFixedError(num_bytes, size).into());
                    }
                    encode(&Value::Fixed(size, bytes), inner, writer)
                }
                Schema::Bytes => encode(&Value::Bytes(decimal.try_into()?), inner, writer),
                _ => Err(Details::ResolveDecimalSchema(SchemaKind::from(*inner.clone())).into()),
            },
            _ => Err(Details::EncodeValueAsSchemaError {
                value_kind: ValueKind::Decimal,
                supported_schema: vec![SchemaKind::Decimal],
            }
            .into()),
        },
        &Value::Duration(duration) => {
            let slice: [u8; 12] = duration.into();
            writer
                .write(&slice)
                .map_err(|e| Details::WriteBytes(e).into())
        }
        Value::Uuid(uuid) => match *schema {
            Schema::Uuid | Schema::String => encode_bytes(
                // we need the call .to_string() to properly convert ASCII to UTF-8
                #[allow(clippy::unnecessary_to_owned)]
                &uuid.to_string(),
                writer,
            ),
            Schema::Fixed(FixedSchema { size, .. }) => {
                if size != 16 {
                    return Err(Details::ConvertFixedToUuid(size).into());
                }

                let bytes = uuid.as_bytes();
                encode_bytes(bytes, writer)
            }
            _ => Err(Details::EncodeValueAsSchemaError {
                value_kind: ValueKind::Uuid,
                supported_schema: vec![SchemaKind::Uuid, SchemaKind::Fixed],
            }
            .into()),
        },
        Value::BigDecimal(bg) => {
            let buf: Vec<u8> = serialize_big_decimal(bg)?;
            writer
                .write(buf.as_slice())
                .map_err(|e| Details::WriteBytes(e).into())
        }
        Value::Bytes(bytes) => match *schema {
            Schema::Bytes => encode_bytes(bytes, writer),
            Schema::Fixed { .. } => writer
                .write(bytes.as_slice())
                .map_err(|e| Details::WriteBytes(e).into()),
            _ => Err(Details::EncodeValueAsSchemaError {
                value_kind: ValueKind::Bytes,
                supported_schema: vec![SchemaKind::Bytes, SchemaKind::Fixed],
            }
            .into()),
        },
        Value::String(s) => match *schema {
            Schema::String | Schema::Uuid => encode_bytes(s, writer),
            Schema::Enum(EnumSchema { ref symbols, .. }) => {
                if let Some(index) = symbols.iter().position(|item| item == s) {
                    encode_int(index as i32, writer)
                } else {
                    error!("Invalid symbol string {:?}.", &s[..]);
                    Err(Details::GetEnumSymbol(s.clone()).into())
                }
            }
            _ => Err(Details::EncodeValueAsSchemaError {
                value_kind: ValueKind::String,
                supported_schema: vec![SchemaKind::String, SchemaKind::Enum],
            }
            .into()),
        },
        Value::Fixed(_, bytes) => writer
            .write(bytes.as_slice())
            .map_err(|e| Details::WriteBytes(e).into()),
        Value::Enum(i, _) => encode_int(*i as i32, writer),
        Value::Union(idx, item) => {
            if let Schema::Union(ref inner) = *schema {
                let inner_schema = inner
                    .schemas
                    .get(*idx as usize)
                    .expect("Invalid Union validation occurred");
                encode_long(*idx as i64, &mut *writer)?;
                encode_internal(item, inner_schema, names, enclosing_namespace, &mut *writer)
            } else {
                error!("invalid schema type for Union: {schema:?}");
                Err(Details::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Union,
                    supported_schema: vec![SchemaKind::Union],
                }
                .into())
            }
        }
        Value::Array(items) => {
            if let Schema::Array(ref inner) = *schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, &mut *writer)?;
                    for item in items.iter() {
                        encode_internal(
                            item,
                            &inner.items,
                            names,
                            enclosing_namespace,
                            &mut *writer,
                        )?;
                    }
                }
                writer
                    .write(&[0u8])
                    .map_err(|e| Details::WriteBytes(e).into())
            } else {
                error!("invalid schema type for Array: {schema:?}");
                Err(Details::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Array,
                    supported_schema: vec![SchemaKind::Array],
                }
                .into())
            }
        }
        Value::Map(items) => {
            if let Schema::Map(ref inner) = *schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, &mut *writer)?;
                    for (key, value) in items {
                        encode_bytes(key, &mut *writer)?;
                        encode_internal(
                            value,
                            &inner.types,
                            names,
                            enclosing_namespace,
                            &mut *writer,
                        )?;
                    }
                }
                writer
                    .write(&[0u8])
                    .map_err(|e| Details::WriteBytes(e).into())
            } else {
                error!("invalid schema type for Map: {schema:?}");
                Err(Details::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Map,
                    supported_schema: vec![SchemaKind::Map],
                }
                .into())
            }
        }
        Value::Record(value_fields) => {
            if let Schema::Record(RecordSchema {
                ref name,
                fields: ref schema_fields,
                ..
            }) = *schema
            {
                let record_namespace = name.fully_qualified_name(enclosing_namespace).namespace;

                let mut lookup = HashMap::new();
                value_fields.iter().for_each(|(name, field)| {
                    lookup.insert(name, field);
                });

                let mut written_bytes = 0;
                for schema_field in schema_fields.iter() {
                    let name = &schema_field.name;
                    let value_opt = lookup.get(name).or_else(|| {
                        if let Some(aliases) = &schema_field.aliases {
                            aliases.iter().find_map(|alias| lookup.get(alias))
                        } else {
                            None
                        }
                    });

                    if let Some(value) = value_opt {
                        written_bytes += encode_internal(
                            value,
                            &schema_field.schema,
                            names,
                            &record_namespace,
                            writer,
                        )?;
                    } else {
                        return Err(Details::NoEntryInLookupTable(
                            name.clone(),
                            format!("{lookup:?}"),
                        )
                        .into());
                    }
                }
                Ok(written_bytes)
            } else if let Schema::Union(UnionSchema { schemas, .. }) = schema {
                let mut union_buffer: Vec<u8> = Vec::new();
                for (index, schema) in schemas.iter().enumerate() {
                    encode_long(index as i64, &mut union_buffer)?;
                    let encode_res = encode_internal(
                        value,
                        schema,
                        names,
                        enclosing_namespace,
                        &mut union_buffer,
                    );
                    match encode_res {
                        Ok(_) => {
                            return writer
                                .write(union_buffer.as_slice())
                                .map_err(|e| Details::WriteBytes(e).into());
                        }
                        Err(_) => {
                            union_buffer.clear(); //undo any partial encoding
                        }
                    }
                }
                Err(Details::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Record,
                    supported_schema: vec![SchemaKind::Record, SchemaKind::Union],
                }
                .into())
            } else {
                error!("invalid schema type for Record: {schema:?}");
                Err(Details::EncodeValueAsSchemaError {
                    value_kind: ValueKind::Record,
                    supported_schema: vec![SchemaKind::Record, SchemaKind::Union],
                }
                .into())
            }
        }
    }
}

pub fn encode_to_vec(value: &Value, schema: &Schema) -> AvroResult<Vec<u8>> {
    let mut buffer = Vec::new();
    encode(value, schema, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
#[allow(clippy::expect_fun_call)]
pub(crate) mod tests {
    use super::*;
    use crate::error::{Details, Error};
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    pub(crate) fn success(value: &Value, schema: &Schema) -> String {
        format!(
            "Value: {:?}\n should encode with schema:\n{:?}",
            &value, &schema
        )
    }

    #[test]
    fn test_encode_empty_array() {
        let mut buf = Vec::new();
        let empty: Vec<Value> = Vec::new();
        encode(
            &Value::Array(empty.clone()),
            &Schema::array(Schema::Int),
            &mut buf,
        )
        .expect(&success(&Value::Array(empty), &Schema::array(Schema::Int)));
        assert_eq!(vec![0u8], buf);
    }

    #[test]
    fn test_encode_empty_map() {
        let mut buf = Vec::new();
        let empty: HashMap<String, Value> = HashMap::new();
        encode(
            &Value::Map(empty.clone()),
            &Schema::map(Schema::Int),
            &mut buf,
        )
        .expect(&success(&Value::Map(empty), &Schema::map(Schema::Int)));
        assert_eq!(vec![0u8], buf);
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_record() {
        let mut buf = Vec::new();
        let schema = Schema::parse_str(
            r#"
            {
                "type":"record",
                "name":"TestStruct",
                "fields": [
                    {
                        "name":"a",
                        "type":{
                            "type":"record",
                            "name": "Inner",
                            "fields": [ {
                                "name":"z",
                                "type":"int"
                            }]
                        }
                    },
                    {
                        "name":"b",
                        "type":"Inner"
                    }
                ]
            }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value =
            Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_array() {
        let mut buf = Vec::new();
        let schema = Schema::parse_str(
            r#"
            {
                "type":"record",
                "name":"TestStruct",
                "fields": [
                    {
                        "name":"a",
                        "type":{
                            "type":"array",
                            "items": {
                                "type":"record",
                                "name": "Inner",
                                "fields": [ {
                                    "name":"z",
                                    "type":"int"
                                }]
                            }
                        }
                    },
                    {
                        "name":"b",
                        "type": {
                            "type":"map",
                            "values":"Inner"
                        }
                    }
                ]
            }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), Value::Array(vec![inner_value1])),
            (
                "b".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
        ]);
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_map() {
        let mut buf = Vec::new();
        let schema = Schema::parse_str(
            r#"
            {
                "type":"record",
                "name":"TestStruct",
                "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"map",
                        "values":"Inner"
                    }
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), inner_value1),
            (
                "b".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
        ]);
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_record_wrapper() {
        let mut buf = Vec::new();
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"record",
                        "name": "InnerWrapper",
                        "fields": [ {
                            "name":"j",
                            "type":"Inner"
                        }]
                    }
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![(
            "j".into(),
            Value::Record(vec![("z".into(), Value::Int(6))]),
        )]);
        let outer_value =
            Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_map_and_array() {
        let mut buf = Vec::new();
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"map",
                        "values": {
                            "type":"record",
                            "name": "Inner",
                            "fields": [ {
                                "name":"z",
                                "type":"int"
                            }]
                        }
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"array",
                        "items":"Inner"
                    }
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            (
                "a".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
            ("b".into(), Value::Array(vec![inner_value1])),
        ]);
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3433_recursive_definition_encode_union() {
        let mut buf = Vec::new();
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":["null", {
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }]
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )
        .unwrap();

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value1 = Value::Record(vec![
            ("a".into(), Value::Union(1, Box::new(inner_value1))),
            ("b".into(), inner_value2.clone()),
        ]);
        encode(&outer_value1, &schema, &mut buf).expect(&success(&outer_value1, &schema));
        assert!(!buf.is_empty());

        buf.drain(..);
        let outer_value2 = Value::Record(vec![
            ("a".into(), Value::Union(0, Box::new(Value::Null))),
            ("b".into(), inner_value2),
        ]);
        encode(&outer_value2, &schema, &mut buf).expect(&success(&outer_value1, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3448_proper_multi_level_encoding_outer_namespace() {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema).unwrap();
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3448_proper_multi_level_encoding_middle_namespace() {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "middle_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema).unwrap();
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3448_proper_multi_level_encoding_inner_namespace() {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "namespace":"inner_namespace",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema).unwrap();
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        assert!(!buf.is_empty());
        buf.drain(..);
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_avro_3585_encode_uuids() {
        let value = Value::String(String::from("00000000-0000-0000-0000-000000000000"));
        let schema = Schema::Uuid;
        let mut buffer = Vec::new();
        let encoded = encode(&value, &schema, &mut buffer);
        assert!(encoded.is_ok());
        assert!(!buffer.is_empty());
    }

    #[test]
    fn avro_3926_encode_decode_uuid_to_fixed_wrong_schema_size() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            size: 15,
            name: "uuid".into(),
            aliases: None,
            doc: None,
            default: None,
            attributes: Default::default(),
        });
        let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);

        let mut buffer = Vec::new();
        match encode(&value, &schema, &mut buffer).map_err(Error::into_details) {
            Err(Details::ConvertFixedToUuid(actual)) => {
                assert_eq!(actual, 15);
            }
            _ => panic!("Expected Details::ConvertFixedToUuid"),
        }

        Ok(())
    }
}
