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

//! Logic for serde-compatible schema-aware serialization
//! which writes directly to a `Write` stream

use crate::{
    bigdecimal::big_decimal_as_bytes,
    encode::{encode_int, encode_long},
    error::{Details, Error},
    schema::{Name, NamesRef, Namespace, RecordField, RecordSchema, Schema},
};
use bigdecimal::BigDecimal;
use serde::{Serialize, ser};
use std::{borrow::Cow, io::Write, str::FromStr};

const COLLECTION_SERIALIZER_ITEM_LIMIT: usize = 1024;
const COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY: usize = 32;
const SINGLE_VALUE_INIT_BUFFER_SIZE: usize = 128;

/// The sequence serializer for [`SchemaAwareWriteSerializer`].
/// [`SchemaAwareWriteSerializeSeq`] may break large arrays up into multiple blocks to avoid having
/// to obtain the length of the entire array before being able to write any data to the underlying
/// [`std::fmt::Write`] stream.  (See the
/// [Data Serialization and Deserialization](https://avro.apache.org/docs/1.12.0/specification/#data-serialization-and-deserialization)
/// section of the Avro spec for more info.)
pub struct SchemaAwareWriteSerializeSeq<'a, 's, W: Write> {
    ser: &'a mut SchemaAwareWriteSerializer<'s, W>,
    item_schema: &'s Schema,
    item_buffer_size: usize,
    item_buffers: Vec<Vec<u8>>,
    bytes_written: usize,
}

impl<'a, 's, W: Write> SchemaAwareWriteSerializeSeq<'a, 's, W> {
    fn new(
        ser: &'a mut SchemaAwareWriteSerializer<'s, W>,
        item_schema: &'s Schema,
        len: Option<usize>,
    ) -> SchemaAwareWriteSerializeSeq<'a, 's, W> {
        SchemaAwareWriteSerializeSeq {
            ser,
            item_schema,
            item_buffer_size: SINGLE_VALUE_INIT_BUFFER_SIZE,
            item_buffers: Vec::with_capacity(
                len.unwrap_or(COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY),
            ),
            bytes_written: 0,
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if !self.item_buffers.is_empty() {
            self.bytes_written +=
                encode_long(self.item_buffers.len() as i64, &mut self.ser.writer)?;
            for item in self.item_buffers.drain(..) {
                self.bytes_written += self
                    .ser
                    .writer
                    .write(item.as_slice())
                    .map_err(Details::WriteBytes)?;
            }
        }

        Ok(())
    }

    fn serialize_element<T: ser::Serialize>(&mut self, value: &T) -> Result<(), Error> {
        let mut item_buffer: Vec<u8> = Vec::with_capacity(self.item_buffer_size);
        let mut item_ser = SchemaAwareWriteSerializer::new(
            &mut item_buffer,
            self.item_schema,
            self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        value.serialize(&mut item_ser)?;

        self.item_buffer_size = std::cmp::max(self.item_buffer_size, item_buffer.len() + 16);

        self.item_buffers.push(item_buffer);

        if self.item_buffers.len() > COLLECTION_SERIALIZER_ITEM_LIMIT {
            self.write_buffered_items()?;
        }

        Ok(())
    }

    fn end(mut self) -> Result<usize, Error> {
        self.write_buffered_items()?;
        self.bytes_written += self.ser.writer.write(&[0u8]).map_err(Details::WriteBytes)?;

        Ok(self.bytes_written)
    }
}

impl<W: Write> ser::SerializeSeq for SchemaAwareWriteSerializeSeq<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_element(&value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<W: Write> ser::SerializeTuple for SchemaAwareWriteSerializeSeq<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

/// The map serializer for [`SchemaAwareWriteSerializer`].
/// [`SchemaAwareWriteSerializeMap`] may break large maps up into multiple blocks to avoid having to
/// obtain the size of the entire map before being able to write any data to the underlying
/// [`std::fmt::Write`] stream.  (See the
/// [Data Serialization and Deserialization](https://avro.apache.org/docs/1.12.0/specification/#data-serialization-and-deserialization)
/// section of the Avro spec for more info.)
pub struct SchemaAwareWriteSerializeMap<'a, 's, W: Write> {
    ser: &'a mut SchemaAwareWriteSerializer<'s, W>,
    item_schema: &'s Schema,
    item_buffer_size: usize,
    item_buffers: Vec<Vec<u8>>,
    bytes_written: usize,
}

impl<'a, 's, W: Write> SchemaAwareWriteSerializeMap<'a, 's, W> {
    fn new(
        ser: &'a mut SchemaAwareWriteSerializer<'s, W>,
        item_schema: &'s Schema,
        len: Option<usize>,
    ) -> SchemaAwareWriteSerializeMap<'a, 's, W> {
        SchemaAwareWriteSerializeMap {
            ser,
            item_schema,
            item_buffer_size: SINGLE_VALUE_INIT_BUFFER_SIZE,
            item_buffers: Vec::with_capacity(
                len.unwrap_or(COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY),
            ),
            bytes_written: 0,
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if !self.item_buffers.is_empty() {
            self.bytes_written +=
                encode_long(self.item_buffers.len() as i64, &mut self.ser.writer)?;
            for item in self.item_buffers.drain(..) {
                self.bytes_written += self
                    .ser
                    .writer
                    .write(item.as_slice())
                    .map_err(Details::WriteBytes)?;
            }
        }

        Ok(())
    }
}

impl<W: Write> ser::SerializeMap for SchemaAwareWriteSerializeMap<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let mut element_buffer: Vec<u8> = Vec::with_capacity(self.item_buffer_size);
        let string_schema = Schema::String;
        let mut key_ser = SchemaAwareWriteSerializer::new(
            &mut element_buffer,
            &string_schema,
            self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        key.serialize(&mut key_ser)?;

        self.item_buffers.push(element_buffer);

        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let last_index = self.item_buffers.len() - 1;
        let element_buffer = &mut self.item_buffers[last_index];
        let mut val_ser = SchemaAwareWriteSerializer::new(
            element_buffer,
            self.item_schema,
            self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        value.serialize(&mut val_ser)?;

        self.item_buffer_size = std::cmp::max(self.item_buffer_size, element_buffer.len() + 16);

        if self.item_buffers.len() > COLLECTION_SERIALIZER_ITEM_LIMIT {
            self.write_buffered_items()?;
        }

        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.write_buffered_items()?;
        self.bytes_written += self.ser.writer.write(&[0u8]).map_err(Details::WriteBytes)?;

        Ok(self.bytes_written)
    }
}

/// The struct serializer for [`SchemaAwareWriteSerializer`], which can serialize Avro records.
/// [`SchemaAwareWriteSerializeStruct`] can accept fields out of order, but doing so incurs a
/// performance penalty, since it requires [`SchemaAwareWriteSerializeStruct`] to buffer serialized
/// values in order to write them to the stream in order.
pub struct SchemaAwareWriteSerializeStruct<'a, 's, W: Write> {
    ser: &'a mut SchemaAwareWriteSerializer<'s, W>,
    record_schema: &'s RecordSchema,
    bytes_written: usize,
}

impl<'a, 's, W: Write> SchemaAwareWriteSerializeStruct<'a, 's, W> {
    fn new(
        ser: &'a mut SchemaAwareWriteSerializer<'s, W>,
        record_schema: &'s RecordSchema,
    ) -> SchemaAwareWriteSerializeStruct<'a, 's, W> {
        SchemaAwareWriteSerializeStruct {
            ser,
            record_schema,
            bytes_written: 0,
        }
    }

    fn serialize_next_field<T>(&mut self, field: &RecordField, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        // If we receive fields in order, write them directly to the main writer
        let mut value_ser = SchemaAwareWriteSerializer::new(
            &mut *self.ser.writer,
            &field.schema,
            self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        self.bytes_written += value.serialize(&mut value_ser)?;

        Ok(())
    }

    fn end(self) -> Result<usize, Error> {
        Ok(self.bytes_written)
    }
}

impl<W: Write> ser::SerializeStruct for SchemaAwareWriteSerializeStruct<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let record_field = self
            .record_schema
            .lookup
            .get(key)
            .and_then(|idx| self.record_schema.fields.get(*idx));

        match record_field {
            Some(field) => {
                // self.item_count += 1;
                self.serialize_next_field(field, value).map_err(|e| {
                    Details::SerializeRecordFieldWithSchema {
                        field_name: key,
                        record_schema: Schema::Record(self.record_schema.clone()),
                        error: Box::new(e),
                    }
                    .into()
                })
            }
            None => Err(Details::FieldName(String::from(key)).into()),
        }
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
        let skipped_field = self
            .record_schema
            .lookup
            .get(key)
            .and_then(|idx| self.record_schema.fields.get(*idx));

        if let Some(skipped_field) = skipped_field {
            // self.item_count += 1;
            skipped_field
                .default
                .serialize(&mut SchemaAwareWriteSerializer::new(
                    self.ser.writer,
                    &skipped_field.schema,
                    self.ser.names,
                    self.ser.enclosing_namespace.clone(),
                ))?;
        } else {
            return Err(Details::GetField(key.to_string()).into());
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<W: Write> ser::SerializeStructVariant for SchemaAwareWriteSerializeStruct<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        ser::SerializeStruct::serialize_field(self, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeStruct::end(self)
    }
}

/// The tuple struct serializer for [`SchemaAwareWriteSerializer`].
/// [`SchemaAwareWriteSerializeTupleStruct`] can serialize to an Avro array, record, or big-decimal.
/// When serializing to a record, fields must be provided in the correct order, since no names are provided.
pub enum SchemaAwareWriteSerializeTupleStruct<'a, 's, W: Write> {
    Record(SchemaAwareWriteSerializeStruct<'a, 's, W>),
    Array(SchemaAwareWriteSerializeSeq<'a, 's, W>),
}

impl<W: Write> SchemaAwareWriteSerializeTupleStruct<'_, '_, W> {
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        use SchemaAwareWriteSerializeTupleStruct::*;
        match self {
            Record(_record_ser) => {
                unimplemented!("Tuple struct serialization to record is not supported!");
            }
            Array(array_ser) => array_ser.serialize_element(&value),
        }
    }

    fn end(self) -> Result<usize, Error> {
        use SchemaAwareWriteSerializeTupleStruct::*;
        match self {
            Record(record_ser) => record_ser.end(),
            Array(array_ser) => array_ser.end(),
        }
    }
}

impl<W: Write> ser::SerializeTupleStruct for SchemaAwareWriteSerializeTupleStruct<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_field(&value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<W: Write> ser::SerializeTupleVariant for SchemaAwareWriteSerializeTupleStruct<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_field(&value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

/// A [`serde::ser::Serializer`] implementation that serializes directly to a [`std::fmt::Write`]
/// using the provided schema.  If [`SchemaAwareWriteSerializer`] isn't able to match the incoming
/// data with its schema, it will return an error.
/// A [`SchemaAwareWriteSerializer`] instance can be re-used to serialize multiple values matching
/// the schema to its [`std::fmt::Write`] stream.
pub struct SchemaAwareWriteSerializer<'s, W: Write> {
    writer: &'s mut W,
    root_schema: &'s Schema,
    names: &'s NamesRef<'s>,
    enclosing_namespace: Namespace,
}

impl<'s, W: Write> SchemaAwareWriteSerializer<'s, W> {
    /// Create a new [`SchemaAwareWriteSerializer`].
    ///
    /// `writer` is the [`std::fmt::Write`] stream to be written to.
    ///
    /// `schema` is the schema of the value to be written.
    ///
    /// `names` is the mapping of schema names to schemas, to be used for type reference lookups
    ///
    /// `enclosing_namespace` is the enclosing namespace to be used for type reference lookups
    pub fn new(
        writer: &'s mut W,
        schema: &'s Schema,
        names: &'s NamesRef<'s>,
        enclosing_namespace: Namespace,
    ) -> SchemaAwareWriteSerializer<'s, W> {
        SchemaAwareWriteSerializer {
            writer,
            root_schema: schema,
            names,
            enclosing_namespace,
        }
    }

    fn get_ref_schema(&self, name: &'s Name) -> Result<&'s Schema, Error> {
        let full_name = match name.namespace {
            Some(_) => Cow::Borrowed(name),
            None => Cow::Owned(Name {
                name: name.name.clone(),
                namespace: self.enclosing_namespace.clone(),
            }),
        };

        let ref_schema = self.names.get(full_name.as_ref()).copied();

        ref_schema.ok_or_else(|| Details::SchemaResolutionError(full_name.as_ref().clone()).into())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let mut bytes_written: usize = 0;

        bytes_written += encode_long(bytes.len() as i64, &mut self.writer)?;
        bytes_written += self.writer.write(bytes).map_err(Details::WriteBytes)?;

        Ok(bytes_written)
    }

    fn serialize_bool_with_schema(&mut self, value: bool, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "bool",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Boolean => self
                .writer
                .write(&[u8::from(value)])
                .map_err(|e| Details::WriteBytes(e).into()),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Boolean => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_bool_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "No matching Schema::Bool found in {:?}",
                    union_schema.schemas
                )))
            }
            expected => Err(create_error(format!("Expected {expected}. Got: Bool"))),
        }
    }

    fn serialize_i32_with_schema(&mut self, value: i32, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "int (i8 | i16 | i32)",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => encode_int(value, &mut self.writer),
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => encode_long(value as i64, &mut self.writer),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Int
                        | Schema::TimeMillis
                        | Schema::Date
                        | Schema::Long
                        | Schema::TimeMicros
                        | Schema::TimestampMillis
                        | Schema::TimestampMicros
                        | Schema::TimestampNanos
                        | Schema::LocalTimestampMillis
                        | Schema::LocalTimestampMicros
                        | Schema::LocalTimestampNanos => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_i32_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching int-like schema in {union_schema:?}"
                )))
            }
            expected => Err(create_error(format!("Expected {expected}. Got: Int/Long"))),
        }
    }

    fn serialize_i64_with_schema(&mut self, value: i64, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "i64",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value =
                    i32::try_from(value).map_err(|cause| create_error(cause.to_string()))?;
                encode_int(int_value, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => encode_long(value, &mut self.writer),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Int
                        | Schema::TimeMillis
                        | Schema::Date
                        | Schema::Long
                        | Schema::TimeMicros
                        | Schema::TimestampMillis
                        | Schema::TimestampMicros
                        | Schema::TimestampNanos
                        | Schema::LocalTimestampMillis
                        | Schema::LocalTimestampMicros
                        | Schema::LocalTimestampNanos => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_i64_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching int/long-like schema in {:?}",
                    union_schema.schemas
                )))
            }
            expected => Err(create_error(format!("Expected: {expected}. Got: Int/Long"))),
        }
    }

    fn serialize_u8_with_schema(&mut self, value: u8, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "u8",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                encode_int(value as i32, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => encode_long(value as i64, &mut self.writer),
            Schema::Bytes => self.write_bytes(&[value]),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Int
                        | Schema::TimeMillis
                        | Schema::Date
                        | Schema::Long
                        | Schema::TimeMicros
                        | Schema::TimestampMillis
                        | Schema::TimestampMicros
                        | Schema::TimestampNanos
                        | Schema::LocalTimestampMillis
                        | Schema::LocalTimestampMicros
                        | Schema::LocalTimestampNanos
                        | Schema::Bytes => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_u8_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching Int-like, Long-like or Bytes schema in {union_schema:?}"
                )))
            }
            expected => Err(create_error(format!("Expected: {expected}. Got: Int"))),
        }
    }

    fn serialize_u32_with_schema(&mut self, value: u32, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "unsigned int (u16 | u32)",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value =
                    i32::try_from(value).map_err(|cause| create_error(cause.to_string()))?;
                encode_int(int_value, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => encode_long(value as i64, &mut self.writer),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Int
                        | Schema::TimeMillis
                        | Schema::Date
                        | Schema::Long
                        | Schema::TimeMicros
                        | Schema::TimestampMillis
                        | Schema::TimestampMicros
                        | Schema::TimestampNanos
                        | Schema::LocalTimestampMillis
                        | Schema::LocalTimestampMicros
                        | Schema::LocalTimestampNanos => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_u32_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching Int-like or Long-like schema in {union_schema:?}"
                )))
            }
            expected => Err(create_error(format!("Expected: {expected}. Got: Int/Long"))),
        }
    }

    fn serialize_u64_with_schema(&mut self, value: u64, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "u64",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value =
                    i32::try_from(value).map_err(|cause| create_error(cause.to_string()))?;
                encode_int(int_value, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                let long_value =
                    i64::try_from(value).map_err(|cause| create_error(cause.to_string()))?;
                encode_long(long_value, &mut self.writer)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Int
                        | Schema::TimeMillis
                        | Schema::Date
                        | Schema::Long
                        | Schema::TimeMicros
                        | Schema::TimestampMillis
                        | Schema::TimestampMicros
                        | Schema::TimestampNanos
                        | Schema::LocalTimestampMillis
                        | Schema::LocalTimestampMicros
                        | Schema::LocalTimestampNanos => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_u64_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching Int-like or Long-like schema in {:?}",
                    union_schema.schemas
                )))
            }
            expected => Err(create_error(format!("Expected {expected}. Got: Int/Long"))),
        }
    }

    fn serialize_f32_with_schema(&mut self, value: f32, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "f32",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Float => self
                .writer
                .write(&value.to_le_bytes())
                .map_err(|e| Details::WriteBytes(e).into()),
            Schema::Double => self
                .writer
                .write(&(value as f64).to_le_bytes())
                .map_err(|e| Details::WriteBytes(e).into()),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Float | Schema::Double => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_f32_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a Float schema in {:?}",
                    union_schema.schemas
                )))
            }
            expected => Err(create_error(format!("Expected: {expected}. Got: Float"))),
        }
    }

    fn serialize_f64_with_schema(&mut self, value: f64, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "f64",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Float => self
                .writer
                .write(&(value as f32).to_le_bytes())
                .map_err(|e| Details::WriteBytes(e).into()),
            Schema::Double => self
                .writer
                .write(&value.to_le_bytes())
                .map_err(|e| Details::WriteBytes(e).into()),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Float | Schema::Double => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_f64_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a Double schema in {:?}",
                    union_schema.schemas
                )))
            }
            expected => Err(create_error(format!("Expected: {expected}. Got: Double"))),
        }
    }

    fn serialize_char_with_schema(&mut self, value: char, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "char",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::String | Schema::Bytes => self.write_bytes(String::from(value).as_bytes()),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::String | Schema::Bytes => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_char_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching String or Bytes schema in {union_schema:?}"
                )))
            }
            expected => Err(create_error(format!("Expected {expected}. Got: char"))),
        }
    }

    fn serialize_str_with_schema(&mut self, value: &str, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "string",
                value: format!("{value}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::String | Schema::Bytes | Schema::Uuid => self.write_bytes(value.as_bytes()),
            Schema::BigDecimal => {
                // If we get a string for a `BigDecimal` type, expect a display string representation, such as "12.75"
                let decimal_val =
                    BigDecimal::from_str(value).map_err(|e| create_error(e.to_string()))?;
                let decimal_bytes = big_decimal_as_bytes(&decimal_val)?;
                self.write_bytes(decimal_bytes.as_slice())
            }
            Schema::Fixed(fixed_schema) => {
                if value.len() == fixed_schema.size {
                    self.writer
                        .write(value.as_bytes())
                        .map_err(|e| Details::WriteBytes(e).into())
                } else {
                    Err(create_error(format!(
                        "Fixed schema size ({}) does not match the value length ({})",
                        fixed_schema.size,
                        value.len()
                    )))
                }
            }
            Schema::Ref { name } => {
                let ref_schema = self.get_ref_schema(name)?;
                self.serialize_str_with_schema(value, ref_schema)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::String
                        | Schema::Bytes
                        | Schema::Uuid
                        | Schema::Fixed(_)
                        | Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_str_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Expected one of the union variants {:?}. Got: String",
                    union_schema.schemas
                )))
            }
            expected => Err(create_error(format!("Expected: {expected}. Got: String"))),
        }
    }

    fn serialize_bytes_with_schema(
        &mut self,
        value: &[u8],
        schema: &Schema,
    ) -> Result<usize, Error> {
        let create_error = |cause: String| {
            use std::fmt::Write;
            let mut v_str = String::with_capacity(value.len());
            for b in value {
                if write!(&mut v_str, "{b:x}").is_err() {
                    v_str.push_str("??");
                }
            }
            Error::new(Details::SerializeValueWithSchema {
                value_type: "bytes",
                value: format!("{v_str}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::String | Schema::Bytes | Schema::Uuid | Schema::BigDecimal => {
                self.write_bytes(value)
            }
            Schema::Fixed(fixed_schema) => {
                if value.len() == fixed_schema.size {
                    self.writer
                        .write(value)
                        .map_err(|e| Details::WriteBytes(e).into())
                } else {
                    Err(create_error(format!(
                        "Fixed schema size ({}) does not match the value length ({})",
                        fixed_schema.size,
                        value.len()
                    )))
                }
            }
            Schema::Duration => {
                if value.len() == 12 {
                    self.writer
                        .write(value)
                        .map_err(|e| Details::WriteBytes(e).into())
                } else {
                    Err(create_error(format!(
                        "Duration length must be 12! Got ({})",
                        value.len()
                    )))
                }
            }
            Schema::Decimal(decimal_schema) => match decimal_schema.inner.as_ref() {
                Schema::Bytes => self.write_bytes(value),
                Schema::Fixed(fixed_schema) => match fixed_schema.size.checked_sub(value.len()) {
                    Some(pad) => {
                        let pad_val = match value.len() {
                            0 => 0,
                            _ => value[0],
                        };
                        let padding = vec![pad_val; pad];
                        self.writer
                            .write(padding.as_slice())
                            .map_err(Details::WriteBytes)?;
                        self.writer
                            .write(value)
                            .map_err(|e| Details::WriteBytes(e).into())
                    }
                    None => Err(Details::CompareFixedSizes {
                        size: fixed_schema.size,
                        n: value.len(),
                    }
                    .into()),
                },
                unsupported => Err(create_error(format!(
                    "Decimal schema's inner should be Bytes or Fixed schema. Got: {unsupported}"
                ))),
            },
            Schema::Ref { name } => {
                let ref_schema = self.get_ref_schema(name)?;
                self.serialize_bytes_with_schema(value, ref_schema)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::String
                        | Schema::Bytes
                        | Schema::Uuid
                        | Schema::BigDecimal
                        | Schema::Fixed(_)
                        | Schema::Duration
                        | Schema::Decimal(_)
                        | Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_bytes_with_schema(value, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching String, Bytes, Uuid, BigDecimal, Fixed, Duration, Decimal or Ref schema in {union_schema:?}"
                )))
            }
            unsupported => Err(create_error(format!(
                "Expected String, Bytes, Uuid, BigDecimal, Fixed, Duration, Decimal, Ref or Union schema. Got: {unsupported}"
            ))),
        }
    }

    fn serialize_none_with_schema(&mut self, schema: &Schema) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "none",
                value: format!("None. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Null => Ok(0),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Null => {
                            return encode_int(i as i32, &mut *self.writer);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching Null schema in {:?}",
                    union_schema.schemas
                )))
            }
            expected => Err(create_error(format!("Expected: {expected}. Got: Null"))),
        }
    }

    fn serialize_some_with_schema<T>(&mut self, value: &T, schema: &Schema) -> Result<usize, Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "some",
                value: format!("Some(?). Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Null => { /* skip */ }
                        _ => {
                            encode_long(i as i64, &mut *self.writer)?;
                            let mut variant_ser = SchemaAwareWriteSerializer::new(
                                &mut *self.writer,
                                variant_schema,
                                self.names,
                                self.enclosing_namespace.clone(),
                            );
                            return value.serialize(&mut variant_ser);
                        }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching Null schema in {:?}",
                    union_schema.schemas
                )))
            }
            _ => value.serialize(self),
        }
    }

    fn serialize_unit_struct_with_schema(
        &mut self,
        name: &'static str,
        schema: &Schema,
    ) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "unit struct",
                value: format!("{name}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Record(sch) => match sch.fields.len() {
                0 => Ok(0),
                too_many => Err(create_error(format!(
                    "Too many fields: {too_many}. Expected: 0"
                ))),
            },
            Schema::Null => Ok(0),
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.serialize_unit_struct_with_schema(name, ref_schema)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Record(record_schema) if record_schema.fields.is_empty() => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_unit_struct_with_schema(name, variant_schema);
                        }
                        Schema::Null | Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_unit_struct_with_schema(name, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Cannot find a matching Null schema in {union_schema:?}"
                )))
            }
            unsupported => Err(create_error(format!(
                "Expected Null or Union schema. Got: {unsupported}"
            ))),
        }
    }

    fn serialize_unit_variant_with_schema(
        &mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        schema: &Schema,
    ) -> Result<usize, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "unit variant",
                value: format!("{name}::{variant} (index={variant_index}). Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Enum(enum_schema) => {
                if variant_index as usize >= enum_schema.symbols.len() {
                    return Err(create_error(format!(
                        "Variant index out of bounds: {}. The Enum schema has '{}' symbols",
                        variant_index,
                        enum_schema.symbols.len()
                    )));
                }

                encode_int(variant_index as i32, &mut self.writer)
            }
            Schema::Union(union_schema) => {
                if variant_index as usize >= union_schema.schemas.len() {
                    return Err(create_error(format!(
                        "Variant index out of bounds: {}. The union schema has '{}' schemas",
                        variant_index,
                        union_schema.schemas.len()
                    )));
                }

                encode_int(variant_index as i32, &mut self.writer)?;
                self.serialize_unit_struct_with_schema(
                    name,
                    &union_schema.schemas[variant_index as usize],
                )
            }
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.serialize_unit_variant_with_schema(name, variant_index, variant, ref_schema)
            }
            unsupported => Err(create_error(format!(
                "Unsupported schema: {unsupported:?}. Expected: Enum, Union or Ref"
            ))),
        }
    }

    fn serialize_newtype_struct_with_schema<T>(
        &mut self,
        _name: &'static str,
        value: &T,
        schema: &Schema,
    ) -> Result<usize, Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let mut inner_ser = SchemaAwareWriteSerializer::new(
            &mut *self.writer,
            schema,
            self.names,
            self.enclosing_namespace.clone(),
        );
        // Treat any newtype struct as a transparent wrapper around the contained type
        value.serialize(&mut inner_ser)
    }

    fn serialize_newtype_variant_with_schema<T>(
        &mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
        schema: &Schema,
    ) -> Result<usize, Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "newtype variant",
                value: format!("{name}::{variant}(?) (index={variant_index}). Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Union(union_schema) => {
                let variant_schema = union_schema
                    .schemas
                    .get(variant_index as usize)
                    .ok_or_else(|| {
                        create_error(format!(
                            "No variant schema at position {variant_index} for {union_schema:?}"
                        ))
                    })?;

                encode_int(variant_index as i32, &mut self.writer)?;
                self.serialize_newtype_struct_with_schema(variant, value, variant_schema)
            }
            _ => Err(create_error(format!(
                "Expected Union schema. Got: {schema}"
            ))),
        }
    }

    fn serialize_seq_with_schema<'a>(
        &'a mut self,
        len: Option<usize>,
        schema: &'s Schema,
    ) -> Result<SchemaAwareWriteSerializeSeq<'a, 's, W>, Error> {
        let create_error = |cause: String| {
            let len_str = len
                .map(|l| format!("{l}"))
                .unwrap_or_else(|| String::from("?"));

            Error::new(Details::SerializeValueWithSchema {
                value_type: "sequence",
                value: format!("sequence (len={len_str}). Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Array(array_schema) => Ok(SchemaAwareWriteSerializeSeq::new(
                self,
                array_schema.items.as_ref(),
                len,
            )),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Array(_) => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_seq_with_schema(len, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Expected Array schema in {union_schema:?}"
                )))
            }
            _ => Err(create_error(format!("Expected: {schema}. Got: Array"))),
        }
    }

    fn serialize_tuple_with_schema<'a>(
        &'a mut self,
        len: usize,
        schema: &'s Schema,
    ) -> Result<SchemaAwareWriteSerializeSeq<'a, 's, W>, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "tuple",
                value: format!("tuple (len={len}). Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Array(array_schema) => Ok(SchemaAwareWriteSerializeSeq::new(
                self,
                array_schema.items.as_ref(),
                Some(len),
            )),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Array(_) => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_tuple_with_schema(len, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Expected Array schema in {union_schema:?}"
                )))
            }
            _ => Err(create_error(format!("Expected: {schema}. Got: Array"))),
        }
    }

    fn serialize_tuple_struct_with_schema<'a>(
        &'a mut self,
        name: &'static str,
        len: usize,
        schema: &'s Schema,
    ) -> Result<SchemaAwareWriteSerializeTupleStruct<'a, 's, W>, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "tuple struct",
                value: format!(
                    "{name}({}). Cause: {cause}",
                    vec!["?"; len].as_slice().join(",")
                ),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Array(sch) => Ok(SchemaAwareWriteSerializeTupleStruct::Array(
                SchemaAwareWriteSerializeSeq::new(self, &sch.items, Some(len)),
            )),
            Schema::Record(sch) => Ok(SchemaAwareWriteSerializeTupleStruct::Record(
                SchemaAwareWriteSerializeStruct::new(self, sch),
            )),
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.serialize_tuple_struct_with_schema(name, len, ref_schema)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Record(inner) => {
                            if inner.fields.len() == len {
                                encode_int(i as i32, &mut *self.writer)?;
                                return self.serialize_tuple_struct_with_schema(
                                    name,
                                    len,
                                    variant_schema,
                                );
                            }
                        }
                        Schema::Array(_) | Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_tuple_struct_with_schema(
                                name,
                                len,
                                variant_schema,
                            );
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Expected Record, Array or Ref schema in {union_schema:?}"
                )))
            }
            _ => Err(create_error(format!(
                "Expected Record, Array, Ref or Union schema. Got: {schema}"
            ))),
        }
    }

    fn serialize_tuple_variant_with_schema<'a>(
        &'a mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
        schema: &'s Schema,
    ) -> Result<SchemaAwareWriteSerializeTupleStruct<'a, 's, W>, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "tuple variant",
                value: format!(
                    "{name}::{variant}({}) (index={variant_index}). Cause: {cause}",
                    vec!["?"; len].as_slice().join(",")
                ),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Union(union_schema) => {
                let variant_schema = union_schema
                    .schemas
                    .get(variant_index as usize)
                    .ok_or_else(|| {
                        create_error(format!(
                            "Cannot find a variant at position {variant_index} in {union_schema:?}"
                        ))
                    })?;

                encode_int(variant_index as i32, &mut self.writer)?;
                self.serialize_tuple_struct_with_schema(variant, len, variant_schema)
            }
            _ => Err(create_error(format!(
                "Expected Union schema. Got: {schema}"
            ))),
        }
    }

    fn serialize_map_with_schema<'a>(
        &'a mut self,
        len: Option<usize>,
        schema: &'s Schema,
    ) -> Result<SchemaAwareWriteSerializeMap<'a, 's, W>, Error> {
        let create_error = |cause: String| {
            let len_str = len
                .map(|l| format!("{l}"))
                .unwrap_or_else(|| String::from("?"));

            Error::new(Details::SerializeValueWithSchema {
                value_type: "map",
                value: format!("map (size={len_str}). Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Map(map_schema) => Ok(SchemaAwareWriteSerializeMap::new(
                self,
                map_schema.types.as_ref(),
                len,
            )),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Map(_) => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_map_with_schema(len, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Expected a Map schema in {union_schema:?}"
                )))
            }
            _ => Err(create_error(format!(
                "Expected Map or Union schema. Got: {schema}"
            ))),
        }
    }

    fn serialize_struct_with_schema<'a>(
        &'a mut self,
        name: &'static str,
        len: usize,
        schema: &'s Schema,
    ) -> Result<SchemaAwareWriteSerializeStruct<'a, 's, W>, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "struct",
                value: format!("{name}{{ ... }}. Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Record(record_schema) => {
                Ok(SchemaAwareWriteSerializeStruct::new(self, record_schema))
            }
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.serialize_struct_with_schema(name, len, ref_schema)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Record(inner)
                            if inner.fields.len() == len && inner.name.name == name =>
                        {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_struct_with_schema(name, len, variant_schema);
                        }
                        Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            return self.serialize_struct_with_schema(name, len, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(format!(
                    "Expected Record or Ref schema in {union_schema:?}"
                )))
            }
            _ => Err(create_error(format!(
                "Expected Record, Ref or Union schema. Got: {schema}"
            ))),
        }
    }

    fn serialize_struct_variant_with_schema<'a>(
        &'a mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
        schema: &'s Schema,
    ) -> Result<SchemaAwareWriteSerializeStruct<'a, 's, W>, Error> {
        let create_error = |cause: String| {
            Error::new(Details::SerializeValueWithSchema {
                value_type: "struct variant",
                value: format!("{name}::{variant}{{ ... }} (size={len}. Cause: {cause})"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Union(union_schema) => {
                let variant_schema = union_schema
                    .schemas
                    .get(variant_index as usize)
                    .ok_or_else(|| {
                        create_error(format!(
                            "Cannot find variant at position {variant_index} in {union_schema:?}"
                        ))
                    })?;

                encode_int(variant_index as i32, &mut self.writer)?;
                self.serialize_struct_with_schema(variant, len, variant_schema)
            }
            _ => Err(create_error(format!(
                "Expected Union schema. Got: {schema}"
            ))),
        }
    }
}

impl<'a, 's, W: Write> ser::Serializer for &'a mut SchemaAwareWriteSerializer<'s, W> {
    type Ok = usize;
    type Error = Error;
    type SerializeSeq = SchemaAwareWriteSerializeSeq<'a, 's, W>;
    type SerializeTuple = SchemaAwareWriteSerializeSeq<'a, 's, W>;
    type SerializeTupleStruct = SchemaAwareWriteSerializeTupleStruct<'a, 's, W>;
    type SerializeTupleVariant = SchemaAwareWriteSerializeTupleStruct<'a, 's, W>;
    type SerializeMap = SchemaAwareWriteSerializeMap<'a, 's, W>;
    type SerializeStruct = SchemaAwareWriteSerializeStruct<'a, 's, W>;
    type SerializeStructVariant = SchemaAwareWriteSerializeStruct<'a, 's, W>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.serialize_bool_with_schema(v, self.root_schema)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(v as i32)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(v as i32)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32_with_schema(v, self.root_schema)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64_with_schema(v, self.root_schema)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_u8_with_schema(v, self.root_schema)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(v as u32)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32_with_schema(v, self.root_schema)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64_with_schema(v, self.root_schema)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f32_with_schema(v, self.root_schema)
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64_with_schema(v, self.root_schema)
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_char_with_schema(v, self.root_schema)
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.serialize_str_with_schema(v, self.root_schema)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.serialize_bytes_with_schema(v, self.root_schema)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_none_with_schema(self.root_schema)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_some_with_schema(value, self.root_schema)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit_struct_with_schema(name, self.root_schema)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit_variant_with_schema(name, variant_index, variant, self.root_schema)
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_newtype_struct_with_schema(name, value, self.root_schema)
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_newtype_variant_with_schema(
            name,
            variant_index,
            variant,
            value,
            self.root_schema,
        )
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.serialize_seq_with_schema(len, self.root_schema)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_tuple_with_schema(len, self.root_schema)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_tuple_struct_with_schema(name, len, self.root_schema)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.serialize_tuple_variant_with_schema(
            name,
            variant_index,
            variant,
            len,
            self.root_schema,
        )
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.serialize_map_with_schema(len, self.root_schema)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_struct_with_schema(name, len, self.root_schema)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.serialize_struct_variant_with_schema(
            name,
            variant_index,
            variant,
            len,
            self.root_schema,
        )
    }

    fn is_human_readable(&self) -> bool {
        crate::util::is_human_readable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Days, Duration, Millis, Months, decimal::Decimal, error::Details, schema::ResolvedSchema,
    };
    use apache_avro_test_helper::TestResult;
    use bigdecimal::BigDecimal;
    use num_bigint::{BigInt, Sign};
    use serde::Serialize;
    use serde_bytes::{ByteArray, Bytes};
    use std::{
        collections::{BTreeMap, HashMap},
        marker::PhantomData,
    };
    use uuid::Uuid;

    #[test]
    fn test_serialize_null() -> TestResult {
        let schema = Schema::Null;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        ().serialize(&mut serializer)?;
        None::<()>.serialize(&mut serializer)?;
        None::<i32>.serialize(&mut serializer)?;
        None::<String>.serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), Vec::<u8>::new().as_slice());

        Ok(())
    }

    #[test]
    fn test_serialize_bool() -> TestResult {
        let schema = Schema::Boolean;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        true.serialize(&mut serializer)?;
        false.serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[1, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_int() -> TestResult {
        let schema = Schema::Int;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        4u8.serialize(&mut serializer)?;
        31u16.serialize(&mut serializer)?;
        13u32.serialize(&mut serializer)?;
        7i8.serialize(&mut serializer)?;
        (-57i16).serialize(&mut serializer)?;
        129i32.serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[8, 62, 26, 14, 113, 130, 2]);

        Ok(())
    }

    #[test]
    fn test_serialize_long() -> TestResult {
        let schema = Schema::Long;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        4u8.serialize(&mut serializer)?;
        31u16.serialize(&mut serializer)?;
        13u32.serialize(&mut serializer)?;
        291u64.serialize(&mut serializer)?;
        7i8.serialize(&mut serializer)?;
        (-57i16).serialize(&mut serializer)?;
        129i32.serialize(&mut serializer)?;
        (-432i64).serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(
            buffer.as_slice(),
            &[8, 62, 26, 198, 4, 14, 113, 130, 2, 223, 6]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_float() -> TestResult {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        4.7f32.serialize(&mut serializer)?;
        (-14.1f64).serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);

        Ok(())
    }

    #[test]
    fn test_serialize_double() -> TestResult {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        4.7f32.serialize(&mut serializer)?;
        (-14.1f64).serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);

        Ok(())
    }

    #[test]
    fn test_serialize_bytes() -> TestResult {
        let schema = Schema::Bytes;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        'a'.serialize(&mut serializer)?;
        "test".serialize(&mut serializer)?;
        Bytes::new(&[12, 3, 7, 91, 4]).serialize(&mut serializer)?;
        assert!(().serialize(&mut serializer).is_err());
        assert!(PhantomData::<String>.serialize(&mut serializer).is_err());

        assert_eq!(
            buffer.as_slice(),
            &[2, b'a', 8, b't', b'e', b's', b't', 10, 12, 3, 7, 91, 4]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_string() -> TestResult {
        let schema = Schema::String;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        'a'.serialize(&mut serializer)?;
        "test".serialize(&mut serializer)?;
        Bytes::new(&[12, 3, 7, 91, 4]).serialize(&mut serializer)?;
        assert!(().serialize(&mut serializer).is_err());
        assert!(PhantomData::<String>.serialize(&mut serializer).is_err());

        assert_eq!(
            buffer.as_slice(),
            &[2, b'a', 8, b't', b'e', b's', b't', 10, 12, 3, 7, 91, 4]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "stringField", "type": "string"},
                {"name": "intField", "type": "int"}
            ]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct GoodTestRecord {
            string_field: String,
            int_field: i32,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct BadTestRecord {
            foo_string_field: String,
            bar_int_field: i32,
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        let good_record = GoodTestRecord {
            string_field: String::from("test"),
            int_field: 10,
        };
        good_record.serialize(&mut serializer)?;

        let bad_record = BadTestRecord {
            foo_string_field: String::from("test"),
            bar_int_field: 10,
        };
        assert!(bad_record.serialize(&mut serializer).is_err());

        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[8, b't', b'e', b's', b't', 20]);

        Ok(())
    }

    #[test]
    fn test_serialize_empty_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "EmptyRecord",
            "fields": []
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        #[derive(Serialize)]
        struct EmptyRecord;
        EmptyRecord.serialize(&mut serializer)?;

        #[derive(Serialize)]
        struct NonEmptyRecord {
            foo: String,
        }
        let record = NonEmptyRecord {
            foo: "bar".to_string(),
        };
        match record
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::FieldName(field_name)) if field_name == "foo" => (),
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        match ().serialize(&mut serializer).map_err(Error::into_details) {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none"); // serialize_unit() delegates to serialize_none()
                assert_eq!(value, "None. Cause: Expected: Record. Got: Null");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.len(), 0);

        Ok(())
    }

    #[test]
    fn test_serialize_enum() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "enum",
            "name": "Suit",
            "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        }"#,
        )?;

        #[derive(Serialize)]
        enum Suit {
            Spades,
            Hearts,
            Diamonds,
            Clubs,
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        Suit::Spades.serialize(&mut serializer)?;
        Suit::Hearts.serialize(&mut serializer)?;
        Suit::Diamonds.serialize(&mut serializer)?;
        Suit::Clubs.serialize(&mut serializer)?;
        match None::<()>
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None. Cause: Expected: Enum. Got: Null");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[0, 2, 4, 6]);

        Ok(())
    }

    #[test]
    fn test_serialize_array() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "array",
            "items": "long"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        let arr: Vec<i64> = vec![10, 5, 400];
        arr.serialize(&mut serializer)?;

        match vec![1_f32]
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "1. Cause: Expected: Long. Got: Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[6, 20, 10, 160, 6, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_map() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "map",
            "values": "long"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        let mut map: BTreeMap<String, i64> = BTreeMap::new();
        map.insert(String::from("item1"), 10);
        map.insert(String::from("item2"), 400);

        map.serialize(&mut serializer)?;

        let mut map: BTreeMap<String, &str> = BTreeMap::new();
        map.insert(String::from("item1"), "value1");
        match map.serialize(&mut serializer).map_err(Error::into_details) {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "string");
                assert_eq!(value, "value1. Cause: Expected: Long. Got: String");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[
                4, 10, b'i', b't', b'e', b'm', b'1', 20, 10, b'i', b't', b'e', b'm', b'2', 160, 6,
                0
            ]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_nullable_union() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": ["null", "long"]
        }"#,
        )?;

        #[derive(Serialize)]
        enum NullableLong {
            Null,
            Long(i64),
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        Some(10i64).serialize(&mut serializer)?;
        None::<i64>.serialize(&mut serializer)?;
        NullableLong::Long(400).serialize(&mut serializer)?;
        NullableLong::Null.serialize(&mut serializer)?;

        match "invalid"
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "string");
                assert_eq!(
                    value,
                    "invalid. Cause: Expected one of the union variants [Null, Long]. Got: String"
                );
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[2, 20, 0, 2, 160, 6, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_union() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": ["null", "long", "string"]
        }"#,
        )?;

        #[derive(Serialize)]
        enum LongOrString {
            Null,
            Long(i64),
            Str(String),
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        LongOrString::Null.serialize(&mut serializer)?;
        LongOrString::Long(400).serialize(&mut serializer)?;
        LongOrString::Str(String::from("test")).serialize(&mut serializer)?;

        match 1_f64
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f64");
                assert_eq!(
                    value,
                    "1. Cause: Cannot find a Double schema in [Null, Long, String]"
                );
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[0, 2, 160, 6, 4, 8, b't', b'e', b's', b't']
        );

        Ok(())
    }

    #[test]
    fn test_serialize_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "size": 8,
            "name": "LongVal"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        Bytes::new(&[10, 124, 31, 97, 14, 201, 3, 88]).serialize(&mut serializer)?;

        // non-8 size
        match Bytes::new(&[123])
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "bytes");
                assert_eq!(
                    value,
                    "7b. Cause: Fixed schema size (8) does not match the value length (1)"
                ); // Bytes represents its values as hexadecimals: '7b' is 123
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        // array
        match [1; 8]
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(value, "tuple (len=8). Cause: Expected: Fixed. Got: Array");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        // slice
        match &[1, 2, 3, 4, 5, 6, 7, 8]
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(*value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(value, "tuple (len=8). Cause: Expected: Fixed. Got: Array");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[10, 124, 31, 97, 14, 201, 3, 88]);

        Ok(())
    }

    #[test]
    fn test_serialize_decimal_bytes() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 16,
            "scale": 2
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        let val = Decimal::from(&[251, 155]);
        val.serialize(&mut serializer)?;

        match ().serialize(&mut serializer).map_err(Error::into_details) {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None. Cause: Expected: Decimal. Got: Null");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[4, 251, 155]);

        Ok(())
    }

    #[test]
    fn test_serialize_decimal_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "name": "FixedDecimal",
            "size": 8,
            "logicalType": "decimal",
            "precision": 16,
            "scale": 2
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        let val = Decimal::from(&[0, 0, 0, 0, 0, 0, 251, 155]);
        val.serialize(&mut serializer)?;

        match ().serialize(&mut serializer).map_err(Error::into_details) {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None. Cause: Expected: Decimal. Got: Null");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[0, 0, 0, 0, 0, 0, 251, 155]);

        Ok(())
    }

    #[test]
    fn test_serialize_bigdecimal() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "bytes",
            "logicalType": "big-decimal"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        let val = BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2);
        val.serialize(&mut serializer)?;

        assert_eq!(buffer.as_slice(), &[10, 6, 0, 195, 104, 4]);

        Ok(())
    }

    #[test]
    fn test_serialize_uuid() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "string",
            "logicalType": "uuid"
        }"#,
        )?;

        assert!(!crate::util::is_human_readable());
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        "8c28da81-238c-4326-bddd-4e3d00cc5099"
            .parse::<Uuid>()?
            .serialize(&mut serializer)?;

        match 1_u8.serialize(&mut serializer).map_err(Error::into_details) {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "u8");
                assert_eq!(value, "1. Cause: Expected: Uuid. Got: Int");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[
                32, 140, 40, 218, 129, 35, 140, 67, 38, 189, 221, 78, 61, 0, 204, 80, 153
            ]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_date() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "int",
            "logicalType": "date"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        100_u8.serialize(&mut serializer)?;
        1000_u16.serialize(&mut serializer)?;
        10000_u32.serialize(&mut serializer)?;
        1000_i16.serialize(&mut serializer)?;
        10000_i32.serialize(&mut serializer)?;

        match 10000_f32
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000. Cause: Expected: Date. Got: Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_time_millis() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "int",
            "logicalType": "time-millis"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        100_u8.serialize(&mut serializer)?;
        1000_u16.serialize(&mut serializer)?;
        10000_u32.serialize(&mut serializer)?;
        1000_i16.serialize(&mut serializer)?;
        10000_i32.serialize(&mut serializer)?;

        match 10000_f32
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000. Cause: Expected: TimeMillis. Got: Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_time_micros() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "long",
            "logicalType": "time-micros"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        100_u8.serialize(&mut serializer)?;
        1000_u16.serialize(&mut serializer)?;
        10000_u32.serialize(&mut serializer)?;
        1000_i16.serialize(&mut serializer)?;
        10000_i32.serialize(&mut serializer)?;
        10000_i64.serialize(&mut serializer)?;

        match 10000_f32
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000. Cause: Expected: TimeMicros. Got: Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[
                200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1, 160, 156, 1
            ]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_timestamp() -> TestResult {
        for precision in ["millis", "micros", "nanos"] {
            let schema = Schema::parse_str(&format!(
                r#"{{
                "type": "long",
                "logicalType": "timestamp-{precision}"
            }}"#
            ))?;

            let mut buffer: Vec<u8> = Vec::new();
            let names = HashMap::new();
            let mut serializer =
                SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

            100_u8.serialize(&mut serializer)?;
            1000_u16.serialize(&mut serializer)?;
            10000_u32.serialize(&mut serializer)?;
            1000_i16.serialize(&mut serializer)?;
            10000_i32.serialize(&mut serializer)?;
            10000_i64.serialize(&mut serializer)?;

            match 10000_f64
                .serialize(&mut serializer)
                .map_err(Error::into_details)
            {
                Err(Details::SerializeValueWithSchema {
                    value_type,
                    value,
                    schema,
                }) => {
                    let mut capital_precision = precision.to_string();
                    if let Some(c) = capital_precision.chars().next() {
                        capital_precision.replace_range(..1, &c.to_uppercase().to_string());
                    }
                    assert_eq!(value_type, "f64");
                    assert_eq!(
                        value,
                        format!(
                            "10000. Cause: Expected: Timestamp{capital_precision}. Got: Double"
                        )
                    );
                    assert_eq!(schema, schema);
                }
                unexpected => panic!("Expected an error. Got: {unexpected:?}"),
            }

            assert_eq!(
                buffer.as_slice(),
                &[
                    200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1, 160, 156, 1
                ]
            );
        }

        Ok(())
    }

    #[test]
    fn test_serialize_duration() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "size": 12,
            "name": "duration",
            "logicalType": "duration"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = SchemaAwareWriteSerializer::new(&mut buffer, &schema, &names, None);

        let duration_bytes =
            ByteArray::new(Duration::new(Months::new(3), Days::new(2), Millis::new(1200)).into());
        duration_bytes.serialize(&mut serializer)?;

        match [1; 12]
            .serialize(&mut serializer)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(
                    value,
                    "tuple (len=12). Cause: Expected: Duration. Got: Array"
                );
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[3, 0, 0, 0, 2, 0, 0, 0, 176, 4, 0, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_recursive_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "stringField", "type": "string"},
                {"name": "intField", "type": "int"},
                {"name": "bigDecimalField", "type": {"type": "bytes", "logicalType": "big-decimal"}},
                {"name": "uuidField", "type": "fixed", "size": 16, "logicalType": "uuid"},
                {"name": "innerRecord", "type": ["null", "TestRecord"]}
            ]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct TestRecord {
            string_field: String,
            int_field: i32,
            big_decimal_field: BigDecimal,
            uuid_field: Uuid,
            // #[serde(skip_serializing_if = "Option::is_none")] => Never ignore None!
            inner_record: Option<Box<TestRecord>>,
        }

        assert!(!crate::util::is_human_readable());
        let mut buffer: Vec<u8> = Vec::new();
        let rs = ResolvedSchema::try_from(&schema)?;
        let mut serializer =
            SchemaAwareWriteSerializer::new(&mut buffer, &schema, rs.get_names(), None);

        let good_record = TestRecord {
            string_field: String::from("test"),
            int_field: 10,
            big_decimal_field: BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2),
            uuid_field: "8c28da81-238c-4326-bddd-4e3d00cc5098".parse::<Uuid>()?,
            inner_record: Some(Box::new(TestRecord {
                string_field: String::from("inner_test"),
                int_field: 100,
                big_decimal_field: BigDecimal::new(BigInt::new(Sign::Plus, vec![20038]), 2),
                uuid_field: "8c28da81-238c-4326-bddd-4e3d00cc5099".parse::<Uuid>()?,
                inner_record: None,
            })),
        };
        good_record.serialize(&mut serializer)?;

        assert_eq!(
            buffer.as_slice(),
            &[
                8, 116, 101, 115, 116, 20, 10, 6, 0, 195, 104, 4, 32, 140, 40, 218, 129, 35, 140,
                67, 38, 189, 221, 78, 61, 0, 204, 80, 152, 2, 20, 105, 110, 110, 101, 114, 95, 116,
                101, 115, 116, 200, 1, 8, 4, 78, 70, 4, 32, 140, 40, 218, 129, 35, 140, 67, 38,
                189, 221, 78, 61, 0, 204, 80, 153, 0
            ]
        );

        Ok(())
    }
}
