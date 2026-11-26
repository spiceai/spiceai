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

//! Handling of Avro magic headers
use uuid::Uuid;

use crate::{AvroResult, Schema, rabin::Rabin, schema::SchemaFingerprint};

/// This trait represents that an object is able to construct an Avro message header. It is
/// implemented for some known header types already. If you need a header type that is not already
/// included here, then you can create your own struct and implement this trait.
pub trait HeaderBuilder {
    fn build_header(&self) -> Vec<u8>;
}

/// HeaderBuilder based on the Rabin schema fingerprint
///
/// This is the default and will be used automatically by the `new` impls in
/// [crate::reader::GenericSingleObjectReader] and [crate::writer::GenericSingleObjectWriter].
pub struct RabinFingerprintHeader {
    fingerprint: SchemaFingerprint,
}

impl RabinFingerprintHeader {
    /// Use this helper to build an instance from an existing Avro `Schema`.
    pub fn from_schema(schema: &Schema) -> Self {
        let fingerprint = schema.fingerprint::<Rabin>();
        RabinFingerprintHeader { fingerprint }
    }
}

impl HeaderBuilder for RabinFingerprintHeader {
    fn build_header(&self) -> Vec<u8> {
        let bytes = &self.fingerprint.bytes;
        vec![
            0xC3, 0x01, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
            bytes[7],
        ]
    }
}

/// HeaderBuilder based on
/// [Glue](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html) schema UUID
///
/// See the function docs for usage details
pub struct GlueSchemaUuidHeader {
    schema_uuid: Uuid,
}

impl GlueSchemaUuidHeader {
    /// Create an instance of the struct from a Glue Schema UUID
    ///
    /// Code for writing messages will most likely want to use this. You will need to determine
    /// via other means the correct Glue schema UUID and use it with this method to be able to
    /// create Avro-encoded messages with the correct headers.
    pub fn from_uuid(schema_uuid: Uuid) -> Self {
        GlueSchemaUuidHeader { schema_uuid }
    }

    /// The minimum length of a Glue header.
    /// 2 bytes for the special prefix (3, 0) plus
    /// 16 bytes for the Uuid
    const GLUE_HEADER_LENGTH: usize = 18;

    /// Create an instance of the struct based on parsing the UUID out of the header of a raw
    /// message
    ///
    /// Code for reading messages will most likely want to use this. Once you receive the raw bytes
    /// of a message, use this function to build the struct from it. That struct can then be used
    /// with the below `schema_uuid` function to retrieve the UUID in order to retrieve the correct
    /// schema for the message. You can then use the raw message, the schema, and the struct
    /// instance to read the message.
    pub fn parse_from_raw_avro(message_payload: &[u8]) -> AvroResult<Self> {
        if message_payload.len() < Self::GLUE_HEADER_LENGTH {
            return Err(crate::error::Details::HeaderMagic.into());
        }
        let schema_uuid = Uuid::from_slice(&message_payload[2..18])
            .map_err(crate::error::Details::UuidFromSlice)?;
        Ok(GlueSchemaUuidHeader { schema_uuid })
    }

    /// Retrieve the UUID from the object
    ///
    /// This is most useful in conjunction with the `parse_from_raw_avro` function to retrieve the
    /// actual UUID from the raw data of a received message.
    pub fn schema_uuid(&self) -> Uuid {
        self.schema_uuid
    }
}

impl HeaderBuilder for GlueSchemaUuidHeader {
    fn build_header(&self) -> Vec<u8> {
        let mut output_vec: Vec<u8> = vec![3, 0];
        output_vec.extend_from_slice(self.schema_uuid.as_bytes());
        output_vec
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Error, error::Details};
    use apache_avro_test_helper::TestResult;

    #[test]
    fn test_rabin_fingerprint_header() -> TestResult {
        let schema_str = r#"
            {
            "type": "record",
            "name": "test",
            "fields": [
                {
                "name": "a",
                "type": "long",
                "default": 42
                },
                {
                "name": "b",
                "type": "string"
                }
            ]
            }
            "#;
        let schema = Schema::parse_str(schema_str)?;
        let header_builder = RabinFingerprintHeader::from_schema(&schema);
        let computed_header = header_builder.build_header();
        let expected_header: Vec<u8> = vec![195, 1, 232, 198, 194, 12, 97, 95, 44, 71];
        assert_eq!(computed_header, expected_header);
        Ok(())
    }

    #[test]
    fn test_glue_schema_header() -> TestResult {
        let schema_uuid = Uuid::parse_str("b2f1cf00-0434-013e-439a-125eb8485a5f")?;
        let header_builder = GlueSchemaUuidHeader::from_uuid(schema_uuid);
        let computed_header = header_builder.build_header();
        let expected_header: Vec<u8> = vec![
            3, 0, 178, 241, 207, 0, 4, 52, 1, 62, 67, 154, 18, 94, 184, 72, 90, 95,
        ];
        assert_eq!(computed_header, expected_header);
        Ok(())
    }

    #[test]
    fn test_glue_header_parse() -> TestResult {
        let incoming_avro_message: Vec<u8> = vec![
            3, 0, 178, 241, 207, 0, 4, 52, 1, 62, 67, 154, 18, 94, 184, 72, 90, 95, 65, 65, 65,
        ];
        let header_builder = GlueSchemaUuidHeader::parse_from_raw_avro(&incoming_avro_message)?;
        let expected_schema_uuid = Uuid::parse_str("b2f1cf00-0434-013e-439a-125eb8485a5f")?;
        assert_eq!(header_builder.schema_uuid(), expected_schema_uuid);
        Ok(())
    }

    #[test]
    fn test_glue_header_parse_err_on_message_too_short() -> TestResult {
        let incoming_message: Vec<u8> = vec![3, 0, 178, 241, 207, 0, 4, 52, 1];
        let header_builder_res = GlueSchemaUuidHeader::parse_from_raw_avro(&incoming_message)
            .map_err(Error::into_details);
        assert!(matches!(header_builder_res, Err(Details::HeaderMagic)));
        Ok(())
    }
}
