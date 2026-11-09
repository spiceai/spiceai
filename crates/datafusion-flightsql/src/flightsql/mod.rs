/*
Copyright 2026 The Spice.ai OSS Authors

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

pub(crate) mod get_catalogs;
pub(crate) mod get_primary_keys;
pub(crate) mod get_schemas;
pub(crate) mod get_sql_info;
pub(crate) mod get_table_types;
pub(crate) mod get_tables;
pub(crate) mod get_xdbc_type_info;
pub(crate) mod prepared_statement_query;
pub(crate) mod prepared_statement_update;
pub(crate) mod statement_query;
pub(crate) mod statement_update;

use std::sync::{Arc, OnceLock};

use arrow_flight::FlightData;
use arrow_schema::SchemaRef;
use arrow_tools::map_entries;

/// Keeps a `DoPut` stream decodable when a client declares a `MAP`'s `entries` field nullable.
///
/// The Arrow map layout forbids that declaration, and the decode refuses it outright — over the
/// one part of the column that holds no data, reporting neither the column nor which of the two
/// map rules was broken. `FlightDataDecoder` reads its schema off the stream rather than taking
/// one from its caller, so the repair has to reach it as bytes: [`Self::repair`] replaces the
/// schema message on the way past with the form that decodes, where a map is labelled as the
/// list it is laid out as.
///
/// What the client declared is kept, because that — not the substitution the decoder goes on to
/// report — is what a `MapEntriesNormalizer` has to be built from for the batches to be put back
/// as the maps they describe. Writing them on under the substitution would bind a `LIST`
/// parameter where the client sent a `MAP`.
#[derive(Clone, Default)]
pub(crate) struct DecodableSchema(Arc<OnceLock<SchemaRef>>);

impl DecodableSchema {
    /// Returns `message` with its schema message replaced by the form a decoder can build
    /// against, recording what it declared. Any other message is passed through untouched.
    pub(crate) fn repair(&self, mut message: FlightData) -> FlightData {
        if let Some((declared, header)) =
            map_entries::decodable_schema_message(&message.data_header)
        {
            drop(self.0.set(declared));
            message.data_header = header.into();
        }
        message
    }

    /// What the client declared: the schema [`Self::repair`] substituted away, or `decoded` when
    /// no substitution was needed and the decoder's own report is the client's.
    pub(crate) fn declared(&self, decoded: SchemaRef) -> SchemaRef {
        self.0.get().map_or(decoded, Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayData, ArrayRef, MapArray, RecordBatch, StringArray, StructArray,
    };
    use arrow::buffer::{Buffer, NullBuffer};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow_flight::FlightData;
    use arrow_tools::map_entries::MapEntriesNormalizer;

    use super::DecodableSchema;

    /// A one-column batch whose `MAP` declares `entries` nullable, built straight from
    /// `ArrayData` the way a client's own encoder does — `MapArray::try_new` refuses this shape,
    /// which is why it only ever arrives over the wire.
    fn map_batch(entry_nulls: Option<NullBuffer>) -> RecordBatch {
        let entry_fields: Fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into();
        let data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entry_fields.clone()),
                true,
            )),
            false,
        );
        let entries = StructArray::try_new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(vec!["k0", "k1"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("v0"), Some("v1")])) as ArrayRef,
            ],
            entry_nulls,
        )
        .expect("entries struct");
        let builder = ArrayData::builder(data_type.clone())
            .len(2)
            .add_buffer(Buffer::from_slice_ref([0_i32, 1, 2]))
            .add_child_data(entries.to_data());
        // SAFETY: the offsets, buffers and child data are well formed. Only the `entries`
        // nullability declaration is what `ArrayData::validate` rejects, and reproducing it is
        // the point of the fixture.
        let data = unsafe { builder.build_unchecked() };

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("m", data_type, true)])),
            vec![Arc::new(MapArray::from(data)) as ArrayRef],
        )
        .expect("map batch")
    }

    /// Drives the seam the way a `DoPut` does: the client's messages go past `repair`, the
    /// schema message it substituted is what the decode is built against, and the batches are
    /// put back under what the client declared.
    fn decode_through_repair(batch: &RecordBatch) -> arrow_tools::map_entries::Result<RecordBatch> {
        let messages: Vec<FlightData> = arrow_flight::utils::batches_to_flight_data(
            batch.schema().as_ref(),
            vec![batch.clone()],
        )
        .expect("encoding the batch");

        let decodable = DecodableSchema::default();
        let repaired: Vec<FlightData> = messages
            .into_iter()
            .map(|message| decodable.repair(message))
            .collect();

        let [schema_message, data_message] = repaired.as_slice() else {
            panic!("a client sends its schema and then its batch");
        };
        let decoded_schema = Arc::new(
            arrow_ipc::convert::try_schema_from_flatbuffer_bytes(&schema_message.data_header)
                .expect("the repaired schema message is readable"),
        );
        let decoded = arrow_flight::utils::flight_data_to_arrow_batch(
            data_message,
            decoded_schema,
            &HashMap::new(),
        )
        .expect("the repaired declaration decodes");

        MapEntriesNormalizer::for_schema(&decodable.declared(decoded.schema())).normalize(decoded)
    }

    /// Regression test for #13495 at the one decode point that reads its schema off the stream:
    /// a client declaring `entries` nullable is brought into line, rather than refused by a
    /// decoder that names neither the column nor the rule.
    #[test]
    fn a_nullable_entries_schema_message_is_repaired_so_the_batch_decodes() {
        let normalized =
            decode_through_repair(&map_batch(None)).expect("the declaration is fixable");

        let schema = normalized.schema();
        let DataType::Map(entries, _) = schema.field(0).data_type() else {
            panic!("the column must be put back as the MAP the client declared");
        };
        assert!(
            !entries.is_nullable(),
            "the batch handed on must carry the corrected declaration"
        );
        assert_eq!(normalized.num_rows(), 2, "every row survives the repair");
    }

    /// The other half of the contract: the substitution is what lets the decode complete, not a
    /// licence to accept what the map layout cannot represent. Entry nulls are still refused,
    /// and by a name the client can act on.
    #[test]
    fn a_repaired_stream_still_refuses_entries_that_carry_nulls() {
        let err = decode_through_repair(&map_batch(Some(NullBuffer::from(vec![true, false]))))
            .expect_err("entries carrying nulls have no map to be put back as");

        assert!(
            err.to_string().contains("'m'"),
            "the refusal must name the column: {err}"
        );
    }
}
