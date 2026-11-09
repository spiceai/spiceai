/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A Flight stream from a producer that numbers its dictionaries differently from `arrow-rs`.
//!
//! The Arrow format leaves dictionary ids to the producer: they only have to agree between the
//! schema message and the dictionary batches that follow it. `arrow-rs` numbers them from 0 in
//! the order it encodes the fields, so a stream it writes cannot show whether a reader keeps the
//! producer's ids. This one is written by `arrow-rs` and then renumbered in place the way another
//! producer might have numbered it.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, DictionaryArray, Int32Array, MapArray, RecordBatch, StringArray,
    StructArray,
};
use arrow::buffer::Buffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter};
use arrow_flight::FlightData;
use bytes::Bytes;

/// Every dictionary column indexes its two values in reverse, so a column decoded against
/// another column's dictionary yields that column's values rather than an error.
const KEYS: [i32; 2] = [1, 0];

const A: [&str; 2] = ["north", "south"];
const MAP_VALUES: [&str; 2] = ["red", "blue"];
const C: [&str; 2] = ["up", "down"];

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

fn dictionary(values: [&str; 2]) -> ArrayRef {
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(KEYS.to_vec()),
            Arc::new(StringArray::from(values.to_vec())),
        )
        .expect("dictionary column"),
    )
}

fn dictionary_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

fn entry_fields() -> Fields {
    vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", dictionary_type(), true),
    ]
    .into()
}

fn map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(entry_fields()),
            true,
        )),
        false,
    )
}

/// Three dictionary columns sharing one value type — `a`, the values of map `m`, and `c` — with
/// `m` declaring its `entries` nullable, the shape the Arrow map layout forbids.
fn batch(a: [&str; 2], map_values: [&str; 2], c: [&str; 2]) -> RecordBatch {
    let entries = StructArray::try_new(
        entry_fields(),
        vec![
            Arc::new(StringArray::from(vec!["k0", "k1"])) as ArrayRef,
            dictionary(map_values),
        ],
        None,
    )
    .expect("entries struct");
    let builder = ArrayData::builder(map_type())
        .len(2)
        .add_buffer(Buffer::from_slice_ref([0_i32, 1, 2]))
        .add_child_data(entries.to_data());
    // SAFETY: the offsets and child data are well formed; the `entries` nullability declaration
    // is the one thing validation rejects, and it is what a non-conforming server sends.
    let map = MapArray::from(unsafe { builder.build_unchecked() });

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", dictionary_type(), true),
            Field::new("m", map_type(), true),
            Field::new("c", dictionary_type(), true),
        ])),
        vec![dictionary(a), Arc::new(map) as ArrayRef, dictionary(c)],
    )
    .expect("batch")
}

/// The schema the producer declares.
pub(crate) fn schema() -> Arc<Schema> {
    batch(A, MAP_VALUES, C).schema()
}

/// The values every column of `batch` decodes to: `a`, the map's values, then `c`.
pub(crate) fn values(batch: &RecordBatch) -> Vec<Vec<String>> {
    let strings = |array: &ArrayRef| {
        cast(array, &DataType::Utf8)
            .expect("cast to Utf8")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8")
            .iter()
            .map(|value| value.expect("no nulls").to_string())
            .collect::<Vec<_>>()
    };
    let map = batch
        .column(1)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("a Map column");
    vec![
        strings(batch.column(0)),
        strings(map.values()),
        strings(batch.column(2)),
    ]
}

/// What [`values`] must return for the batch the producer sends.
pub(crate) fn expected_values() -> Vec<Vec<String>> {
    let reversed = |values: [&str; 2]| vec![values[1].to_string(), values[0].to_string()];
    vec![reversed(A), reversed(MAP_VALUES), reversed(C)]
}

/// One IPC message: its flatbuffer header and the body that follows it.
struct Message {
    header: Vec<u8>,
    body: Vec<u8>,
}

fn split(bytes: &[u8]) -> Vec<Message> {
    let mut messages = Vec::new();
    let mut at = 0;
    loop {
        assert_eq!(bytes[at..at + 4], CONTINUATION_MARKER, "framed message");
        let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("length"));
        let len = usize::try_from(len).expect("length fits");
        at += 8;
        if len == 0 {
            return messages;
        }
        let header = bytes[at..at + len].to_vec();
        at += len;
        let body_len = arrow::ipc::root_as_message(&header)
            .expect("a message")
            .bodyLength();
        let body_len = usize::try_from(body_len).expect("body length");
        messages.push(Message {
            header,
            body: bytes[at..at + body_len].to_vec(),
        });
        at += body_len;
    }
}

/// Where the stored `id` of every dictionary encoding in `header`'s schema lives, with the id
/// it holds.
fn schema_id_slots(header: &[u8]) -> Vec<(usize, i64)> {
    fn visit(field: arrow::ipc::Field<'_>, slots: &mut Vec<(usize, i64)>) {
        if let Some(dictionary) = field.dictionary() {
            let slot = dictionary
                ._tab
                .vtable()
                .get(arrow::ipc::DictionaryEncoding::VT_ID);
            assert_ne!(slot, 0, "the fixture stores every schema id explicitly");
            slots.push((dictionary._tab.loc() + usize::from(slot), dictionary.id()));
        }
        for child in field.children().into_iter().flatten() {
            visit(child, slots);
        }
    }
    let message = arrow::ipc::root_as_message(header).expect("a message");
    let schema = message.header_as_schema().expect("a schema message");
    let mut slots = Vec::new();
    for field in schema.fields().into_iter().flatten() {
        visit(field, &mut slots);
    }
    slots
}

/// The `FlightData` of a producer that numbers `a`, the map's values and `c` as 2, 0 and 1 —
/// `a` and `c` in the reverse of the order `arrow-rs` numbers them in. Every id is one `arrow-rs`
/// would also use, so a reader that renumbers them finds a dictionary for every column: just
/// the wrong one, and without an error.
pub(crate) fn reordered_ids_flight_data() -> Vec<FlightData> {
    const FIELD_IDS: [i64; 3] = [2, 0, 1];

    // `arrow-rs` sends the dictionary of the column it numbered `k` as batch `k`. For that batch
    // to be the dictionary of the field this producer numbers `k`, each column carries the
    // dictionary of the field that takes over its number.
    let encoded = batch(MAP_VALUES, C, A);
    let schema = encoded.schema();
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("stream writer");
    writer.write(&encoded).expect("write the batch");
    writer.finish().expect("finish the stream");
    drop(writer);
    let mut messages = split(&bytes);

    // Advancing the tracker once makes every id in this schema message nonzero, so each is
    // stored explicitly and can be rewritten in place; it is otherwise the message `arrow-rs`
    // wrote, with `arrow-rs`'s id `k` written as `k + 1`.
    let mut tracker = DictionaryTracker::new(false);
    tracker.next_dict_id();
    let mut header = IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(&schema, &mut tracker, &IpcWriteOptions::default())
        .ipc_message;
    let slots = schema_id_slots(&header);
    assert_eq!(slots.len(), FIELD_IDS.len(), "one id per dictionary field");
    for (at, written) in slots {
        let k = usize::try_from(written - 1).expect("an arrow-rs id");
        header[at..at + 8].copy_from_slice(&FIELD_IDS[k].to_le_bytes());
    }
    messages[0].header = header;

    messages
        .into_iter()
        .map(|message| FlightData {
            data_header: Bytes::from(message.header),
            data_body: Bytes::from(message.body),
            ..FlightData::default()
        })
        .collect()
}
