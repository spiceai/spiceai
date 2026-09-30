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

//! Brings foreign Arrow data in line with the Arrow `Map` layout rules.
//!
//! The Arrow specification requires a map's `entries` field to be non-nullable, and
//! `MapArray::try_new` enforces both halves of that: it rejects an `entries` field
//! declared nullable *and* an entries array that carries nulls. A producer that builds its
//! arrays itself is held to neither — `MapArray::from(ArrayData)` performs no check — so it
//! hands us a column that arrives intact and then fails in whichever kernel first rebuilds
//! it. Every such failure reports the same message, `MapArray entries cannot contain nulls`,
//! whether or not a null is involved.
//!
//! [`MapEntriesNormalizer`] relabels the declaration (metadata only — no buffer is touched)
//! and refuses the one shape that cannot be relabelled without inventing an answer: entries
//! that actually contain nulls.
//!
//! A decode is the one arrival that cannot be repaired after the fact: `ArrayData` validation
//! runs inside it, so a stream carrying the forbidden declaration yields an error instead of the
//! batches whose buffers are all well formed, and that error names neither the column nor which
//! rule it broke. [`decodable_schema`] answers that by handing the decoder the `List` a map is
//! laid out as, which carries no map rules at all; the batches come back fully validated, and
//! [`MapEntriesNormalizer::normalize`] puts the map label back — or refuses, by name, the one
//! shape that has no map to go back to. [`read_ipc_stream`] applies both to a stream of bytes,
//! which is what keeps data written under the older declaration readable.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array, ArrayData, ArrayRef, RecordBatch, make_array};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message};
use snafu::prelude::*;

use crate::type_rewrite::{MapAsList, MapEntriesNonNullable, apply_rules, target_child_types};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "column '{column}' holds a MAP whose entries contain nulls, which the Arrow map layout has no way to represent"
    ))]
    MapEntriesContainNulls { column: String },

    #[snafu(display("column '{column}' could not be rebuilt: {source}"))]
    UnableToNormalizeColumn {
        column: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("the normalized columns do not fit schema '{schema}': {source}"))]
    UnableToRebuildRecordBatch {
        schema: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("the Arrow IPC stream could not be decoded: {source}"))]
    UndecodableStream { source: arrow::error::ArrowError },
}

/// What the batches of one Arrow stream need, resolved once from the stream's schema.
///
/// An Arrow IPC stream carries a single schema, so whether a map declaration has to be
/// relabelled — and what it becomes — is fixed for every batch. Deciding it per batch would
/// rebuild the same schema over and over and hand each batch a distinct `SchemaRef`.
pub struct MapEntriesNormalizer {
    /// The stream's own schema, as its producer declared it.
    declared: SchemaRef,
    /// The schema every batch is relabelled to, shared by all of them. `None` when the
    /// stream's own declarations already conform.
    target: Option<SchemaRef>,
    /// The schema a decoder has to build these batches against, with every `Map` labelled as
    /// the `List` it is laid out as. `None` when the stream's own declarations already conform
    /// and the decoder can be given them as they stand.
    decodable: Option<SchemaRef>,
    /// Whether any field holds a `Map` at all. When none does, no batch can carry entry
    /// nulls, so no column is ever inspected.
    holds_map: bool,
}

impl MapEntriesNormalizer {
    #[must_use]
    pub fn for_schema(schema: &SchemaRef) -> Self {
        let holds_map = schema
            .fields()
            .iter()
            .any(|field| contains(field.data_type(), &is_map));

        let target = holds_map
            .then(|| conforming_schema(Arc::clone(schema)))
            .filter(|target| !Arc::ptr_eq(target, schema));

        // Only a schema that needs relabelling needs the substitution: one that already conforms
        // decodes as it stands, and giving its maps a list label would put every one of them
        // through a rebuild for nothing.
        let decodable = target.is_some().then(|| decodable_schema(schema));

        Self {
            declared: Arc::clone(schema),
            target,
            decodable,
            holds_map,
        }
    }

    /// The schema a decoder must be handed to build this stream's batches at all.
    ///
    /// It is [`decodable_schema`] of the stream's own, resolved once. A batch decoded under it
    /// is what [`Self::normalize`] expects; a batch that arrived already built under the
    /// stream's own declarations is equally acceptable there, since the two describe the same
    /// buffers in the same order.
    #[must_use]
    pub fn decode_schema(&self) -> &SchemaRef {
        self.decodable.as_ref().unwrap_or(&self.declared)
    }

    /// The schema every batch this normalizer returns carries: the relabelled one when the
    /// stream's declarations needed correcting, otherwise the stream's own.
    ///
    /// A decode point that hands its batches on under a separately declared schema — an
    /// `ExecutionPlan`'s output schema, a `RecordBatchStreamAdapter`'s — must declare this one,
    /// or it describes its batches with a type they no longer carry.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        self.target.as_ref().unwrap_or(&self.declared)
    }

    /// Returns `batch` with every `Map` column — nested ones included — declaring its
    /// `entries` field non-nullable, as the Arrow specification requires.
    ///
    /// `batch` may carry the stream's own declarations or the [`Self::decode_schema`] form a
    /// decoder had to be given; both describe the same buffers in the same order, and which one
    /// arrived is read off the column rather than assumed. Nullability lives in the type rather
    /// than in any buffer and a map and a list share one layout, so this only relabels: the
    /// offsets, validity and child arrays are carried over by reference, and a column that
    /// already carries its target type is passed through untouched.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MapEntriesContainNulls`] when an entries array carries nulls. That is
    /// the one shape relabelling cannot fix, and it is not recoverable by guessing: Arrow
    /// gives a null entry no meaning, so treating it as a null map and treating it as a pair
    /// to drop are both inventions, and each yields different rows.
    pub fn normalize(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if !self.holds_map {
            return Ok(batch);
        }

        let Some(target) = self.target.as_ref() else {
            // Nothing to relabel, but entry nulls fail downstream whatever the declaration
            // says, so they are still refused here where the column can be named.
            self.refuse_entry_nulls(&batch)?;
            return Ok(batch);
        };

        let columns = self
            .declared
            .fields()
            .iter()
            .zip(batch.columns())
            .zip(target.fields())
            .map(|((field, column), target_field)| {
                if !contains(field.data_type(), &is_map) {
                    return Ok(Arc::clone(column));
                }
                let data = column.to_data();
                refuse_entry_nulls_under(field.data_type(), &data, field.name())?;
                if data.data_type() == target_field.data_type() {
                    return Ok(Arc::clone(column));
                }
                let relabelled = rebuild_under(data, target_field.data_type()).context(
                    UnableToNormalizeColumnSnafu {
                        column: field.name(),
                    },
                )?;
                Ok(make_array(relabelled))
            })
            .collect::<Result<Vec<ArrayRef>>>()?;

        RecordBatch::try_new(Arc::clone(target), columns).context(UnableToRebuildRecordBatchSnafu {
            schema: target.to_string(),
        })
    }

    fn refuse_entry_nulls(&self, batch: &RecordBatch) -> Result<()> {
        for (field, column) in self.declared.fields().iter().zip(batch.columns()) {
            if contains(field.data_type(), &is_map) {
                refuse_entry_nulls_under(field.data_type(), &column.to_data(), field.name())?;
            }
        }
        Ok(())
    }
}

/// The form of `schema` an Arrow decoder can build batches against, with every `Map` — nested
/// ones included — labelled as the `List` it is laid out as (see [`MapAsList`]).
///
/// A schema whose map declarations already conform is handed back untouched, so a caller can
/// test whether anything changed with [`Arc::ptr_eq`] and pay nothing when nothing did. The
/// batches a decoder returns under this schema are brought back to the map they describe by
/// [`MapEntriesNormalizer::normalize`], which is where the substitution is undone and where the
/// one shape that cannot be undone is refused.
#[must_use]
pub fn decodable_schema(schema: &SchemaRef) -> SchemaRef {
    if schema
        .fields()
        .iter()
        .any(|field| contains(field.data_type(), &declares_nullable_entries))
    {
        Arc::new(apply_rules(schema, &[&MapAsList]))
    } else {
        Arc::clone(schema)
    }
}

/// The Arrow-conforming form of `schema`: every `Map` field, nested ones included, declares
/// its `entries` non-nullable.
///
/// A schema that already conforms is handed back untouched, so a caller can test whether
/// anything changed with [`Arc::ptr_eq`] and pay nothing when nothing did.
#[must_use]
pub fn conforming_schema(schema: SchemaRef) -> SchemaRef {
    if schema
        .fields()
        .iter()
        .any(|field| contains(field.data_type(), &declares_nullable_entries))
    {
        Arc::new(apply_rules(&schema, &[&MapEntriesNonNullable]))
    } else {
        schema
    }
}

/// The four bytes every message of an Arrow IPC stream begins with. `arrow-ipc` keeps its own
/// copy private.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// Decodes an Arrow IPC stream, repairing a `Map` declaration the Arrow layout forbids.
///
/// A stream declaring its map `entries` nullable can be written but not read back: the decode
/// builds each column against the schema the stream carries, and `ArrayData` validation rejects
/// that declaration — so the batches are refused over the one part of them that holds no data,
/// while every buffer in the stream is well formed. Bytes already at rest were written before
/// the declaration was corrected, so the repair belongs on the way in: the stream's schema
/// message is replaced by the [`decodable_schema`] form, under which the decode completes with
/// every buffer validated, and the batch messages — which carry no declaration of their own —
/// are decoded untouched against it. Each batch is then brought back to the map it describes.
///
/// The returned batches carry the conforming schema. A stream that already conforms is decoded
/// directly and pays nothing for this.
///
/// # Errors
///
/// Returns [`Error::UndecodableStream`] for a stream that does not decode — including one too
/// short to hold the schema message it claims — and [`Error::MapEntriesContainNulls`] for the
/// one map shape no relabelling can repair.
pub fn read_ipc_stream(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).context(UndecodableStreamSnafu)?;
    let declared = reader.schema();
    let normalizer = MapEntriesNormalizer::for_schema(&declared);
    let decodable = normalizer.decode_schema();
    if Arc::ptr_eq(decodable, &declared) {
        return reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(UndecodableStreamSnafu);
    }
    drop(reader);

    let repaired = with_schema_message(bytes, decodable).context(UndecodableStreamSnafu)?;
    StreamReader::try_new(Cursor::new(repaired), None)
        .and_then(Iterator::collect::<std::result::Result<Vec<_>, _>>)
        .context(UndecodableStreamSnafu)?
        .into_iter()
        .map(|batch| normalizer.normalize(batch))
        .collect()
}

/// The IPC schema-message header a decoder has to be handed in place of `data_header`, together
/// with the schema that header declared.
///
/// `None` when `data_header` is not a readable schema message, or when what it declares decodes
/// as it stands — in both cases the message is passed on untouched.
///
/// This is the [`decodable_schema`] repair for a decoder that reads its schema off the stream
/// rather than taking one from its caller, which is how `arrow_flight`'s `FlightDataDecoder`
/// works: the substitution has to reach it as bytes, before it builds anything. The caller keeps
/// the returned schema, because that — not the substituted one the decoder will report — is what
/// [`MapEntriesNormalizer::for_schema`] has to be built from for the batches to be put back.
#[must_use]
pub fn decodable_schema_message(data_header: &[u8]) -> Option<(SchemaRef, Vec<u8>)> {
    let declared: SchemaRef = Arc::new(try_schema_from_flatbuffer_bytes(data_header).ok()?);
    let decodable = decodable_schema(&declared);
    if Arc::ptr_eq(&decodable, &declared) {
        return None;
    }
    let header = schema_message(&decodable);
    Some((declared, header))
}

/// The IPC schema-message header a decoder that hands its batches straight on has to be given
/// in place of `data_header`: the [`conforming_schema`] form, which needs nothing done to the
/// batches afterwards.
///
/// `None` when `data_header` is not a readable schema message, or when what it declares already
/// conforms — in both cases the message is passed on untouched.
///
/// This is the repair for a seam with no normalizer behind it, which is what separates it from
/// [`decodable_schema_message`]: there, the list substitution is undone on the way past and the
/// one shape that cannot be undone is refused by name; here, the batches the decoder builds are
/// the batches the caller gets, so they have to come out of the decode already conforming. The
/// cost is that a producer whose entries really do hold nulls is refused by `ArrayData`
/// validation rather than by name — the decode is the only thing left to refuse it.
#[must_use]
pub fn conforming_schema_message(data_header: &[u8]) -> Option<Vec<u8>> {
    let declared: SchemaRef = Arc::new(try_schema_from_flatbuffer_bytes(data_header).ok()?);
    let conforming = conforming_schema(Arc::clone(&declared));
    if Arc::ptr_eq(&conforming, &declared) {
        return None;
    }
    Some(schema_message(&conforming))
}

/// The flatbuffer an IPC schema message carries for `schema`, which is what a Flight message
/// holds in its `data_header`.
fn schema_message(schema: &Schema) -> Vec<u8> {
    IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
        )
        .ipc_message
}

/// `bytes` with its leading schema message replaced by one declaring `schema`.
fn with_schema_message(bytes: &[u8], schema: &Schema) -> std::result::Result<Vec<u8>, ArrowError> {
    let rest = bytes.get(schema_message_len(bytes)?..).ok_or_else(|| {
        ArrowError::ParseError(
            "Arrow IPC stream ends inside the schema message it declares".to_string(),
        )
    })?;

    let options = IpcWriteOptions::default();
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut DictionaryTracker::new(false),
        &options,
    );

    // The message is the flatbuffer plus its continuation marker and length prefix.
    let mut repaired =
        Vec::with_capacity(encoded.ipc_message.len() + CONTINUATION_MARKER.len() + 4 + rest.len());
    write_message(&mut repaired, encoded, &options)?;
    repaired.extend_from_slice(rest);
    Ok(repaired)
}

/// How many bytes the stream's leading schema message occupies, which — a schema message
/// having no body — is where the next message begins.
fn schema_message_len(bytes: &[u8]) -> std::result::Result<usize, ArrowError> {
    // The continuation marker is absent only in the pre-0.15 framing, which writes the
    // metadata length first and nothing else.
    let prefix = if bytes.starts_with(&CONTINUATION_MARKER) {
        CONTINUATION_MARKER.len()
    } else {
        0
    };
    let declared: [u8; 4] = bytes
        .get(prefix..prefix + 4)
        .and_then(|length| length.try_into().ok())
        .ok_or_else(|| {
            ArrowError::ParseError(
                "Arrow IPC stream is too short to carry a schema message".to_string(),
            )
        })?;
    let metadata_len = usize::try_from(u32::from_le_bytes(declared)).map_err(|_| {
        ArrowError::ParseError("Arrow IPC schema message length does not fit in memory".to_string())
    })?;

    Ok(prefix + 4 + metadata_len)
}

/// Normalizes the batches of a decode point that learns its schema only from the batches
/// themselves.
///
/// An Arrow Flight `DoGet` stream hands out decoded `RecordBatch`es without surfacing the
/// schema message that preceded them, so the schema is whatever the first batch carries. It is
/// still one schema per stream, so the resolved [`MapEntriesNormalizer`] is kept and reused for
/// as long as the batches keep arriving under the same `SchemaRef`; a stream that does send a
/// second schema message resolves a second normalizer rather than relabelling to the first one's
/// target.
#[derive(Default)]
pub struct StreamNormalizer {
    resolved: Option<MapEntriesNormalizer>,
}

impl StreamNormalizer {
    #[must_use]
    pub fn new() -> Self {
        Self { resolved: None }
    }

    /// Returns `batch` with every `Map` column declaring its `entries` field non-nullable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MapEntriesContainNulls`] when an entries array carries nulls — see
    /// [`MapEntriesNormalizer::normalize`], which this delegates to.
    pub fn normalize(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let normalizer = self
            .resolved
            .take()
            .filter(|resolved| Arc::ptr_eq(&resolved.declared, &schema))
            .unwrap_or_else(|| MapEntriesNormalizer::for_schema(&schema));

        let outcome = normalizer.normalize(batch);
        self.resolved = Some(normalizer);
        outcome
    }
}

fn is_map(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Map(_, _))
}

fn declares_nullable_entries(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Map(entries, _) if entries.is_nullable())
}

/// Returns `true` when `predicate` holds for `data_type` or for any type nested inside it.
///
/// Allocation-free and short-circuiting. Asking [`crate::type_rewrite::rewrite_data_type`] for
/// a rewritten copy and comparing it would answer the same question, but it rebuilds the whole
/// type tree — every nested `Field`, name included — to produce one bit.
fn contains(data_type: &DataType, predicate: &impl Fn(&DataType) -> bool) -> bool {
    if predicate(data_type) {
        return true;
    }
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => contains(field.data_type(), predicate),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains(field.data_type(), predicate)),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains(field.data_type(), predicate)),
        DataType::Dictionary(_, value_type) => contains(value_type, predicate),
        _ => false,
    }
}

/// Walks `data` against `declared` — the type its producer meant — and fails on the first `Map`
/// whose entries array carries nulls.
///
/// The walk is driven by the declared type rather than by the array's own, because the two can
/// disagree on exactly the point at issue: a batch decoded under [`decodable_schema`] carries a
/// `List` wherever the producer declared a `Map`, and the two are indistinguishable from the
/// array alone. A genuine list's child is free to hold nulls, so reading the label off the array
/// would either miss the map that broke the rule or refuse the list that did not. Both describe
/// the same buffers in the same order, so the declared type is a sound guide to either.
fn refuse_entry_nulls_under(declared: &DataType, data: &ArrayData, column: &str) -> Result<()> {
    let children = data.child_data();
    match declared {
        DataType::Map(entries, _) => {
            let Some(decoded) = children.first() else {
                return Ok(());
            };
            ensure!(
                decoded.null_count() == 0,
                MapEntriesContainNullsSnafu { column }
            );
            refuse_entry_nulls_under(entries.data_type(), decoded, column)
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => match children.first() {
            Some(child) => refuse_entry_nulls_under(field.data_type(), child, column),
            None => Ok(()),
        },
        // `ArrayData` holds the run ends first and the values second.
        DataType::RunEndEncoded(_, values) => match children.get(1) {
            Some(child) => refuse_entry_nulls_under(values.data_type(), child, column),
            None => Ok(()),
        },
        DataType::Struct(fields) => fields.iter().zip(children).try_for_each(|(field, child)| {
            refuse_entry_nulls_under(field.data_type(), child, column)
        }),
        DataType::Union(fields, _) => {
            fields
                .iter()
                .zip(children)
                .try_for_each(|((_, field), child)| {
                    refuse_entry_nulls_under(field.data_type(), child, column)
                })
        }
        DataType::Dictionary(_, values) => match children.first() {
            Some(child) => refuse_entry_nulls_under(values, child, column),
            None => Ok(()),
        },
        _ => Ok(()),
    }
}

/// Rebuilds `data` under `target`, relabelling each level that differs and carrying every buffer
/// across by reference.
///
/// Both types are this module's own rewrites of one declared schema — the substitution
/// [`decodable_schema`] hands the decoder, and the conforming form [`conforming_schema`] produces
/// — so they describe the same buffers by construction, and the general guard
/// [`crate::type_rewrite::relabel_array_data`] applies against a caller-supplied target has
/// nothing to catch here. It would in fact refuse this one: putting a map's label back on the
/// list it was decoded as is a change of type constructor, which that guard exists to reject.
/// `build` still validates every level, so a target the buffers do not support is refused rather
/// than reinterpreted.
fn rebuild_under(data: ArrayData, target: &DataType) -> std::result::Result<ArrayData, ArrowError> {
    if data.data_type() == target {
        return Ok(data);
    }

    let targets = target_child_types(target);
    // A child count that disagrees with the target is a layout disagreement; `build` refuses it
    // below rather than leaving a rebuilt parent over children still carrying the old type.
    let children_change = targets.len() == data.child_data().len()
        && data
            .child_data()
            .iter()
            .zip(&targets)
            .any(|(child, target)| child.data_type() != *target);

    if !children_change {
        return data.into_builder().data_type(target.clone()).build();
    }

    let children = data
        .child_data()
        .iter()
        .zip(&targets)
        .map(|(child, target)| rebuild_under(child.clone(), target))
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;

    data.into_builder()
        .data_type(target.clone())
        .child_data(children)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
    };
    use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Fields, Int32Type, Schema};
    use arrow::error::ArrowError;

    /// Every batch of a stream is normalized against that stream's schema; a test holds one
    /// batch, so its own schema is the stream's.
    fn normalize(batch: RecordBatch) -> Result<RecordBatch> {
        MapEntriesNormalizer::for_schema(&batch.schema()).normalize(batch)
    }

    fn entry_fields() -> Fields {
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into()
    }

    fn map_type(entries_nullable: bool) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entry_fields()),
                entries_nullable,
            )),
            false,
        )
    }

    /// Builds a `MapArray` bypassing `MapArray::try_new`, the way the IPC reader does:
    /// `From<ArrayData>` performs neither of the two `entries` checks, which is why a
    /// non-conforming map reaches us at all.
    fn map_from_parts(
        entries_nullable: bool,
        entry_nulls: Option<NullBuffer>,
        offsets: &[i32],
        keys: Vec<&str>,
        values: Vec<Option<&str>>,
    ) -> MapArray {
        let entries = StructArray::try_new(
            entry_fields(),
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(StringArray::from(values)) as ArrayRef,
            ],
            entry_nulls,
        )
        .expect("entries struct");

        let builder = ArrayData::builder(map_type(entries_nullable))
            .len(offsets.len() - 1)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_child_data(entries.to_data());
        // SAFETY: the offsets, buffers and child data are all well formed. The only
        // thing `ArrayData::validate` objects to is the `entries` nullability
        // declaration, which is exactly what this fixture exists to reproduce — the
        // IPC reader builds such a map without either check.
        let data = unsafe { builder.build_unchecked() };

        MapArray::from(data)
    }

    fn batch_of(column: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "col_map",
                column.data_type().clone(),
                true,
            )])),
            vec![column],
        )
        .expect("batch")
    }

    /// Rebuilds `map` through the public `MapArray` constructor, the one every kernel that
    /// touches a map column goes through, keeping the declared `entries` field.
    fn rebuild_through_public_constructor(map: &MapArray) -> Result<MapArray, ArrowError> {
        let (field, offsets, entries, nulls, ordered) = map.clone().into_parts();
        MapArray::try_new(field, offsets, entries, nulls, ordered)
    }

    /// Writes `batches` as an Arrow IPC stream under `schema`, the way a producer that
    /// declared its map entries nullable already wrote the bytes now at rest.
    fn ipc_stream(schema: &SchemaRef, batches: &[RecordBatch]) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, schema)
            .expect("ipc stream writer");
        for batch in batches {
            writer.write(batch).expect("write a batch");
        }
        writer.finish().expect("finish the stream");
        bytes
    }

    /// A stream written under the forbidden declaration is refused by the decode itself, so
    /// nothing downstream ever gets to relabel it: `ArrayData` validation runs inside the IPC
    /// reader. The rows are still all there, which is why the repair is worth making.
    #[test]
    fn an_ipc_stream_declaring_nullable_entries_is_read_back_conforming() {
        let map = map_from_parts(
            true,
            None,
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), None],
        );
        let batch = batch_of(Arc::new(map) as ArrayRef);
        let bytes = ipc_stream(&batch.schema(), std::slice::from_ref(&batch));

        let err = StreamReader::try_new(Cursor::new(&bytes), None)
            .expect("the schema message itself is readable")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect_err("arrow refuses the declaration while decoding");
        assert!(
            err.to_string()
                .contains("The nullable should be set to false for the map entries field"),
            "unexpected error: {err}"
        );

        let read_back = read_ipc_stream(&bytes).expect("the repaired stream decodes");
        assert_eq!(read_back.len(), 1);
        let repaired = &read_back[0];
        match repaired.schema().field(0).data_type() {
            DataType::Map(entries, _) => assert!(
                !entries.is_nullable(),
                "entries must be read back relabelled non-nullable"
            ),
            other => panic!("expected a Map, got {other}"),
        }
        let map = repaired
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map after");
        rebuild_through_public_constructor(map)
            .expect("a spec-conforming map rebuilds through the public constructor");
        assert_eq!(map.offsets(), &OffsetBuffer::new(vec![0, 1, 2].into()));
        assert_eq!(
            map.entries()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("keys")
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("k0"), Some("k1")],
            "every key survives the repair"
        );
    }

    /// Replacing the schema message shifts every message after it, so the repair is only
    /// sound if the rest of the stream is position-independent. A dictionary-encoded column
    /// and a second batch are what test that: the dictionary message is decoded against the
    /// replacement schema, and the second batch refers back to it.
    #[test]
    fn repairing_the_schema_message_leaves_the_rest_of_the_stream_readable() {
        let map = map_from_parts(
            true,
            None,
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), None],
        );
        let labels: DictionaryArray<Int32Type> = ["north", "south"].into_iter().collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_map", map.data_type().clone(), true),
            Field::new("label", labels.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(map) as ArrayRef, Arc::new(labels) as ArrayRef],
        )
        .expect("batch");
        let bytes = ipc_stream(&schema, &[batch.clone(), batch]);

        let read_back = read_ipc_stream(&bytes).expect("the repaired stream decodes");
        assert_eq!(read_back.len(), 2, "both batches are still there");
        for batch in &read_back {
            assert_eq!(batch.num_rows(), 2);
            let labels = batch
                .column(1)
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("dictionary column");
            assert_eq!(
                labels
                    .downcast_dict::<StringArray>()
                    .expect("string values")
                    .into_iter()
                    .collect::<Vec<_>>(),
                vec![Some("north"), Some("south")],
                "the dictionary message decodes against the replacement schema"
            );
        }
    }

    /// Repairing the declaration alone is not enough, and the way it falls short is the reason
    /// the decoder is handed a list rather than a conforming map: under a conforming
    /// declaration the same stream is refused a second time, by the *other* map rule, with a
    /// message that names no column and mentions no map the caller declared. Both refusals are
    /// Arrow's, and neither tells a client which of its columns to fix.
    #[test]
    fn an_ipc_stream_whose_entries_carry_nulls_is_refused_by_column_name() {
        let map = map_from_parts(
            true,
            Some(NullBuffer::from(vec![true, false])),
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), Some("v1")],
        );
        let batch = batch_of(Arc::new(map) as ArrayRef);
        let bytes = ipc_stream(&batch.schema(), std::slice::from_ref(&batch));

        let declared = StreamReader::try_new(Cursor::new(&bytes), None)
            .expect("the schema message itself is readable")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect_err("arrow refuses the declaration while decoding");
        assert!(
            declared
                .to_string()
                .contains("The nullable should be set to false for the map entries field"),
            "unexpected error: {declared}"
        );

        let conformed = with_schema_message(&bytes, &conforming_schema(batch.schema()))
            .expect("the schema message is replaceable");
        let conformed = StreamReader::try_new(Cursor::new(conformed), None)
            .expect("the replacement schema message is readable")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect_err("arrow refuses the entry nulls while decoding");
        assert!(
            conformed.to_string().contains("contains nulls not present"),
            "unexpected error: {conformed}"
        );
        assert!(
            !conformed.to_string().contains("col_map"),
            "premise of this test: Arrow's own refusal names no column: {conformed}"
        );

        let err = read_ipc_stream(&bytes).expect_err("entry nulls have no map to relabel to");
        assert!(
            matches!(err, Error::MapEntriesContainNulls { .. }),
            "expected the entry-null refusal, got {err:?}"
        );
        assert!(
            err.to_string().contains("'col_map'"),
            "the refusal must name the column: {err}"
        );
    }

    /// The repair a decoder that reads its own schema off the stream needs, which is how
    /// `arrow_flight`'s `FlightDataDecoder` works: the substitution has to reach it as the
    /// schema message's bytes. What the producer declared is handed back with it, because the
    /// substitution is not what the batches are put back under.
    #[test]
    fn a_schema_message_is_replaced_by_one_a_decoder_can_build_against() {
        let map = map_from_parts(
            true,
            None,
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), None],
        );
        let batch = batch_of(Arc::new(map) as ArrayRef);
        let header = schema_message(&batch.schema());

        let (declared, repaired) =
            decodable_schema_message(&header).expect("a forbidden declaration needs replacing");
        assert_eq!(
            declared.field(0).data_type(),
            &map_type(true),
            "what the producer declared is handed back as it stands"
        );

        let decodable = try_schema_from_flatbuffer_bytes(&repaired)
            .expect("the replacement is a readable schema message");
        let DataType::List(entries) = decodable.field(0).data_type() else {
            panic!(
                "the map must be offered to the decoder as the list it is laid out as, got {}",
                decodable.field(0).data_type()
            );
        };
        assert_eq!(
            entries.as_ref(),
            &Field::new("entries", DataType::Struct(entry_fields()), true),
            "the entries field is carried across untouched — only the label above it changes"
        );

        assert!(
            decodable_schema_message(&schema_message(&conforming_schema(batch.schema()))).is_none(),
            "a declaration that decodes as it stands is passed on untouched"
        );
        assert!(
            decodable_schema_message(b"this is not a flatbuffer").is_none(),
            "a message that is not a readable schema is passed on untouched"
        );
    }

    /// A stream that already conforms is handed back exactly as it decoded, schema included.
    #[test]
    fn a_conforming_ipc_stream_is_read_back_unchanged() {
        let map = map_from_parts(
            false,
            None,
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), None],
        );
        let batch = batch_of(Arc::new(map) as ArrayRef);
        let bytes = ipc_stream(&batch.schema(), std::slice::from_ref(&batch));

        let read_back = read_ipc_stream(&bytes).expect("a conforming stream decodes");
        assert_eq!(read_back, vec![batch]);
    }

    /// Regression test for #7307: a `MAP` column whose `entries` field arrives declared
    /// nullable cannot be rebuilt by any kernel, and the refusal reports nulls the data does
    /// not contain — which is why the declaration was not the first suspect.
    #[test]
    fn a_nullable_entries_declaration_is_relabelled_so_the_column_can_be_rebuilt() {
        let map = map_from_parts(
            true,
            None,
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), None],
        );
        let batch = batch_of(Arc::new(map) as ArrayRef);

        let before_map = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map before");
        assert_eq!(
            before_map.entries().null_count(),
            0,
            "the data holds no nulls at all"
        );
        let err = rebuild_through_public_constructor(before_map)
            .expect_err("a nullable entries field is refused on its own");
        assert!(
            err.to_string()
                .contains("MapArray entries cannot contain nulls"),
            "unexpected error: {err}"
        );

        let normalized = normalize(batch.clone()).expect("normalization");

        match normalized.schema().field(0).data_type() {
            DataType::Map(entries, _) => assert!(
                !entries.is_nullable(),
                "entries must be relabelled non-nullable"
            ),
            other => panic!("expected a Map, got {other}"),
        }

        let after = normalized
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map after");
        rebuild_through_public_constructor(after)
            .expect("a spec-conforming map rebuilds through the public constructor");

        assert_eq!(normalized.num_rows(), batch.num_rows());
        assert_eq!(after.offsets(), before_map.offsets());
        assert_eq!(after.keys(), before_map.keys());
        assert_eq!(after.values(), before_map.values());
        assert_eq!(after.nulls(), before_map.nulls());
    }

    /// A null map row — validity 0 with an empty offset range — is preserved, since that is
    /// the layout the relabelling exists to keep reachable.
    #[test]
    fn a_null_map_row_survives_normalization() {
        let entries = StructArray::try_new(
            entry_fields(),
            vec![
                Arc::new(StringArray::from(vec!["k0"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("v0")])) as ArrayRef,
            ],
            None,
        )
        .expect("entries");
        let builder = ArrayData::builder(map_type(true))
            .len(2)
            .add_buffer(Buffer::from_slice_ref([0i32, 1, 1]))
            .nulls(Some(NullBuffer::from(vec![true, false])))
            .add_child_data(entries.to_data());
        // SAFETY: the offsets, buffers and child data are all well formed. The only
        // thing `ArrayData::validate` objects to is the `entries` nullability
        // declaration, which is exactly what this fixture exists to reproduce — the
        // IPC reader builds such a map without either check.
        let data = unsafe { builder.build_unchecked() };
        let batch = batch_of(Arc::new(MapArray::from(data)) as ArrayRef);

        let normalized = normalize(batch).expect("normalization");
        let map = normalized
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map");
        assert_eq!(map.len(), 2);
        assert!(map.is_valid(0), "row 0 holds a map");
        assert!(map.is_null(1), "row 1 is a null map");
        assert_eq!(map.value(0).len(), 1);
    }

    /// Entries that genuinely carry nulls cannot be relabelled: Arrow gives a null entry no
    /// meaning, so the column is refused by name rather than guessed at.
    #[test]
    fn entry_level_nulls_are_refused_by_column_name() {
        let map = map_from_parts(
            true,
            Some(NullBuffer::from(vec![true, false])),
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), None],
        );
        let err = normalize(batch_of(Arc::new(map) as ArrayRef))
            .expect_err("entry nulls must be refused");
        assert!(
            matches!(&err, Error::MapEntriesContainNulls { column } if column == "col_map"),
            "unexpected error: {err}"
        );
    }

    /// The refusal holds for a map nested inside another container, where the offending
    /// entries array is not the column's own child.
    #[test]
    fn entry_level_nulls_nested_in_a_list_are_refused() {
        let map = map_from_parts(
            true,
            Some(NullBuffer::from(vec![false])),
            &[0, 1],
            vec!["k0"],
            vec![None],
        );
        let list = ListArray::try_new(
            Arc::new(Field::new("item", map.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(map) as ArrayRef,
            None,
        )
        .expect("list of maps");

        let err = normalize(batch_of(Arc::new(list) as ArrayRef))
            .expect_err("nested entry nulls must be refused");
        assert!(
            matches!(&err, Error::MapEntriesContainNulls { column } if column == "col_map"),
            "unexpected error: {err}"
        );
    }

    /// A map nested inside a struct inside a list is relabelled too — the rewrite has to
    /// reach every depth, not just a top-level map column.
    #[test]
    fn a_deeply_nested_map_declaration_is_relabelled() {
        let map = map_from_parts(true, None, &[0, 1], vec!["k0"], vec![Some("v0")]);
        let map_type_before = map.data_type().clone();
        let struct_fields: Fields = vec![
            Field::new("m", map_type_before, true),
            Field::new("n", DataType::Int32, true),
        ]
        .into();
        let inner = StructArray::try_new(
            struct_fields.clone(),
            vec![
                Arc::new(map) as ArrayRef,
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
            ],
            None,
        )
        .expect("struct");
        let list = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Struct(struct_fields), true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(inner) as ArrayRef,
            None,
        )
        .expect("list");

        let batch = batch_of(Arc::new(list) as ArrayRef);

        let normalized = normalize(batch).expect("normalization");

        // Spelled out rather than asked of the rewrite rule: the map is relabelled, and the
        // field names and the untouched `n` beside it come through as they were.
        let expected_fields: Fields = vec![
            Field::new("m", map_type(false), true),
            Field::new("n", DataType::Int32, true),
        ]
        .into();
        assert_eq!(
            normalized.schema().field(0).data_type(),
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(expected_fields),
                true
            )))
        );

        // The values are still reachable through the relabelled type.
        let list = normalized
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let inner = list
            .value(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct")
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map")
            .value(0);
        assert_eq!(inner.len(), 1);
    }

    /// A map nested under each wrapper type the rewrite descends into is relabelled, children
    /// included.
    ///
    /// The rewrite rule reaches a map through `ListView`, `Union`, `Dictionary` and
    /// `RunEndEncoded` as readily as through `List` and `Struct`, so the relabel has to rebuild
    /// those children too. Where it does not, the wrapper is rebuilt carrying the new type over
    /// children that still hold the old one, and Arrow rejects the mismatch — the query returns
    /// an error rather than rows.
    #[test]
    fn a_map_under_every_traversed_wrapper_is_relabelled() {
        // Every wrapper is reported, not just the first to break: a loop that panics on wrapper
        // one leaves the rest unproven.
        let mut failures: Vec<String> = Vec::new();
        for (name, wrapped) in wrapped_maps() {
            let expected = crate::type_rewrite::rewrite_data_type(
                wrapped.data_type(),
                &[&MapEntriesNonNullable],
            );
            assert!(
                holds_nullable_entries(wrapped.data_type()) && !holds_nullable_entries(&expected),
                "{name}: the fixture must start non-conforming and have a conforming rewrite"
            );

            match normalize(batch_of(Arc::clone(&wrapped))) {
                Err(e) => failures.push(format!("{name}: {e}")),
                Ok(normalized) => {
                    let schema = normalized.schema();
                    let published = schema.field(0).data_type();
                    if published != &expected {
                        failures.push(format!("{name}: published {published}, want {expected}"));
                    }
                    // The child array carries the relabelled type too — a parent-only rewrite
                    // is what Arrow rejects.
                    if normalized.column(0).data_type() != &expected {
                        failures.push(format!(
                            "{name}: column type {}, want {expected}",
                            normalized.column(0).data_type()
                        ));
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "wrappers not normalized: {failures:#?}"
        );
    }

    /// Independent of the rewrite rule: walks the type for a nullable `entries` field.
    fn holds_nullable_entries(data_type: &DataType) -> bool {
        match data_type {
            DataType::Map(entries, _) => {
                entries.is_nullable() || holds_nullable_entries(entries.data_type())
            }
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::RunEndEncoded(_, f) => holds_nullable_entries(f.data_type()),
            DataType::Struct(fields) => {
                fields.iter().any(|f| holds_nullable_entries(f.data_type()))
            }
            DataType::Union(fields, _) => fields
                .iter()
                .any(|(_, f)| holds_nullable_entries(f.data_type())),
            DataType::Dictionary(_, value) => holds_nullable_entries(value),
            _ => false,
        }
    }

    /// One single-element array per wrapper type, each holding a map whose `entries` field is
    /// declared nullable.
    fn wrapped_maps() -> Vec<(&'static str, ArrayRef)> {
        use arrow::array::{
            DictionaryArray, FixedSizeListArray, Int32Array, ListViewArray, UnionArray,
        };
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::{Int32Type, UnionFields};

        let map = || {
            Arc::new(map_from_parts(
                true,
                None,
                &[0, 1],
                vec!["k0"],
                vec![Some("v0")],
            ))
        };
        let item = |array: &ArrayRef| Arc::new(Field::new("item", array.data_type().clone(), true));

        let mut out: Vec<(&'static str, ArrayRef)> = Vec::new();

        let m = map() as ArrayRef;
        out.push((
            "FixedSizeList",
            Arc::new(FixedSizeListArray::new(item(&m), 1, Arc::clone(&m), None)),
        ));

        let m = map() as ArrayRef;
        out.push((
            "ListView",
            Arc::new(
                ListViewArray::try_new(
                    item(&m),
                    ScalarBuffer::from(vec![0]),
                    ScalarBuffer::from(vec![1]),
                    Arc::clone(&m),
                    None,
                )
                .expect("list view of a map"),
            ),
        ));

        let m = map() as ArrayRef;
        out.push((
            "Dictionary",
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), Arc::clone(&m))
                    .expect("dictionary of maps"),
            ),
        ));

        let m = map() as ArrayRef;
        // `RunArray::try_new` validates its values child, which rejects the `entries`
        // declaration under test, so the run-end array is assembled from its parts.
        let ree_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", m.data_type().clone(), true)),
        );
        let ree_builder = ArrayData::builder(ree_type)
            .len(1)
            .add_child_data(Int32Array::from(vec![1]).to_data())
            .add_child_data(m.to_data());
        // SAFETY: run ends and values are well formed; only the nested `entries`
        // declaration is what validation rejects, and that is the shape under test.
        out.push((
            "RunEndEncoded",
            make_array(unsafe { ree_builder.build_unchecked() }),
        ));

        let m = map() as ArrayRef;
        let union_fields: UnionFields =
            [(0_i8, Arc::new(Field::new("m", m.data_type().clone(), true)))]
                .into_iter()
                .collect();
        out.push((
            "Union",
            Arc::new(
                UnionArray::try_new(
                    union_fields,
                    ScalarBuffer::from(vec![0_i8]),
                    Some(ScalarBuffer::from(vec![0_i32])),
                    vec![Arc::clone(&m)],
                )
                .expect("union of maps"),
            ),
        ));

        out
    }

    /// A batch that already conforms is handed back as-is, so the common case pays nothing.
    #[test]
    fn a_conforming_batch_is_returned_unchanged() {
        let map = map_from_parts(false, None, &[0, 1], vec!["k0"], vec![Some("v0")]);
        let batch = batch_of(Arc::new(map) as ArrayRef);
        let normalized = normalize(batch.clone()).expect("normalization");
        assert_eq!(normalized.schema(), batch.schema());
        assert_eq!(normalized.column(0).to_data(), batch.column(0).to_data());
    }

    /// A decode point that hands its batches on under a separately declared schema — an
    /// `ExecutionPlan`'s output schema, a `RecordBatchStreamAdapter`'s — has to declare the one
    /// the batches actually carry, or it describes them with a type they no longer have.
    #[test]
    fn the_reported_schema_is_the_one_the_normalized_batches_carry() {
        let declared = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(true),
            true,
        )]));
        let normalizer = MapEntriesNormalizer::for_schema(&declared);
        assert_eq!(
            normalizer.schema().field(0).data_type(),
            &map_type(false),
            "the reported schema still declares the entries the batches no longer do"
        );

        let batch = batch_of(Arc::new(map_from_parts(
            true,
            None,
            &[0, 1],
            vec!["k0"],
            vec![Some("v0")],
        )) as ArrayRef);
        let relabelled = normalizer.normalize(batch).expect("normalization");
        assert_eq!(&relabelled.schema(), normalizer.schema());
    }

    /// A stream whose declarations already conform is reported under its own schema, by
    /// reference — nothing is rebuilt to say the same thing.
    #[test]
    fn a_conforming_stream_is_reported_under_its_own_schema() {
        let declared = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(false),
            true,
        )]));
        let normalizer = MapEntriesNormalizer::for_schema(&declared);
        assert!(Arc::ptr_eq(normalizer.schema(), &declared));
    }

    /// `conforming_schema` hands back the same `Arc` when nothing needs correcting, so a caller
    /// can tell "already conformed" from "relabelled" and pay nothing in the common case.
    #[test]
    fn conforming_schema_shares_a_schema_that_already_conforms() {
        let conforming = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(false),
            true,
        )]));
        assert!(Arc::ptr_eq(
            &conforming_schema(Arc::clone(&conforming)),
            &conforming
        ));

        let nullable_entries = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(true),
            true,
        )]));
        let corrected = conforming_schema(Arc::clone(&nullable_entries));
        assert!(!Arc::ptr_eq(&corrected, &nullable_entries));
        assert_eq!(corrected.field(0).data_type(), &map_type(false));
    }

    /// A schema holding no `Map` at all is shared rather than rebuilt, whatever else it holds.
    #[test]
    fn conforming_schema_shares_a_schema_holding_no_map() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)]));
        assert!(Arc::ptr_eq(
            &conforming_schema(Arc::clone(&schema)),
            &schema
        ));
    }

    /// A stream that only learns its schema from its batches resolves one normalizer and reuses
    /// it: every batch comes back under the *same* `SchemaRef`, which is what a downstream
    /// operator comparing schemas by pointer relies on.
    #[test]
    fn a_stream_normalizer_reuses_one_resolved_schema_across_batches() {
        let mut normalizer = StreamNormalizer::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(true),
            true,
        )]));

        let of_schema = |keys: Vec<&str>, values: Vec<Option<&str>>, offsets: &[i32]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(map_from_parts(true, None, offsets, keys, values)) as ArrayRef],
            )
            .expect("batch")
        };

        let first = normalizer
            .normalize(of_schema(vec!["k0"], vec![Some("v0")], &[0, 1]))
            .expect("first batch");
        let second = normalizer
            .normalize(of_schema(vec!["k1"], vec![Some("v1")], &[0, 1]))
            .expect("second batch");

        assert_eq!(first.schema().field(0).data_type(), &map_type(false));
        assert!(
            Arc::ptr_eq(&first.schema(), &second.schema()),
            "every batch of one stream must come out under one schema"
        );
    }

    /// A second schema message mid-stream resolves a second normalizer: relabelling the new
    /// batches to the first schema's target would rename their columns and change their types.
    #[test]
    fn a_stream_normalizer_re_resolves_when_the_schema_changes() {
        let mut normalizer = StreamNormalizer::new();

        let with_map = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "col_map",
                map_type(true),
                true,
            )])),
            vec![Arc::new(map_from_parts(
                true,
                None,
                &[0, 1],
                vec!["k0"],
                vec![Some("v0")],
            )) as ArrayRef],
        )
        .expect("map batch");
        let without_map = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .expect("int batch");

        let first = normalizer.normalize(with_map).expect("map batch");
        assert_eq!(first.schema().field(0).data_type(), &map_type(false));

        let second = normalizer
            .normalize(without_map.clone())
            .expect("a batch under a new schema");
        assert_eq!(second.schema(), without_map.schema());
        assert_eq!(second.column(0).to_data(), without_map.column(0).to_data());
    }

    /// The one shape relabelling cannot fix is refused through the streaming form too, naming
    /// the column — a stream must not hand on a map whose entries carry nulls.
    #[test]
    fn a_stream_normalizer_refuses_entry_nulls() {
        let mut normalizer = StreamNormalizer::new();
        let entry_nulls = NullBuffer::from(vec![true, false]);
        let batch = batch_of(Arc::new(map_from_parts(
            true,
            Some(entry_nulls),
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), Some("v1")],
        )) as ArrayRef);

        let err = normalizer
            .normalize(batch)
            .expect_err("entries carrying nulls have no representation to relabel to");
        assert!(
            err.to_string().contains("col_map"),
            "the refusal must name the column: {err}"
        );
    }

    /// An empty map column — zero rows — normalizes without touching offsets.
    #[test]
    fn an_empty_map_column_normalizes() {
        let map = map_from_parts(true, None, &[0], vec![], vec![]);
        let normalized = normalize(batch_of(Arc::new(map) as ArrayRef))
            .expect("normalization of an empty column");
        assert_eq!(normalized.num_rows(), 0);
        assert_eq!(normalized.schema().field(0).data_type(), &map_type(false));
    }
}
