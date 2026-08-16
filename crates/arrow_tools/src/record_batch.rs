/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use arrow::{
    array::{
        Array, ArrayRef, BinaryViewArray, GenericByteViewArray, ListArray, MutableArrayData,
        RecordBatch, RecordBatchOptions, StringViewArray, StructArray, make_array, new_null_array,
    },
    buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{
        BinaryViewType, ByteViewType, DataType, Field, SchemaRef, StringViewType, TimeUnit,
    },
    error::ArrowError,
};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::Schema;
use datafusion::common::metadata::ScalarAndMetadata;
use datafusion::{common::ParamValues, error::DataFusionError, scalar::ScalarValue};
use snafu::{ResultExt, prelude::*};
use std::sync::Arc;

use crate::format::{FormatOperation, format_column_data};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error converting record batch: {source}"))]
    UnableToConvertRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Field is not nullable: {field}"))]
    FieldNotNullable { field: String },
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        match e {
            Error::UnableToConvertRecordBatch {
                source: arrow_error,
            } => DataFusionError::ArrowError(Box::new(arrow_error), None),
            Error::FieldNotNullable { .. } => {
                DataFusionError::ArrowError(Box::new(ArrowError::SchemaError(e.to_string())), None)
            }
        }
    }
}

/// Cast a given record batch into a new record batch with the given schema.
///
/// # Errors
///
/// This function will return an error if the record batch cannot be cast.
pub fn try_cast_to(record_batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let existing_schema = record_batch.schema();

    // When schema is superset of the existing schema, including a new column, and nullable column,
    // return a new RecordBatch to reflect the change
    if schema.contains(&existing_schema) {
        return record_batch
            .with_schema(schema)
            .context(UnableToConvertRecordBatchSnafu);
    }

    let cast_options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };

    let cols = schema
        .fields()
        .into_iter()
        .map(|field| {
            if let (Ok(existing_field), Some(column)) = (
                record_batch.schema().field_with_name(field.name()),
                record_batch.column_by_name(field.name()),
            ) {
                if field.contains(existing_field) {
                    Ok(Arc::clone(column))
                } else {
                    cast_column(column, existing_field.data_type(), field, &cast_options)
                }
            } else if field.is_nullable() {
                Ok(new_null_array(field.data_type(), record_batch.num_rows()))
            } else {
                FieldNotNullableSnafu {
                    field: field.name(),
                }
                .fail()
            }
        })
        .collect::<Result<Vec<Arc<dyn Array>>>>()?;

    // Handle empty schema case (e.g., for aggregate queries like `SELECT COUNT(1) FROM table`).
    // Arrow requires either columns or an explicit row count when creating a RecordBatch.
    if cols.is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            cols,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(record_batch.num_rows())),
        )
        .context(UnableToConvertRecordBatchSnafu);
    }

    RecordBatch::try_new(schema, cols).context(UnableToConvertRecordBatchSnafu)
}

/// Returns `true` when `source` → `target` is a timestamp-to-timestamp cast that
/// only changes the time unit (and possibly the timezone string), meaning the
/// underlying physical values need rescaling and may overflow on far-future/past
/// sentinel dates.
fn is_timestamp_unit_cast(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
    ) && timestamp_unit(source) != timestamp_unit(target)
}

fn timestamp_unit(dt: &DataType) -> Option<&TimeUnit> {
    match dt {
        DataType::Timestamp(unit, _) => Some(unit),
        _ => None,
    }
}

/// Cast a single column, with special handling for timestamp unit conversions
/// that may overflow (e.g. far-future sentinel values like year 9999 when
/// converting from microseconds to nanoseconds).
fn cast_column(
    column: &ArrayRef,
    source_type: &DataType,
    target_field: &Field,
    strict_options: &CastOptions,
) -> Result<ArrayRef> {
    match cast_with_options(column.as_ref(), target_field.data_type(), strict_options) {
        Ok(casted) => Ok(casted),
        Err(ref e)
            if is_timestamp_unit_cast(source_type, target_field.data_type())
                && is_overflow_error(e) =>
        {
            tracing::warn!(
                "Timestamp overflow casting column '{}' from {source_type:?} to {:?}. Values outside the representable range will be NULL.",
                target_field.name(),
                target_field.data_type(),
            );
            let safe_options = CastOptions {
                safe: true,
                ..strict_options.clone()
            };
            cast_with_options(column.as_ref(), target_field.data_type(), &safe_options)
                .context(UnableToConvertRecordBatchSnafu)
        }
        Err(e) => Err(e).context(UnableToConvertRecordBatchSnafu),
    }
}

fn is_overflow_error(e: &ArrowError) -> bool {
    matches!(
        e,
        ArrowError::CastError(msg) | ArrowError::ArithmeticOverflow(msg)
            if msg.contains("Overflow") || msg.contains("overflow")
    )
}

/// Flattens a list of struct types with a single field into a list of primitive types.
/// The struct field must be a primitive type.
/// If the struct has multiple fields, all except the first field will be ignored.
///
/// # Errors
///
/// This function will return an error if the column cannot be cast to a list of struct types with a single field.
pub fn to_primitive_type_list(
    column: &ArrayRef,
    field: &Arc<Field>,
) -> Result<(ArrayRef, Arc<Field>), ArrowError> {
    if let DataType::List(inner_field) = field.data_type()
        && let DataType::Struct(struct_fields) = inner_field.data_type()
        && struct_fields.len() == 1
    {
        let list_item_field = Arc::clone(&struct_fields[0]);

        let original_list_array =
            column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or(ArrowError::CastError(
                    "Failed to downcast to ListArray".into(),
                ))?;

        let struct_array = original_list_array
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or(ArrowError::CastError(
                "Failed to downcast to StructArray".into(),
            ))?;

        let struct_column_array = Arc::clone(struct_array.column(0));

        let new_list_field = Arc::new(Field::new(
            field.name(),
            DataType::List(Arc::clone(&list_item_field)),
            field.is_nullable(),
        ));
        let new_list_array = ListArray::new(
            list_item_field,
            OffsetBuffer::new(Buffer::from_slice_ref(original_list_array.value_offsets()).into()),
            struct_column_array,
            original_list_array.logical_nulls(),
        );

        return Ok((Arc::new(new_list_array), new_list_field));
    }

    Err(ArrowError::CastError("Invalid column type".into()))
}

/// Recursively truncates the data in a [`RecordBatch`] to the specified maximum number of characters.
/// The truncation is applies to [`DataType::Utf8`] and [`DataType::Utf8View`] data.
///
/// # Errors
///
/// This function will return an error if arrow conversion fails.
pub fn truncate_string_columns(
    record_batch: &RecordBatch,
    max_characters: usize,
) -> Result<RecordBatch, ArrowError> {
    let schema = record_batch.schema();
    let columns = record_batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            format_column_data(
                Arc::clone(column),
                field,
                FormatOperation::TruncateUtf8Length(max_characters),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, columns)
}

/// Truncates any column in the [`RecordBatch`] that is a list of numerical values to the first `max_elements` elements.
///
/// # Errors
///
/// This function will return an error if arrow conversion fails.
pub fn truncate_numeric_column_length(
    record_batch: &RecordBatch,
    max_elements: usize,
) -> Result<RecordBatch, ArrowError> {
    let schema = record_batch.schema();
    let column_and_fields = record_batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if is_numeric_list(field) {
                let new_column = format_column_data(
                    Arc::clone(column),
                    field,
                    FormatOperation::TruncateListLength(max_elements),
                )?;
                let new_field = Arc::new(Field::new(
                    field.name(),
                    new_column.data_type().clone(),
                    field.is_nullable(),
                ));
                Ok((new_column, new_field))
            } else {
                Ok((Arc::clone(column), Arc::clone(field)))
            }
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;

    let (columns, fields) = column_and_fields
        .into_iter()
        .unzip::<_, _, Vec<_>, Vec<_>>();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

/// Converts a record batch with a single row into `ParamValues`
///
/// # Errors
/// Returns an error when a value in an array cannot be converted into a scalar.
pub fn record_to_param_values(batch: &RecordBatch) -> Result<ParamValues, DataFusionError> {
    let num_columns = batch.num_columns();

    // Fast path: empty batch
    if num_columns == 0 {
        return Ok(ParamValues::from(Vec::<ScalarValue>::new()));
    }

    let schema = batch.schema_ref();

    // Pre-allocate with exact capacity to avoid reallocation
    let mut list_params: Vec<(usize, ScalarValue)> = Vec::with_capacity(num_columns);
    let mut named_params: Vec<(String, ScalarValue)> = Vec::with_capacity(num_columns);
    let mut is_list = true;
    let mut needs_sort = false;
    let mut prev_index = 0usize;
    let mut has_prev_index = false;

    // Single pass: determine type and collect values simultaneously
    for col_index in 0..num_columns {
        let array = batch.column(col_index);
        let scalar = ScalarValue::try_from_array(array, 0)?;
        let name = schema.field(col_index).name();

        // Check if name is a parameter index (with or without $ prefix)
        let index = if let Some(stripped) = name.strip_prefix('$') {
            stripped.parse::<usize>().ok()
        } else {
            name.parse::<usize>().ok()
        };
        if let Some(index) = index {
            if has_prev_index && index < prev_index {
                needs_sort = true;
            }
            prev_index = index;
            has_prev_index = true;
            list_params.push((index, scalar));
            continue;
        }

        // Not a numbered parameter - switch to named mode
        is_list = false;
        named_params.push((name.clone(), scalar));
    }

    if is_list && !list_params.is_empty() {
        if needs_sort {
            list_params.sort_unstable_by_key(|(index, _)| *index);
        }

        // Extract just the values (compiler can optimize this to a move)
        Ok(ParamValues::List(
            list_params
                .into_iter()
                .map(|(_, value)| ScalarAndMetadata::from(value))
                .collect(),
        ))
    } else {
        // Convert list_params back to named if we have mixed types
        // IMPORTANT: Preserve the '$' prefix for positional parameters to maintain consistency
        // with DataFusion's parameter naming convention. DataFusion's SQL parser and parameter
        // resolution expect positional parameters to be named "$1", "$2", etc.
        // Mixed mode occurs when we have both "$1" style and "param_name" style parameters.
        if !list_params.is_empty() {
            for (index, value) in list_params {
                // Preserve the '$' prefix format: "$1", "$2", etc.
                named_params.push((format!("${index}"), value));
            }
        }
        Ok(ParamValues::Map(
            named_params
                .into_iter()
                .map(|(k, v)| (k, ScalarAndMetadata::from(v)))
                .collect(),
        ))
    }
}

fn is_numeric_list(field: &Arc<Field>) -> bool {
    match field.data_type() {
        DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::List(inner) => inner.data_type().is_numeric(),
        _ => false,
    }
}

/// For a given [`RecordBatch`], replace a given column, by name, with a new [`ArrayRef`] data.
///
/// If `col` is not in [`RecordBatch`], no change occurs.
///
/// # Errors
///
/// This function will return an error if it unexpectedly fails to create a new [`RecordBatch`].
pub fn replace_column_in_record(
    rb: RecordBatch,
    col: &str,
    data: &ArrayRef,
) -> Result<RecordBatch, ArrowError> {
    let Some((idx, _)) = rb.schema().column_with_name(col) else {
        return Ok(rb);
    };
    let schema = Schema::new(
        rb.schema()
            .fields()
            .iter()
            .map(|f| {
                if f.name() == col {
                    Arc::unwrap_or_clone(Arc::clone(f))
                        .with_data_type(data.data_type().clone())
                        .into()
                } else {
                    Arc::clone(f)
                }
            })
            .collect::<Vec<_>>(),
    );

    let columns = rb
        .columns()
        .iter()
        .enumerate()
        .map(|(i, arr)| {
            if i == idx {
                Arc::clone(data)
            } else {
                Arc::clone(arr)
            }
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(schema.into(), columns)
}

/// How many times its own rows' worth of memory a column may retain before a
/// compact copy is worth making. A slice that keeps twice what it needs is
/// still cheaper to hold than to rebuild.
const COMPACTION_RETENTION_RATIO: usize = 2;

/// How many bytes a compaction must actually reclaim — summed across the
/// batch's columns — to be worth its copies.
///
/// The floor is per batch, not per column: the consumers of this pass hold
/// batches for a long time (a results cache entry, an in-memory index), where
/// a wide result wasting a little per column still wastes a lot per entry.
/// A ~30-column join row can retain ~40 KiB beyond its own bytes with no
/// single column near a per-column floor (issue #13172). The per-column ratio
/// gate already bounds every copy's cost by what it reclaims, so the floor
/// only needs to keep near-compact batches — the common case — copy-free.
const COMPACTION_MIN_RECLAIMED_BYTES: usize = 4 * 1024;

/// Whether a type holds data in variadic buffers, which
/// [`ArrayData::get_slice_memory_size`] does not walk.
///
/// The view types are the ones that do: they keep their bytes in data buffers
/// the layout does not describe, so `get_slice_memory_size` counts only their
/// 16-byte views. A *top-level* view column is measured and compacted by the
/// view-specific path below; one nested inside a container is reachable by
/// neither, so such a column would be measured as if its data buffers were
/// free, and must be left alone rather than copied on every store for nothing.
fn contains_view_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => contains_view_type(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_view_type(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_view_type(field.data_type())),
        DataType::Dictionary(_, value_type) => contains_view_type(value_type),
        _ => false,
    }
}

/// Whether `data_type` is, or holds anywhere within it, a dictionary.
///
/// This recurses because its consumer does: `MutableArrayData::new` builds a
/// child `MutableArrayData` for every struct field, list value and union
/// variant, so a dictionary nested inside a container reaches the same
/// dictionary-key overflow that a top-level one does.
fn contains_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_dictionary(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_dictionary(field.data_type())),
        _ => false,
    }
}

/// How many bytes are reclaimable from a column retaining `retained` where its
/// rows need `needed`, or `None` when the copy would not pay for itself.
///
/// The gate here is only the retention *ratio*; whether the total reclaim is
/// worth the copies is decided per batch, against
/// [`COMPACTION_MIN_RECLAIMED_BYTES`].
fn worth_compacting(retained: usize, needed: usize) -> Option<usize> {
    let reclaimed = retained.checked_sub(needed)?;
    (reclaimed > 0 && retained >= needed.saturating_mul(COMPACTION_RETENTION_RATIO))
        .then_some(reclaimed)
}

/// How many bytes a view column retains beyond the bytes its own rows use.
///
/// A view array's values live in shared data buffers that slicing never
/// narrows — `slice` only trims the 16-byte views — so a one-row slice of a
/// wide-string batch keeps every data buffer of the batch it came from. This
/// matters by default rather than in a corner: `DataFusion` reads Parquet
/// `Utf8`/`Binary` columns as `Utf8View`/`BinaryView`
/// (`schema_force_view_types`), so ordinary string results take this path.
///
/// The ratio this decides on is arrow's own: `InProgressByteViewArray` gcs a
/// source when its data buffers exceed twice the bytes its views reference
/// (`arrow-select`, `coalesce/byte_view.rs`), for the same reason.
fn view_reclaimable_bytes<B: ByteViewType>(column: &ArrayRef) -> Option<usize> {
    let array = column.as_any().downcast_ref::<GenericByteViewArray<B>>()?;

    // Every allocation [`compact_view_array`] rebuilds is counted: the views,
    // the data buffers they point into, and the null buffer. A short-string
    // column can be almost all views, and a wide-string one almost all data.
    // For a slice, all three are the parent's whole allocations — including
    // when the slice's own rows all fit inline, where the data buffers hold
    // nothing the slice references and are dropped entirely.
    let retained = array.views().inner().capacity()
        + array
            .data_buffers()
            .iter()
            .map(Buffer::capacity)
            .sum::<usize>()
        + array
            .nulls()
            .map_or(0, |nulls| nulls.inner().inner().capacity());
    let views_bytes = array.len().saturating_mul(std::mem::size_of::<u128>());
    let null_bytes = array.nulls().map_or(0, |_| array.len().div_ceil(8));

    // `total_buffer_bytes_used` walks every view, so bound the reclaim first:
    // the compacted array cannot be smaller than its views alone. This is what
    // keeps an already-compact column — the common case — off that walk.
    worth_compacting(retained, views_bytes)?;

    let bytes_used = array.total_buffer_bytes_used();

    worth_compacting(retained, views_bytes + bytes_used + null_bytes)
}

/// How many bytes compacting `column` would reclaim, or `None` when the copy
/// would not pay for itself.
fn reclaimable_bytes(column: &ArrayRef) -> Option<usize> {
    match column.data_type() {
        DataType::Utf8View => return view_reclaimable_bytes::<StringViewType>(column),
        DataType::BinaryView => return view_reclaimable_bytes::<BinaryViewType>(column),
        data_type if contains_view_type(data_type) => return None,
        // A dictionary is declined on both counts. `MutableArrayData` shares
        // the values wholesale rather than narrowing them, so only the keys
        // could be reclaimed; and it panics outright building an extend for a
        // dictionary whose value count does not fit its key type — a
        // `Dictionary(UInt8, _)` holding exactly 256 values is valid Arrow and
        // trips it (`build_extend_dictionary` returns `None`, which
        // `MutableArrayData::with_capacities` unwraps with `expect`). Both
        // reasons hold for a dictionary at any depth: the extend is built per
        // child, so a `Struct<Dictionary<UInt8, _>>` reaches the same `expect`.
        data_type if contains_dictionary(data_type) => return None,
        _ => {}
    }

    // `Err` means the type's buffers cannot be measured from its layout, which
    // is the same situation as a nested view type: there is no honest
    // comparison to make, so leave the column alone.
    let needed = column.to_data().get_slice_memory_size().ok()?;

    worth_compacting(column.get_array_memory_size(), needed)
}

/// Rebuilds a view array so every allocation it holds is sized for exactly
/// its own rows.
///
/// `gc` rebuilds the views and the data buffers they point into on its main
/// path, but it has two fast paths — a column with no data buffers, and a
/// slice whose rows all fit inline — that reuse the incoming views allocation,
/// and it reuses the null buffer on every path. For a slice those are the
/// parent's whole allocations, so both are rebuilt here after `gc` runs.
/// Copying the views verbatim is sound: they reference the gc result's own
/// data buffers, which are carried over unchanged.
fn compact_view_array<B: ByteViewType>(array: &GenericByteViewArray<B>) -> ArrayRef {
    let gced = array.gc();
    let views = ScalarBuffer::from(gced.views().to_vec());
    let nulls = gced
        .nulls()
        .map(|nulls| NullBuffer::new(nulls.inner().iter().collect()));

    match GenericByteViewArray::<B>::try_new(views, gced.data_buffers().to_vec(), nulls) {
        Ok(rebuilt) => Arc::new(rebuilt),
        Err(e) => {
            // The rebuild changes no view and no buffer, so this is
            // unreachable; the gc result is still a valid compaction.
            tracing::warn!("Failed to rebuild a compacted view column, keeping the gc result: {e}");
            Arc::new(gced)
        }
    }
}

/// Copies `column`'s rows into buffers sized for exactly those rows.
///
/// For most types this is what [`arrow::compute::concat`] does for more than
/// one array; `concat` cannot be used because it returns a single input
/// untouched. A view array instead goes through [`compact_view_array`] —
/// `MutableArrayData` copies a view array's data buffers wholesale.
fn compact_column(column: &ArrayRef) -> ArrayRef {
    match column.data_type() {
        DataType::Utf8View => {
            if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
                return compact_view_array(array);
            }
        }
        DataType::BinaryView => {
            if let Some(array) = column.as_any().downcast_ref::<BinaryViewArray>() {
                return compact_view_array(array);
            }
        }
        _ => {}
    }

    let data = column.to_data();
    let mut compacted = MutableArrayData::new(vec![&data], false, column.len());
    compacted.extend(0, 0, column.len());
    make_array(compacted.freeze())
}

/// Returns `batch` with every column that retains substantially more memory
/// than its own rows need replaced by a compact copy.
///
/// [`RecordBatch::slice`] is zero-copy by design: a sliced batch keeps its
/// parent's whole buffers alive, and `get_array_memory_size` reports those
/// retained buffers rather than the rows the slice contains. A single row
/// carved out of a scan batch — what `LIMIT`/`OFFSET` produces — therefore
/// costs, and is billed, the entire batch it came from. Anything that holds a
/// batch past the scan that produced it (the SQL results cache, an in-memory
/// index) should compact it first, so the memory it pins is proportional to
/// the rows it kept.
///
/// Columns that are already compact are shared, not copied, so a batch with
/// nothing to reclaim costs a reference-count clone.
///
/// See [`compacted_memory_size`] for deciding whether the copy is worth making
/// *before* paying for it.
#[must_use]
pub fn compact_retained_buffers(batch: &RecordBatch) -> RecordBatch {
    let (plan, total_reclaimable) = compaction_plan(batch);

    if total_reclaimable < COMPACTION_MIN_RECLAIMED_BYTES {
        return batch.clone();
    }

    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(&plan)
        .map(|(column, reclaim)| {
            if reclaim.is_some() {
                compact_column(column)
            } else {
                Arc::clone(column)
            }
        })
        .collect();

    // The row count is carried explicitly so a batch with no columns keeps it.
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    match RecordBatch::try_new_with_options(batch.schema(), columns, &options) {
        Ok(compacted) => compacted,
        Err(e) => {
            // Compaction preserves every column's type and length, so this is
            // unreachable; keeping the original is always safe.
            tracing::warn!("Failed to compact a record batch, keeping it as read: {e}");
            batch.clone()
        }
    }
}

/// Roughly what `batch` would occupy once [`compact_retained_buffers`] has run
/// over it, computed without copying anything.
///
/// A caller that holds a memory budget should bill a batch this, not
/// `get_array_memory_size`, and should only compact a batch it has decided to
/// keep — otherwise a result too large to store is copied in full before being
/// discarded.
///
/// It is an estimate, not the exact figure: buffer capacities are rounded up on
/// allocation, so the compacted batch can measure a little either side of this.
/// It is meant for deciding whether a copy is worth making, and a caller that
/// must not exceed a hard limit should still measure what it actually built.
#[must_use]
pub fn compacted_memory_size(batch: &RecordBatch) -> usize {
    let (_, total_reclaimable) = compaction_plan(batch);

    // Below the floor, `compact_retained_buffers` leaves the batch as it is,
    // so what would be reclaimable is still what the batch occupies.
    let reclaimed = if total_reclaimable < COMPACTION_MIN_RECLAIMED_BYTES {
        0
    } else {
        total_reclaimable
    };

    batch.get_array_memory_size().saturating_sub(reclaimed)
}

/// Per-column reclaimable bytes — `None` where a copy would not pay for
/// itself or the type cannot be measured — and the batch-wide total.
///
/// [`compact_retained_buffers`] and [`compacted_memory_size`] share this so
/// what one predicts is what the other frees, including the batch-level floor
/// both apply to the total.
fn compaction_plan(batch: &RecordBatch) -> (Vec<Option<usize>>, usize) {
    let plan: Vec<Option<usize>> = batch.columns().iter().map(reclaimable_bytes).collect();
    let total = plan.iter().flatten().sum();
    (plan, total)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        json::ReaderBuilder,
    };

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]))
    }

    fn to_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::LargeUtf8, false),
            Field::new("c", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]))
    }

    fn batch_input() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(StringArray::from(vec![
                    "2024-01-13 03:18:09.000000",
                    "2024-01-13 03:18:09",
                    "2024-01-13 03:18:09.000",
                ])),
            ],
        )
        .expect("record batch should not panic")
    }

    #[test]
    fn test_string_to_timestamp_conversion() {
        let result = try_cast_to(batch_input(), to_schema()).expect("converted");
        assert_eq!(3, result.num_rows());
    }

    /// Test that `try_cast_to` handles empty schema correctly.
    /// This is needed for aggregate queries like `SELECT COUNT(1) FROM table`
    /// which have an empty projection (no columns selected from the table).
    #[test]
    fn test_try_cast_to_empty_schema() {
        // Input batch has columns but we want to cast to an empty schema
        let input_batch = batch_input();
        assert_eq!(3, input_batch.num_rows());
        assert_eq!(3, input_batch.num_columns());

        // Target schema has no columns (like projection=[] for COUNT queries)
        let empty_schema = Arc::new(Schema::empty());

        // This should succeed, preserving the row count
        let result = try_cast_to(input_batch, empty_schema).expect("should handle empty schema");
        assert_eq!(3, result.num_rows(), "row count should be preserved");
        assert_eq!(0, result.num_columns(), "should have no columns");
    }

    fn parse_json_to_batch(json_data: &str, schema: SchemaRef) -> RecordBatch {
        let reader = ReaderBuilder::new(schema)
            .build(std::io::Cursor::new(json_data))
            .expect("Failed to create JSON reader");

        reader
            .into_iter()
            .next()
            .expect("Expected a record batch")
            .expect("Failed to read record batch")
    }

    #[test]
    fn test_to_primitive_type_list() {
        let input_batch_json_data = r#"
            {"labels": [{"id": 1}, {"id": 2}]}
            {"labels": null}
            {"labels": null}
            {"labels": null}
            {"labels": [{"id": 3}, {"id": null}]}
            {"labels": [{"id": 4,"name":"test"}, {"id": null,"name":null}]}
            {"labels": null}
            "#;

        let input_batch = parse_json_to_batch(
            input_batch_json_data,
            Arc::new(Schema::new(vec![Field::new(
                "labels",
                DataType::List(Arc::new(Field::new(
                    "struct",
                    DataType::Struct(vec![Field::new("id", DataType::Int32, true)].into()),
                    true,
                ))),
                true,
            )])),
        );

        let expected_list_json_data = r#"
            {"labels": [1, 2]}
            {"labels": null}
            {"labels": null}
            {"labels": null}
            {"labels": [3, null]}
            {"labels": [4, null]}
            {"labels": null}
            "#;

        let expected_list_batch = parse_json_to_batch(
            expected_list_json_data,
            Arc::new(Schema::new(vec![Field::new(
                "labels",
                DataType::List(Arc::new(Field::new("id", DataType::Int32, true))),
                true,
            )])),
        );

        let (processed_array, processed_field) = to_primitive_type_list(
            input_batch.column(0),
            &Arc::new(input_batch.schema().field(0).clone()),
        )
        .expect("to_primitive_type_list should succeed");

        let processed_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![processed_field])),
            vec![processed_array],
        )
        .expect("should create new record batch");

        assert_eq!(expected_list_batch, processed_batch);
    }

    #[test]
    fn test_truncate_record_batch_data_complex_data() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "struct",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int32, true),
                        Field::new("name", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        )]));

        let input_batch_json_data = r#"
            {"labels": [{"id": 1, "name": "123"}, {"id": 2, "name": "12345"}, {"id": 1, "name": "123456789"}]}
            {"labels": null}
            {"labels": [{"id": 4,"name":"test12345"}, {"id": null,"name":null}]}
            "#;

        let input_batch = parse_json_to_batch(input_batch_json_data, Arc::clone(&schema));

        let processed_batch = truncate_string_columns(&input_batch, 5)
            .expect("truncate_record_batch_data should succeed");

        let expected_batch_json_data = r#"
            {"labels": [{"id": 1, "name": "123"}, {"id": 2, "name": "12345"}, {"id": 1, "name": "12345"}]}
            {"labels": null}
            {"labels": [{"id": 4,"name":"test1"}, {"id": null,"name":null}]}
            "#;

        let expected_batch = parse_json_to_batch(expected_batch_json_data, schema);

        assert_eq!(processed_batch, expected_batch);
    }

    fn create_record_batch(
        schema: Vec<(&str, DataType)>,
        columns: Vec<Arc<dyn arrow::array::Array>>,
    ) -> RecordBatch {
        let fields = schema
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).expect("new RecordBatch")
    }

    fn assert_param_values_eq(result: ParamValues, expected: ParamValues) {
        match (result, expected) {
            (ParamValues::List(result_vec), ParamValues::List(expected_vec)) => {
                assert_eq!(result_vec.len(), expected_vec.len(), "List lengths differ");
                for (r, e) in result_vec.iter().zip(expected_vec.iter()) {
                    // ScalarAndMetadata doesn't impl PartialEq, compare the value field
                    assert_eq!(r.value(), e.value(), "ScalarValue mismatch");
                }
            }
            (ParamValues::Map(result_map), ParamValues::Map(expected_map)) => {
                assert_eq!(result_map.len(), expected_map.len(), "Map lengths differ");
                for (key, expected_value) in expected_map {
                    let result_value = result_map.get(&key).expect("key in result map");
                    // ScalarAndMetadata doesn't impl PartialEq, compare the value field
                    assert_eq!(
                        result_value.value(),
                        expected_value.value(),
                        "ScalarValue mismatch for key {key}",
                    );
                }
            }
            (result, expected) => {
                panic!("Mismatched ParamValues variants: got {result:?}, expected {expected:?}")
            }
        }
    }

    #[test]
    fn record_to_param_values_list_parameters() {
        let batch = create_record_batch(
            vec![("$1", DataType::Int32), ("$2", DataType::Utf8)],
            vec![
                Arc::new(Int32Array::from(vec![Some(42)])),
                Arc::new(StringArray::from(vec![Some("hello")])),
            ],
        );

        let result = record_to_param_values(&batch).expect("record to param values");
        let expected = ParamValues::from(vec![
            ScalarValue::Int32(Some(42)),
            ScalarValue::Utf8(Some("hello".to_string())),
        ]);

        assert_param_values_eq(result, expected);
    }

    #[test]
    fn record_to_param_values_list_parameters_no_dollar() {
        let batch = create_record_batch(
            vec![("1", DataType::Int32), ("2", DataType::Utf8)],
            vec![
                Arc::new(Int32Array::from(vec![Some(42)])),
                Arc::new(StringArray::from(vec![Some("hello")])),
            ],
        );

        let result = record_to_param_values(&batch).expect("record to param values");
        let expected = ParamValues::from(vec![
            ScalarValue::Int32(Some(42)),
            ScalarValue::Utf8(Some("hello".to_string())),
        ]);

        assert_param_values_eq(result, expected);
    }

    #[test]
    fn record_to_param_values_named_parameters() {
        let batch = create_record_batch(
            vec![("param1", DataType::Int32), ("param2", DataType::Utf8)],
            vec![
                Arc::new(Int32Array::from(vec![Some(100)])),
                Arc::new(StringArray::from(vec![Some("world")])),
            ],
        );

        let result = record_to_param_values(&batch).expect("record to param values");
        let mut expected_map = HashMap::new();
        expected_map.insert("param1".to_string(), ScalarValue::Int32(Some(100)));
        expected_map.insert(
            "param2".to_string(),
            ScalarValue::Utf8(Some("world".to_string())),
        );
        let expected = ParamValues::from(expected_map);

        assert_param_values_eq(result, expected);
    }

    #[test]
    fn record_to_param_values_mixed_parameters() {
        let batch = create_record_batch(
            vec![("$1", DataType::Int32), ("param2", DataType::Utf8)],
            vec![
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("test")])),
            ],
        );

        let result = record_to_param_values(&batch).expect("record to param values");
        let mut expected_map = HashMap::new();
        // Preserve the '$' prefix for positional parameters in mixed mode
        expected_map.insert("$1".to_string(), ScalarValue::Int32(Some(10)));
        expected_map.insert(
            "param2".to_string(),
            ScalarValue::Utf8(Some("test".to_string())),
        );
        let expected = ParamValues::from(expected_map);

        assert_param_values_eq(result, expected);
    }

    #[test]
    fn record_to_param_values_list_parameters_out_of_order() {
        let batch = create_record_batch(
            vec![("$2", DataType::Int32), ("$1", DataType::Utf8)],
            vec![
                Arc::new(Int32Array::from(vec![Some(200)])),
                Arc::new(StringArray::from(vec![Some("first")])),
            ],
        );

        let result = record_to_param_values(&batch).expect("record to param values");
        let expected = ParamValues::from(vec![
            ScalarValue::Utf8(Some("first".to_string())),
            ScalarValue::Int32(Some(200)),
        ]);

        assert_param_values_eq(result, expected);
    }

    #[test]
    fn record_to_param_values_single_column_list() {
        let batch = create_record_batch(
            vec![("$1", DataType::Int32)],
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        );

        let result = record_to_param_values(&batch).expect("record to param values");
        let expected = ParamValues::from(vec![ScalarValue::Int32(Some(1))]);

        assert_param_values_eq(result, expected);
    }

    #[test]
    fn record_to_param_values_single_column_named() {
        let batch = create_record_batch(
            vec![("x", DataType::Utf8)],
            vec![Arc::new(StringArray::from(vec![Some("value")]))],
        );

        let result = record_to_param_values(&batch).expect("record to param values");
        let mut expected_map = HashMap::new();
        expected_map.insert(
            "x".to_string(),
            ScalarValue::Utf8(Some("value".to_string())),
        );
        let expected = ParamValues::from(expected_map);

        assert_param_values_eq(result, expected);
    }

    /// Casting Decimal128(38,9) → Decimal128(38,27) must return an error when
    /// the upscale would overflow, instead of silently producing NULL.
    #[test]
    fn test_try_cast_to_decimal_overflow_returns_error() {
        use arrow::array::Decimal128Array;

        // Value with 12 integer digits: 110_367_043_872.497010000
        // Internal i128 at scale 9 = 110367043872497010000
        let value_i128: i128 = 110_367_043_872_497_010_000;

        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "sum_charge",
            DataType::Decimal128(38, 9),
            true,
        )]));

        let source_array = Decimal128Array::from(vec![Some(value_i128)])
            .with_precision_and_scale(38, 9)
            .expect("valid Decimal128(38,9)");

        let batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_array)]).expect("valid batch");

        // Target schema with wider scale (38,27) — only allows 11 integer digits
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "sum_charge",
            DataType::Decimal128(38, 27),
            true,
        )]));

        let err =
            try_cast_to(batch, target_schema).expect_err("Decimal overflow should return an error");
        assert!(
            matches!(err, Error::UnableToConvertRecordBatch { .. }),
            "Expected UnableToConvertRecordBatch, got: {err:?}"
        );
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("is too large to store in a Decimal128"),
            "Expected overflow message, got: {err_msg}"
        );
    }

    /// Casting Decimal128 with values that fit should succeed.
    #[test]
    fn test_try_cast_to_decimal_no_overflow_succeeds() {
        use arrow::array::Decimal128Array;

        // Value with 11 integer digits: 99_999_999_999.000000000 (fits in 38-27=11 digits)
        let value_i128: i128 = 99_999_999_999_000_000_000;

        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 9),
            true,
        )]));

        let source_array = Decimal128Array::from(vec![Some(value_i128)])
            .with_precision_and_scale(38, 9)
            .expect("valid Decimal128(38,9)");

        let batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_array)]).expect("valid batch");

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 27),
            true,
        )]));

        let result = try_cast_to(batch, target_schema);
        assert!(
            result.is_ok(),
            "Decimal cast should succeed when value fits: {result:?}"
        );
    }

    /// Casting Timestamp(Microsecond) → Timestamp(Nanosecond) with a far-future
    /// sentinel value (year 9999) should not panic. Overflowing values become NULL
    /// via the safe-cast fallback.
    #[test]
    fn test_try_cast_to_timestamp_us_to_ns_overflow_produces_null() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;

        // 9999-12-31T23:59:59.999 in microseconds — overflows when multiplied by 1000
        let sentinel_us: i64 = 253_402_300_799_999_000;
        let normal_us: i64 = 1_700_000_000_000_000; // ~2023-11-14

        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));

        let source_array =
            TimestampMicrosecondArray::from(vec![Some(sentinel_us), Some(normal_us), None])
                .with_timezone("UTC");

        let batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_array)]).expect("valid batch");

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )]));

        let result = try_cast_to(batch, target_schema);
        assert!(
            result.is_ok(),
            "timestamp µs→ns cast should not fail on overflow: {:?}",
            result.err()
        );

        let casted = result.expect("already checked");
        let ts_col = casted
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .expect("should be TimestampNanosecondArray");

        // Overflowing sentinel becomes NULL
        assert!(ts_col.is_null(0), "overflowing sentinel should be NULL");
        // Normal value is correctly scaled (µs * 1000 = ns)
        assert_eq!(ts_col.value(1), normal_us * 1000);
        // Original NULL stays NULL
        assert!(ts_col.is_null(2), "original NULL should stay NULL");
    }

    /// Casting Timestamp(Microsecond) → Timestamp(Nanosecond) when all values fit
    /// should succeed with exact values (no fallback needed).
    #[test]
    fn test_try_cast_to_timestamp_us_to_ns_no_overflow() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;

        let value_us: i64 = 1_700_000_000_000_000; // ~2023-11-14

        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));

        let source_array =
            TimestampMicrosecondArray::from(vec![Some(value_us)]).with_timezone("UTC");

        let batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_array)]).expect("valid batch");

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )]));

        let result = try_cast_to(batch, target_schema);
        assert!(result.is_ok(), "cast should succeed: {:?}", result.err());

        let casted = result.expect("already checked");
        let ts_col = casted
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .expect("should be TimestampNanosecondArray");
        assert_eq!(ts_col.value(0), value_us * 1000);
    }

    /// Non-timestamp overflow errors should still propagate (no fallback).
    #[test]
    fn test_try_cast_to_non_timestamp_overflow_still_errors() {
        use arrow::array::Decimal128Array;

        // A value that overflows Decimal128(10, 2)
        let value: i128 = 99_999_999_999_000_000_000;

        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 9),
            true,
        )]));

        let source_array = Decimal128Array::from(vec![Some(value)])
            .with_precision_and_scale(38, 9)
            .expect("valid");

        let batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(source_array)]).expect("valid batch");

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]));

        let result = try_cast_to(batch, target_schema);
        assert!(
            result.is_err(),
            "non-timestamp overflow should still return an error"
        );
    }

    /// `rows` strings of `value_len` identical characters, varying by row so a
    /// compaction that took the wrong row is visible.
    fn payloads(rows: usize, value_len: usize) -> Vec<String> {
        (0..rows)
            .map(|row| {
                std::iter::repeat_n(
                    char::from(b'a' + u8::try_from(row % 26).unwrap_or_default()),
                    value_len,
                )
                .collect()
            })
            .collect()
    }

    /// A batch of wide strings, big enough that slicing one row out of it
    /// retains far more than that row needs.
    fn wide_string_batch(rows: usize, value_len: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let ids: Vec<i32> = (0..i32::try_from(rows).expect("rows fits in i32")).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(payloads(rows, value_len))),
            ],
        )
        .expect("valid batch")
    }

    fn row_payload(batch: &RecordBatch, row: usize) -> String {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload is a StringArray")
            .value(row)
            .to_string()
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/12921>.
    /// A one-row slice of a large batch must not keep the whole batch alive.
    #[test]
    fn compact_retained_buffers_releases_a_slices_parent_buffers() {
        let batch = wide_string_batch(2_000, 4_096);
        let sliced = batch.slice(1_000, 1);
        assert_eq!(
            sliced.get_array_memory_size(),
            batch.get_array_memory_size(),
            "a slice retains its parent's buffers, which is the defect under test"
        );

        let compacted = compact_retained_buffers(&sliced);

        assert_eq!(compacted.num_rows(), 1);
        assert_eq!(compacted.schema(), sliced.schema());
        assert_eq!(
            row_payload(&compacted, 0),
            row_payload(&sliced, 0),
            "compaction must preserve the row's value"
        );
        assert!(
            compacted.get_array_memory_size() * 100 < sliced.get_array_memory_size(),
            "a one-row slice of a 2000-row batch should retain a small fraction of it, got {} of {}",
            compacted.get_array_memory_size(),
            sliced.get_array_memory_size()
        );
    }

    /// Every row of a multi-row slice survives, in order.
    #[test]
    fn compact_retained_buffers_preserves_every_row_of_a_slice() {
        let batch = wide_string_batch(500, 1_024);
        let sliced = batch.slice(100, 300);

        let compacted = compact_retained_buffers(&sliced);

        assert_eq!(compacted.num_rows(), sliced.num_rows());
        for row in 0..sliced.num_rows() {
            assert_eq!(
                row_payload(&compacted, row),
                row_payload(&sliced, row),
                "row {row} changed under compaction"
            );
        }
        let ids = compacted
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is an Int32Array");
        assert_eq!(ids.value(0), 100, "the slice's first row must be preserved");
    }

    /// Nulls inside the slice survive, and nulls outside it are not adopted.
    #[test]
    fn compact_retained_buffers_preserves_nulls_within_the_slice() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]));
        let values: Vec<Option<String>> = payloads(2_000, 4_096)
            .into_iter()
            .enumerate()
            .map(|(row, value)| (row % 3 != 0).then_some(value))
            .collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))])
            .expect("valid batch");
        let sliced = batch.slice(999, 3);

        let compacted = compact_retained_buffers(&sliced);

        let before = sliced
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        let after = compacted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(after.null_count(), before.null_count());
        for row in 0..before.len() {
            assert_eq!(after.is_null(row), before.is_null(row), "row {row} nullity");
            if !before.is_null(row) {
                assert_eq!(after.value(row), before.value(row), "row {row} value");
            }
        }
    }

    /// A batch that retains nothing extra is shared, not copied.
    #[test]
    fn compact_retained_buffers_leaves_a_compact_batch_untouched() {
        let batch = wide_string_batch(4, 16);

        let compacted = compact_retained_buffers(&batch);

        for (idx, column) in batch.columns().iter().enumerate() {
            assert!(
                Arc::ptr_eq(column, compacted.column(idx)),
                "column {idx} of an already-compact batch was copied"
            );
        }
    }

    /// A slice small enough that copying it would not pay for itself is left
    /// alone, so the common case of many small batches costs no copies.
    #[test]
    fn compact_retained_buffers_ignores_a_slice_below_the_reclaim_floor() {
        let batch = wide_string_batch(16, 64);
        let sliced = batch.slice(1, 1);

        let compacted = compact_retained_buffers(&sliced);

        assert!(
            Arc::ptr_eq(sliced.column(1), compacted.column(1)),
            "a slice retaining under the floor should not be copied"
        );
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/13172>.
    /// The reclaim floor applies to the batch, not to each column: a wide
    /// result can waste well under the floor in every column and still waste
    /// many times its own bytes per entry.
    #[test]
    fn compact_retained_buffers_compacts_a_wide_batch_wasting_little_per_column() {
        let columns = 24;
        let fields: Vec<Field> = (0..columns)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, false))
            .collect();
        let arrays: Vec<ArrayRef> = (0..columns)
            .map(|_| Arc::new(StringArray::from(payloads(100, 100))) as ArrayRef)
            .collect();
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("valid batch");
        let sliced = batch.slice(50, 1);

        let compacted = compact_retained_buffers(&sliced);

        assert!(
            compacted.get_array_memory_size() * 10 < sliced.get_array_memory_size(),
            "a one-row slice of a wide batch should be billed a fraction of its parent, got {} of {}",
            compacted.get_array_memory_size(),
            sliced.get_array_memory_size()
        );
        for column in 0..columns {
            let before = sliced
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray");
            let after = compacted
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray");
            assert_eq!(after.value(0), before.value(0), "column {column} changed");
        }
    }

    fn wide_view_batch(rows: usize, value_len: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8View,
            true,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringViewArray::from_iter_values(payloads(
                rows, value_len,
            )))],
        )
        .expect("valid batch")
    }

    /// Slicing a view array trims only its 16-byte views — the data buffers
    /// holding the values are kept whole. `DataFusion` reads Parquet strings as
    /// `Utf8View` by default (`schema_force_view_types`), so this is the
    /// ordinary path for a string result, not a corner of it.
    #[test]
    fn compact_retained_buffers_releases_a_view_slices_data_buffers() {
        let batch = wide_view_batch(2_000, 4_096);
        let sliced = batch.slice(1_000, 1);
        assert!(
            sliced.get_array_memory_size() > 4 * 1024 * 1024,
            "a view slice retains its parent's data buffers, which is the defect under test; got {}",
            sliced.get_array_memory_size()
        );

        let compacted = compact_retained_buffers(&sliced);

        assert_eq!(compacted.num_rows(), 1);
        let before = sliced
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray");
        let after = compacted
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray");
        assert_eq!(after.value(0), before.value(0), "the row's value changed");
        assert!(
            compacted.get_array_memory_size() * 100 < sliced.get_array_memory_size(),
            "a one-row view slice should retain a small fraction of its parent, got {} of {}",
            compacted.get_array_memory_size(),
            sliced.get_array_memory_size()
        );
    }

    /// A view column whose buffers are already sized for its rows is shared,
    /// not copied. The builder rounds data blocks up (8 KiB minimum), so the
    /// fixture is gc'd first to get a genuinely compact column — the
    /// builder-rounded original is itself reclaimable, by design.
    #[test]
    fn compact_retained_buffers_leaves_a_compact_view_column_untouched() {
        let raw = wide_view_batch(4, 16);
        let column: ArrayRef = Arc::new(
            raw.column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("StringViewArray")
                .gc(),
        );
        let batch = RecordBatch::try_new(raw.schema(), vec![column]).expect("valid batch");

        let compacted = compact_retained_buffers(&batch);

        assert!(
            Arc::ptr_eq(batch.column(0), compacted.column(0)),
            "an already-compact view column was copied"
        );
    }

    /// A view nested inside a container cannot be measured or reached by the
    /// view path, so such a column is left alone rather than copied blindly.
    #[test]
    fn compact_retained_buffers_leaves_a_nested_view_column_alone() {
        let inner: ArrayRef = Arc::new(StringViewArray::from_iter_values(payloads(2_000, 4_096)));
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("payload", DataType::Utf8View, true)),
            inner,
        )]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wrapper",
            struct_array.data_type().clone(),
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(struct_array)]).expect("valid batch");
        let sliced = batch.slice(1_000, 1);

        let compacted = compact_retained_buffers(&sliced);

        assert!(
            Arc::ptr_eq(sliced.column(0), compacted.column(0)),
            "a nested view column must not be compacted"
        );
    }

    /// A `Dictionary(UInt8, _)` holding exactly 256 values is valid Arrow, but
    /// arrow's `MutableArrayData` panics building an extend for it. Compaction
    /// must never reach that path — a cache write is not allowed to abort the
    /// query that filled it.
    #[test]
    fn compact_retained_buffers_leaves_a_full_range_dictionary_alone() {
        use arrow::array::{DictionaryArray, UInt8Array};
        use arrow::datatypes::UInt8Type;

        let values = StringArray::from(
            (0..256)
                .map(|value| format!("value-{value}"))
                .collect::<Vec<_>>(),
        );
        // A key buffer far past the reclaim floor, so nothing but the type
        // check can keep this column away from compaction.
        let keys = UInt8Array::from(
            (0..200_000)
                .map(|row| u8::try_from(row % 256).unwrap_or_default())
                .collect::<Vec<_>>(),
        );
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<UInt8Type>::try_new(keys, Arc::new(values))
                .expect("valid dictionary"),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            dictionary.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![dictionary]).expect("valid batch");
        let sliced = batch.slice(100_000, 1);

        // Must not panic, and must hand back the column untouched.
        let compacted = compact_retained_buffers(&sliced);

        assert!(
            Arc::ptr_eq(sliced.column(0), compacted.column(0)),
            "a dictionary column must not be compacted"
        );
        assert_eq!(compacted.num_rows(), 1);
    }

    /// `MutableArrayData` builds an extend per child, so a dictionary nested in
    /// a struct reaches the same key-overflow `expect` as a top-level one. The
    /// guard has to recurse to keep it away from that path.
    #[test]
    fn compact_retained_buffers_leaves_a_struct_wrapped_full_range_dictionary_alone() {
        use arrow::array::{DictionaryArray, StructArray, UInt8Array};
        use arrow::datatypes::UInt8Type;

        let values = StringArray::from(
            (0..256)
                .map(|value| format!("value-{value}"))
                .collect::<Vec<_>>(),
        );
        let keys = UInt8Array::from(
            (0..200_000)
                .map(|row| u8::try_from(row % 256).unwrap_or_default())
                .collect::<Vec<_>>(),
        );
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<UInt8Type>::try_new(keys, Arc::new(values))
                .expect("valid dictionary"),
        );
        let inner = Field::new("d", dictionary.data_type().clone(), true);
        let column: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(inner),
            Arc::clone(&dictionary),
        )]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            column.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![column]).expect("valid batch");
        let sliced = batch.slice(100_000, 1);

        // Must not panic, and must hand back the column untouched.
        let compacted = compact_retained_buffers(&sliced);

        assert!(
            Arc::ptr_eq(sliced.column(0), compacted.column(0)),
            "a struct holding a dictionary must not be compacted"
        );
        assert_eq!(compacted.num_rows(), 1);
    }

    /// A slice whose values all fit inline still shares its parent's views
    /// allocation — 16 bytes per parent row — so it is rebuilt into a views
    /// buffer sized for its own rows. Part of issue #13172's `LIMIT`/`OFFSET`
    /// route: short-string tables produce exactly this shape.
    #[test]
    fn compact_retained_buffers_releases_an_inline_view_slices_views_buffer() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8View,
            true,
        )]));
        // 12 bytes or fewer is stored inline, with no data buffer.
        let values: Vec<Option<String>> = (0..200_000)
            .map(|row| (row % 7 != 0).then(|| format!("r{row:0>8}")))
            .collect();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringViewArray::from_iter(values))])
                .expect("valid batch");
        let sliced = batch.slice(100_000, 2);

        let compacted = compact_retained_buffers(&sliced);

        let before = sliced
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray");
        let after = compacted
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray");
        for row in 0..before.len() {
            assert_eq!(after.is_null(row), before.is_null(row), "row {row} nullity");
            if !before.is_null(row) {
                assert_eq!(after.value(row), before.value(row), "row {row} value");
            }
        }
        assert!(
            compacted.get_array_memory_size() * 100 < sliced.get_array_memory_size(),
            "a two-row inline slice should release its parent's views and null buffers, got {} of {}",
            compacted.get_array_memory_size(),
            sliced.get_array_memory_size()
        );
    }

    /// A column that does have data buffers, sliced down to rows whose values
    /// all fit inline: nothing in the data buffers is referenced, so they are
    /// dropped entirely along with the parent's views allocation.
    #[test]
    fn compact_retained_buffers_drops_an_inline_slices_unreferenced_data_buffers() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8View,
            true,
        )]));
        // Row 0 is long enough to force a data buffer; every other row is
        // inline, so a slice past row 0 references none of it.
        let mut values: Vec<String> = vec![std::iter::repeat_n('L', 65_536).collect()];
        values.extend((1..200_000).map(|row| format!("r{row:0>8}")));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringViewArray::from_iter_values(values))],
        )
        .expect("valid batch");
        let sliced = batch.slice(100_000, 1);

        let column = sliced
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray");
        assert!(
            !column.data_buffers().is_empty() && column.total_buffer_bytes_used() == 0,
            "the slice must retain a data buffer while referencing none of it"
        );

        let compacted = compact_retained_buffers(&sliced);

        let after = compacted
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray");
        assert_eq!(after.value(0), column.value(0), "the row's value changed");
        assert!(
            after.data_buffers().is_empty(),
            "no view references out-of-line data, so no data buffer should survive"
        );
        assert!(
            compacted.get_array_memory_size() * 100 < sliced.get_array_memory_size(),
            "an inline-only slice should release both halves of its parent, got {} of {}",
            compacted.get_array_memory_size(),
            sliced.get_array_memory_size()
        );
    }

    /// The pre-copy estimate is what a caller bills a batch before deciding to
    /// pay for the copy, so it has to track what compaction actually produces —
    /// within the rounding that buffer allocation adds.
    #[test]
    fn compacted_memory_size_tracks_the_compacted_batch() {
        for (name, sliced) in [
            (
                "string slice",
                wide_string_batch(2_000, 4_096).slice(1_000, 1),
            ),
            ("view slice", wide_view_batch(2_000, 4_096).slice(1_000, 1)),
            ("already compact", wide_string_batch(8, 16)),
        ] {
            let predicted = compacted_memory_size(&sliced);
            let actual = compact_retained_buffers(&sliced).get_array_memory_size();
            let slack = actual / 10 + 4_096;
            assert!(
                predicted + slack >= actual && predicted <= actual + slack,
                "{name}: estimate {predicted} is not within {slack} of the compacted {actual}"
            );
        }
    }

    /// The estimate is what stops a batch too large to cache from being copied
    /// in full and then discarded, so it must be far below the retained size
    /// for exactly the slices compaction targets.
    #[test]
    fn compacted_memory_size_is_a_small_fraction_of_a_slices_retained_size() {
        for sliced in [
            wide_string_batch(2_000, 4_096).slice(1_000, 1),
            wide_view_batch(2_000, 4_096).slice(1_000, 1),
        ] {
            let predicted = compacted_memory_size(&sliced);
            assert!(
                predicted * 100 < sliced.get_array_memory_size(),
                "estimated {predicted} against a retained {}",
                sliced.get_array_memory_size()
            );
        }
    }

    /// A batch with no columns keeps its row count.
    #[test]
    fn compact_retained_buffers_preserves_a_column_less_row_count() {
        let options = RecordBatchOptions::new().with_row_count(Some(7));
        let batch =
            RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options)
                .expect("valid batch");

        let compacted = compact_retained_buffers(&batch);

        assert_eq!(compacted.num_rows(), 7);
    }
}
