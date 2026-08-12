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

//! Compound (writethrough) search indexes.
//!
//! A [`CompoundSearchIndex`] (and its vector counterpart, [`CompoundVectorIndex`]) pairs a
//! primary index A with a secondary index B:
//!
//!  - **Writes** go to both A and B (writethrough).
//!  - **List & query** are served from A. With
//!    [`CompoundReadMode::FallbackToSecondary`], a list/query that returns zero rows from A
//!    is retried against B at execution time.
//!
//! The two indexes must be compatible: same search column, same primary-key fields, and the
//! same trait variant (both plain [`SearchIndex`]es, or both [`VectorIndex`]es with equal
//! embedding dimensions). Compatibility is validated at construction and violations return a
//! structured [`Error`].

mod fallback;
mod search_index;
#[cfg(test)]
mod tests;
mod vector_index;

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{ArrowError, Field, FieldRef, Schema};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use itertools::Itertools;
use snafu::{ResultExt, Snafu, ensure};
use spice_table::{Index, WriteWindow};

pub use search_index::CompoundSearchIndex;
pub use vector_index::CompoundVectorIndex;

use crate::index::SearchIndex;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to create compound search index: the primary index searches column '{primary_column}' but the secondary index searches column '{secondary_column}'. Configure both indexes on the same column."
    ))]
    SearchColumnMismatch {
        primary_column: String,
        secondary_column: String,
    },

    #[snafu(display(
        "Failed to create compound search index: the primary and secondary indexes have different primary-key fields (primary: [{primary_fields}]; secondary: [{secondary_fields}]). Configure both indexes with the same primary keys."
    ))]
    PrimaryFieldsMismatch {
        primary_fields: String,
        secondary_fields: String,
    },

    #[snafu(display(
        "Failed to create compound search index: the {vector_index} index is a vector index but the {plain_index} index is not. Both indexes must be the same kind (both vector indexes, or both full-text indexes)."
    ))]
    IndexVariantMismatch {
        vector_index: &'static str,
        plain_index: &'static str,
    },

    #[snafu(display(
        "Failed to create compound vector index: the primary index has embedding dimension {primary_dimension} but the secondary index has dimension {secondary_dimension}. Configure both indexes with the same embedding model/dimension."
    ))]
    DimensionMismatch {
        primary_dimension: i32,
        secondary_dimension: i32,
    },

    #[snafu(display("Failed to write to the primary index of a compound search index: {source}"))]
    PrimaryIndexWrite {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to write to the secondary index of a compound search index: {source}"
    ))]
    SecondaryIndexWrite {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to write to compound search index: the primary index returned {primary_rows} rows but the secondary index returned {secondary_rows} rows for the same input. The indexes are out of sync; this is a bug in one of the underlying indexes."
    ))]
    WriteRowCountMismatch {
        primary_rows: usize,
        secondary_rows: usize,
    },

    #[snafu(display(
        "Failed to write to compound search index: could not combine the primary and secondary index outputs: {source}"
    ))]
    MergeWriteOutputs { source: ArrowError },
}

/// How a compound index serves list & query operations.
///
/// Writes always go to both indexes; this mode only affects reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompoundReadMode {
    /// List & query only the primary index (writethrough without fallback).
    #[default]
    PrimaryOnly,
    /// List & query the primary index; if it returns zero rows, list/query the secondary
    /// index instead. The fallback decision is made at execution time, per query.
    FallbackToSecondary,
}

/// Validate that two indexes can be compounded: same search column, same primary-key fields
/// and the same trait variant (with matching dimensions for vector indexes).
fn validate_compatibility(
    primary: &Arc<dyn SearchIndex>,
    secondary: &Arc<dyn SearchIndex>,
) -> Result<(), Error> {
    ensure!(
        primary.search_column() == secondary.search_column(),
        SearchColumnMismatchSnafu {
            primary_column: primary.search_column(),
            secondary_column: secondary.search_column(),
        }
    );

    // Primary-key fields must agree on name, type and nullability. Order is irrelevant —
    // the fields define a key, not a physical layout.
    let normalize = |mut fields: Vec<Field>| {
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        fields
            .into_iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect::<Vec<_>>()
    };
    let display = |fields: &[Field]| {
        fields
            .iter()
            .sorted_by(|a, b| a.name().cmp(b.name()))
            .map(|f| {
                format!(
                    "{}: {}{}",
                    f.name(),
                    f.data_type(),
                    if f.is_nullable() { " (nullable)" } else { "" }
                )
            })
            .join(", ")
    };
    let primary_pk = primary.primary_fields();
    let secondary_pk = secondary.primary_fields();
    ensure!(
        normalize(primary_pk.clone()) == normalize(secondary_pk.clone()),
        PrimaryFieldsMismatchSnafu {
            primary_fields: display(&primary_pk),
            secondary_fields: display(&secondary_pk),
        }
    );

    match (
        Arc::clone(primary).as_vector_index(),
        Arc::clone(secondary).as_vector_index(),
    ) {
        (Some(primary_vector), Some(secondary_vector)) => {
            ensure!(
                primary_vector.dimension() == secondary_vector.dimension(),
                DimensionMismatchSnafu {
                    primary_dimension: primary_vector.dimension(),
                    secondary_dimension: secondary_vector.dimension(),
                }
            );
        }
        (None, None) => {}
        (Some(_), None) => {
            return IndexVariantMismatchSnafu {
                vector_index: "primary",
                plain_index: "secondary",
            }
            .fail();
        }
        (None, Some(_)) => {
            return IndexVariantMismatchSnafu {
                vector_index: "secondary",
                plain_index: "primary",
            }
            .fail();
        }
    }
    Ok(())
}

/// Write `record` to both indexes and merge their outputs: the primary's columns win, and
/// secondary columns not present on the primary output are appended (so columns derived by
/// either index survive for downstream acceleration). Both writes run concurrently and both
/// are driven to completion even if one fails, so neither index is left mid-write.
async fn compound_write(
    primary: &dyn SearchIndex,
    secondary: &dyn SearchIndex,
    record: RecordBatch,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let (primary_result, secondary_result) =
        futures::join!(primary.write(record.clone()), secondary.write(record));
    let primary_out = primary_result.context(PrimaryIndexWriteSnafu).boxed()?;
    let secondary_out = secondary_result.context(SecondaryIndexWriteSnafu).boxed()?;

    if primary_out.num_rows() != secondary_out.num_rows() {
        return WriteRowCountMismatchSnafu {
            primary_rows: primary_out.num_rows(),
            secondary_rows: secondary_out.num_rows(),
        }
        .fail()
        .boxed();
    }

    let (schema, mut arrays, _) = primary_out.into_parts();
    let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
    let secondary_schema = secondary_out.schema();
    for (i, field) in secondary_schema.fields().iter().enumerate() {
        if schema.column_with_name(field.name()).is_none() {
            fields.push(Arc::clone(field));
            arrays.push(Arc::clone(secondary_out.column(i)));
        }
    }
    // Preserve the primary output's schema-level metadata — some indexes rely on it
    // (e.g. `DuckDBVectorIndex` forwards its source schema metadata).
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        arrays,
    )
    .context(MergeWriteOutputsSnafu)
    .boxed()
}

/// Union of the two indexes' required columns, preserving the primary's order.
fn compound_required_columns(
    primary: &dyn SearchIndex,
    secondary: &dyn SearchIndex,
) -> Vec<String> {
    let mut columns = primary.required_columns();
    for column in secondary.required_columns() {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    columns
}

/// Start a bounded write window on both indexes, applying each half's own
/// [`Index::write_start_failure_is_fatal`] to *that half's* failure.
///
/// A compound index has two halves that can classify their own start failure differently, and a
/// single `write_start_failure_is_fatal` answer cannot say which half failed. So the decision is
/// made here, where the failing half is known: a half that declares its own start failure
/// best-effort is logged and the write continues; one that declares it fatal returns the error,
/// which the compound's [`Index::write_start_failure_is_fatal`] reports as fatal unconditionally.
///
/// Either answer a single combined flag can give is wrong for one of the halves:
///
///  - Answering "fatal" (a fatal half paired with a best-effort one) abandons the whole write
///    when the *best-effort* half's start fails — an Elasticsearch `refresh_interval` override
///    the write does not depend on would fail the refresh it was only tuning.
///  - Answering "best-effort" (two best-effort halves) rolls the primary's window back and then
///    writes anyway, turning a staged [`WriteWindow::ReplaceAll`] into an in-place write: readers
///    observe a partially rebuilt index and rows the source dropped are never cleared.
///
/// A half whose own start failed is never rolled back here: a start that fails partway owns its
/// cleanup (`ElasticsearchIndexWriteMaintenance::abandon_write_cycle` is the example), and
/// `on_write_failed` restores state set up by a *successful* start, so calling it could "restore"
/// settings that were never overridden. That ownership is also what lets `on_write_failed` and
/// `on_write_complete` keep fanning out to both halves after a best-effort start failure — the
/// half that failed has already closed its own cycle, so its cleanup short-circuits.
async fn compound_on_write_start(
    primary: &dyn SearchIndex,
    secondary: &dyn SearchIndex,
    window: WriteWindow,
) -> Result<(), DataFusionError> {
    let primary_started = match primary.on_write_start(window).await {
        Ok(()) => true,
        Err(primary_err) if primary.write_start_failure_is_fatal() => return Err(primary_err),
        Err(primary_err) => {
            tracing::warn!(
                "The primary index of a compound search index failed to start a write: {primary_err}. Continuing with the write, because that index's start is best-effort."
            );
            false
        }
    };

    if let Err(secondary_err) = secondary.on_write_start(window).await {
        if !secondary.write_start_failure_is_fatal() {
            tracing::warn!(
                "The secondary index of a compound search index failed to start a write: {secondary_err}. Continuing with the write, because that index's start is best-effort."
            );
            return Ok(());
        }

        // The write is being abandoned, so the primary's window has to close with it — but only
        // if it opened. A primary whose own start failed owns its cleanup (see above), which is
        // why `primary_started` is tested before the call and not after it.
        if primary_started && let Err(rollback_err) = primary.on_write_failed().await {
            tracing::warn!(
                "Failed to roll back the primary index of a compound search index after the secondary index failed to start a write: {rollback_err}"
            );
        }
        return Err(secondary_err);
    }
    Ok(())
}

/// What a compound index reports from [`Index::write_start_failure_is_fatal`].
///
/// Always `true`: [`compound_on_write_start`] has already applied each half's own policy to its
/// own failure and swallowed the best-effort ones, so any error it *does* return came from a half
/// that declares a start failure fatal. Answering from the two halves' flags instead — under
/// either combining rule — misclassifies one of them, as [`compound_on_write_start`] describes.
pub(super) const COMPOUND_WRITE_START_FAILURE_IS_FATAL: bool = true;

/// Finalize both indexes, applying each half's own [`Index::write_complete_failure_is_fatal`] to
/// *that half's* failure.
///
/// Both completion callbacks always run — a failure on one half must not skip the other's
/// finalize — and the primary's error is surfaced first. Fatality is then decided per half for the
/// same reason [`compound_on_write_start`] decides it per half: one combined answer cannot say
/// which half failed, so a fatal half turns the *other* half's best-effort finalize failure into a
/// failed write. Elasticsearch's `_forcemerge` beside a tantivy primary is the live pairing —
/// force-merge is a segment-count optimization the indexed rows do not depend on, but the tantivy
/// half declares its own commit fatal, so the combined answer failed the write whenever
/// force-merge did.
async fn compound_on_write_complete(
    primary: &dyn Index,
    secondary: &dyn Index,
) -> Result<(), DataFusionError> {
    let (primary_result, secondary_result) =
        futures::join!(primary.on_write_complete(), secondary.on_write_complete());
    let primary_outcome = finalize_outcome("primary", primary, primary_result);
    let secondary_outcome = finalize_outcome("secondary", secondary, secondary_result);
    primary_outcome.and(secondary_outcome)
}

/// Keep `result` only if `index` declares its own finalize failure fatal; otherwise log it and
/// report success — what the sink does with that same flag on a standalone index.
fn finalize_outcome(
    half: &str,
    index: &dyn Index,
    result: Result<(), DataFusionError>,
) -> Result<(), DataFusionError> {
    match result {
        Err(err) if !index.write_complete_failure_is_fatal() => {
            tracing::warn!(
                "The {half} index of a compound search index failed to finalize a write: {err}. Reporting the write as successful, because that index's finalize is best-effort."
            );
            Ok(())
        }
        result => result,
    }
}

/// What a compound index reports from [`Index::write_complete_failure_is_fatal`].
///
/// Always `true`, for the reason [`COMPOUND_WRITE_START_FAILURE_IS_FATAL`] is:
/// [`compound_on_write_complete`] has already applied each half's own policy to its own failure.
pub(super) const COMPOUND_WRITE_COMPLETE_FAILURE_IS_FATAL: bool = true;

/// Delete `keys` from both indexes (full/both-scope, per [`Index::delete_by_keys`]'s contract).
/// Both deletes run concurrently and both are driven to completion even if one fails, matching
/// [`compound_on_write_start`]'s "neither index left inconsistent" approach.
async fn compound_delete_by_keys(
    primary: &dyn Index,
    secondary: &dyn Index,
    keys: RecordBatch,
) -> DataFusionResult<()> {
    let (primary_result, secondary_result) = futures::join!(
        primary.delete_by_keys(keys.clone()),
        secondary.delete_by_keys(keys)
    );
    primary_result.and(secondary_result)
}
