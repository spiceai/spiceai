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
//! A [`CompoundIndex`] pairs a primary index A with a secondary index B:
//!
//!  - **Writes** go to both A and B (writethrough).
//!  - **List & query** are served from A. With
//!    [`CompoundReadMode::FallbackToSecondary`], a list/query that returns zero rows from A
//!    is retried against B at execution time.
//!
//! [`CompoundSearchIndex`] and [`CompoundVectorIndex`] are the trait-object aliases used when
//! the underlying index types are only known at runtime.
//!
//! The two indexes must be compatible: same search column, same primary-key fields, and the
//! same trait variant (both plain [`SearchIndex`]es, or both [`VectorIndex`]es with equal
//! embedding dimensions). Compatibility is validated at construction and violations return a
//! structured [`Error`].

mod fallback;
mod index;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_schema::{ArrowError, Field};
use itertools::Itertools;
use snafu::{Snafu, ensure};

pub use index::CompoundIndex;

use crate::index::{SearchIndex, VectorIndex};

/// A [`CompoundIndex`] over two dynamically-typed [`SearchIndex`]es.
pub type CompoundSearchIndex = CompoundIndex<dyn SearchIndex, dyn SearchIndex>;

/// A [`CompoundIndex`] over two dynamically-typed [`VectorIndex`]es.
pub type CompoundVectorIndex = CompoundIndex<dyn VectorIndex, dyn VectorIndex>;

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
fn validate_compatibility<A, B>(primary: &Arc<A>, secondary: &Arc<B>) -> Result<(), Error>
where
    A: SearchIndex + ?Sized,
    B: SearchIndex + ?Sized,
{
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
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
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
