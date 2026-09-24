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

//! Internal contracts for Cayenne's in-memory covering indexes.
//!
//! This module deliberately contains no planner hook, page builder, or durable
//! representation. It is the single vocabulary used by those later owners so
//! a source/page reference, a key encoding, and a page lease retain identical
//! meanings throughout the implementation.
//!
//! # Contract map
//!
//! - `SourceId`, `KeyPageId`, `PayloadPageId`, and `CoveredRowRef` make every physical
//!   address generation-qualified.
//! - `SchemaIdentity` and `IndexDefinition` capture the exact schema, canonical
//!   key-column ordering, stored Arrow fields, and key codec version.
//! - `KeyPage`, `PayloadPage`, `PageLease`, and `CoveringPageStore` retain immutable
//!   values and their memory pins.
//! - `IndexCatalog`, `CoveringReadView`, `CoverageDecision`, `ProbeRequest`,
//!   `ProbeMatch`, and `ProbeCursor` are the complete plan/probe vocabulary. No
//!   later owner should make an alternate equivalent.
//!
//! # Key eligibility
//!
//! | Stored type and expression-domain type | Decision |
//! | --- | --- |
//! | Identical supported `RowConverter` type | Eligible |
//! | `Utf8` and `Utf8View`, in either direction | Eligible |
//! | `Binary` and `BinaryView`, in either direction | Eligible |
//! | Floating point as a key | Rejected by existing key resolution |
//! | Any numeric, decimal-scale, timezone, or arbitrary cast adaptation | Declined |
//! | Unsupported row-converter type | Declined |
//!
//! A NULL in any component makes that one full equality tuple ineligible; it
//! does not remove the stored row from another index. Payload type eligibility
//! is independent from key eligibility.
//!
//! # Error boundary
//!
//! A missing page, malformed offset, source/page mismatch, incompatible schema,
//! or checked arithmetic overflow is a typed `Error`, never an empty probe. Only
//! a proven optional-path refusal becomes `CoverageDecision::Unavailable`. The
//! page traversal not introduced by this contracts step returns typed
//! `Error::NotImplemented`, never a placeholder panic.

#![expect(
    dead_code,
    unused_imports,
    reason = "these contracts are introduced before their builder, publisher, and executor steps"
)]

mod build;
mod directory;
mod exec;
mod executor;
mod join;
mod key;
mod page_store;
mod view;
mod visibility;

#[cfg(test)]
mod tests;

pub(crate) use build::{
    BuiltCoveredSource, CoveringIndexState, IncrementalCoveringIndexBuilder, build_source,
    build_sources,
};
pub(crate) use directory::{
    IndexRun, KeyDirectory, KeyDirectoryEntry, LiteralSeekSpan, PreparedLiteralSeek, RunId,
};
pub(crate) use exec::{CayenneIndexScanExec, CoveringIndexAccess, CoveringIndexCapability};
pub(crate) use executor::CayenneIndexExecutor;
pub(crate) use join::{CayenneIndexJoinExec, IndexJoinMapping};
pub(crate) use key::{
    CoveredRowRef, EncodedKey, IndexColumn, IndexDefinition, KEY_CODEC_VERSION, KeyPageId,
    PayloadPageId, SchemaIdentity, SourceId,
};
pub(crate) use page_store::{
    AllocationOwner, CoveringPageStore, KeyPage, KeyPageLease, MemoryPageStore, PageLease,
    PayloadPage, PayloadPageLease, ReservationToken,
};
pub(crate) use view::{
    CapturedSource, CoverageDecision, CoverageUnavailableReason, CoveringIndexCatalog,
    CoveringReadView, GatherBatch, IndexCatalog, IndexedSource, ProbeCursor, ProbeMatch,
    ProbeRequest, ProbeStep, SourceRows, gather, gather_stored, prepare_literal_seek, probe_many,
    probe_prepared_literal, try_cover,
};
pub(crate) use visibility::{PrimaryKeyLayout, SourceRole, VisibilityAdapter};

use snafu::prelude::*;

/// Result type for covering-index contract operations.
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that indicate broken contracts rather than an optional-path refusal.
#[derive(Debug, Snafu)]
pub(crate) enum Error {
    /// A source, page, row, or schema invariant was violated.
    #[snafu(display("Invalid covering-index contract: {message}"))]
    InvalidContract {
        /// A precise invariant failure for developers.
        message: String,
    },

    /// An input could not be adapted without changing equality semantics.
    #[snafu(display("Unsupported covering-index key adaptation from {from} to {to}"))]
    UnsupportedKeyAdaptation {
        /// The expression-domain type.
        from: String,
        /// The stored index type.
        to: String,
    },

    /// A required page did not exist in the admitted catalog.
    #[snafu(display("Covering-index page {page} is not available"))]
    MissingPage {
        /// Debug representation of the requested generation-qualified page.
        page: String,
    },

    /// Checked byte, offset, or count arithmetic overflowed.
    #[snafu(display("Covering-index {operation} overflowed"))]
    Overflow {
        /// The checked operation that overflowed.
        operation: &'static str,
    },

    /// A future backend operation has no implementation in this contracts step.
    #[snafu(display("Covering-index operation is not implemented: {operation}"))]
    NotImplemented {
        /// The unavailable operation.
        operation: &'static str,
    },

    /// Optional covering work could not be admitted or completed.
    #[snafu(display("Covering-index work is unavailable: {operation}"))]
    Unavailable {
        /// The construction, executor, or allocation operation that declined.
        operation: String,
    },

    /// A background build was cancelled before publication.
    #[snafu(display("Covering-index build was cancelled"))]
    Cancelled,

    /// Arrow rejected an array, schema, or record batch.
    #[snafu(transparent)]
    Arrow {
        /// The underlying Arrow error.
        source: arrow::error::ArrowError,
    },
}
