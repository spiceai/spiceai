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

//! Sources whose append refresh reads what changed since a cursor, instead of a time
//! window.
//!
//! An append refresh normally reads the rows whose `time_column` is after
//! `max(time_column) − refresh_append_overlap`, and loses a row that lands later with
//! an older time. A source that records which rows each of its commits added (an
//! Iceberg table through its snapshots, a Delta table through its versions) can instead
//! tell the refresh exactly what to read since the point it last loaded.
//! [`IncrementalAppendSource`] is that contract; the refresh task keeps the source's
//! cursor and runs the same keep-newest selection or exact-row dedupe over whatever
//! rows the source hands back, seeded from the [`SeedMark`] the source reports.
//!
//! The cursor is opaque text the source alone reads and writes, so one checkpoint
//! table serves every source, and a source refuses a cursor another one wrote.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use spice_table::{LayerWalk, find_concrete};
use util::timestamp_filter::TimestampFormat;

/// The dataset's `time_column`, as a source needs it: its name, and how the acceleration
/// stores it (`None` when the stored type is not one the refresh can compare a source's
/// bound with).
#[derive(Debug, Clone)]
pub struct TimeColumn {
    pub name: String,
    pub format: Option<TimestampFormat>,
}

/// Where the refresh's keep-newest selection, or its exact-row dedupe, starts reading the
/// rows the acceleration already holds: at the lowest `time_column` value the incoming
/// rows can hold, inclusive, or at every stored row when no bound is safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedMark {
    /// The lowest value the incoming rows can hold, in UTC nanoseconds since the epoch.
    Lowest(u128),
    /// Every stored row must be compared, for the reason given.
    AllStoredRows(&'static str),
}

impl std::fmt::Display for SeedMark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lowest(nanos) => write!(f, "lowest time bound {nanos}ns"),
            Self::AllStoredRows(reason) => write!(f, "all stored rows ({reason})"),
        }
    }
}

/// The read of the rows the source's commits added since the saved cursor.
#[derive(Debug, Clone)]
pub struct AddedRowsRead {
    /// The cursor to save once the rows are written.
    pub cursor: String,
    /// Reads the added rows; registered in place of the dataset's own provider, so the
    /// dataset's `refresh_sql` applies to it unchanged.
    pub provider: Arc<dyn TableProvider>,
    /// Whether the commits added no row at all: nothing to read, and nothing a replay
    /// could lose, so the cursor can move at once.
    pub is_empty: bool,
    /// `None` when the dataset has no `time_column`.
    pub seed: Option<SeedMark>,
}

/// What one append refresh reads, decided by the source from the saved cursor.
///
/// The reads that go through the dataset's own scan carry a provider pinned to the
/// commit the cursor names, so a commit that lands during the read waits for the next
/// refresh and the saved cursor names what was read.
#[derive(Debug, Clone)]
pub enum AppendPlan {
    /// The source is at the saved cursor: nothing to read.
    Unchanged,
    /// Read the source as the append refresh does by time, then save `cursor`: the
    /// first refresh with no cursor, or an interval the source cannot express as added
    /// rows. Both are `None` when the source has no commit to pin or save.
    TimeFiltered {
        cursor: Option<String>,
        source: Option<Arc<dyn TableProvider>>,
    },
    /// Reload the whole source as an overwrite, then save `cursor`.
    FullReload {
        cursor: Option<String>,
        source: Option<Arc<dyn TableProvider>>,
        reason: String,
    },
    /// Read the rows the commits since the cursor added.
    AddedRows(AddedRowsRead),
}

/// A source that can say what its append refresh should read since a saved cursor.
#[async_trait]
pub trait IncrementalAppendSource: Send + Sync {
    /// Decides what the append refresh of `dataset` reads, given the cursor the refresh
    /// last saved (`None` on the first refresh, or after the acceleration was rebuilt)
    /// and the dataset's time column. A cursor this source cannot read is treated as
    /// none.
    ///
    /// Reads the source's metadata only; the plan's providers read the data.
    async fn plan_append(
        &self,
        dataset: &str,
        saved_cursor: Option<&str>,
        time_column: Option<&TimeColumn>,
    ) -> DFResult<AppendPlan>;
}

/// The [`IncrementalAppendSource`] beneath `provider`, through any layer the runtime stacked
/// on it (metadata enrichment, federation, deletion, cluster routing), or `None` when the
/// dataset's source does not record its commits.
///
/// Each source that implements the contract is named here, as the refresh-skip check
/// names its providers: a `dyn TableProvider` cannot be upcast to another trait object.
#[must_use]
pub fn incremental_append_source(provider: &dyn TableProvider) -> Option<&dyn IncrementalAppendSource> {
    if let Some(iceberg) =
        find_concrete::<iceberg_datafusion::IcebergTableProvider>(provider, LayerWalk::Read)
    {
        return Some(iceberg);
    }
    None
}
