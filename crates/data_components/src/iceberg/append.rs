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

//! Append refresh that follows Iceberg snapshots.
//!
//! An append refresh of an Iceberg table used to read the rows whose `time_column` is
//! after `max(time_column) − refresh_append_overlap`. A writer that commits rows out of
//! time order (several shards, one of them behind) then leaves rows behind for good.
//! Iceberg already records which data files each commit added, so the refresh can
//! follow the table's snapshots instead: it saves the snapshot it last loaded (the
//! [`AppendCursor`]), and the next refresh reads the data files that the snapshots
//! committed since then added, whatever their time values.
//!
//! The planner (the [`IncrementalAppendSource`] implementation for the Iceberg catalog
//! provider) decides what one refresh reads. [`AddedFilesProvider`] reads the added
//! files as of their own snapshots, so a compaction that already rewrote them does not
//! hide them. Which rows of that read are kept, against the rows the acceleration
//! already holds, is decided by the refresh's existing keep-newest or exact-row
//! dedupe, seeded from the [`SeedMark`] this module reports.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use futures::future::try_join_all;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataContentType, Datum, ManifestContentType, ManifestEntryRef, ManifestFile, ManifestStatus,
    NameMapping, Operation, PartitionSpecRef, PrimitiveLiteral, PrimitiveType, Schema, SnapshotRef,
    TableMetadata,
};
use iceberg::table::Table;
use iceberg_datafusion::IcebergTableProvider;
use iceberg_datafusion::physical_plan::IcebergTableScan;
use iceberg_datafusion::to_datafusion_error;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use util::timestamp_filter::TimestampFormat;

use crate::incremental_append::{
    AddedRowsRead, AppendPlan, Fallback, IncrementalAppendSource, PinnedRead, RewrittenRows,
    SeedMark, TimeColumn,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load Iceberg table '{table}': {source}"))]
    LoadTable {
        table: String,
        #[snafu(source(from(iceberg::Error, Box::new)))]
        source: Box<iceberg::Error>,
    },

    #[snafu(display(
        "Failed to read the manifest list of Iceberg snapshot {snapshot_id} of table '{table}': {source}"
    ))]
    ManifestList {
        table: String,
        snapshot_id: i64,
        #[snafu(source(from(iceberg::Error, Box::new)))]
        source: Box<iceberg::Error>,
    },

    #[snafu(display(
        "Failed to read Iceberg manifest '{path}' of snapshot {snapshot_id} of table '{table}': {source}"
    ))]
    Manifest {
        table: String,
        snapshot_id: i64,
        path: String,
        #[snafu(source(from(iceberg::Error, Box::new)))]
        source: Box<iceberg::Error>,
    },

    #[snafu(display("The saved Iceberg append cursor cannot be read: {source}"))]
    CursorFormat { source: serde_json::Error },

    #[snafu(display(
        "The saved append cursor was written for source '{source_kind}' version {version}, not for an Iceberg table"
    ))]
    CursorSource { source_kind: String, version: u32 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The Iceberg snapshot an append refresh last loaded, with the UUID of the table it
/// belongs to so a table dropped and recreated under the same name is told apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendCursor {
    pub table_uuid: String,
    pub snapshot_id: i64,
}

/// The stored form of [`AppendCursor`]: versioned and tagged with its source kind, so a
/// later source can share the checkpoint table and an older runtime refuses a newer
/// layout instead of misreading it.
#[derive(Serialize, Deserialize)]
struct StoredCursor {
    version: u32,
    source: String,
    table_uuid: String,
    snapshot_id: i64,
}

const CURSOR_VERSION: u32 = 1;
const CURSOR_SOURCE: &str = "iceberg";

impl AppendCursor {
    /// The JSON this cursor is checkpointed as.
    ///
    /// # Panics
    ///
    /// Never: the stored form holds only a string and integers, which always serialize.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string(&StoredCursor {
            version: CURSOR_VERSION,
            source: CURSOR_SOURCE.to_string(),
            table_uuid: self.table_uuid.clone(),
            snapshot_id: self.snapshot_id,
        })
        .unwrap_or_else(|e| unreachable!("a cursor of plain fields serializes: {e}"))
    }

    /// Reads a cursor [`Self::to_json`] wrote.
    ///
    /// # Errors
    ///
    /// Returns an error when the text is not a cursor, or was written for another
    /// source kind or a newer layout.
    pub fn from_json(json: &str) -> Result<Self> {
        let stored: StoredCursor = serde_json::from_str(json).context(CursorFormatSnafu)?;
        if stored.version != CURSOR_VERSION || stored.source != CURSOR_SOURCE {
            return CursorSourceSnafu {
                source_kind: stored.source,
                version: stored.version,
            }
            .fail();
        }
        Ok(Self {
            table_uuid: stored.table_uuid,
            snapshot_id: stored.snapshot_id,
        })
    }
}

/// Why an append refresh reads the table by time instead of following its snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FallbackReason {
    /// The table was dropped and recreated under the same name.
    TableReplaced {
        saved_uuid: String,
        current_uuid: String,
    },
    /// The saved snapshot is no longer an ancestor of the current one: a snapshot
    /// between them expired, the table was rolled back, or the branch changed.
    SnapshotNotAncestor { saved: i64, current: i64 },
    /// The table has no snapshot at all any more.
    TableHasNoSnapshot { saved: i64 },
    /// A snapshot since the saved one rewrote rows, and the dataset has no primary key
    /// to tell a rewritten copy from a new row.
    RowsRewritten { saved: i64, walked: WalkedSnapshots },
}

impl FallbackReason {
    /// The `reason` label of the fallback metric.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::TableReplaced { .. } => "table_replaced",
            Self::SnapshotNotAncestor { .. } => "snapshot_not_ancestor",
            Self::TableHasNoSnapshot { .. } => "table_has_no_snapshot",
            Self::RowsRewritten { .. } => "rows_rewritten_without_primary_key",
        }
    }

    fn into_fallback(self) -> Fallback {
        Fallback {
            reason: self.code(),
            detail: self.to_string(),
        }
    }
}

impl std::fmt::Display for FallbackReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableReplaced {
                saved_uuid,
                current_uuid,
            } => write!(
                f,
                "the Iceberg table was replaced (its UUID changed from {saved_uuid} to {current_uuid}), so rows of the old table stay in the acceleration"
            ),
            Self::SnapshotNotAncestor { saved, current } => write!(
                f,
                "the last loaded Iceberg snapshot {saved} is no longer an ancestor of the current snapshot {current} (a snapshot between them expired, the table was rolled back, or it is on another branch), so rows removed at the source stay in the acceleration"
            ),
            Self::TableHasNoSnapshot { saved } => write!(
                f,
                "the Iceberg table has no snapshot any more (the last loaded one was {saved}), so its rows stay in the acceleration"
            ),
            Self::RowsRewritten { saved, walked } => write!(
                f,
                "{walked} since the last loaded Iceberg snapshot {saved}, and an overwrite rewrote rows, which a dataset without a primary key cannot tell from new rows"
            ),
        }
    }
}

/// The snapshots one refresh walked, counted by the operation each was classified as.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalkedSnapshots {
    pub append: usize,
    pub replace: usize,
    pub delete: usize,
    pub overwrite: usize,
}

impl WalkedSnapshots {
    #[must_use]
    pub fn total(self) -> usize {
        self.append + self.replace + self.delete + self.overwrite
    }
}

impl std::fmt::Display for WalkedSnapshots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} new Iceberg snapshot(s) ({} append, {} replace, {} delete, {} overwrite)",
            self.total(),
            self.append,
            self.replace,
            self.delete,
            self.overwrite
        )
    }
}

/// A data file a snapshot in the interval added, with the partition spec its manifest
/// was written under and the position deletes that same snapshot added.
#[derive(Debug, Clone)]
pub struct AddedFile {
    pub entry: ManifestEntryRef,
    pub partition_spec: PartitionSpecRef,
    /// Empty for a file an `append` added. For a file an overwrite added, the position
    /// deletes (and deletion vectors) of that commit: a row it wrote and deleted in one
    /// commit was never in the table. The reader applies each to the file it names.
    pub deletes: Arc<Vec<FileScanTaskDeleteFile>>,
}

fn pinned(provider: &IcebergTableProvider, snapshot_id: i64) -> Arc<dyn TableProvider> {
    Arc::new(provider.clone().with_snapshot_id(Some(snapshot_id)))
}

#[async_trait]
impl IncrementalAppendSource for IcebergTableProvider {
    async fn plan_append(
        &self,
        dataset: &str,
        saved_cursor: Option<&str>,
        time_column: Option<&TimeColumn>,
        rewritten_rows: RewrittenRows,
    ) -> DFResult<AppendPlan> {
        let cursor = saved_cursor.and_then(|json| match AppendCursor::from_json(json) {
            Ok(cursor) => Some(cursor),
            Err(e) => {
                tracing::debug!(
                    "Dataset '{dataset}' ignores its saved append cursor and reads the table by time: {e}"
                );
                None
            }
        });
        plan_append(dataset, self, cursor.as_ref(), time_column, rewritten_rows)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn pin_current(&self) -> DFResult<Option<PinnedRead>> {
        let table = load_table(self)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(pinned_read(
            self,
            &table,
            table.metadata().current_snapshot_id(),
        ))
    }
}

async fn load_table(provider: &IcebergTableProvider) -> Result<Table> {
    let ident = provider.table_ident();
    provider
        .catalog()
        .load_table(ident)
        .await
        .with_context(|_| LoadTableSnafu {
            table: ident.to_string(),
        })
}

/// `provider` pinned to `snapshot_id` of `table`, and the cursor naming it.
fn pinned_read(
    provider: &IcebergTableProvider,
    table: &Table,
    snapshot_id: Option<i64>,
) -> Option<PinnedRead> {
    let snapshot_id = snapshot_id?;
    Some(PinnedRead {
        cursor: AppendCursor {
            table_uuid: table.metadata().uuid().to_string(),
            snapshot_id,
        }
        .to_json(),
        provider: pinned(provider, snapshot_id),
    })
}

/// Decides what the append refresh of `dataset` reads.
///
/// Loads the table from its catalog once (no data is read), finds the snapshots
/// between the saved cursor and the current snapshot by walking parent pointers, and
/// classifies each from the manifests it added (see [`classify_interval`]).
///
/// # Errors
///
/// Returns an error when the catalog or a manifest cannot be read.
pub async fn plan_append(
    dataset: &str,
    provider: &IcebergTableProvider,
    cursor: Option<&AppendCursor>,
    time_column: Option<&TimeColumn>,
    rewritten_rows: RewrittenRows,
) -> Result<AppendPlan> {
    let table = load_table(provider).await?;
    let metadata = table.metadata();
    let table_uuid = metadata.uuid().to_string();
    let current = metadata.current_snapshot_id();

    let Some(saved) = cursor else {
        tracing::debug!(
            "Dataset '{dataset}' has no Iceberg append cursor; its append refresh reads the table by time as of snapshot {current:?} and saves it"
        );
        return Ok(AppendPlan::TimeFiltered {
            pinned: pinned_read(provider, &table, current),
            fallback: None,
        });
    };

    // A cursor the planner cannot follow: the refresh reads by time once, as it would
    // without a cursor, and follows snapshots again from the current one.
    let fall_back = |reason: FallbackReason| AppendPlan::TimeFiltered {
        pinned: pinned_read(provider, &table, current),
        fallback: Some(reason.into_fallback()),
    };

    if saved.table_uuid != table_uuid {
        return Ok(fall_back(FallbackReason::TableReplaced {
            saved_uuid: saved.table_uuid.clone(),
            current_uuid: table_uuid.clone(),
        }));
    }

    let Some(current) = current else {
        return Ok(fall_back(FallbackReason::TableHasNoSnapshot {
            saved: saved.snapshot_id,
        }));
    };

    if current == saved.snapshot_id {
        tracing::debug!(
            "Dataset '{dataset}' is at Iceberg snapshot {current}, the snapshot its append refresh last loaded; nothing to read"
        );
        return Ok(AppendPlan::Unchanged);
    }

    let Some(interval) = snapshots_between(metadata, saved.snapshot_id, current) else {
        return Ok(fall_back(FallbackReason::SnapshotNotAncestor {
            saved: saved.snapshot_id,
            current,
        }));
    };

    let (walked, added) = match classify_interval(&table, &interval, rewritten_rows).await? {
        Interval::Added { walked, files } => (walked, files),
        Interval::Rewritten { walked } => {
            return Ok(fall_back(FallbackReason::RowsRewritten {
                saved: saved.snapshot_id,
                walked,
            }));
        }
    };

    let cursor = AppendCursor {
        table_uuid: table_uuid.clone(),
        snapshot_id: current,
    }
    .to_json();
    let seed = time_column.map(|time| seed_mark(&added, time, metadata.current_schema()));
    let name_mapping = metadata
        .table_properties()
        .default_name_mapping()
        .with_context(|_| LoadTableSnafu {
            table: provider.table_ident().to_string(),
        })?
        .map(Arc::new);
    let reader = AddedFilesProvider {
        table,
        snapshot_id: current,
        schema: provider.schema(),
        files: added,
        name_mapping,
    };
    tracing::info!(
        "Dataset '{dataset}' follows {walked} since snapshot {}: reading {} data file(s) ({} row(s))",
        saved.snapshot_id,
        reader.files.len(),
        reader.record_count(),
    );
    tracing::debug!(
        "Dataset '{dataset}' reads the added files pinned to Iceberg snapshot {current}, comparing stored rows from {}",
        seed.map_or_else(|| "no time column".to_string(), |seed| seed.to_string())
    );
    Ok(AppendPlan::AddedRows(AddedRowsRead {
        cursor,
        is_empty: reader.is_empty(),
        provider: Arc::new(reader),
        seed,
    }))
}

/// The snapshots after `saved` up to and including `current`, oldest first, found by
/// walking parent pointers from `current`. `None` when `saved` is not reached: a
/// snapshot between the two expired, or `current` is on another line of history.
///
/// `saved` itself may have expired: its child's parent pointer still names it, and the
/// snapshots after it are exactly the chain's, so following goes on. Only a gap inside
/// the chain, or a chain that ends elsewhere, breaks it.
///
/// Written here rather than with `ancestors_between`, which walks to the root when
/// `saved` is missing and so cannot tell that case apart.
#[must_use]
pub fn snapshots_between(
    metadata: &TableMetadata,
    saved: i64,
    current: i64,
) -> Option<Vec<SnapshotRef>> {
    let mut interval = Vec::new();
    let mut visited = HashSet::new();
    let mut next = Some(current);
    while let Some(id) = next {
        if id == saved {
            interval.reverse();
            return Some(interval);
        }
        // A parent pointer naming a snapshot the metadata no longer holds is an
        // expired snapshot; a repeated id would be corrupt metadata. Both end the walk.
        if !visited.insert(id) {
            return None;
        }
        let snapshot = metadata.snapshot_by_id(id)?;
        next = snapshot.parent_snapshot_id();
        interval.push(Arc::clone(snapshot));
    }
    None
}

/// What [`classify_interval`] found.
enum Interval {
    /// The data files to read.
    Added {
        walked: WalkedSnapshots,
        files: Vec<AddedFile>,
    },
    /// A snapshot rewrote rows that the refresh cannot tell from new ones.
    Rewritten { walked: WalkedSnapshots },
}

/// What one snapshot's manifests show.
#[derive(Default)]
struct SnapshotFiles {
    added: Vec<AddedFile>,
    /// It removed a data file or added a delete file.
    removed_rows: bool,
    position_deletes: Vec<FileScanTaskDeleteFile>,
}

/// Classifies every snapshot of `interval` from the manifests it added, and collects
/// the data files the refresh reads. Each snapshot is decided on its own, so a commit
/// that cannot be read as added rows does not hide the appends around it:
///
/// - `replace` (by its summary) is skipped: a compaction or manifest rewrite changes no
///   row.
/// - A snapshot that added no data file only deleted rows, which an append refresh does
///   not apply: skipped.
/// - A snapshot that added data files and removed none is an append: its files are read.
/// - A snapshot that added data files and also removed files or added delete files
///   rewrote rows (an overwrite). Its files hold copies of stored rows beside new ones.
///   With [`RewrittenRows::KeepNewest`] they are read with that commit's own position
///   deletes, and keep-newest drops the copies; otherwise the interval is read by time.
///
/// The manifest lists are read first, and the manifests only where the lists leave the
/// outcome open; each pass reads concurrently.
async fn classify_interval(
    table: &Table,
    interval: &[SnapshotRef],
    rewritten_rows: RewrittenRows,
) -> Result<Interval> {
    let table_name = table.identifier().to_string();
    let table_name = &table_name;
    let mut walked = WalkedSnapshots::default();

    let lists = try_join_all(
        interval
            .iter()
            // A compaction or manifest rewrite changes no row, and reading its manifests
            // would only show the files it moved; the summary is trusted for this one case.
            .filter(|snapshot| snapshot.summary().operation != Operation::Replace)
            .map(|snapshot| async move {
                let snapshot_id = snapshot.snapshot_id();
                let list = table
                    .manifest_list_reader(snapshot)
                    .load()
                    .await
                    .with_context(|_| ManifestListSnafu {
                        table: table_name.clone(),
                        snapshot_id,
                    })?;
                let added: Vec<ManifestFile> = list
                    .entries()
                    .iter()
                    .filter(|manifest| manifest.added_snapshot_id == snapshot_id)
                    .cloned()
                    .collect();
                Ok((snapshot_id, added))
            }),
    )
    .await?;
    walked.replace = interval.len() - lists.len();

    let mut to_read = Vec::new();
    for (snapshot_id, manifests) in lists {
        // A manifest list without file counts (an old writer) leaves both open until
        // the manifests are read.
        let adds_no_data = manifests
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .all(|m| m.added_files_count == Some(0));
        let removes = manifests.iter().any(|m| {
            m.content == ManifestContentType::Deletes
                || m.deleted_files_count.is_some_and(|count| count > 0)
        });
        let adds_data = manifests.iter().any(|m| {
            m.content == ManifestContentType::Data
                && m.added_files_count.is_some_and(|count| count > 0)
        });
        if adds_no_data {
            walked.delete += 1;
        } else if adds_data && removes && rewritten_rows == RewrittenRows::ReadByTime {
            walked.overwrite += 1;
            return Ok(Interval::Rewritten { walked });
        } else {
            to_read.push((snapshot_id, manifests));
        }
    }

    let reads = to_read.iter().flat_map(|(snapshot_id, manifests)| {
        manifests
            .iter()
            .filter(|m| {
                // Delete manifests matter only for the position deletes an overwrite's
                // own files are read with.
                m.content == ManifestContentType::Data
                    || rewritten_rows == RewrittenRows::KeepNewest
            })
            .map(move |manifest_file| async move {
                table
                    .manifest_reader()
                    .read(manifest_file)
                    .await
                    .map(|manifest| (*snapshot_id, manifest_file, manifest))
                    .with_context(|_| ManifestSnafu {
                        table: table_name.clone(),
                        snapshot_id: *snapshot_id,
                        path: manifest_file.manifest_path.clone(),
                    })
            })
    });
    let manifests = try_join_all(reads).await?;

    let mut by_snapshot: std::collections::HashMap<i64, SnapshotFiles> =
        std::collections::HashMap::new();
    for (snapshot_id, manifest_file, manifest) in &manifests {
        let files = by_snapshot.entry(*snapshot_id).or_default();
        if manifest_file.content == ManifestContentType::Deletes {
            files.removed_rows = true;
        }
        let partition_spec: PartitionSpecRef =
            Arc::new(manifest.metadata().partition_spec().clone());
        for entry in manifest.entries() {
            match (entry.status(), entry.content_type()) {
                // A manifest list without file counts shows a removed file only here.
                (ManifestStatus::Deleted, _) => files.removed_rows = true,
                (ManifestStatus::Added, DataContentType::Data) if entry.record_count() > 0 => {
                    files.added.push(AddedFile {
                        entry: Arc::clone(entry),
                        partition_spec: Arc::clone(&partition_spec),
                        deletes: Arc::default(),
                    });
                }
                (ManifestStatus::Added, DataContentType::PositionDeletes) => {
                    files.removed_rows = true;
                    files
                        .position_deletes
                        .push(delete_file(entry, manifest_file.partition_spec_id));
                }
                (ManifestStatus::Added, DataContentType::EqualityDeletes) => {
                    // An equality delete applies only to data files of a lower sequence
                    // number, so never to the files its own commit added.
                    files.removed_rows = true;
                }
                // An existing entry was carried over from an older manifest, and an
                // empty file holds nothing to read.
                (ManifestStatus::Added | ManifestStatus::Existing, _) => {}
            }
        }
    }

    let mut added = Vec::new();
    for (snapshot_id, _) in &to_read {
        let files = by_snapshot.remove(snapshot_id).unwrap_or_default();
        if files.added.is_empty() {
            walked.delete += 1;
        } else if files.removed_rows {
            walked.overwrite += 1;
            if rewritten_rows == RewrittenRows::ReadByTime {
                return Ok(Interval::Rewritten { walked });
            }
            let deletes = Arc::new(files.position_deletes);
            added.extend(files.added.into_iter().map(|file| AddedFile {
                deletes: Arc::clone(&deletes),
                ..file
            }));
        } else {
            walked.append += 1;
            added.extend(files.added);
        }
    }
    Ok(Interval::Added {
        walked,
        files: added,
    })
}

/// The scan-task form of the position delete file (or deletion vector) `entry`.
fn delete_file(entry: &ManifestEntryRef, partition_spec_id: i32) -> FileScanTaskDeleteFile {
    let data_file = entry.data_file();
    FileScanTaskDeleteFile::builder()
        .with_file_path(entry.file_path().to_string())
        .with_file_size_in_bytes(entry.file_size_in_bytes())
        .with_file_type(entry.content_type())
        .with_file_format(entry.file_format())
        .with_partition_spec_id(partition_spec_id)
        .with_equality_ids(data_file.equality_ids())
        .with_referenced_data_file(data_file.referenced_data_file())
        .with_content_offset(data_file.content_offset())
        .with_content_size_in_bytes(data_file.content_size_in_bytes())
        .with_record_count(Some(entry.record_count()))
        .with_key_metadata(data_file.key_metadata().map(Box::from))
        .build()
}

/// Where keep-newest or the exact-row dedupe starts reading stored rows for `files`.
///
/// Any file without a usable lower bound for the time column, or with NULL times (a
/// NULL-time row must meet its key's stored timed version, which no bound covers),
/// widens the read to every stored row.
fn seed_mark(files: &[AddedFile], time: &TimeColumn, schema: &Schema) -> SeedMark {
    let format = match &time.format {
        // A string column's bounds compare as strings, which misorders values with
        // different UTC offsets, so they are never used as an instant bound.
        Some(TimestampFormat::Iso8601(_)) => return SeedMark::AllStoredRows("string time column"),
        Some(format) => format,
        None => return SeedMark::AllStoredRows("time column type not comparable"),
    };
    let Some(field_id) = schema.field_id_by_name(&time.name).or_else(|| {
        schema
            .field_by_name_case_insensitive(&time.name)
            .map(|field| field.id)
    }) else {
        return SeedMark::AllStoredRows("time column not in the Iceberg schema");
    };

    let mut lowest: Option<u128> = None;
    for file in files {
        let data_file = file.entry.data_file();
        match data_file.null_value_counts().get(&field_id) {
            Some(0) => {}
            Some(_) => return SeedMark::AllStoredRows("a file holds NULL time values"),
            None => return SeedMark::AllStoredRows("a file has no null count for the time column"),
        }
        let Some(bound) = data_file.lower_bounds().get(&field_id) else {
            return SeedMark::AllStoredRows("a file has no lower bound for the time column");
        };
        let Some(nanos) = bound_nanos(bound, format) else {
            return SeedMark::AllStoredRows("a file's time bound is not comparable as an instant");
        };
        lowest = Some(lowest.map_or(nanos, |low| low.min(nanos)));
    }
    match lowest {
        Some(nanos) => SeedMark::Lowest(nanos),
        // Nothing was added, so no stored row needs comparing; the highest possible
        // mark compares none.
        None => SeedMark::Lowest(u128::MAX),
    }
}

/// `bound` as UTC nanoseconds since the epoch, or `None` when it is not an instant
/// Spice can compare: a string, a float, a pre-1970 value, or an integer whose unit the
/// acceleration's format does not give.
fn bound_nanos(bound: &Datum, format: &TimestampFormat) -> Option<u128> {
    let scaled = |value: i64, scale: u128| u128::try_from(value).ok()?.checked_mul(scale);
    match (bound.data_type(), bound.literal(), format) {
        (
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
            PrimitiveLiteral::Long(micros),
            _,
        ) => scaled(*micros, 1_000),
        (
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
            PrimitiveLiteral::Long(nanos),
            _,
        ) => u128::try_from(*nanos).ok(),
        (PrimitiveType::Date, PrimitiveLiteral::Int(days), _) => {
            scaled(i64::from(*days), 86_400_000_000_000)
        }
        (
            PrimitiveType::Long,
            PrimitiveLiteral::Long(value),
            TimestampFormat::UnixTimestamp { scale },
        ) => scaled(*value, *scale),
        (
            PrimitiveType::Int,
            PrimitiveLiteral::Int(value),
            TimestampFormat::UnixTimestamp { scale },
        ) => scaled(i64::from(*value), *scale),
        _ => None,
    }
}

/// Reads the data files the interval's snapshots added, each as of the snapshot that
/// added it.
///
/// The scan tasks are built from the manifest entries the planner already read, so a
/// later `replace` (compaction) that removed those files from the current snapshot does
/// not hide them: the files themselves stay on storage until the snapshot that added
/// them expires. A later commit's deletes are not applied, since append refresh does
/// not remove rows; only an overwrite's own position deletes are (see [`AddedFile`]).
#[derive(Debug)]
pub struct AddedFilesProvider {
    table: Table,
    snapshot_id: i64,
    schema: ArrowSchemaRef,
    files: Vec<AddedFile>,
    name_mapping: Option<Arc<NameMapping>>,
}

impl AddedFilesProvider {
    /// Whether there is no file to read.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    fn record_count(&self) -> u64 {
        self.files.iter().map(|f| f.entry.record_count()).sum()
    }

    fn scan_tasks(&self, projection: Option<&Vec<usize>>) -> DFResult<Vec<FileScanTask>> {
        let iceberg_schema = Arc::clone(self.table.metadata().current_schema());
        let names: Vec<&str> = match projection {
            Some(indices) => indices
                .iter()
                .map(|&i| self.schema.field(i).name().as_str())
                .collect(),
            None => self
                .schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect(),
        };
        let field_ids = names
            .iter()
            .map(|name| {
                iceberg_schema.field_id_by_name(name).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Column '{name}' is not in the Iceberg table's current schema, so the append refresh cannot read it"
                    ))
                })
            })
            .collect::<DFResult<Vec<i32>>>()?;

        self.files
            .iter()
            .map(|file| {
                let entry = &file.entry;
                let data_file = entry.data_file();
                FileScanTask::builder()
                    .with_file_size_in_bytes(entry.file_size_in_bytes())
                    .with_start(0)
                    .with_length(entry.file_size_in_bytes())
                    .with_record_count(Some(entry.record_count()))
                    .with_first_row_id(data_file.first_row_id())
                    .with_data_sequence_number(entry.sequence_number())
                    .with_data_file_path(entry.file_path().to_string())
                    .with_data_file_format(entry.file_format())
                    .with_schema(Arc::clone(&iceberg_schema))
                    .with_project_field_ids(field_ids.clone())
                    .with_partition(Some(data_file.partition().clone()))
                    .with_partition_spec(Some(Arc::clone(&file.partition_spec)))
                    .with_name_mapping(self.name_mapping.clone())
                    .with_case_sensitive(true)
                    .with_key_metadata(data_file.key_metadata().map(Box::from))
                    .with_deletes(file.deletes.as_ref().clone())
                    .build()
                    .map_err(to_datafusion_error)
            })
            .collect()
    }
}

#[async_trait]
impl TableProvider for AddedFilesProvider {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // The tasks carry no predicate, so every filter is re-applied above the scan.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let tasks = self.scan_tasks(projection)?;
        // At least one partition, so an empty read still plans and executes.
        let partitions = state.config().target_partitions().min(tasks.len()).max(1);
        let mut buckets: Vec<Vec<FileScanTask>> = (0..partitions).map(|_| Vec::new()).collect();
        for (i, task) in tasks.into_iter().enumerate() {
            buckets[i % partitions].push(task);
        }
        Ok(Arc::new(IcebergTableScan::new_with_tasks(
            self.table.clone(),
            Some(self.snapshot_id),
            Arc::clone(&self.schema),
            projection,
            filters,
            limit,
            buckets,
            Partitioning::UnknownPartitioning(partitions),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::AsArray as _;
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::prelude::SessionContext;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

    use super::*;

    const NAMESPACE: &str = "append_ns";
    const TABLE: &str = "events";

    /// 2026-01-01T10:00:00Z in microseconds, the instant the rows below are stamped from.
    const BASE_MICROS: i64 = 1_767_261_600_000_000;

    /// An in-memory catalog with one table of `(id long, ts timestamp, note string)`.
    async fn catalog_with_table() -> (Arc<dyn Catalog>, TableIdent) {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory:/warehouse".to_string(),
                )]),
            )
            .await
            .expect("memory catalog loads");
        let namespace = NamespaceIdent::new(NAMESPACE.to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("namespace is created");
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
                NestedField::optional(3, "note", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema builds");
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(TABLE.to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("table is created");
        (
            Arc::new(catalog),
            TableIdent::new(namespace, TABLE.to_string()),
        )
    }

    async fn plan_for(
        dataset: &str,
        provider: &IcebergTableProvider,
        cursor: Option<&AppendCursor>,
        time_column: Option<&TimeColumn>,
    ) -> AppendPlan {
        plan_append(
            dataset,
            provider,
            cursor,
            time_column,
            RewrittenRows::KeepNewest,
        )
        .await
        .expect("plans")
    }

    fn cursor_of(json: &str) -> AppendCursor {
        AppendCursor::from_json(json).expect("the plan's cursor is an Iceberg cursor")
    }

    async fn provider_for(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> IcebergTableProvider {
        IcebergTableProvider::try_new(
            Arc::clone(catalog),
            ident.namespace().clone(),
            ident.name().to_string(),
        )
        .await
        .expect("provider is constructed")
    }

    /// Commits one `append` snapshot holding `rows` as `(id, seconds after BASE)`, with a
    /// NULL time for a negative offset.
    async fn append(catalog: &Arc<dyn Catalog>, ident: &TableIdent, rows: &[(i64, i64)]) -> i64 {
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider_for(catalog, ident).await))
            .expect("provider registers");
        let values = rows
            .iter()
            .map(|(id, offset)| {
                if *offset < 0 {
                    format!("({id}, NULL, 'row-{id}')")
                } else {
                    format!(
                        "({id}, arrow_cast({}, 'Timestamp(Microsecond, None)'), 'row-{id}')",
                        BASE_MICROS + offset * 1_000_000
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        ctx.sql(&format!("INSERT INTO t VALUES {values}"))
            .await
            .expect("append plans")
            .collect()
            .await
            .expect("append commits");
        current_snapshot(catalog, ident).await
    }

    async fn current_snapshot(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> i64 {
        catalog
            .load_table(ident)
            .await
            .expect("table loads")
            .metadata()
            .current_snapshot_id()
            .expect("the table has a snapshot")
    }

    async fn table_uuid(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> String {
        catalog
            .load_table(ident)
            .await
            .expect("table loads")
            .metadata()
            .uuid()
            .to_string()
    }

    fn temporal(name: &str) -> TimeColumn {
        TimeColumn {
            name: name.to_string(),
            format: Some(TimestampFormat::Timestamptz(None)),
        }
    }

    const UNIX_SECONDS: TimestampFormat = TimestampFormat::UnixTimestamp {
        scale: 1_000_000_000,
    };
    const UNIX_MILLIS: TimestampFormat = TimestampFormat::UnixTimestamp { scale: 1_000_000 };
    const UNIX_NANOS: TimestampFormat = TimestampFormat::UnixTimestamp { scale: 1 };

    /// `SELECT id FROM <provider> ORDER BY id`.
    async fn ids_read_by(provider: Arc<dyn TableProvider>) -> Vec<i64> {
        let ctx = SessionContext::new();
        ctx.register_table("added", provider)
            .expect("provider registers");
        let batches = ctx
            .sql("SELECT id FROM added ORDER BY id")
            .await
            .expect("scan plans")
            .collect()
            .await
            .expect("scan executes");
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|value| value.expect("id is not null"))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn no_cursor_is_an_initial_load_at_the_current_snapshot() {
        let (catalog, ident) = catalog_with_table().await;
        let provider = provider_for(&catalog, &ident).await;

        let plan = plan_for("events", &provider, None, Some(&temporal("ts"))).await;
        assert!(
            matches!(
                plan,
                AppendPlan::TimeFiltered {
                    pinned: None,
                    fallback: None
                }
            ),
            "an empty table has nothing to pin or save: {plan:?}"
        );

        let first = append(&catalog, &ident, &[(1, 0)]).await;
        let plan = plan_for("events", &provider, None, Some(&temporal("ts"))).await;
        let AppendPlan::TimeFiltered {
            pinned: Some(pinned),
            fallback: None,
        } = plan
        else {
            panic!("no cursor reads the table as today and saves the current snapshot: {plan:?}");
        };
        let cursor = cursor_of(&pinned.cursor);
        assert_eq!(cursor.snapshot_id, first);
        assert_eq!(cursor.table_uuid, table_uuid(&catalog, &ident).await);
    }

    #[tokio::test]
    async fn a_cursor_at_the_current_snapshot_reads_nothing() {
        let (catalog, ident) = catalog_with_table().await;
        let provider = provider_for(&catalog, &ident).await;
        let first = append(&catalog, &ident, &[(1, 0)]).await;
        let cursor = AppendCursor {
            table_uuid: table_uuid(&catalog, &ident).await,
            snapshot_id: first,
        };

        let plan = plan_for("events", &provider, Some(&cursor), Some(&temporal("ts"))).await;
        assert!(matches!(plan, AppendPlan::Unchanged), "{plan:?}");
    }

    /// The core of the change: the files later snapshots added are read whatever their
    /// time values, and the stored rows are compared from the lowest time those files
    /// hold, not from a high-water mark.
    #[tokio::test]
    async fn appends_since_the_cursor_are_read_from_their_own_files_with_the_lowest_time_bound() {
        let (catalog, ident) = catalog_with_table().await;
        let provider = provider_for(&catalog, &ident).await;
        let first = append(&catalog, &ident, &[(1, 100), (2, 200)]).await;
        // Two later commits, one of them with a row far older than anything stored.
        let second = append(&catalog, &ident, &[(3, 300)]).await;
        let third = append(&catalog, &ident, &[(4, 5), (5, 400)]).await;
        let cursor = AppendCursor {
            table_uuid: table_uuid(&catalog, &ident).await,
            snapshot_id: first,
        };

        let plan = plan_for("events", &provider, Some(&cursor), Some(&temporal("ts"))).await;
        let AppendPlan::AddedRows(read) = plan else {
            panic!("two appends since the cursor are read from their files: {plan:?}");
        };
        assert_eq!(
            cursor_of(&read.cursor).snapshot_id,
            third,
            "the cursor to save is T, after {second}"
        );
        assert!(!read.is_empty);
        let table = catalog.load_table(&ident).await.expect("table loads");
        let interval = snapshots_between(table.metadata(), first, third).expect("an interval");
        let Interval::Added {
            walked,
            files: added,
        } = classify_interval(&table, &interval, RewrittenRows::KeepNewest)
            .await
            .expect("the interval classifies")
        else {
            panic!("a pure-append interval is read from its files");
        };
        assert_eq!(
            walked,
            WalkedSnapshots {
                append: 2,
                replace: 0,
                delete: 0,
                overwrite: 0
            }
        );
        assert_eq!(
            (
                added.len(),
                added.iter().map(|f| f.entry.record_count()).sum::<u64>()
            ),
            (2, 3)
        );
        assert_eq!(
            read.seed,
            Some(SeedMark::Lowest(
                u128::try_from((BASE_MICROS + 5 * 1_000_000) * 1_000).expect("positive")
            )),
            "the mark is the lowest time any added file holds, not max − overlap"
        );
        assert_eq!(
            ids_read_by(read.provider).await,
            vec![3, 4, 5],
            "only the rows the later snapshots added are read"
        );
    }

    #[tokio::test]
    async fn a_file_with_null_times_compares_every_stored_row() {
        let (catalog, ident) = catalog_with_table().await;
        let provider = provider_for(&catalog, &ident).await;
        let first = append(&catalog, &ident, &[(1, 100)]).await;
        append(&catalog, &ident, &[(2, 200), (3, -1)]).await;
        let cursor = AppendCursor {
            table_uuid: table_uuid(&catalog, &ident).await,
            snapshot_id: first,
        };

        let plan = plan_for("events", &provider, Some(&cursor), Some(&temporal("ts"))).await;
        let AppendPlan::AddedRows(read) = plan else {
            panic!("{plan:?}");
        };
        assert_eq!(
            read.seed,
            Some(SeedMark::AllStoredRows("a file holds NULL time values")),
            "a NULL-time row must meet its key's stored timed version, which no bound covers"
        );
    }

    #[tokio::test]
    async fn a_string_time_column_compares_every_stored_row() {
        let (catalog, ident) = catalog_with_table().await;
        let provider = provider_for(&catalog, &ident).await;
        let first = append(&catalog, &ident, &[(1, 100)]).await;
        append(&catalog, &ident, &[(2, 200)]).await;
        let cursor = AppendCursor {
            table_uuid: table_uuid(&catalog, &ident).await,
            snapshot_id: first,
        };
        let text = TimeColumn {
            name: "note".to_string(),
            format: Some(TimestampFormat::Iso8601(arrow::datatypes::DataType::Utf8)),
        };

        let plan = plan_for("events", &provider, Some(&cursor), Some(&text)).await;
        let AppendPlan::AddedRows(read) = plan else {
            panic!("{plan:?}");
        };
        assert_eq!(
            read.seed,
            Some(SeedMark::AllStoredRows("string time column"))
        );
    }

    #[tokio::test]
    async fn a_replaced_table_and_an_unknown_saved_snapshot_read_by_time_once() {
        let (catalog, ident) = catalog_with_table().await;
        let provider = provider_for(&catalog, &ident).await;
        let first = append(&catalog, &ident, &[(1, 100)]).await;
        let uuid = table_uuid(&catalog, &ident).await;

        let replaced = AppendCursor {
            table_uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            snapshot_id: first,
        };
        let plan = plan_for("events", &provider, Some(&replaced), Some(&temporal("ts"))).await;
        let AppendPlan::TimeFiltered {
            pinned: Some(pinned),
            fallback: Some(reason),
        } = plan
        else {
            panic!("a UUID the table does not have means it was recreated: {plan:?}");
        };
        let cursor = cursor_of(&pinned.cursor);
        assert_eq!(
            (cursor.snapshot_id, cursor.table_uuid),
            (first, uuid.clone())
        );
        assert_eq!(reason.reason, "table_replaced");
        assert_eq!(
            reason.detail,
            format!(
                "the Iceberg table was replaced (its UUID changed from 00000000-0000-0000-0000-000000000000 to {uuid}), so rows of the old table stay in the acceleration"
            )
        );

        let expired = AppendCursor {
            table_uuid: uuid,
            snapshot_id: first + 1,
        };
        let plan = plan_for("events", &provider, Some(&expired), Some(&temporal("ts"))).await;
        let AppendPlan::TimeFiltered {
            fallback: Some(reason),
            ..
        } = plan
        else {
            panic!("a saved snapshot the walk from the current one never meets: {plan:?}");
        };
        assert_eq!(reason.reason, "snapshot_not_ancestor");
        assert_eq!(
            reason.detail,
            format!(
                "the last loaded Iceberg snapshot {} is no longer an ancestor of the current snapshot {first} (a snapshot between them expired, the table was rolled back, or it is on another branch), so rows removed at the source stay in the acceleration",
                first + 1
            )
        );
    }

    #[tokio::test]
    async fn snapshots_between_walks_parent_pointers_oldest_first() {
        let (catalog, ident) = catalog_with_table().await;
        let first = append(&catalog, &ident, &[(1, 1)]).await;
        let second = append(&catalog, &ident, &[(2, 2)]).await;
        let third = append(&catalog, &ident, &[(3, 3)]).await;
        let table = catalog.load_table(&ident).await.expect("table loads");
        let metadata = table.metadata();

        let ids = |snapshots: Vec<SnapshotRef>| {
            snapshots
                .iter()
                .map(|s| s.snapshot_id())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            snapshots_between(metadata, first, third).map(ids),
            Some(vec![second, third])
        );
        assert_eq!(
            snapshots_between(metadata, second, third).map(ids),
            Some(vec![third])
        );
        assert_eq!(
            snapshots_between(metadata, third, third).map(ids),
            Some(vec![]),
            "an interval that is empty"
        );
        assert_eq!(
            snapshots_between(metadata, third + 1, third),
            None,
            "a saved snapshot the walk never meets"
        );
        assert_eq!(
            snapshots_between(metadata, third, first),
            None,
            "a saved snapshot after the current one (a rollback)"
        );
    }

    #[test]
    fn bound_nanos_converts_each_iceberg_type_or_declines() {
        let micros = 1_700_000_000_123_456_i64;
        let temporal = TimestampFormat::Timestamptz(None);
        assert_eq!(
            bound_nanos(&Datum::timestamp_micros(micros), &temporal),
            Some(1_700_000_000_123_456_000)
        );
        assert_eq!(
            bound_nanos(&Datum::timestamptz_micros(micros), &UNIX_SECONDS),
            Some(1_700_000_000_123_456_000),
            "a temporal Iceberg type carries its own unit, whatever the stored format says"
        );
        assert_eq!(
            bound_nanos(&Datum::date(20_000), &temporal),
            Some(20_000 * 86_400_000_000_000)
        );
        assert_eq!(
            bound_nanos(&Datum::long(1_700_000_000_i64), &UNIX_SECONDS),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(
            bound_nanos(&Datum::long(1_700_000_000_123_i64), &UNIX_MILLIS),
            Some(1_700_000_000_123_000_000)
        );
        assert_eq!(bound_nanos(&Datum::long(7_i64), &UNIX_NANOS), Some(7));
        assert_eq!(
            bound_nanos(&Datum::int(1_700_000), &UNIX_SECONDS),
            Some(1_700_000_000_000_000)
        );
        assert_eq!(
            bound_nanos(&Datum::long(1_700_000_000_i64), &temporal),
            None,
            "an integer column needs a unit"
        );
        assert_eq!(
            bound_nanos(&Datum::string("2026-01-01T00:00:00Z"), &temporal),
            None
        );
        assert_eq!(
            bound_nanos(&Datum::timestamp_micros(-1), &temporal),
            None,
            "a pre-1970 bound is not representable as a mark"
        );
        assert_eq!(
            bound_nanos(&Datum::long(i64::MAX), &UNIX_SECONDS),
            Some(9_223_372_036_854_775_807_000_000_000),
            "the widest integer bound scales without wrapping"
        );
    }

    #[test]
    fn cursor_json_round_trips_and_rejects_other_sources() {
        let cursor = AppendCursor {
            table_uuid: "9f2a1c4e-7b3d-4e1f-a2b5-6c7d8e9f0a1b".to_string(),
            snapshot_id: 7_636_546_968_563_739_082,
        };
        let json = cursor.to_json();
        assert_eq!(
            json,
            r#"{"version":1,"source":"iceberg","table_uuid":"9f2a1c4e-7b3d-4e1f-a2b5-6c7d8e9f0a1b","snapshot_id":7636546968563739082}"#
        );
        assert_eq!(AppendCursor::from_json(&json).expect("round trips"), cursor);

        let other = r#"{"version":1,"source":"delta","table_uuid":"x","snapshot_id":1}"#;
        let error = AppendCursor::from_json(other).expect_err("another source");
        assert_eq!(
            error.to_string(),
            "The saved append cursor was written for source 'delta' version 1, not for an Iceberg table"
        );
        let newer = r#"{"version":2,"source":"iceberg","table_uuid":"x","snapshot_id":1}"#;
        AppendCursor::from_json(newer).expect_err("a newer layout");
        AppendCursor::from_json("not json").expect_err("not a cursor");
    }

    #[test]
    fn walked_snapshots_display_counts_by_operation() {
        let walked = WalkedSnapshots {
            append: 3,
            replace: 1,
            delete: 0,
            overwrite: 1,
        };
        assert_eq!(
            walked.to_string(),
            "5 new Iceberg snapshot(s) (3 append, 1 replace, 0 delete, 1 overwrite)"
        );
    }

    /// The refresh reads the source in the dataset's place, so it finds the source only
    /// through layers whose scan returns the source's rows unchanged.
    #[tokio::test]
    async fn the_source_is_found_only_through_layers_that_leave_reads_unchanged() {
        /// A layer that does not say its reads are unchanged, as one computing a column.
        #[derive(Debug)]
        struct ComputesAColumn;
        impl spice_table::TableLayer for ComputesAColumn {}

        let (catalog, ident) = catalog_with_table().await;
        let source: Arc<dyn TableProvider> = Arc::new(provider_for(&catalog, &ident).await);
        let described = crate::metadata_enriched_table_provider(
            Arc::clone(&source),
            HashMap::from([("description".to_string(), "events".to_string())]),
            crate::FieldMetadata::new(),
        );
        assert!(
            crate::incremental_append::incremental_append_source(described.as_ref()).is_some(),
            "a table description leaves reads unchanged"
        );
        let computed: Arc<dyn TableProvider> =
            spice_table::SpiceTable::over(Arc::new(ComputesAColumn), source);
        assert!(
            crate::incremental_append::incremental_append_source(computed.as_ref()).is_none(),
            "a layer that changes what a scan returns hides the source"
        );
    }
}
