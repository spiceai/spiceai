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

//! `on_conflict: upsert_dedup_by_time_column`: keep, per primary key, only rows newer than
//! the version of that key already kept.
//!
//! A refresh streams its rows through a [`LatestByTime`] selector that holds one entry per
//! key — a 128-bit key identity, and the greatest `time_column` kept so far with a 64-bit
//! hash of that row's contents — never the rest of the row. A row is passed to the write
//! only if it beats that entry: a greater time, or an equal time and a greater content
//! hash, so ties pick the same row whatever the read order. A row with the same time and
//! content is a re-read and writes nothing. Every row not written is counted in
//! `dataset_acceleration_refresh_rows_superseded`. On append, the selector is first seeded
//! with the rows the acceleration already stores from the append window start, so a late
//! row never replaces a newer stored version. Within a refresh, the rows passed for a key
//! arrive in increasing `(time, hash)` order, so the last one written for a key wins.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, UInt64Array};
use arrow::compute::{CastOptions, cast_with_options, concat_batches, filter_record_batch};
use arrow::datatypes::{
    DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit, TimestampNanosecondType, UInt64Type,
};
use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};
use datafusion::datasource::file_format::options::ArrowReadOptions;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::disk_manager::{DiskManager, RefCountedTempFile};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext, col};
use futures::TryStreamExt;
use opentelemetry::KeyValue;
use runtime_acceleration::dataupdate::StreamingDataUpdate;
use runtime_component::dataset::TimeFormat;
use runtime_metrics::acceleration as metrics;
use twox_hash::XxHash3_128;

const DOCS: &str =
    "https://spiceai.org/docs/features/data-acceleration/constraints#upsert_dedup_by_time_column";

/// A refresh this mode refused to apply, worded as the cause the refresh log line shows
/// after `Failed to refresh dataset <name> (<connector>):`. It crosses the write as a
/// [`DataFusionError::External`] so the refresh can recognise it and show it unwrapped.
#[derive(Debug)]
pub(crate) struct RefreshNotApplied(String);

impl fmt::Display for RefreshNotApplied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RefreshNotApplied {}

/// `message`, with the docs link every message of this mode ends with, as an error that
/// fails the refresh.
pub(crate) fn not_applied(message: &str) -> DataFusionError {
    DataFusionError::External(Box::new(RefreshNotApplied(format!(
        "{message} See: {DOCS}"
    ))))
}

/// The message of a [`RefreshNotApplied`] anywhere in `error`'s source chain, however the
/// write wrapped it.
pub(crate) fn not_applied_message(error: &DataFusionError) -> Option<String> {
    std::iter::successors(Some(error as &(dyn std::error::Error + 'static)), |e| {
        e.source()
    })
    .find_map(|e| e.downcast_ref::<RefreshNotApplied>())
    .map(ToString::to_string)
}

fn plural(count: usize, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 { one } else { many }
}

/// The `time_format` value as a user writes it in the Spicepod.
fn time_format_name(time_format: Option<TimeFormat>) -> &'static str {
    match time_format.unwrap_or_default() {
        TimeFormat::Timestamp => "timestamp",
        TimeFormat::Timestamptz => "timestamptz",
        TimeFormat::UnixSeconds => "unix_seconds",
        TimeFormat::UnixMillis => "unix_millis",
        TimeFormat::UnixNanos => "unix_nanos",
        TimeFormat::ISO8601 => "ISO8601",
        TimeFormat::Date => "date",
    }
}

/// Rows a batch did not pass on, by `reason`.
#[derive(Debug, Default, PartialEq, Eq)]
struct Superseded {
    older: u64,
    equal_time: u64,
    unchanged: u64,
}

impl Superseded {
    /// Count `version`, which lost to `winner`.
    fn count(&mut self, version: Kept, winner: Kept) {
        if version.time < winner.time {
            self.older += 1;
        } else if version.hash == winner.hash {
            self.unchanged += 1;
        } else {
            self.equal_time += 1;
        }
    }
}

/// The version of a key kept so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Kept {
    /// The greatest `time_column` kept, in UTC nanoseconds.
    time: i64,
    /// A hash of the kept row's contents: it settles equal times, and an equal hash means
    /// the same row read again.
    hash: u64,
}

/// How a key becomes its 128-bit identity in the map.
enum KeyEncoding {
    /// Every key column is integer-backed and, with one NULL flag bit each, the columns
    /// fit in 128 bits, so the identity is the key itself. Holds each column's value width.
    Inline(Vec<u32>),
    /// The XXH3-128 digest of the key's `RowConverter` bytes.
    Digest(RowConverter),
}

/// The value width of a key column that can be packed inline, and whether it is read as
/// signed (cast to `Int64`) or unsigned (cast to `UInt64`).
fn inline_width(data_type: &DataType) -> Option<(u32, bool)> {
    match data_type {
        DataType::Int8 => Some((8, true)),
        DataType::Int16 => Some((16, true)),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => Some((32, true)),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Some((64, true)),
        DataType::UInt8 => Some((8, false)),
        DataType::UInt16 => Some((16, false)),
        DataType::UInt32 => Some((32, false)),
        DataType::UInt64 => Some((64, false)),
        _ => None,
    }
}

/// The newest kept version of each key, by `time_column`, for one refresh.
pub(crate) struct LatestByTime {
    dataset: String,
    key_columns: Vec<String>,
    /// The type each key column is encoded as: the incoming rows' types, so stored keys
    /// read back from an engine that rewrites types still encode to the same identity.
    key_types: Vec<DataType>,
    time_column: String,
    time_format: Option<TimeFormat>,
    /// The incoming rows' columns: a row's content hash is over these, cast to these types,
    /// so a stored copy of a row hashes like the incoming one.
    hashed: SchemaRef,
    encoding: KeyEncoding,
    latest: HashMap<u128, Kept>,
    /// The share of the query memory pool `latest` is charged to; `None` without a pool.
    reservation: Option<MemoryReservation>,
    /// Where the map spills when the pool refuses to grow it.
    disk: Option<Arc<DiskManager>>,
    runtime_env: Option<Arc<RuntimeEnv>>,
    /// The map's entries spilled so far, each a run sorted by key.
    runs: Vec<RefCountedTempFile>,
    /// Once the map has spilled it no longer knows every key, so every later row is
    /// written here and decided after the last one, against every run.
    deferred: Option<DeferredRows>,
    older_labels: [KeyValue; 2],
    equal_time_labels: [KeyValue; 2],
    unchanged_labels: [KeyValue; 2],
}

impl LatestByTime {
    /// A selector for rows of `schema` keyed on `key_columns`, ordered by `time_column`.
    ///
    /// # Errors
    ///
    /// Returns a [`RefreshNotApplied`] error if a key column or the time column is missing
    /// from `schema`.
    pub(crate) fn try_new(
        dataset: &str,
        schema: &SchemaRef,
        key_columns: Vec<String>,
        time_column: String,
        time_format: Option<TimeFormat>,
    ) -> Result<Self, DataFusionError> {
        let key_types = key_columns
            .iter()
            .map(|name| {
                schema
                    .field_with_name(name)
                    .map(|f| f.data_type().clone())
                    .map_err(|_| {
                        not_applied(&format!(
                            "primary key column '{name}' is not in the rows the refresh reads, so versions of a key cannot be matched. Include it in 'acceleration.refresh_sql', or remove it from 'acceleration.primary_key'."
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if schema.field_with_name(&time_column).is_err() {
            return Err(not_applied(&format!(
                "'time_column' '{time_column}' is not in the rows the refresh reads, so versions of a key cannot be ordered. Include it in 'acceleration.refresh_sql'."
            )));
        }
        let widths: Option<Vec<u32>> = key_types
            .iter()
            .map(|t| inline_width(t).map(|(width, _)| width))
            .collect();
        let encoding = match widths {
            Some(widths) if widths.iter().map(|w| w + 1).sum::<u32>() <= 128 => {
                KeyEncoding::Inline(widths)
            }
            _ => KeyEncoding::Digest(RowConverter::new(
                key_types
                    .iter()
                    .map(|t| SortField::new(t.clone()))
                    .collect(),
            )?),
        };
        let labels = |reason: &'static str| {
            [
                KeyValue::new("dataset", dataset.to_string()),
                KeyValue::new("reason", reason),
            ]
        };
        Ok(Self {
            older_labels: labels("older"),
            equal_time_labels: labels("equal_time"),
            unchanged_labels: labels("unchanged"),
            dataset: dataset.to_string(),
            key_columns,
            key_types,
            time_column,
            time_format,
            hashed: Arc::clone(schema),
            encoding,
            latest: HashMap::new(),
            reservation: None,
            disk: None,
            runtime_env: None,
            runs: Vec::new(),
            deferred: None,
        })
    }

    /// Charge the map to `runtime_env`'s memory pool, spilling to its disk manager when
    /// the pool refuses to grow it.
    #[must_use]
    pub(crate) fn with_runtime_env(mut self, runtime_env: Arc<RuntimeEnv>) -> Self {
        self.reservation = Some(
            MemoryConsumer::new(format!("UpsertDedupByTimeColumnKeys[{}]", self.dataset))
                .register(&runtime_env.memory_pool),
        );
        self.disk = Some(Arc::clone(&runtime_env.disk_manager));
        self.runtime_env = Some(runtime_env);
        self
    }

    /// Grow the reservation to hold the map with up to `additional` more keys. `true`
    /// when the map may grow (always, without a pool). The map only reallocates when it
    /// outgrows its capacity, and then at least doubles, so that is what is charged.
    fn reserve(&self, additional: usize) -> bool {
        let needed = self.latest.len().saturating_add(additional);
        if needed <= self.latest.capacity() {
            return true;
        }
        self.reservation.as_ref().is_none_or(|reservation| {
            reservation
                .try_resize(map_bytes(needed.max(self.latest.capacity() + 1)))
                .is_ok()
        })
    }

    /// Write the map to a new run and empty it.
    async fn spill_map(&mut self) -> Result<(), DataFusionError> {
        if self.latest.is_empty() {
            return Ok(());
        }
        let entries: Vec<(u128, Kept)> = self.latest.drain().collect();
        self.latest.shrink_to_fit();
        if let Some(reservation) = &self.reservation {
            reservation.free();
        }
        let run = self.write_run(entries).await?;
        self.runs.push(run);
        Ok(())
    }

    async fn write_run(
        &self,
        entries: Vec<(u128, Kept)>,
    ) -> Result<RefCountedTempFile, DataFusionError> {
        let disk = self.spill_disk()?;
        tokio::task::spawn_blocking(move || write_run(&disk, entries))
            .await
            .map_err(|e| spill_failed(&e))?
            .map_err(|e| spill_failed(&e))
    }

    fn spill_disk(&self) -> Result<Arc<DiskManager>, DataFusionError> {
        self.disk
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| spill_failed(&"no spill directory is configured"))
    }

    /// Publish every `reason` series at `0`, so a dashboard sees the series before the
    /// first superseded row and an alert can fire on its rise.
    pub(crate) fn publish_zero(&self) {
        for labels in [
            &self.older_labels,
            &self.equal_time_labels,
            &self.unchanged_labels,
        ] {
            metrics::REFRESH_ROWS_SUPERSEDED.add(0, labels);
        }
    }

    /// Record the keys and times already stored. Rows with a NULL stored time are
    /// skipped: any incoming version replaces them.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch lacks a key or time column, or a time cannot be read.
    pub(crate) async fn seed(&mut self, stored: &RecordBatch) -> Result<(), DataFusionError> {
        if !self.reserve(stored.num_rows()) {
            self.spill_map().await?;
            if !self.reserve(stored.num_rows()) {
                // Not even this batch fits: it becomes a run of its own.
                let mut entries = HashMap::new();
                self.seed_into(&mut entries, stored)?;
                let run = self.write_run(entries.into_iter().collect()).await?;
                self.runs.push(run);
                return Ok(());
            }
        }
        let mut latest = std::mem::take(&mut self.latest);
        let seeded = self.seed_into(&mut latest, stored);
        self.latest = latest;
        seeded
    }

    fn seed_into(
        &self,
        latest: &mut HashMap<u128, Kept>,
        stored: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let keys = self.keys(stored)?;
        let column = self.time_column_of(stored)?;
        let times = time_nanos(&column, self.time_format)?;
        let hashes = self.content_hashes(stored)?;
        for (row, key) in keys.into_iter().enumerate() {
            if times.is_null(row) {
                continue;
            }
            let version = Kept {
                time: times.value(row),
                hash: hashes[row],
            };
            match latest.entry(key) {
                Entry::Occupied(mut entry) => {
                    if version > *entry.get() {
                        entry.insert(version);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(version);
                }
            }
        }
        Ok(())
    }

    /// Keep the rows of `batch` that are newer than the version of their key kept so far,
    /// and record them as kept. Within the batch only the last kept row of each key is
    /// passed on, so a write never holds two versions of a key from one batch.
    ///
    /// # Errors
    ///
    /// Returns a [`RefreshNotApplied`] error if a time is NULL or cannot be read: the
    /// refresh then writes nothing, rather than choosing a version without one.
    pub(crate) async fn select(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, DataFusionError> {
        if batch.num_rows() == 0 {
            return Ok(batch.clone());
        }
        if self.deferred.is_none() && self.runs.is_empty() && self.reserve(batch.num_rows()) {
            let (selected, superseded) = self.select_counted(batch)?;
            self.record(&superseded);
            return Ok(selected);
        }
        // The map no longer fits, or a seed already spilled, so it cannot decide this
        // row alone: check the times now, then defer the row to after the last one.
        let times = self.times(batch)?;
        let keys = self.keys(batch)?;
        let hashes = self.content_hashes(batch)?;
        if self.deferred.is_none() {
            self.spill_map().await?;
            self.deferred = Some(DeferredRows::new(&batch.schema(), &self.spill_disk()?)?);
        }
        if let Some(deferred) = self.deferred.as_mut() {
            deferred.write(batch, &keys, &times, hashes).await?;
        }
        Ok(batch.slice(0, 0))
    }

    fn record(&self, superseded: &Superseded) {
        for (count, labels) in [
            (superseded.older, &self.older_labels),
            (superseded.equal_time, &self.equal_time_labels),
            (superseded.unchanged, &self.unchanged_labels),
        ] {
            if count > 0 {
                metrics::REFRESH_ROWS_SUPERSEDED.add(count, labels);
            }
        }
    }

    /// After the last row: decide every deferred row against every run, and return the
    /// rows to write, or `None` when nothing was deferred.
    async fn finish(&mut self) -> Result<Option<SendableRecordBatchStream>, DataFusionError> {
        let Some(deferred) = self.deferred.take() else {
            return Ok(None);
        };
        let file = deferred.finish().await?;
        let disk = self.spill_disk()?;
        let runs = std::mem::take(&mut self.runs);
        let merged = tokio::task::spawn_blocking(move || merge_runs(&disk, &runs))
            .await
            .map_err(|e| spill_failed(&e))?
            .map_err(|e| spill_failed(&e))?;
        let runtime_env = self
            .runtime_env
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| spill_failed(&"no spill directory is configured"))?;
        // The map's reservation was freed when it spilled, but the pool is short, so the
        // sort's up-front merge reservation is kept to a small share of it.
        let merge_reservation = match runtime_env.memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => (limit / 8).min(SORT_MERGE_RESERVATION),
            MemoryLimit::Infinite | MemoryLimit::Unknown => SORT_MERGE_RESERVATION,
        };
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_sort_spill_reservation_bytes(merge_reservation),
            runtime_env,
        );
        let path = file.path().to_string_lossy().to_string();
        let sorted = ctx
            .read_arrow(
                path,
                ArrowReadOptions {
                    file_extension: "",
                    ..ArrowReadOptions::default()
                },
            )
            .await
            .map_err(|e| spill_failed(&e))?
            .sort(vec![
                col(KEY_HI).sort(true, false),
                col(KEY_LO).sort(true, false),
                col(SEQ).sort(true, false),
            ])?
            .execute_stream()
            .await
            .map_err(|e| spill_failed(&e))?;
        let resolver = DeferredResolver {
            sorted,
            run: merged.map(RunCursor::new),
            group: None,
            superseded: Superseded::default(),
            _file: file,
        };
        let labels = [
            self.older_labels.clone(),
            self.equal_time_labels.clone(),
            self.unchanged_labels.clone(),
        ];
        Ok(Some(resolver.into_stream(labels)))
    }

    /// [`Self::select`], returning the rows it did not pass on by reason instead of
    /// recording them.
    fn select_counted(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, Superseded), DataFusionError> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok((batch.clone(), Superseded::default()));
        }
        let times = self.times(batch)?;
        let keys = self.keys(batch)?;
        let hashes = self.content_hashes(batch)?;
        let mut keep = vec![false; num_rows];
        let mut superseded = Superseded::default();
        for (row, keep_row) in keep.iter_mut().enumerate() {
            let version = Kept {
                time: times.value(row),
                hash: hashes[row],
            };
            match self.latest.entry(keys[row]) {
                Entry::Occupied(mut entry) => {
                    let kept = entry.get_mut();
                    if version > *kept {
                        *kept = version;
                        *keep_row = true;
                    } else {
                        superseded.count(version, *kept);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(version);
                    *keep_row = true;
                }
            }
        }
        // A key kept more than once in this batch: only its last (newest) row is written,
        // and the earlier ones count as superseded by it.
        let mut last_kept: HashMap<u128, usize> = HashMap::new();
        for (row, _) in keep.iter().enumerate().filter(|(_, keep_row)| **keep_row) {
            last_kept.insert(keys[row], row);
        }
        for (row, keep_row) in keep.iter_mut().enumerate() {
            if *keep_row && last_kept.get(&keys[row]) != Some(&row) {
                *keep_row = false;
                let version = Kept {
                    time: times.value(row),
                    hash: hashes[row],
                };
                let last = last_kept[&keys[row]];
                superseded.count(
                    version,
                    Kept {
                        time: times.value(last),
                        hash: hashes[last],
                    },
                );
            }
        }
        Ok((
            filter_record_batch(batch, &BooleanArray::from(keep))?,
            superseded,
        ))
    }

    /// Each row's content hash: every incoming column, cast to its incoming type, with
    /// floats normalized as an engine may store them (`-0.0` as `0.0`, `NaN` as NULL), so
    /// a stored copy of a row hashes like the row it came from.
    fn content_hashes(&self, batch: &RecordBatch) -> Result<Vec<u64>, DataFusionError> {
        let columns = self
            .hashed
            .fields()
            .iter()
            .map(|field| {
                let column = batch.column_by_name(field.name()).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "column '{}' missing from a batch of dataset '{}'",
                        field.name(),
                        self.dataset
                    ))
                })?;
                let column = if column.data_type() == field.data_type() {
                    Arc::clone(column)
                } else {
                    cast_with_options(column, field.data_type(), &CastOptions::default())?
                };
                normalize_floats(&column)
            })
            .collect::<Result<Vec<ArrayRef>, DataFusionError>>()?;
        let mut hashes = vec![0_u64; batch.num_rows()];
        create_hashes(&columns, &RandomState::default(), &mut hashes)?;
        Ok(hashes)
    }

    /// The key columns of `batch`, cast to the types the selector encodes keys as.
    fn key_arrays(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>, DataFusionError> {
        self.key_columns
            .iter()
            .zip(&self.key_types)
            .map(|(name, data_type)| {
                let column = batch.column_by_name(name).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "primary key column '{name}' missing from a batch of dataset '{}'",
                        self.dataset
                    ))
                })?;
                if column.data_type() == data_type {
                    Ok(Arc::clone(column))
                } else {
                    Ok(cast_with_options(
                        column,
                        data_type,
                        &CastOptions::default(),
                    )?)
                }
            })
            .collect()
    }

    /// Each row's 128-bit key identity.
    fn keys(&self, batch: &RecordBatch) -> Result<Vec<u128>, DataFusionError> {
        let columns = self.key_arrays(batch)?;
        match &self.encoding {
            KeyEncoding::Inline(widths) => {
                let mut keys = vec![0_u128; batch.num_rows()];
                for (column, &width) in columns.iter().zip(widths) {
                    pack_inline(&mut keys, column, width)?;
                }
                Ok(keys)
            }
            KeyEncoding::Digest(converter) => {
                let rows = converter.convert_columns(&columns)?;
                Ok(rows
                    .iter()
                    .map(|row| XxHash3_128::oneshot(row.as_ref()))
                    .collect())
            }
        }
    }

    fn time_column_of(&self, batch: &RecordBatch) -> Result<ArrayRef, DataFusionError> {
        batch
            .column_by_name(&self.time_column)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "time column '{}' missing from a batch of dataset '{}'",
                    self.time_column, self.dataset
                ))
            })
    }

    /// The time column as UTC nanoseconds, interpreted per `time_format`. Values without
    /// a zone are read as UTC; strings are parsed, so offsets compare by instant.
    fn times(&self, batch: &RecordBatch) -> Result<arrow::array::Int64Array, DataFusionError> {
        let column = self.time_column_of(batch)?;
        let nulls = column.null_count();
        if nulls > 0 {
            return Err(not_applied(&format!(
                "'time_column' '{time_column}' is NULL in {nulls} {rows}, so this refresh was not applied and the previous data is still served. Fill '{time_column}' at the source, or exclude those rows with 'acceleration.refresh_sql'.",
                time_column = self.time_column,
                rows = plural(nulls, "row", "rows"),
            )));
        }
        time_nanos(&column, self.time_format).map_err(|_| {
            let unreadable = unreadable_times(&column, self.time_format);
            not_applied(&format!(
                "'time_column' '{}' has {unreadable} {} that cannot be read as '{}', so this refresh was not applied and the previous data is still served. Correct the source values or set 'time_format' to match them.",
                self.time_column,
                plural(unreadable, "value", "values"),
                time_format_name(self.time_format),
            ))
        })
    }
}

/// Bytes a map of `entries` keys holds: hashbrown keeps at least one bucket in eight
/// empty and rounds buckets to a power of two, each an entry plus one control byte.
fn map_bytes(entries: usize) -> usize {
    let buckets = (entries.saturating_mul(8) / 7).max(1).next_power_of_two();
    buckets.saturating_mul(std::mem::size_of::<(u128, Kept)>() + 1)
}

fn spill_failed(cause: &dyn fmt::Display) -> DataFusionError {
    not_applied(&format!(
        "'acceleration.on_conflict: upsert_dedup_by_time_column' could not spill its key versions to 'runtime.query.temp_directory', so this refresh was not applied and the previous data is still served. Free space there, or raise 'runtime.query.memory_limit'. Cause: {cause}."
    ))
}

/// The most the deferred rows' sort reserves up front for merging its spill files
/// (`DataFusion`'s default for queries).
const SORT_MERGE_RESERVATION: usize = 10 * 1024 * 1024;

/// Bytes one spilled entry takes: its key, time and content hash.
const RUN_ENTRY_BYTES: usize = 16 + 8 + 8;
/// Entries read from a run at a time.
const RUN_CHUNK: usize = 32_768;

/// Write `entries`, sorted by key, to a new spill file.
fn write_run(
    disk: &Arc<DiskManager>,
    mut entries: Vec<(u128, Kept)>,
) -> Result<RefCountedTempFile, DataFusionError> {
    use std::io::Write as _;
    entries.sort_unstable_by_key(|(key, _)| *key);
    let mut file = disk.create_tmp_file("upsert_dedup_by_time_column key spill")?;
    {
        let mut out = std::io::BufWriter::new(file.inner().as_file());
        for (key, kept) in &entries {
            out.write_all(&key.to_le_bytes())?;
            out.write_all(&kept.time.to_le_bytes())?;
            out.write_all(&kept.hash.to_le_bytes())?;
        }
        out.flush()?;
    }
    file.update_disk_usage()?;
    Ok(file)
}

/// Reads a run back in key order.
struct RunReader {
    reader: std::io::BufReader<std::fs::File>,
}

impl RunReader {
    fn open(run: &RefCountedTempFile) -> Result<Self, DataFusionError> {
        Ok(Self {
            reader: std::io::BufReader::new(std::fs::File::open(run.path())?),
        })
    }

    fn next_entry(&mut self) -> Result<Option<(u128, Kept)>, DataFusionError> {
        use std::io::Read as _;
        let mut record = [0_u8; RUN_ENTRY_BYTES];
        match self.reader.read_exact(&mut record) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        let mut key = [0_u8; 16];
        key.copy_from_slice(&record[..16]);
        let mut time = [0_u8; 8];
        time.copy_from_slice(&record[16..24]);
        let mut hash = [0_u8; 8];
        hash.copy_from_slice(&record[24..32]);
        Ok(Some((
            u128::from_le_bytes(key),
            Kept {
                time: i64::from_le_bytes(time),
                hash: u64::from_le_bytes(hash),
            },
        )))
    }
}


/// Merge `runs` into one run holding each key's newest version, or `None` without runs.
fn merge_runs(
    disk: &Arc<DiskManager>,
    runs: &[RefCountedTempFile],
) -> Result<Option<RefCountedTempFile>, DataFusionError> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use std::io::Write as _;
    if runs.is_empty() {
        return Ok(None);
    }
    let mut readers = runs
        .iter()
        .map(RunReader::open)
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    for (source, reader) in readers.iter_mut().enumerate() {
        if let Some((key, kept)) = reader.next_entry()? {
            heap.push(Reverse((key, source, kept.time, kept.hash)));
        }
    }
    let mut file = disk.create_tmp_file("upsert_dedup_by_time_column merged key spill")?;
    {
        let mut out = std::io::BufWriter::new(file.inner().as_file());
        let mut current: Option<(u128, Kept)> = None;
        while let Some(Reverse((key, source, time, hash))) = heap.pop() {
            if let Some((next_key, kept)) = readers[source].next_entry()? {
                heap.push(Reverse((next_key, source, kept.time, kept.hash)));
            }
            let entry = Kept { time, hash };
            current = match current {
                Some((current_key, kept)) if current_key == key => Some((key, kept.max(entry))),
                Some((current_key, kept)) => {
                    out.write_all(&current_key.to_le_bytes())?;
                    out.write_all(&kept.time.to_le_bytes())?;
                    out.write_all(&kept.hash.to_le_bytes())?;
                    Some((key, entry))
                }
                None => Some((key, entry)),
            };
        }
        if let Some((key, kept)) = current {
            out.write_all(&key.to_le_bytes())?;
            out.write_all(&kept.time.to_le_bytes())?;
            out.write_all(&kept.hash.to_le_bytes())?;
        }
        out.flush()?;
    }
    file.update_disk_usage()?;
    Ok(Some(file))
}

/// The merged run, read in chunks off the async runtime.
struct RunCursor {
    file: RefCountedTempFile,
    reader: Option<RunReader>,
    buffer: VecDeque<(u128, Kept)>,
    exhausted: bool,
}

impl RunCursor {
    fn new(file: RefCountedTempFile) -> Self {
        Self {
            file,
            reader: None,
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }

    async fn refill(&mut self) -> Result<(), DataFusionError> {
        let reader = match self.reader.take() {
            Some(reader) => reader,
            None => RunReader::open(&self.file)?,
        };
        let (reader, chunk) = tokio::task::spawn_blocking(move || {
            let mut reader = reader;
            let mut chunk = VecDeque::with_capacity(RUN_CHUNK);
            while chunk.len() < RUN_CHUNK {
                match reader.next_entry()? {
                    Some(entry) => chunk.push_back(entry),
                    None => break,
                }
            }
            Ok::<_, DataFusionError>((reader, chunk))
        })
        .await
        .map_err(|e| spill_failed(&e))??;
        self.exhausted = chunk.len() < RUN_CHUNK;
        self.reader = Some(reader);
        self.buffer = chunk;
        Ok(())
    }

    /// The stored or kept version of `key`, skipping keys before it. Keys must be asked
    /// for in increasing order.
    async fn version_of(&mut self, key: u128) -> Result<Option<Kept>, DataFusionError> {
        loop {
            while let Some(&(next, kept)) = self.buffer.front() {
                match next.cmp(&key) {
                    std::cmp::Ordering::Less => {
                        self.buffer.pop_front();
                    }
                    std::cmp::Ordering::Equal => return Ok(Some(kept)),
                    std::cmp::Ordering::Greater => return Ok(None),
                }
            }
            if self.exhausted && self.reader.is_some() {
                return Ok(None);
            }
            self.refill().await?;
            if self.buffer.is_empty() {
                return Ok(None);
            }
        }
    }
}

/// The low 64 bits of `key`.
fn key_half(key: u128) -> u64 {
    u64::try_from(key & u128::from(u64::MAX)).unwrap_or_default()
}

const KEY_HI: &str = "__spice_upsert_dedup_key_hi";
const KEY_LO: &str = "__spice_upsert_dedup_key_lo";
const TIME: &str = "__spice_upsert_dedup_time";
const SEQ: &str = "__spice_upsert_dedup_seq";
const HASH: &str = "__spice_upsert_dedup_hash";
/// The helper columns deferral appends after a row's own columns.
const HELPERS: usize = 5;

/// Rows read after the map spilled, with their key, time and read order, in an Arrow IPC
/// spill file.
struct DeferredRows {
    file: RefCountedTempFile,
    writer: Option<arrow::ipc::writer::FileWriter<std::fs::File>>,
    schema: SchemaRef,
    next_seq: u64,
}

impl DeferredRows {
    fn new(rows: &SchemaRef, disk: &Arc<DiskManager>) -> Result<Self, DataFusionError> {
        let mut fields: Vec<Arc<Field>> = rows.fields().iter().cloned().collect();
        fields.extend([
            Arc::new(Field::new(KEY_HI, DataType::UInt64, false)),
            Arc::new(Field::new(KEY_LO, DataType::UInt64, false)),
            Arc::new(Field::new(TIME, DataType::Int64, false)),
            Arc::new(Field::new(SEQ, DataType::UInt64, false)),
            Arc::new(Field::new(HASH, DataType::UInt64, false)),
        ]);
        let schema = Arc::new(Schema::new(fields));
        let file = disk
            .create_tmp_file("upsert_dedup_by_time_column deferred rows")
            .map_err(|e| spill_failed(&e))?;
        let writer = arrow::ipc::writer::FileWriter::try_new(
            file.inner().reopen().map_err(|e| spill_failed(&e))?,
            &schema,
        )
        .map_err(|e| spill_failed(&e))?;
        Ok(Self {
            file,
            writer: Some(writer),
            schema,
            next_seq: 0,
        })
    }

    async fn write(
        &mut self,
        batch: &RecordBatch,
        keys: &[u128],
        times: &Int64Array,
        hashes: Vec<u64>,
    ) -> Result<(), DataFusionError> {
        let rows = u64::try_from(keys.len()).unwrap_or(u64::MAX);
        let seq: UInt64Array = (self.next_seq..self.next_seq + rows).collect();
        self.next_seq += rows;
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.extend([
            Arc::new(
                keys.iter()
                    .map(|k| key_half(*k >> 64))
                    .collect::<UInt64Array>(),
            ) as ArrayRef,
            Arc::new(keys.iter().map(|k| key_half(*k)).collect::<UInt64Array>()) as ArrayRef,
            Arc::new(times.clone()) as ArrayRef,
            Arc::new(seq) as ArrayRef,
            Arc::new(UInt64Array::from(hashes)) as ArrayRef,
        ]);
        let tagged = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| DataFusionError::Internal("deferred rows already finished".into()))?;
        let writer = tokio::task::spawn_blocking(move || writer.write(&tagged).map(|()| writer))
            .await
            .map_err(|e| spill_failed(&e))?
            .map_err(|e| spill_failed(&e))?;
        self.writer = Some(writer);
        Ok(())
    }

    async fn finish(mut self) -> Result<RefCountedTempFile, DataFusionError> {
        if let Some(mut writer) = self.writer.take() {
            tokio::task::spawn_blocking(move || writer.finish())
                .await
                .map_err(|e| spill_failed(&e))?
                .map_err(|e| spill_failed(&e))?;
        }
        self.file
            .update_disk_usage()
            .map_err(|e| spill_failed(&e))?;
        Ok(self.file)
    }
}

/// The deferred rows of the key being decided.
struct Group {
    key: u128,
    /// The winning deferred row so far, as a one-row slice.
    best: RecordBatch,
    best_version: Kept,
    /// The other deferred rows' versions.
    others: Vec<Kept>,
}

/// Decides each deferred row, in key order, against the merged run.
struct DeferredResolver {
    sorted: SendableRecordBatchStream,
    run: Option<RunCursor>,
    group: Option<Group>,
    superseded: Superseded,
    /// Keeps the deferred-rows spill file alive while it is read.
    _file: RefCountedTempFile,
}

impl DeferredResolver {
    /// Count `group`'s rows and return its row to write, if any.
    async fn close(&mut self, group: Group) -> Result<Option<RecordBatch>, DataFusionError> {
        let kept = match self.run.as_mut() {
            Some(run) => run.version_of(group.key).await?,
            None => None,
        };
        let write = kept.is_none_or(|kept| group.best_version > kept);
        let winner = match kept {
            Some(kept) if !write => kept,
            _ => group.best_version,
        };
        for &version in &group.others {
            self.superseded.count(version, winner);
        }
        if write {
            Ok(Some(group.best))
        } else {
            self.superseded.count(group.best_version, winner);
            Ok(None)
        }
    }

    /// The rows of the next sorted batch to write, without the helper columns; `None`
    /// after the last batch.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        let Some(batch) = self.sorted.try_next().await.map_err(|e| spill_failed(&e))? else {
            return match self.group.take() {
                Some(group) => Ok(self
                    .close(group)
                    .await?
                    .map(|batch| strip_helpers(&batch))
                    .transpose()?),
                None => Ok(None),
            };
        };
        let hi = column_of::<UInt64Type>(&batch, KEY_HI)?;
        let lo = column_of::<UInt64Type>(&batch, KEY_LO)?;
        let times = column_of::<Int64Type>(&batch, TIME)?;
        let hashes = column_of::<UInt64Type>(&batch, HASH)?;
        let mut out = Vec::new();
        for row in 0..batch.num_rows() {
            let key = (u128::from(hi.value(row)) << 64) | u128::from(lo.value(row));
            let version = Kept {
                time: times.value(row),
                hash: hashes.value(row),
            };
            match self.group.as_mut() {
                Some(group) if group.key == key => {
                    if version > group.best_version {
                        group.others.push(group.best_version);
                        group.best = batch.slice(row, 1);
                        group.best_version = version;
                    } else {
                        group.others.push(version);
                    }
                }
                _ => {
                    let next = Group {
                        key,
                        best: batch.slice(row, 1),
                        best_version: version,
                        others: Vec::new(),
                    };
                    if let Some(done) = self.group.replace(next)
                        && let Some(written) = self.close(done).await?
                    {
                        out.push(written);
                    }
                }
            }
        }
        if out.is_empty() {
            return Ok(Some(strip_helpers(&batch.slice(0, 0))?));
        }
        Ok(Some(strip_helpers(&concat_batches(
            &batch.schema(),
            &out,
        )?)?))
    }

    fn into_stream(self, labels: [[KeyValue; 2]; 3]) -> SendableRecordBatchStream {
        let schema = self.sorted.schema();
        let fields = &schema.fields()[..schema.fields().len() - HELPERS];
        let out_schema = Arc::new(Schema::new(fields.to_vec()));
        let stream = futures::stream::try_unfold(Some(self), move |state| {
            let labels = labels.clone();
            async move {
                let Some(mut resolver) = state else {
                    return Ok(None);
                };
                if let Some(batch) = resolver.next_batch().await? {
                    Ok(Some((batch, Some(resolver))))
                } else {
                    let s = &resolver.superseded;
                    for (count, labels) in [
                        (s.older, &labels[0]),
                        (s.equal_time, &labels[1]),
                        (s.unchanged, &labels[2]),
                    ] {
                        if count > 0 {
                            metrics::REFRESH_ROWS_SUPERSEDED.add(count, labels);
                        }
                    }
                    Ok(None)
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(out_schema, stream))
    }
}

fn column_of<'a, T: arrow::datatypes::ArrowPrimitiveType>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a arrow::array::PrimitiveArray<T>, DataFusionError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_primitive_opt::<T>())
        .ok_or_else(|| DataFusionError::Internal(format!("deferred rows lack '{name}'")))
}

/// `batch` without the helper columns deferral appends.
fn strip_helpers(batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
    let keep: Vec<usize> = (0..batch.num_columns() - HELPERS).collect();
    Ok(batch.project(&keep)?)
}

/// `column` with `-0.0` read as `0.0` and `NaN` as NULL, as engines that do not keep them
/// store them; other columns unchanged.
fn normalize_floats(column: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    Ok(match column.data_type() {
        DataType::Float32 => {
            let normalized: arrow::array::Float32Array = column
                .as_primitive::<arrow::datatypes::Float32Type>()
                .iter()
                .map(|v| v.filter(|v| !v.is_nan()).map(|v| v + 0.0))
                .collect();
            Arc::new(normalized)
        }
        DataType::Float64 => {
            let normalized: arrow::array::Float64Array = column
                .as_primitive::<arrow::datatypes::Float64Type>()
                .iter()
                .map(|v| v.filter(|v| !v.is_nan()).map(|v| v + 0.0))
                .collect();
            Arc::new(normalized)
        }
        _ => Arc::clone(column),
    })
}

/// Shift each key left by `width + 1` bits and append `column`'s value and a NULL flag.
fn pack_inline(keys: &mut [u128], column: &ArrayRef, width: u32) -> Result<(), DataFusionError> {
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1_u64 << width) - 1
    };
    let null_flag = 1_u128 << width;
    let signed = inline_width(column.data_type()).is_some_and(|(_, signed)| signed);
    let pack = |keys: &mut [u128], bits: &dyn Fn(usize) -> u64| {
        for (row, key) in keys.iter_mut().enumerate() {
            *key <<= width + 1;
            if column.is_null(row) {
                *key |= null_flag;
            } else {
                *key |= u128::from(bits(row) & mask);
            }
        }
    };
    if signed {
        let values = cast_with_options(column, &DataType::Int64, &CastOptions::default())?;
        let values = values.as_primitive::<Int64Type>();
        pack(keys, &|row| values.value(row).cast_unsigned());
    } else {
        let values = cast_with_options(column, &DataType::UInt64, &CastOptions::default())?;
        let values = values.as_primitive::<UInt64Type>();
        pack(keys, &|row| values.value(row));
    }
    Ok(())
}

/// How many non-NULL values of `column` cannot be read as a time under `time_format`.
fn unreadable_times(column: &ArrayRef, time_format: Option<TimeFormat>) -> usize {
    if column.data_type().is_integer() {
        let scale = unix_scale(time_format);
        return cast_with_options(column, &DataType::Int64, &CastOptions::default()).map_or(
            column.len() - column.null_count(),
            |values| {
                let values = values.as_primitive::<Int64Type>();
                let unreadable = values
                    .iter()
                    .flatten()
                    .filter(|v| v.checked_mul(scale).is_none())
                    .count();
                unreadable + values.null_count() - column.null_count()
            },
        );
    }
    let utc = DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()));
    cast_with_options(column, &utc, &CastOptions::default())
        .map_or(column.len() - column.null_count(), |timestamps| {
            timestamps.null_count() - column.null_count()
        })
}

fn unix_scale(time_format: Option<TimeFormat>) -> i64 {
    match time_format {
        Some(TimeFormat::UnixNanos) => 1,
        Some(TimeFormat::UnixMillis) => 1_000_000,
        _ => 1_000_000_000,
    }
}

/// `column` as UTC nanoseconds since the epoch.
fn time_nanos(
    column: &ArrayRef,
    time_format: Option<TimeFormat>,
) -> Result<arrow::array::Int64Array, ArrowError> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    if column.data_type().is_integer() {
        let scale = unix_scale(time_format);
        let values = cast_with_options(column, &DataType::Int64, &strict)?;
        return values.as_primitive::<Int64Type>().try_unary(|v| {
            v.checked_mul(scale).ok_or_else(|| {
                ArrowError::ComputeError("time value overflows nanoseconds".to_string())
            })
        });
    }
    let utc = DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()));
    let timestamps = cast_with_options(column, &utc, &strict)?;
    Ok(timestamps
        .as_primitive::<TimestampNanosecondType>()
        .reinterpret_cast::<Int64Type>())
}

/// Wrap a refresh's rows so only rows newer than their key's kept version reach the write.
/// Rows deferred after a spill follow the last input batch.
pub(crate) fn select_latest(
    selector: LatestByTime,
    update: StreamingDataUpdate,
) -> StreamingDataUpdate {
    enum State {
        Selecting(Box<LatestByTime>, SendableRecordBatchStream),
        Resolving(SendableRecordBatchStream),
    }
    let schema = update.data.schema();
    let stream = futures::stream::try_unfold(
        State::Selecting(Box::new(selector), update.data),
        |state| async move {
            match state {
                State::Selecting(mut selector, mut input) => match input.try_next().await? {
                    Some(batch) => {
                        let selected = selector.select(&batch).await?;
                        Ok(Some((selected, State::Selecting(selector, input))))
                    }
                    None => match selector.finish().await? {
                        Some(mut resolved) => match resolved.try_next().await? {
                            Some(batch) => Ok(Some((batch, State::Resolving(resolved)))),
                            None => Ok(None),
                        },
                        None => Ok(None),
                    },
                },
                State::Resolving(mut resolved) => match resolved.try_next().await? {
                    Some(batch) => Ok(Some((batch, State::Resolving(resolved)))),
                    None => Ok(None),
                },
            }
        },
    );
    StreamingDataUpdate::new(
        Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
        update.update_type,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Date32Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "occurred_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("v", DataType::Utf8, false),
        ]))
    }

    fn batch(rows: &[(i64, Option<i64>, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(
                    rows.iter()
                        .map(|r| r.1)
                        .collect::<TimestampMicrosecondArray>(),
                ),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
            ],
        )
        .expect("valid batch")
    }

    fn selector() -> LatestByTime {
        LatestByTime::try_new(
            "events",
            &schema(),
            vec!["id".to_string()],
            "occurred_at".to_string(),
            None,
        )
        .expect("valid selector")
    }

    fn values(batch: &RecordBatch) -> Vec<String> {
        batch
            .column(2)
            .as_string::<i32>()
            .iter()
            .map(|v| v.unwrap_or_default().to_string())
            .collect()
    }

    #[tokio::test]
    async fn keeps_only_rows_newer_than_the_kept_version() {
        let mut s = selector();
        let first = s
            .select(&batch(&[(1, Some(10), "a10"), (2, Some(5), "b5")]))
            .await
            .expect("selects");
        assert_eq!(values(&first), ["a10", "b5"]);
        // Older and equal versions are dropped; the newer one passes.
        let second = s
            .select(&batch(&[
                (1, Some(8), "a8"),
                (1, Some(10), "a10"),
                (2, Some(7), "b7"),
            ]))
            .await
            .expect("selects");
        assert_eq!(values(&second), ["b7"]);
    }

    #[tokio::test]
    async fn writes_only_the_newest_kept_row_of_a_key_within_a_batch() {
        let mut s = selector();
        let out = s
            .select(&batch(&[
                (1, Some(1), "a1"),
                (1, Some(3), "a3"),
                (1, Some(2), "a2"),
            ]))
            .await
            .expect("selects");
        assert_eq!(values(&out), ["a3"]);
    }

    #[tokio::test]
    async fn a_seeded_stored_version_blocks_older_and_equal_rows() {
        let mut s = selector();
        s.seed(&batch(&[(1, Some(10), "stored")]))
            .await
            .expect("seeds");
        let out = s
            .select(&batch(&[
                (1, Some(8), "late"),
                (1, Some(10), "stored"),
                (2, Some(1), "new"),
            ]))
            .await
            .expect("selects");
        assert_eq!(values(&out), ["new"]);
        let newer = s
            .select(&batch(&[(1, Some(12), "newer")]))
            .await
            .expect("selects");
        assert_eq!(values(&newer), ["newer"]);
    }

    #[tokio::test]
    async fn a_null_time_fails_the_refresh_and_reports_a_count() {
        let mut s = selector();
        let err = s
            .select(&batch(&[(1, None, "x"), (2, None, "y"), (3, Some(1), "z")]))
            .await
            .expect_err("NULL time fails");
        let message = not_applied_message(&err).expect("a refresh-not-applied error");
        assert_eq!(
            message,
            "'time_column' 'occurred_at' is NULL in 2 rows, so this refresh was not applied and the previous data is still served. Fill 'occurred_at' at the source, or exclude those rows with 'acceleration.refresh_sql'. See: https://spiceai.org/docs/features/data-acceleration/constraints#upsert_dedup_by_time_column"
        );
        assert!(
            !message.contains("id="),
            "no key values in the message: {message}"
        );
    }

    #[test]
    fn iso8601_strings_compare_by_instant() {
        let column: ArrayRef = Arc::new(StringArray::from(vec![
            "2026-01-01T10:00:00+00:00",
            "2026-01-01T11:00:00+05:00",
            "2026-01-01T10:00:00",
        ]));
        let nanos = time_nanos(&column, Some(TimeFormat::ISO8601)).expect("parses");
        // 11:00+05:00 is 06:00 UTC, earlier than 10:00 UTC; a zone-less value is UTC.
        assert!(nanos.value(1) < nanos.value(0));
        assert_eq!(nanos.value(2), nanos.value(0));
    }

    #[test]
    fn unix_formats_scale_to_nanoseconds() {
        let column: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        assert_eq!(
            time_nanos(&column, Some(TimeFormat::UnixMillis))
                .expect("scales")
                .value(0),
            2_000_000
        );
        assert_eq!(
            time_nanos(&column, Some(TimeFormat::UnixSeconds))
                .expect("scales")
                .value(0),
            2_000_000_000
        );
    }

    #[tokio::test]
    async fn a_single_null_time_is_one_row() {
        let err = selector()
            .select(&batch(&[(1, None, "x")]))
            .await
            .expect_err("NULL time fails");
        let message = not_applied_message(&err).expect("a refresh-not-applied error");
        assert!(message.contains("is NULL in 1 row,"), "{message}");
    }

    #[tokio::test]
    async fn unreadable_times_report_a_count_and_the_format_but_no_value() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("occurred_at", DataType::Utf8, false),
        ]));
        let rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "not-a-time",
                    "2026-01-01T00:00:00Z",
                    "also-bad",
                ])),
            ],
        )
        .expect("valid batch");
        let mut s = LatestByTime::try_new(
            "events",
            &schema,
            vec!["id".to_string()],
            "occurred_at".to_string(),
            Some(TimeFormat::ISO8601),
        )
        .expect("valid selector");
        let err = s.select(&rows).await.expect_err("unreadable times fail");
        let message = not_applied_message(&err).expect("a refresh-not-applied error");
        assert_eq!(
            message,
            "'time_column' 'occurred_at' has 2 values that cannot be read as 'ISO8601', so this refresh was not applied and the previous data is still served. Correct the source values or set 'time_format' to match them. See: https://spiceai.org/docs/features/data-acceleration/constraints#upsert_dedup_by_time_column"
        );
        assert!(!message.contains("not-a-time"), "{message}");
    }

    #[test]
    fn a_column_the_refresh_does_not_read_is_named_with_its_fix() {
        let missing_time = LatestByTime::try_new(
            "events",
            &schema(),
            vec!["id".to_string()],
            "updated_at".to_string(),
            None,
        )
        .err()
        .expect("missing time column fails");
        assert_eq!(
            not_applied_message(&missing_time).as_deref(),
            Some(
                "'time_column' 'updated_at' is not in the rows the refresh reads, so versions of a key cannot be ordered. Include it in 'acceleration.refresh_sql'. See: https://spiceai.org/docs/features/data-acceleration/constraints#upsert_dedup_by_time_column"
            )
        );
        let missing_key = LatestByTime::try_new(
            "events",
            &schema(),
            vec!["tenant_id".to_string()],
            "occurred_at".to_string(),
            None,
        )
        .err()
        .expect("missing key column fails");
        assert_eq!(
            not_applied_message(&missing_key).as_deref(),
            Some(
                "primary key column 'tenant_id' is not in the rows the refresh reads, so versions of a key cannot be matched. Include it in 'acceleration.refresh_sql', or remove it from 'acceleration.primary_key'. See: https://spiceai.org/docs/features/data-acceleration/constraints#upsert_dedup_by_time_column"
            )
        );
    }

    #[tokio::test]
    async fn the_same_time_and_content_is_a_re_read_that_writes_nothing() {
        let mut s = selector();
        s.seed(&batch(&[(1, Some(10), "stored")]))
            .await
            .expect("seeds");
        let (out, superseded) = s
            .select_counted(&batch(&[
                (1, Some(10), "stored"),
                (1, Some(8), "late"),
                (2, Some(5), "first"),
            ]))
            .expect("selects");
        assert_eq!(values(&out), ["first"]);
        assert_eq!(
            superseded,
            Superseded {
                older: 1,
                equal_time: 0,
                unchanged: 1,
            }
        );
    }

    /// The row a key ends with: the last one written, as the engine keeps it.
    fn last_written(seed: Option<&RecordBatch>, batches: &[RecordBatch]) -> (String, Superseded) {
        let mut s = selector();
        if let Some(seed) = seed {
            let mut latest = std::mem::take(&mut s.latest);
            s.seed_into(&mut latest, seed).expect("seeds");
            s.latest = latest;
        }
        let mut last = seed.map(|b| values(b)[0].clone()).unwrap_or_default();
        let mut total = Superseded::default();
        for b in batches {
            let (out, superseded) = s.select_counted(b).expect("selects");
            if let Some(v) = values(&out).last() {
                last.clone_from(v);
            }
            total.older += superseded.older;
            total.equal_time += superseded.equal_time;
            total.unchanged += superseded.unchanged;
        }
        (last, total)
    }

    /// Two different rows with one time: the same one wins whatever order they are read
    /// in, within a batch, across batches, or against the stored copy, and the loser
    /// counts as `equal_time`.
    #[test]
    fn a_tie_keeps_the_same_row_whatever_the_read_order() {
        let a = || batch(&[(1, Some(10), "a")]);
        let b = || batch(&[(1, Some(10), "b")]);
        let (across_ab, sup_ab) = last_written(None, &[a(), b()]);
        let (across_ba, sup_ba) = last_written(None, &[b(), a()]);
        let (within_ab, _) = last_written(None, &[batch(&[(1, Some(10), "a"), (1, Some(10), "b")])]);
        let (within_ba, _) = last_written(None, &[batch(&[(1, Some(10), "b"), (1, Some(10), "a")])]);
        let (stored_a, _) = last_written(Some(&a()), &[b()]);
        let (stored_b, _) = last_written(Some(&b()), &[a()]);
        let winner = across_ab.clone();
        assert!(winner == "a" || winner == "b");
        for got in [&across_ba, &within_ab, &within_ba, &stored_a, &stored_b] {
            assert_eq!(got, &winner);
        }
        // Read loser first: it is written, then replaced. Winner first: the loser is
        // superseded and counted.
        assert_eq!(sup_ab.equal_time + sup_ba.equal_time, 1);
        assert_eq!(sup_ab.unchanged + sup_ba.unchanged, 0);
    }

    #[test]
    fn negative_zero_and_nan_hash_like_the_values_an_engine_stores_for_them() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("t", DataType::Int64, false),
            Field::new("f", DataType::Float64, true),
        ]));
        let rows = |f: Vec<Option<f64>>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1; f.len()])),
                    Arc::new(Int64Array::from(vec![1; f.len()])),
                    Arc::new(arrow::array::Float64Array::from(f)),
                ],
            )
            .expect("batch")
        };
        let s = LatestByTime::try_new(
            "events",
            &schema,
            vec!["id".to_string()],
            "t".to_string(),
            Some(TimeFormat::UnixSeconds),
        )
        .expect("selector");
        let incoming = s
            .content_hashes(&rows(vec![Some(-0.0), Some(f64::NAN), Some(1.5)]))
            .expect("hashes");
        let stored = s
            .content_hashes(&rows(vec![Some(0.0), None, Some(1.5)]))
            .expect("hashes");
        assert_eq!(incoming, stored);
        assert_ne!(incoming[0], incoming[2]);
    }

    #[test]
    fn earlier_copies_of_a_key_within_a_batch_count_as_older() {
        let (out, superseded) = selector()
            .select_counted(&batch(&[
                (1, Some(1), "a1"),
                (1, Some(3), "a3"),
                (1, Some(2), "a2"),
            ]))
            .expect("selects");
        assert_eq!(values(&out), ["a3"]);
        // a2 is older than a3 when read; a1 was kept and then superseded within the batch.
        assert_eq!(superseded.older, 2);
    }

    #[test]
    fn a_map_entry_is_at_most_32_bytes() {
        assert!(std::mem::size_of::<(u128, Kept)>() <= 32);
    }

    /// Keys that fit in 128 bits with a NULL flag per column are stored as themselves, so
    /// distinct keys never share an entry; wider keys fall back to a digest.
    #[test]
    fn narrow_keys_are_inline_and_wide_keys_are_digested() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Date32, true),
            Field::new("c", DataType::Int64, false),
            Field::new("d", DataType::Int64, false),
            Field::new("t", DataType::Int64, false),
        ]));
        let encoding = |keys: &[&str]| {
            LatestByTime::try_new(
                "events",
                &schema,
                keys.iter().map(ToString::to_string).collect(),
                "t".to_string(),
                Some(TimeFormat::UnixSeconds),
            )
            .expect("valid selector")
            .encoding
        };
        assert!(matches!(encoding(&["c"]), KeyEncoding::Inline(_)));
        assert!(matches!(encoding(&["a", "b"]), KeyEncoding::Inline(_)));
        assert!(matches!(encoding(&["a", "c"]), KeyEncoding::Inline(_)));
        assert!(matches!(encoding(&["c", "d"]), KeyEncoding::Digest(_)));

        let rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(-1), None, Some(0), Some(0)])),
                Arc::new(Date32Array::from(vec![Some(0), Some(0), None, Some(0)])),
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
            ],
        )
        .expect("valid batch");
        let s = LatestByTime::try_new(
            "events",
            &schema,
            vec!["a".to_string(), "b".to_string()],
            "t".to_string(),
            Some(TimeFormat::UnixSeconds),
        )
        .expect("valid selector");
        let keys = s.keys(&rows).expect("encodes");
        // (-1, 0), (NULL, 0), (0, NULL) and (0, 0) are four different keys.
        let distinct: std::collections::HashSet<u128> = keys.iter().copied().collect();
        assert_eq!(distinct.len(), 4, "{keys:?}");
    }

    /// The rows a refresh writes for `batches`, with the map charged to a pool of
    /// `pool_bytes` (unbounded when `None`): the last copy written of each key, as the
    /// engine keeps it.
    async fn refresh(
        batches: Vec<RecordBatch>,
        pool_bytes: Option<usize>,
    ) -> Result<Vec<String>, DataFusionError> {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        let mut s = selector();
        if let Some(bytes) = pool_bytes {
            let env = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(bytes)))
                .build_arc()
                .expect("runtime env");
            s = s.with_runtime_env(env);
        }
        let input = Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        let update = select_latest(
            s,
            StreamingDataUpdate::new(
                input,
                runtime_acceleration::dataupdate::UpdateType::Overwrite,
            ),
        );
        let written: Vec<RecordBatch> = update.data.try_collect().await?;
        let mut last: std::collections::BTreeMap<i64, String> = std::collections::BTreeMap::new();
        for batch in &written {
            let ids = batch.column(0).as_primitive::<Int64Type>();
            for (row, value) in values(batch).into_iter().enumerate() {
                last.insert(ids.value(row), value);
            }
        }
        Ok(last.into_values().collect())
    }

    const KEYS: i64 = 50 * 8_192;
    const ROUNDS: i64 = 3;
    const BATCH_ROWS: i64 = 8_192;

    /// Each key's time rises and falls across rounds, so later rounds carry both newer
    /// and older versions.
    fn time_of(id: i64, round: i64) -> i64 {
        (round * 7 + id) % 5 + round % 2
    }

    /// Every key's versions, round by round, in batches of `BATCH_ROWS`.
    fn versions() -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        for round in 0..ROUNDS {
            for start in (0..KEYS).step_by(usize::try_from(BATCH_ROWS).expect("batch rows")) {
                let rows: Vec<(i64, Option<i64>, String)> = (start..start + BATCH_ROWS)
                    .map(|id| {
                        let time = time_of(id, round);
                        (id, Some(time), format!("{id}@{time}"))
                    })
                    .collect();
                let refs: Vec<(i64, Option<i64>, &str)> =
                    rows.iter().map(|(a, b, c)| (*a, *b, c.as_str())).collect();
                batches.push(batch(&refs));
            }
        }
        batches
    }

    /// A refresh whose key map does not fit the pool spills mid-refresh and keeps the same
    /// rows as one that fits.
    #[tokio::test]
    async fn a_pool_smaller_than_the_map_spills_and_keeps_the_same_rows() {
        let newest: Vec<String> = (0..KEYS)
            .map(|id| {
                let time = (0..ROUNDS)
                    .map(|round| time_of(id, round))
                    .max()
                    .expect("rounds");
                format!("{id}@{time}")
            })
            .collect();
        let unbounded = refresh(versions(), None).await.expect("refresh");
        assert_eq!(unbounded, newest);
        // The map of every key does not fit; one batch, sorted, does.
        let pool = 8 * 1024 * 1024;
        assert!(map_bytes(usize::try_from(KEYS).expect("keys")) > pool);
        let spilled = refresh(versions(), Some(pool))
            .await
            .expect("refresh spills");
        assert_eq!(spilled, newest);
    }

    /// A pool too small to sort even one deferred batch fails the refresh with the spill
    /// message, rather than writing an unresolved version.
    #[tokio::test]
    async fn a_pool_too_small_to_spill_into_fails_the_refresh() {
        let err = refresh(versions(), Some(16 * 1024))
            .await
            .expect_err("nothing fits");
        let message = not_applied_message(&err).expect("a refresh-not-applied error");
        assert!(
            message.starts_with("'acceleration.on_conflict: upsert_dedup_by_time_column' could not spill its key versions to 'runtime.query.temp_directory', so this refresh was not applied and the previous data is still served. Free space there, or raise 'runtime.query.memory_limit'. Cause: "),
            "{message}"
        );
    }

    /// Per-batch cost of hashing every column of a row, next to today's per-row selection,
    /// for a narrow and a wide schema. Run with `--release -- --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark"]
    fn bench_row_hash_cost() {
        use arrow::array::{Float64Array, StringArray as Utf8};
        use datafusion::common::hash_utils::{RandomState, create_hashes};
        use std::time::Instant;

        const ROWS: usize = 8_192;
        const BATCHES: usize = 300;
        let words: Vec<String> = (0..4096).map(|i| format!("w{i:05}-{:x<14}", i % 977)).collect();
        let make = |wide: bool, b: usize| -> RecordBatch {
            let start = i64::try_from(b * ROWS).expect("start");
            let n = i64::try_from(ROWS).expect("rows");
            let mut fields = vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "occurred_at",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ];
            let mut cols: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(start..start + n)),
                Arc::new(TimestampMicrosecondArray::from_iter_values(start..start + n)),
            ];
            if wide {
                for c in 0..8_i64 {
                    fields.push(Field::new(format!("i{c}"), DataType::Int64, false));
                    cols.push(Arc::new(Int64Array::from_iter_values(
                        (start..start + n).map(|v| v.wrapping_mul(0x9E37_79B9 + c)),
                    )));
                }
                for c in 0..8_usize {
                    fields.push(Field::new(format!("s{c}"), DataType::Utf8, false));
                    cols.push(Arc::new(Utf8::from_iter_values(
                        (0..ROWS).map(|r| words[(r * 31 + c * 7 + b) % words.len()].as_str()),
                    )));
                }
                for c in 0..2_u32 {
                    fields.push(Field::new(format!("f{c}"), DataType::Float64, false));
                    cols.push(Arc::new(Float64Array::from_iter_values(
                        (0..ROWS).map(|r| f64::from(u32::try_from(r).expect("r")) * 1.5 + f64::from(c)),
                    )));
                }
            } else {
                fields.push(Field::new("v", DataType::Int64, false));
                cols.push(Arc::new(Int64Array::from_iter_values(start..start + n)));
            }
            RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("batch")
        };
        let stats = |mut us: Vec<f64>| {
            us.sort_by(f64::total_cmp);
            let p50 = us[us.len() / 2];
            let p99 = us[us.len() * 99 / 100];
            (p50, p99)
        };
        for wide in [false, true] {
            let batches: Vec<RecordBatch> = (0..BATCHES).map(|b| make(wide, b)).collect();
            let schema = batches[0].schema();
            let label = if wide { "wide(20 cols)" } else { "narrow(3 cols)" };

            // Today: select per row (distinct keys, so every row inserts).
            let mut selector = LatestByTime::try_new(
                "bench",
                &schema,
                vec!["id".to_string()],
                "occurred_at".to_string(),
                None,
            )
            .expect("selector");
            let mut us = Vec::new();
            for batch in &batches {
                let t = Instant::now();
                std::hint::black_box(selector.select_counted(batch).expect("select"));
                us.push(t.elapsed().as_secs_f64() * 1e6);
            }
            let (p50, p99) = stats(us);
            eprintln!("{label} selector_today       p50={p50:8.1}us p99={p99:8.1}us  ns/row(p99)={:.1}", p99 * 1e3 / ROWS as f64);

            // RowConverter over every column, then XXH3-64 per row.
            let converter = RowConverter::new(
                schema.fields().iter().map(|f| SortField::new(f.data_type().clone())).collect(),
            )
            .expect("converter");
            let mut us = Vec::new();
            for batch in &batches {
                let t = Instant::now();
                let rows = converter.convert_columns(batch.columns()).expect("rows");
                let hashes: Vec<u64> = rows
                    .iter()
                    .map(|row| twox_hash::XxHash3_64::oneshot(row.as_ref()))
                    .collect();
                std::hint::black_box(hashes);
                us.push(t.elapsed().as_secs_f64() * 1e6);
            }
            let (p50, p99) = stats(us);
            eprintln!("{label} rowconv+xxh3_64      p50={p50:8.1}us p99={p99:8.1}us  ns/row(p99)={:.1}", p99 * 1e3 / ROWS as f64);

            // DataFusion's vectorized column hashing with its fixed state.
            let state = RandomState::default();
            let mut buffer = vec![0_u64; ROWS];
            let mut us = Vec::new();
            for batch in &batches {
                let t = Instant::now();
                buffer.iter_mut().for_each(|h| *h = 0);
                create_hashes(batch.columns(), &state, &mut buffer).expect("hashes");
                std::hint::black_box(&buffer);
                us.push(t.elapsed().as_secs_f64() * 1e6);
            }
            let (p50, p99) = stats(us);
            eprintln!("{label} datafusion_hashes    p50={p50:8.1}us p99={p99:8.1}us  ns/row(p99)={:.1}", p99 * 1e3 / ROWS as f64);
        }
    }
}
