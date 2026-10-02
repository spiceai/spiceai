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
//! key — the key's encoded bytes and the greatest `time_column` kept so far, never the rest
//! of the row. A row is passed to the write only if its time is strictly greater than that
//! entry; every other row is dropped and counted in
//! `dataset_acceleration_refresh_rows_superseded`. On append, the selector is first seeded
//! with the keys and times the acceleration already stores from the append window start, so
//! a late row never replaces a newer stored version. Within a refresh, the rows passed for
//! a key arrive in increasing time order, so the last one written for a key is its newest.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch};
use arrow::compute::{CastOptions, cast_with_options, filter_record_batch};
use arrow::datatypes::{DataType, Int64Type, SchemaRef, TimeUnit, TimestampNanosecondType};
use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use opentelemetry::KeyValue;
use runtime_acceleration::dataupdate::StreamingDataUpdate;
use runtime_component::dataset::TimeFormat;
use runtime_metrics::acceleration as metrics;

/// The newest kept version of each key, by `time_column`, for one refresh.
pub(crate) struct LatestByTime {
    dataset: String,
    key_columns: Vec<String>,
    /// The type each key column is encoded as: the incoming rows' types, so stored keys
    /// read back from an engine that rewrites types still encode to the same bytes.
    key_types: Vec<DataType>,
    time_column: String,
    time_format: Option<TimeFormat>,
    converter: RowConverter,
    /// Encoded key → greatest `time_column` kept, in UTC nanoseconds.
    latest: HashMap<Box<[u8]>, i64>,
    older_labels: [KeyValue; 2],
    equal_time_labels: [KeyValue; 2],
}

impl LatestByTime {
    /// A selector for rows of `schema` keyed on `key_columns`, ordered by `time_column`.
    ///
    /// # Errors
    ///
    /// Returns an error if a key column or the time column is missing from `schema`.
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
                        DataFusionError::Plan(format!(
                            "Failed to refresh dataset '{dataset}': primary key column '{name}' is not in the rows the refresh reads, so 'acceleration.on_conflict: upsert_dedup_by_time_column' cannot match versions of a key. Include it in 'acceleration.refresh_sql', or remove it from 'acceleration.primary_key'."
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if schema.field_with_name(&time_column).is_err() {
            return Err(DataFusionError::Plan(format!(
                "Failed to refresh dataset '{dataset}': 'time_column' '{time_column}' is not in the rows the refresh reads, so 'acceleration.on_conflict: upsert_dedup_by_time_column' cannot order versions of a key. Include it in 'acceleration.refresh_sql'."
            )));
        }
        let converter = RowConverter::new(
            key_types
                .iter()
                .map(|t| SortField::new(t.clone()))
                .collect(),
        )?;
        Ok(Self {
            older_labels: [
                KeyValue::new("dataset", dataset.to_string()),
                KeyValue::new("reason", "older"),
            ],
            equal_time_labels: [
                KeyValue::new("dataset", dataset.to_string()),
                KeyValue::new("reason", "equal_time"),
            ],
            dataset: dataset.to_string(),
            key_columns,
            key_types,
            time_column,
            time_format,
            converter,
            latest: HashMap::new(),
        })
    }

    /// Publish both `reason` series at `0`, so a dashboard sees the series before the
    /// first superseded row and an alert can fire on its rise.
    pub(crate) fn publish_zero(&self) {
        metrics::REFRESH_ROWS_SUPERSEDED.add(0, &self.older_labels);
        metrics::REFRESH_ROWS_SUPERSEDED.add(0, &self.equal_time_labels);
    }

    /// Record the keys and times already stored. Rows with a NULL stored time are
    /// skipped: any incoming version replaces them.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch lacks a key or time column, or a time cannot be read.
    pub(crate) fn seed(&mut self, stored: &RecordBatch) -> Result<(), DataFusionError> {
        let keys = self.encode_keys(stored)?;
        let times = self.times(stored)?;
        for row in 0..stored.num_rows() {
            if times.is_null(row) {
                continue;
            }
            let time = times.value(row);
            match self.latest.entry(Box::from(keys.row(row).as_ref())) {
                Entry::Occupied(mut entry) => {
                    if time > *entry.get() {
                        entry.insert(time);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(time);
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
    /// Returns an error if a time is NULL or cannot be read: the refresh then writes
    /// nothing, rather than choosing a version without one.
    pub(crate) fn select(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch.clone());
        }
        let times = self.times(batch)?;
        if times.null_count() > 0 {
            return Err(DataFusionError::Execution(format!(
                "Failed to refresh dataset '{}': 'time_column' '{}' is NULL in {} rows, so their latest version cannot be chosen, this refresh was not applied, and the acceleration keeps its previous data. Fill '{}' at the source, or exclude those rows with 'acceleration.refresh_sql'. See: https://spiceai.org/docs/features/data-acceleration/constraints#upsert_dedup_by_time_column",
                self.dataset,
                self.time_column,
                times.null_count(),
                self.time_column,
            )));
        }
        let keys = self.encode_keys(batch)?;
        let mut keep = vec![false; num_rows];
        // Row index of the last kept row of each key within this batch.
        let mut kept_in_batch: HashMap<&[u8], usize> = HashMap::new();
        let mut older = 0_u64;
        let mut equal_time = 0_u64;
        for (row, keep_row) in keep.iter_mut().enumerate() {
            let key = keys.row(row);
            let time = times.value(row);
            let newer = match self.latest.get_mut(key.as_ref()) {
                Some(kept) if time > *kept => {
                    *kept = time;
                    true
                }
                Some(kept) => {
                    if time == *kept {
                        equal_time += 1;
                    } else {
                        older += 1;
                    }
                    false
                }
                None => {
                    self.latest.insert(Box::from(key.as_ref()), time);
                    true
                }
            };
            if newer {
                *keep_row = true;
            }
        }
        // A key kept more than once in this batch: only its last (newest) row is written,
        // and the earlier ones count as superseded by it.
        for (row, _) in keep.iter().enumerate().filter(|(_, keep_row)| **keep_row) {
            kept_in_batch.insert(keys.row(row).data(), row);
        }
        let last_rows: std::collections::HashSet<usize> = kept_in_batch.into_values().collect();
        for (row, keep_row) in keep.iter_mut().enumerate() {
            if *keep_row && !last_rows.contains(&row) {
                *keep_row = false;
                older += 1;
            }
        }
        if older > 0 {
            metrics::REFRESH_ROWS_SUPERSEDED.add(older, &self.older_labels);
        }
        if equal_time > 0 {
            metrics::REFRESH_ROWS_SUPERSEDED.add(equal_time, &self.equal_time_labels);
        }
        Ok(filter_record_batch(batch, &BooleanArray::from(keep))?)
    }

    fn encode_keys(&self, batch: &RecordBatch) -> Result<arrow::row::Rows, DataFusionError> {
        let columns = self
            .key_columns
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
            .collect::<Result<Vec<ArrayRef>, DataFusionError>>()?;
        Ok(self.converter.convert_columns(&columns)?)
    }

    /// The time column as UTC nanoseconds, interpreted per `time_format`. Values without
    /// a zone are read as UTC; strings are parsed, so offsets compare by instant.
    fn times(&self, batch: &RecordBatch) -> Result<arrow::array::Int64Array, DataFusionError> {
        let column = batch.column_by_name(&self.time_column).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "time column '{}' missing from a batch of dataset '{}'",
                self.time_column, self.dataset
            ))
        })?;
        time_nanos(column, self.time_format).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to refresh dataset '{}': 'time_column' '{}' has values that cannot be read as times, so this refresh was not applied and the acceleration keeps its previous data. Correct the source values or set 'time_format' to match them. Cause: {e}. See: https://spiceai.org/docs/features/data-acceleration/constraints#upsert_dedup_by_time_column",
                self.dataset, self.time_column
            ))
        })
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
        let scale: i64 = match time_format {
            Some(TimeFormat::UnixNanos) => 1,
            Some(TimeFormat::UnixMillis) => 1_000_000,
            _ => 1_000_000_000,
        };
        let values = cast_with_options(column, &DataType::Int64, &strict)?;
        return values.as_primitive::<Int64Type>().try_unary(|v| {
            v.checked_mul(scale).ok_or_else(|| {
                ArrowError::ComputeError(format!("time value {v} overflows nanoseconds"))
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
pub(crate) fn select_latest(
    mut selector: LatestByTime,
    update: StreamingDataUpdate,
) -> StreamingDataUpdate {
    let schema = update.data.schema();
    let selected = update
        .data
        .map(move |batch| batch.and_then(|batch| selector.select(&batch)));
    StreamingDataUpdate::new(
        Box::pin(RecordBatchStreamAdapter::new(schema, selected)),
        update.update_type,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
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

    #[test]
    fn keeps_only_rows_newer_than_the_kept_version() {
        let mut s = selector();
        let first = s
            .select(&batch(&[(1, Some(10), "a10"), (2, Some(5), "b5")]))
            .expect("selects");
        assert_eq!(values(&first), ["a10", "b5"]);
        // Older and equal versions are dropped; the newer one passes.
        let second = s
            .select(&batch(&[
                (1, Some(8), "a8"),
                (1, Some(10), "a10-again"),
                (2, Some(7), "b7"),
            ]))
            .expect("selects");
        assert_eq!(values(&second), ["b7"]);
    }

    #[test]
    fn writes_only_the_newest_kept_row_of_a_key_within_a_batch() {
        let mut s = selector();
        let out = s
            .select(&batch(&[
                (1, Some(1), "a1"),
                (1, Some(3), "a3"),
                (1, Some(2), "a2"),
            ]))
            .expect("selects");
        assert_eq!(values(&out), ["a3"]);
    }

    #[test]
    fn a_seeded_stored_version_blocks_older_and_equal_rows() {
        let mut s = selector();
        s.seed(&batch(&[(1, Some(10), "stored")])).expect("seeds");
        let out = s
            .select(&batch(&[
                (1, Some(8), "late"),
                (1, Some(10), "same"),
                (2, Some(1), "new"),
            ]))
            .expect("selects");
        assert_eq!(values(&out), ["new"]);
        let newer = s
            .select(&batch(&[(1, Some(12), "newer")]))
            .expect("selects");
        assert_eq!(values(&newer), ["newer"]);
    }

    #[test]
    fn a_null_time_fails_the_refresh_and_reports_a_count() {
        let mut s = selector();
        let err = s
            .select(&batch(&[(1, None, "x"), (2, None, "y"), (3, Some(1), "z")]))
            .expect_err("NULL time fails");
        let message = err.to_string();
        assert!(message.contains("is NULL in 2 rows"), "{message}");
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
}
