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

//! Secondary indexes over a memory-mode table's rows.
//!
//! A `mode: memory` table keeps every row in memory-tier segments of Arrow
//! batches and writes no files, so its `indexes` live beside those batches
//! rather than over files. Every batch of a segment gets its own index — each
//! row's key hash, sorted, with the row that holds it — built when the segment
//! is written. A segment's batches change only when a delete rewrites some of
//! them, and exactly those are re-indexed, so an index is always as current as
//! the batches it describes, including in every scan's captured view of the
//! tier.
//!
//! A lookup hashes its literal key, binary-searches each batch's hashes and
//! takes only the rows whose hash matches. Those rows then pass the same
//! tombstone visibility check and every original predicate as an ordinary
//! scan, so a hash collision costs a row the predicate drops, never a wrong
//! result. A batch without an index — the memory pool could not fit it, or it
//! lacks a key column — is read whole.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array};
use datafusion_common::ScalarValue;

use super::lookup_index::{
    Counters, KeyColumn, KeySpec, LookupIndexCounters, ProbeOutcome, cast_to, key_converter,
    record_probe_outcome,
};
use super::memory_account::{CayenneMemoryAccount, LookupIndexReservation};
use crate::row_converter::RowConverter;

/// What one indexed row costs per key: its hash and its row number.
const BYTES_PER_ENTRY: usize = size_of::<u64>() + size_of::<u32>();

/// One key of a memory-mode table, resolved against the table's stored schema.
struct ResolvedKey {
    label: String,
    columns: Vec<KeyColumn>,
    converter: RowConverter,
}

/// A memory-mode table's secondary indexes: its keys, and the account every
/// batch index reserves its bytes in.
pub(crate) struct MemTierIndexer {
    table_name: String,
    keys: Vec<ResolvedKey>,
    account: Arc<CayenneMemoryAccount>,
    /// Whether a refused batch has been reported yet, so the warning is logged
    /// once per table rather than once per batch.
    refusal_reported: AtomicBool,
    counters: Counters,
}

/// The key a lookup's filters pin, ready to probe with.
pub(crate) struct ProbeKey<'a> {
    /// Which of the table's keys, in declaration order.
    pub(crate) position: usize,
    pub(crate) label: &'a str,
    /// The literal key's hash, or `None` when a literal is NULL — which no row
    /// can equal.
    pub(crate) hash: Option<u64>,
}

impl MemTierIndexer {
    /// The indexer for `specs` over a table with stored `schema`, or `None` when
    /// no key can be indexed.
    pub(crate) fn new(
        table_name: &str,
        specs: &[KeySpec],
        schema: &arrow_schema::Schema,
        account: Arc<CayenneMemoryAccount>,
    ) -> Option<Arc<Self>> {
        let mut keys = Vec::with_capacity(specs.len());
        for spec in specs {
            let resolved = spec
                .columns()
                .iter()
                .map(|column| KeyColumn::resolve(schema, column))
                .collect::<Result<Vec<_>, String>>()
                .and_then(|columns| {
                    key_converter(&columns).map(|converter| ResolvedKey {
                        label: spec.label().to_string(),
                        columns,
                        converter,
                    })
                });
            match resolved {
                Ok(key) => keys.push(key),
                Err(error) => tracing::warn!(
                    table = %table_name,
                    "Dataset '{table_name}' (cayenne): the secondary index on {} cannot be built, so lookups on it scan the whole table. Cause: {error}",
                    spec.label()
                ),
            }
        }
        if keys.is_empty() {
            return None;
        }
        let labels: Vec<&str> = keys.iter().map(|key| key.label.as_str()).collect();
        tracing::info!(
            table = %table_name,
            "Dataset '{table_name}' (cayenne): maintaining in-memory secondary indexes on {}",
            labels.join(", ")
        );
        Some(Arc::new(Self {
            table_name: table_name.to_string(),
            keys,
            account,
            refusal_reported: AtomicBool::new(false),
            counters: Counters::default(),
        }))
    }

    /// Indexes one batch, or `None` when it has no rows, lacks a key column, or
    /// the memory pool cannot fit its index. A lookup reads such a batch whole.
    pub(crate) fn index_batch(&self, batch: &RecordBatch) -> Option<Arc<BatchIndex>> {
        let num_rows = u32::try_from(batch.num_rows()).ok()?;
        if num_rows == 0 {
            return None;
        }
        let mut keys = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            let columns = key
                .columns
                .iter()
                .map(|column| {
                    let array = batch.column_by_name(&column.name)?;
                    cast_to(array, &column.data_type).ok()
                })
                .collect::<Option<Vec<ArrayRef>>>()?;
            let encoded = key.converter.convert_columns(&columns).ok()?;
            let mut entries: Vec<(u64, u32)> = Vec::with_capacity(num_rows as usize);
            for row in 0..num_rows {
                let at = row as usize;
                // A NULL never satisfies an equality predicate, so an incomplete
                // key is not indexed.
                if columns.iter().any(|column| column.is_null(at)) {
                    continue;
                }
                entries.push((
                    hash_index::hash_key_bytes_oneshot(encoded.row(at).as_ref()),
                    row,
                ));
            }
            entries.sort_unstable();
            let (hashes, rows): (Vec<u64>, Vec<u32>) = entries.into_iter().unzip();
            keys.push(BatchKeyIndex {
                hashes: hashes.into_boxed_slice(),
                rows: rows.into_boxed_slice(),
            });
        }
        let bytes = keys
            .iter()
            .map(|key| key.hashes.len() * BYTES_PER_ENTRY)
            .sum::<usize>();
        let Some(reservation) = self.account.try_reserve_lookup_index(bytes) else {
            self.counters
                .builds_unpublished
                .fetch_add(1, Ordering::Relaxed);
            if !self.refusal_reported.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    table = %self.table_name,
                    "Dataset '{}' (cayenne): part of its secondary index was not built because the query memory pool cannot fit it, so lookups read those rows in full. Raise `runtime.query.memory_limit` or remove the entry from `indexes`. See: https://spiceai.org/docs/components/data-accelerators/cayenne",
                    self.table_name
                );
            }
            return None;
        };
        self.counters
            .builds_published
            .fetch_add(1, Ordering::Relaxed);
        Some(Arc::new(BatchIndex { keys, reservation }))
    }

    /// Indexes every batch of a new segment.
    pub(crate) fn index_segment(&self, batches: &[RecordBatch]) -> SegmentIndex {
        SegmentIndex {
            batches: batches
                .iter()
                .map(|batch| self.index_batch(batch))
                .collect(),
        }
    }

    /// The first key whose columns `scalar_for` all pins, hashed as the build
    /// hashes stored keys. `None` when no key is pinned, or a pinned literal
    /// cannot be cast to its column's type — the lookup then scans.
    ///
    /// `scalar_for` must only answer for predicates that compare the bare column
    /// with a value: a cast on the column side can hold for stored values other
    /// than the literal.
    pub(crate) fn probe_key(
        &self,
        scalar_for: &dyn Fn(&str) -> Option<ScalarValue>,
    ) -> Option<ProbeKey<'_>> {
        for (position, key) in self.keys.iter().enumerate() {
            let Some(values) = key
                .columns
                .iter()
                .map(|column| scalar_for(&column.name))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            let Some(literals) = values
                .iter()
                .zip(&key.columns)
                .map(|(value, column)| {
                    value
                        .cast_to(&column.data_type)
                        .ok()
                        .and_then(|value| value.to_array_of_size(1).ok())
                })
                .collect::<Option<Vec<ArrayRef>>>()
            else {
                continue;
            };
            if literals.iter().any(|literal| literal.is_null(0)) {
                return Some(ProbeKey {
                    position,
                    label: &key.label,
                    hash: None,
                });
            }
            let Ok(encoded) = key.converter.convert_columns(&literals) else {
                continue;
            };
            return Some(ProbeKey {
                position,
                label: &key.label,
                hash: Some(hash_index::hash_key_bytes_oneshot(encoded.row(0).as_ref())),
            });
        }
        None
    }

    /// Records how a lookup ended and how many rows it read.
    pub(crate) fn record(&self, label: &str, outcome: ProbeOutcome, candidate_rows: u64) {
        self.counters
            .candidate_rows
            .fetch_add(candidate_rows, Ordering::Relaxed);
        record_probe_outcome(&self.table_name, &self.counters, label, outcome);
    }

    pub(crate) fn counters(&self) -> LookupIndexCounters {
        let index_bytes = self.account.snapshot().lookup_index;
        self.counters
            .snapshot(u64::try_from(index_bytes).unwrap_or(u64::MAX))
    }
}

/// The index of one batch: per key, every row's key hash in order, with the row
/// holding it.
pub(crate) struct BatchIndex {
    keys: Vec<BatchKeyIndex>,
    /// This index's bytes in the table's memory account, released with it.
    reservation: LookupIndexReservation,
}

impl std::fmt::Debug for BatchIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchIndex")
            .field("keys", &self.keys.len())
            .field("bytes", &self.reservation.bytes())
            .finish()
    }
}

struct BatchKeyIndex {
    /// Key hashes, sorted.
    hashes: Box<[u64]>,
    /// The row holding `hashes[i]`; ascending among equal hashes.
    rows: Box<[u32]>,
}

impl BatchKeyIndex {
    /// The rows whose key hashes to `hash`, in row order.
    fn rows_for(&self, hash: u64) -> &[u32] {
        let start = self.hashes.partition_point(|&h| h < hash);
        let end = start + self.hashes[start..].partition_point(|&h| h == hash);
        &self.rows[start..end]
    }
}

/// The indexes of one memory-tier segment, one per batch in batch order. `None`
/// marks a batch a lookup must read whole.
#[derive(Debug)]
pub(crate) struct SegmentIndex {
    batches: Vec<Option<Arc<BatchIndex>>>,
}

/// The rows of one segment a lookup must read.
pub(crate) struct Candidates {
    pub(crate) batches: Vec<RecordBatch>,
    /// Whether any batch was read whole for want of an index.
    pub(crate) read_whole: bool,
}

impl SegmentIndex {
    pub(crate) fn from_batches(batches: Vec<Option<Arc<BatchIndex>>>) -> Self {
        Self { batches }
    }

    /// The index of the batch at `position`, if it has one.
    pub(crate) fn batch(&self, position: usize) -> Option<Arc<BatchIndex>> {
        self.batches.get(position).cloned().flatten()
    }

    /// The rows of `batches` — the segment this index describes — a lookup of
    /// the key `probe` must read.
    pub(crate) fn candidates(
        &self,
        batches: &[RecordBatch],
        probe: &ProbeKey<'_>,
    ) -> datafusion_common::Result<Candidates> {
        let mut candidates = Candidates {
            batches: Vec::new(),
            read_whole: false,
        };
        let Some(hash) = probe.hash else {
            return Ok(candidates);
        };
        // An index that does not line up with its batches describes something
        // else; read the segment whole rather than trust it.
        if self.batches.len() != batches.len() {
            candidates.batches = batches.to_vec();
            candidates.read_whole = true;
            return Ok(candidates);
        }
        for (batch, index) in batches.iter().zip(&self.batches) {
            let Some(key) = index
                .as_ref()
                .and_then(|index| index.keys.get(probe.position))
            else {
                candidates.batches.push(batch.clone());
                candidates.read_whole = true;
                continue;
            };
            let rows = key.rows_for(hash);
            if rows.is_empty() {
                continue;
            }
            let indices = UInt32Array::from(rows.to_vec());
            candidates
                .batches
                .push(arrow::compute::take_record_batch(batch, &indices)?);
        }
        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Int64, true),
            Field::new("service", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, false),
        ]))
    }

    fn batch(rows: &[(Option<i64>, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(tenant, _)| *tenant).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(_, service)| Some(*service))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..rows.len()).map(|i| format!("p{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    fn indexer(pool: &Arc<dyn MemoryPool>) -> Arc<MemTierIndexer> {
        MemTierIndexer::new(
            "memory_index_test",
            &KeySpec::from_indexes(&[vec!["tenant".to_string(), "service".to_string()]]),
            &schema(),
            Arc::new(CayenneMemoryAccount::new("memory_index_test", pool)),
        )
        .expect("indexer")
    }

    fn pinned(tenant: Option<i64>, service: &str) -> impl Fn(&str) -> Option<ScalarValue> {
        let service = service.to_string();
        move |column| match column {
            "tenant" => Some(ScalarValue::Int64(tenant)),
            "service" => Some(ScalarValue::Utf8(Some(service.clone()))),
            _ => None,
        }
    }

    fn payloads(candidates: &Candidates) -> Vec<String> {
        let mut found: Vec<String> = candidates
            .batches
            .iter()
            .flat_map(|batch| {
                let payload = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("payload");
                (0..payload.len())
                    .map(|i| payload.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        found.sort();
        found
    }

    #[test]
    fn lookups_read_exactly_the_rows_holding_the_key() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let indexer = indexer(&pool);
        let batches = vec![
            batch(&[(Some(1), "a"), (Some(2), "b"), (Some(1), "a"), (None, "a")]),
            batch(&[(Some(1), "a"), (Some(3), "c")]),
        ];
        let index = indexer.index_segment(&batches);

        let probe = indexer.probe_key(&pinned(Some(1), "a")).expect("pinned");
        let candidates = index.candidates(&batches, &probe).expect("candidates");
        assert!(!candidates.read_whole);
        // Rows 0 and 2 of the first batch and row 0 of the second; the NULL
        // tenant row is not a candidate.
        assert_eq!(payloads(&candidates), vec!["p0", "p0", "p2"]);

        let miss = indexer.probe_key(&pinned(Some(9), "a")).expect("pinned");
        assert!(
            index
                .candidates(&batches, &miss)
                .expect("miss")
                .batches
                .is_empty()
        );

        let null = indexer.probe_key(&pinned(None, "a")).expect("pinned");
        assert_eq!(null.hash, None);
        assert!(
            index
                .candidates(&batches, &null)
                .expect("null")
                .batches
                .is_empty()
        );

        assert!(
            indexer
                .probe_key(&|column| (column == "tenant").then_some(ScalarValue::Int64(Some(1))))
                .is_none(),
            "a key with an unpinned column is not probed"
        );
        // A literal of another type is cast to the column's before hashing.
        let widened = indexer
            .probe_key(&|column| match column {
                "tenant" => Some(ScalarValue::Int32(Some(1))),
                "service" => Some(ScalarValue::LargeUtf8(Some("a".to_string()))),
                _ => None,
            })
            .expect("pinned");
        assert_eq!(widened.hash, probe.hash);
    }

    #[test]
    fn a_batch_the_pool_cannot_fit_is_read_whole_and_holds_nothing() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64));
        let indexer = indexer(&pool);
        let batches = vec![batch(&[
            (Some(1), "a"),
            (Some(2), "b"),
            (Some(3), "c"),
            (Some(4), "d"),
            (Some(5), "e"),
            (Some(6), "f"),
        ])];
        let index = indexer.index_segment(&batches);
        assert_eq!(pool.reserved(), 0, "a refused batch index reserves nothing");
        let probe = indexer.probe_key(&pinned(Some(1), "a")).expect("pinned");
        let candidates = index.candidates(&batches, &probe).expect("candidates");
        assert!(candidates.read_whole);
        assert_eq!(candidates.batches[0].num_rows(), 6);
        assert_eq!(indexer.counters().builds_unpublished, 1);
    }

    #[test]
    fn an_index_holds_its_bytes_only_while_it_lives() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let indexer = indexer(&pool);
        let batches = vec![batch(&[(Some(1), "a"), (Some(2), "b")])];
        let index = indexer.index_segment(&batches);
        assert_eq!(pool.reserved(), 2 * BYTES_PER_ENTRY);
        let kept = index.batch(0).expect("indexed");
        drop(index);
        assert_eq!(
            pool.reserved(),
            2 * BYTES_PER_ENTRY,
            "a batch index shared with a newer segment stays"
        );
        drop(kept);
        assert_eq!(pool.reserved(), 0);
    }
}
