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
//! A lookup hashes every key tuple its filters pin — an equality or an `IN`
//! list per key column, their cartesian product up to a bound —
//! binary-searches each batch's hashes and takes only the rows whose hash
//! matches one of them. Those rows then pass the same
//! tombstone visibility check and every original predicate as an ordinary
//! scan, so a hash collision costs a row the predicate drops, never a wrong
//! result. A batch without an index — the memory pool could not fit it, or it
//! lacks a key column — is read whole.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;

use super::lookup_index::{
    Counters, Coverage, KeyColumn, KeySpec, LookupIndexCounters, LookupIndexScanReason, WarnOnce,
    cast_to, key_tuples, record_probe_outcome,
};
use super::memory_account::{CayenneMemoryAccount, LookupIndexReservation};
use key_index::{KeyEncoder, KeyField};

/// What one indexed row costs per key: its hash and its row number.
const BYTES_PER_ENTRY: usize = size_of::<u64>() + size_of::<u32>();

/// One key of a memory-mode table, resolved against the table's stored schema.
struct ResolvedKey {
    label: String,
    columns: Vec<KeyColumn>,
    /// Per column, the type `encoder` encodes it in: its own, or a
    /// dictionary's value type (see [`key_index::key_type`]). Build and probe
    /// both cast a value to the column's own type first, then to this one, so
    /// they encode it identically.
    encoded_types: Vec<DataType>,
    /// The file-mode index's encoding, so SQL-equal floats (`-0.0` and `0.0`,
    /// every NaN) share a hash and a lookup never misses a row its predicate
    /// keeps.
    encoder: KeyEncoder,
}

/// The hash of every row of `columns` under `encoder`, `None` for a row with a
/// NULL key value, which no equality predicate matches.
fn key_hashes(encoder: &KeyEncoder, columns: &[ArrayRef]) -> Option<Vec<Option<u64>>> {
    let bound = encoder.bind(columns).ok()?;
    let mut key = Vec::new();
    Some(
        (0..bound.num_rows())
            .map(|row| {
                (!bound.has_null(row)).then(|| {
                    key.clear();
                    bound.encode_row(row, &mut key);
                    hash_index::hash_key_bytes_oneshot(&key)
                })
            })
            .collect(),
    )
}

/// A memory-mode table's secondary indexes: its keys, and the account every
/// batch index reserves its bytes in.
pub(crate) struct MemTierIndexer {
    table_name: String,
    keys: Vec<ResolvedKey>,
    account: Arc<CayenneMemoryAccount>,
    /// A refused batch, reported at `warn` once per table rather than once
    /// per batch.
    refusal: WarnOnce,
    counters: Counters,
}

/// The keys a lookup's filters pin, ready to probe with.
pub(crate) struct ProbeKey<'a> {
    /// Which of the table's keys, in declaration order.
    pub(crate) position: usize,
    pub(crate) label: &'a str,
    /// The pinned key tuples' hashes, sorted and distinct. A tuple with a NULL
    /// literal is left out, since no row can equal it, so an empty set matches
    /// no row.
    pub(crate) hashes: Vec<u64>,
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
                    let encoded_types: Vec<DataType> = columns
                        .iter()
                        .map(|column| key_index::key_type(&column.data_type).clone())
                        .collect();
                    let encoder = KeyEncoder::new(
                        encoded_types
                            .iter()
                            .map(|data_type| KeyField::new(data_type.clone(), true))
                            .collect(),
                    )
                    .map_err(|e| e.to_string())?;
                    Ok(ResolvedKey {
                        label: spec.label().to_string(),
                        encoded_types,
                        columns,
                        encoder,
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
            refusal: WarnOnce::default(),
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
                .zip(&key.encoded_types)
                .map(|(column, encoded_type)| {
                    let array = batch.column_by_name(&column.name)?;
                    let array = cast_to(array, &column.data_type).ok()?;
                    cast_to(&array, encoded_type).ok()
                })
                .collect::<Option<Vec<ArrayRef>>>()?;
            // A NULL never satisfies an equality predicate, so an incomplete
            // key is not indexed.
            let mut entries: Vec<(u64, u32)> = key_hashes(&key.encoder, &columns)?
                .into_iter()
                .zip(0..num_rows)
                .filter_map(|(hash, row)| hash.map(|hash| (hash, row)))
                .collect();
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
            self.refusal.report(
                &self.table_name,
                &format!(
                    "Dataset '{}' (cayenne): part of its secondary index was not built because the query memory pool cannot fit it, so lookups read those rows in full. Raise `runtime.query.memory_limit` or remove the entry from `indexes`. See: https://spiceai.org/docs/components/data-accelerators/cayenne",
                    self.table_name
                ),
            );
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

    /// The first key whose columns `values_for` all pins, every tuple of their
    /// cartesian product hashed as the build hashes stored keys. `values_for`
    /// answers with the values a column is pinned to: one for an equality,
    /// several for an `IN` list. `None` when no key is pinned, the product
    /// exceeds the lookup bound, or a pinned literal cannot be cast to its
    /// column's type — the lookup then scans, for the reason returned.
    ///
    /// `values_for` must only answer for predicates that compare the bare
    /// column with a value: a cast on the column side can hold for stored
    /// values other than the literal.
    pub(crate) fn probe_key(
        &self,
        values_for: &dyn Fn(&str) -> Option<Vec<ScalarValue>>,
    ) -> Result<ProbeKey<'_>, LookupIndexScanReason> {
        // Why a pinned key could not be probed outranks "no key pinned".
        let mut reason = LookupIndexScanReason::NoKeyPinned;
        for (position, key) in self.keys.iter().enumerate() {
            let names: Vec<String> = key
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect();
            if !names.iter().all(|name| values_for(name).is_some()) {
                continue;
            }
            let Some(tuples) = key_tuples(&names, values_for) else {
                reason = LookupIndexScanReason::TooManyKeys;
                continue;
            };
            let Some(hashes) = Self::hash_tuples(key, &tuples) else {
                reason = LookupIndexScanReason::ValueNotIndexable;
                continue;
            };
            return Ok(ProbeKey {
                position,
                label: &key.label,
                hashes,
            });
        }
        Err(reason)
    }

    /// The sorted, distinct hashes of `tuples` under `key`'s encoding, leaving
    /// out tuples with a NULL value. `None` when a value cannot be cast to its
    /// column's type.
    fn hash_tuples(key: &ResolvedKey, tuples: &[Vec<ScalarValue>]) -> Option<Vec<u64>> {
        if tuples.is_empty() {
            return Some(Vec::new());
        }
        let columns = key
            .columns
            .iter()
            .zip(&key.encoded_types)
            .enumerate()
            .map(|(at, (column, encoded_type))| {
                let values = tuples
                    .iter()
                    .map(|tuple| {
                        tuple[at]
                            .cast_to(&column.data_type)
                            .and_then(|value| value.cast_to(encoded_type))
                            .ok()
                    })
                    .collect::<Option<Vec<_>>>()?;
                ScalarValue::iter_to_array(values).ok()
            })
            .collect::<Option<Vec<ArrayRef>>>()?;
        let mut hashes: Vec<u64> = key_hashes(&key.encoder, &columns)?
            .into_iter()
            .flatten()
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        Some(hashes)
    }

    /// Records how a lookup ended and how many rows it read.
    pub(crate) fn record(&self, label: &str, coverage: Coverage, candidate_rows: u64) {
        self.counters
            .candidate_rows
            .fetch_add(candidate_rows, Ordering::Relaxed);
        record_probe_outcome(&self.table_name, &self.counters, label, coverage);
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
    /// Batches read whole for want of an index.
    pub(crate) read_whole: usize,
    /// Batches narrowed by their index.
    pub(crate) indexed: usize,
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
            read_whole: 0,
            indexed: 0,
        };
        if probe.hashes.is_empty() {
            return Ok(candidates);
        }
        // An index that does not line up with its batches describes something
        // else; read the segment whole rather than trust it.
        if self.batches.len() != batches.len() {
            candidates.batches = batches.to_vec();
            candidates.read_whole = batches.len();
            return Ok(candidates);
        }
        for (batch, index) in batches.iter().zip(&self.batches) {
            let Some(key) = index
                .as_ref()
                .and_then(|index| index.keys.get(probe.position))
            else {
                candidates.batches.push(batch.clone());
                candidates.read_whole += 1;
                continue;
            };
            candidates.indexed += 1;
            // Distinct hashes hold disjoint rows, so the union needs only
            // sorting back into row order.
            let mut rows: Vec<u32> = probe
                .hashes
                .iter()
                .flat_map(|&hash| key.rows_for(hash).iter().copied())
                .collect();
            if rows.is_empty() {
                continue;
            }
            rows.sort_unstable();
            let indices = UInt32Array::from(rows);
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
    use arrow::array::{Array, Int64Array, StringArray};
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

    fn pinned(tenant: Option<i64>, service: &str) -> impl Fn(&str) -> Option<Vec<ScalarValue>> {
        let service = service.to_string();
        move |column| match column {
            "tenant" => Some(vec![ScalarValue::Int64(tenant)]),
            "service" => Some(vec![ScalarValue::Utf8(Some(service.clone()))]),
            _ => None,
        }
    }

    /// `tenant IN (tenants) AND service IN (services)`, as the scan's filters
    /// pin them.
    fn pinned_lists(
        tenants: Vec<Option<i64>>,
        services: Vec<Option<&str>>,
    ) -> impl Fn(&str) -> Option<Vec<ScalarValue>> {
        let services: Vec<Option<String>> = services
            .into_iter()
            .map(|service| service.map(str::to_string))
            .collect();
        move |column| match column {
            "tenant" => Some(
                tenants
                    .iter()
                    .map(|tenant| ScalarValue::Int64(*tenant))
                    .collect(),
            ),
            "service" => Some(
                services
                    .iter()
                    .map(|service| ScalarValue::Utf8(service.clone()))
                    .collect(),
            ),
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
        assert_eq!(candidates.read_whole, 0);
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
        assert!(null.hashes.is_empty(), "a NULL literal matches no row");
        assert!(
            index
                .candidates(&batches, &null)
                .expect("null")
                .batches
                .is_empty()
        );

        assert_eq!(
            indexer
                .probe_key(&|column| (column == "tenant").then(|| vec![ScalarValue::Int64(Some(1))]))
                .err(),
            Some(LookupIndexScanReason::NoKeyPinned),
            "a key with an unpinned column is not probed"
        );
        // A literal of another type is cast to the column's before hashing.
        let widened = indexer
            .probe_key(&|column| match column {
                "tenant" => Some(vec![ScalarValue::Int32(Some(1))]),
                "service" => Some(vec![ScalarValue::LargeUtf8(Some("a".to_string()))]),
                _ => None,
            })
            .expect("pinned");
        assert_eq!(widened.hashes, probe.hashes);
    }

    /// `tenant = $t AND service IN ($s)`, and `IN` lists on both columns, over
    /// a compound key: every subset of tenants and of services, including
    /// empty lists and lists holding NULL, reads exactly the rows the
    /// predicate keeps, in row order.
    #[test]
    fn in_lists_over_a_compound_key_read_exactly_the_matching_rows() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let indexer = indexer(&pool);
        let tenants = [None, Some(0), Some(1), Some(2), Some(3)];
        let services = ["a", "b", "c", "d"];
        let rows: Vec<(Option<i64>, &str)> = (0..60)
            .map(|i| {
                (
                    tenants[i * 7 % tenants.len()],
                    services[i * 3 % services.len()],
                )
            })
            .collect();
        let batches = vec![batch(&rows[..25]), batch(&rows[25..])];
        let index = indexer.index_segment(&batches);
        // The payload a row of `batch` gets: its position within its batch.
        let payload_of = |at: usize| {
            let within = if at < 25 { at } else { at - 25 };
            format!("p{within}")
        };
        let domain_tenants = [Some(0), Some(1), Some(2), Some(9), None];
        let domain_services = [Some("a"), Some("b"), Some("c"), None];
        for tenant_mask in 0..1_u32 << domain_tenants.len() {
            for service_mask in 0..1_u32 << domain_services.len() {
                let chosen_tenants: Vec<Option<i64>> = (0..domain_tenants.len())
                    .filter(|bit| tenant_mask & (1 << bit) != 0)
                    .map(|bit| domain_tenants[bit])
                    .collect();
                let chosen_services: Vec<Option<&str>> = (0..domain_services.len())
                    .filter(|bit| service_mask & (1 << bit) != 0)
                    .map(|bit| domain_services[bit])
                    .collect();
                let probe = indexer
                    .probe_key(&pinned_lists(
                        chosen_tenants.clone(),
                        chosen_services.clone(),
                    ))
                    .expect("both key columns pinned");
                let candidates = index.candidates(&batches, &probe).expect("candidates");
                assert_eq!(candidates.read_whole, 0);
                let mut expected: Vec<String> = rows
                    .iter()
                    .enumerate()
                    .filter(|(_, (tenant, service))| {
                        tenant.is_some()
                            && chosen_tenants.contains(tenant)
                            && chosen_services.contains(&Some(*service))
                    })
                    .map(|(at, _)| payload_of(at))
                    .collect();
                expected.sort();
                assert_eq!(
                    payloads(&candidates),
                    expected,
                    "tenant IN {chosen_tenants:?} AND service IN {chosen_services:?}"
                );
                for batch in &candidates.batches {
                    let payload = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("payload");
                    let order: Vec<usize> = (0..payload.len())
                        .map(|i| payload.value(i)[1..].parse().expect("row number"))
                        .collect();
                    assert!(
                        order.windows(2).all(|pair| pair[0] < pair[1]),
                        "rows out of order: {order:?}"
                    );
                }
            }
        }
    }

    /// A key whose lists multiply past the lookup bound is not probed, so the
    /// lookup scans rather than enumerating the tuples.
    #[test]
    fn in_lists_past_the_bound_are_not_probed() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let indexer = indexer(&pool);
        let many = |column: &str| match column {
            "tenant" => Some(
                (0..100)
                    .map(|tenant| ScalarValue::Int64(Some(tenant)))
                    .collect(),
            ),
            "service" => Some(
                (0..100)
                    .map(|service| ScalarValue::Utf8(Some(format!("s{service}"))))
                    .collect(),
            ),
            _ => None,
        };
        assert_eq!(
            indexer.probe_key(&many).err(),
            Some(LookupIndexScanReason::TooManyKeys)
        );
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
        assert!(candidates.read_whole > 0);
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
