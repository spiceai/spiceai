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

//! Group commit for concurrent appends to one accelerated dataset.
//!
//! A write to an accelerated dataset holds the dataset's accelerator write lock
//! for its whole length, so concurrent appends to one dataset are written one at
//! a time, and a small append pays the accelerator's fixed cost of a write for
//! few rows. An append (`DataFusion::write_data`) that finds another append to
//! the same dataset being written queues its rows here instead of waiting with
//! them. The queued append that next takes the dataset's turn writes a group of
//! queued appends, in queue order, as one append, and tells each member the
//! outcome. An append that finds none being written or queued is written at
//! once, exactly as it would be without the queue. Every other append to the
//! dataset queues, even one that cannot share a write (it is written in a group
//! of its own), so no append is written ahead of one queued before it.
//!
//! A group never holds two appends that share a value of the dataset's key
//! (`AcceleratedTable::append_group_key`): the first queued append that shares
//! one with an earlier member starts the next group, which is written after this
//! one. Inside a group, then, each key value comes from one append, so the
//! accelerator keeps and counts what it would for that append alone, and the
//! group stores what the appends would store written one after another in queue
//! order. Nothing about which rows are stored depends on the key being the one
//! the accelerator resolves: a key it does not resolve only splits groups more
//! often.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow_row::{RowConverter, SortField};
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::{OwnedRwLockReadGuard, oneshot};

use crate::accelerated::AppendGrouping;

/// The most rows one append may bring to a group. A larger append is written in
/// a group of its own, in queue order.
const MEMBER_MAX_ROWS: usize = 8_192;

/// The most in-memory bytes one append may bring to a group.
const MEMBER_MAX_BYTES: usize = 8 * 1024 * 1024;

/// The most rows one group writes. A group always takes the append at the head
/// of the queue, so no append waits on this bound behind more than the groups
/// ahead of it.
const GROUP_MAX_ROWS: usize = 32_768;

/// The most in-memory bytes one group writes.
const GROUP_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Which step of an insert failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertStage {
    /// Building the insert's plan.
    Plan,
    /// Running it.
    Execute,
}

/// An insert's failure, shareable by every append its rows came from.
#[derive(Debug, Clone)]
pub(crate) struct InsertFailure {
    pub(crate) stage: InsertStage,
    pub(crate) error: Arc<DataFusionError>,
}

impl InsertFailure {
    pub(crate) fn new(stage: InsertStage, error: DataFusionError) -> Self {
        Self {
            stage,
            error: Arc::new(error),
        }
    }

    /// The error, unwrapped when no other append shares it.
    pub(crate) fn into_error(self) -> DataFusionError {
        Arc::try_unwrap(self.error).unwrap_or_else(DataFusionError::Shared)
    }
}

/// What a queued append is told once its group's write returns.
pub(crate) type MemberOutcome = std::result::Result<(), InsertFailure>;

/// The key values an append's rows carry, which no other member of its group
/// may carry.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RowKeys {
    /// The dataset has no key: any appends may share a group.
    Unkeyed,
    /// The append is written in a group of its own, still in queue order: it is
    /// too large to share a write, or holds rows the accelerator refuses (see
    /// [`row_keys`]), which in a group would fail every member.
    Alone,
    /// The encoded key of each row.
    Keyed(HashSet<Box<[u8]>>),
}

/// A dataset's turn to write, taken without waiting.
pub(crate) enum FreeTurn {
    /// No append is queued: the append that took the turn is written at once,
    /// on its own.
    Idle(tokio::sync::OwnedMutexGuard<()>),
    /// Appends are queued, and were queued first: the append that took the turn
    /// queues behind them and, holding the turn, writes the groups ahead of it
    /// before its own, so it cannot overtake an earlier append of a key.
    Behind(tokio::sync::OwnedMutexGuard<()>),
}

/// The appends waiting for one dataset's turn to write.
#[derive(Default)]
pub(crate) struct AppendQueue {
    /// Held by the append to this dataset being written through this queue,
    /// alone or as a group, until its write returns.
    turn: Arc<tokio::sync::Mutex<()>>,
    pending: ParkingMutex<Pending>,
    /// Groups claimed from the queue, for tests to tell a shared write ran.
    groups: AtomicU64,
    /// Appends written as members of those groups.
    members: AtomicU64,
}

#[derive(Default)]
struct Pending {
    next_id: u64,
    appends: VecDeque<PendingAppend>,
}

struct PendingAppend {
    id: u64,
    /// The name the append's caller wrote to, for its follow-up work.
    table_reference: TableReference,
    provider: Arc<dyn TableProvider>,
    schema: SchemaRef,
    data: Arc<Vec<RecordBatch>>,
    rows: usize,
    bytes: usize,
    keys: RowKeys,
    /// The append's hold on the dataset's provider, released once its rows are
    /// written, as an append written alone releases it.
    rebind_guard: OwnedRwLockReadGuard<()>,
    claimed: oneshot::Sender<()>,
    outcome: oneshot::Sender<MemberOutcome>,
}

/// An append waiting in a dataset's queue.
///
/// Dropping it while its rows are still queued withdraws them, so they are never
/// written, as an append dropped while waiting for its turn is never written.
/// Once a group has claimed them, the group's write owns them and writes them
/// whether or not this handle is still alive.
pub(crate) struct QueuedAppend {
    queue: Arc<AppendQueue>,
    id: u64,
    /// Resolves when a group claims this append, whichever append leads it.
    pub(crate) claimed: oneshot::Receiver<()>,
    /// Resolves with this append's outcome once its group's write returns.
    pub(crate) outcome: oneshot::Receiver<MemberOutcome>,
}

impl Drop for QueuedAppend {
    fn drop(&mut self) {
        let withdrawn = {
            let mut pending = self.queue.pending.lock();
            pending
                .appends
                .iter()
                .position(|append| append.id == self.id)
                .and_then(|position| pending.appends.remove(position))
        };
        // Released outside the queue's lock: it hands the dataset's provider back
        // to a schema change that may be waiting for it.
        drop(withdrawn);
    }
}

/// Appends claimed from a queue to be written as one append, in queue order.
pub(crate) struct AppendGroup {
    pub(crate) provider: Arc<dyn TableProvider>,
    pub(crate) schema: SchemaRef,
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) rows: usize,
    /// Each member's own rows and name, in queue order, for the work that
    /// follows its committed write.
    pub(crate) members: Vec<MemberWrite>,
    /// The members' holds on the dataset's provider, to release once the group's
    /// write returns, as an append written alone releases its own.
    pub(crate) rebind_guards: Vec<OwnedRwLockReadGuard<()>>,
    pub(crate) completion: GroupCompletion,
}

/// One member's own write, as its caller made it.
pub(crate) struct MemberWrite {
    pub(crate) table_reference: TableReference,
    pub(crate) data: Arc<Vec<RecordBatch>>,
}

/// Delivers a group's outcome to each of its members.
///
/// Dropped without [`Self::complete`] (the group's write panicked), every member
/// is told its outcome is unknown.
pub(crate) struct GroupCompletion {
    outcomes: Vec<oneshot::Sender<MemberOutcome>>,
}

impl GroupCompletion {
    /// Tell every member the outcome of the group's write: each the same
    /// success, or each the same failure, so no member of a failed write is told
    /// it succeeded.
    pub(crate) fn complete(self, result: &MemberOutcome) {
        for outcome in self.outcomes {
            // A member that stopped waiting has no one to tell; its rows were
            // written with the group's.
            let _ = outcome.send(result.clone());
        }
    }
}

impl AppendQueue {
    /// The dataset's turn to write, if no append to it is being written through
    /// this queue now.
    pub(crate) fn try_take_turn(&self) -> Option<FreeTurn> {
        let turn = Arc::clone(&self.turn).try_lock_owned().ok()?;
        // Read while holding the turn. Only a turn holder claims appends, so an
        // append queued before this read is still queued and is seen here.
        if self.pending.lock().appends.is_empty() {
            Some(FreeTurn::Idle(turn))
        } else {
            Some(FreeTurn::Behind(turn))
        }
    }

    /// The dataset's turn to write, once the append holding it is written.
    pub(crate) async fn take_turn(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.turn).lock_owned().await
    }

    /// Queue an append's rows to be written with the appends around it. `keys`
    /// are [`member_keys`]' answer for the rows.
    pub(crate) fn enqueue(
        self: &Arc<Self>,
        table_reference: TableReference,
        provider: Arc<dyn TableProvider>,
        schema: SchemaRef,
        data: Arc<Vec<RecordBatch>>,
        keys: RowKeys,
        rebind_guard: OwnedRwLockReadGuard<()>,
    ) -> QueuedAppend {
        let rows = data.iter().map(RecordBatch::num_rows).sum();
        let bytes = data.iter().map(RecordBatch::get_array_memory_size).sum();
        let (claimed_tx, claimed_rx) = oneshot::channel();
        let (outcome_tx, outcome_rx) = oneshot::channel();
        let id = {
            let mut pending = self.pending.lock();
            let id = pending.next_id;
            pending.next_id = pending.next_id.wrapping_add(1);
            pending.appends.push_back(PendingAppend {
                id,
                table_reference,
                provider,
                schema,
                data,
                rows,
                bytes,
                keys,
                rebind_guard,
                claimed: claimed_tx,
                outcome: outcome_tx,
            });
            id
        };
        QueuedAppend {
            queue: Arc::clone(self),
            id,
            claimed: claimed_rx,
            outcome: outcome_rx,
        }
    }

    /// Claim the group at the head of the queue for an append that holds the
    /// dataset's turn while `queued` is still waiting, or `None` when a group has
    /// already claimed `queued`.
    ///
    /// The group takes appends from the head while they go to the head's
    /// provider with the head's schema, share no key value with an append already
    /// taken, and fit the group's bounds. It always takes the head, and it may
    /// stop before `queued`, which then waits for a later group.
    pub(crate) fn claim(&self, queued: &QueuedAppend) -> Option<AppendGroup> {
        let taken = {
            let mut pending = self.pending.lock();
            if !pending.appends.iter().any(|append| append.id == queued.id) {
                return None;
            }
            let mut head = pending.appends.pop_front()?;
            let mut rows = head.rows;
            let mut bytes = head.bytes;
            let mut group_keys = std::mem::replace(&mut head.keys, RowKeys::Unkeyed);
            let provider = Arc::clone(&head.provider);
            let schema = Arc::clone(&head.schema);
            let mut taken = vec![head];
            while let Some(next) = pending.appends.front() {
                let fits = rows.saturating_add(next.rows) <= GROUP_MAX_ROWS
                    && bytes.saturating_add(next.bytes) <= GROUP_MAX_BYTES;
                let same_table = same_provider(&provider, &next.provider)
                    && (Arc::ptr_eq(&schema, &next.schema) || schema == next.schema);
                let disjoint = match (&group_keys, &next.keys) {
                    (RowKeys::Keyed(group), RowKeys::Keyed(keys)) => {
                        keys.iter().all(|key| !group.contains(key))
                    }
                    (RowKeys::Unkeyed, RowKeys::Unkeyed) => true,
                    // An append written alone shares a group with none, and one
                    // table's other appends are all keyed or all not.
                    _ => false,
                };
                if !fits || !same_table || !disjoint {
                    break;
                }
                let Some(mut next) = pending.appends.pop_front() else {
                    break;
                };
                rows = rows.saturating_add(next.rows);
                bytes = bytes.saturating_add(next.bytes);
                if let (RowKeys::Keyed(group), RowKeys::Keyed(keys)) = (
                    &mut group_keys,
                    std::mem::replace(&mut next.keys, RowKeys::Unkeyed),
                ) {
                    group.extend(keys);
                }
                taken.push(next);
            }
            taken
        };

        self.groups.fetch_add(1, Ordering::Relaxed);
        self.members
            .fetch_add(taken.len() as u64, Ordering::Relaxed);
        let provider = Arc::clone(&taken[0].provider);
        let schema = Arc::clone(&taken[0].schema);
        let mut rows = 0;
        let mut batches = Vec::with_capacity(taken.iter().map(|append| append.data.len()).sum());
        let mut members = Vec::with_capacity(taken.len());
        let mut outcomes = Vec::with_capacity(taken.len());
        let mut rebind_guards = Vec::with_capacity(taken.len());
        for append in taken {
            // A member that stopped waiting is written anyway: it was claimed.
            let _ = append.claimed.send(());
            rows += append.rows;
            batches.extend(append.data.iter().cloned());
            members.push(MemberWrite {
                table_reference: append.table_reference,
                data: append.data,
            });
            outcomes.push(append.outcome);
            rebind_guards.push(append.rebind_guard);
        }
        Some(AppendGroup {
            provider,
            schema,
            batches,
            rows,
            members,
            rebind_guards,
            completion: GroupCompletion { outcomes },
        })
    }

    /// How many appends are waiting to be claimed.
    #[must_use]
    pub(crate) fn queued(&self) -> usize {
        self.pending.lock().appends.len()
    }

    /// How many groups have been claimed, and how many appends they held.
    #[must_use]
    pub(crate) fn claimed(&self) -> (u64, u64) {
        (
            self.groups.load(Ordering::Relaxed),
            self.members.load(Ordering::Relaxed),
        )
    }
}

/// Whether two handles name the same provider.
fn same_provider(a: &Arc<dyn TableProvider>, b: &Arc<dyn TableProvider>) -> bool {
    std::ptr::eq(Arc::as_ptr(a).cast::<()>(), Arc::as_ptr(b).cast::<()>())
}

/// The key values `data` brings to a group: [`RowKeys::Alone`] when it is too
/// large to share a write or [`member_keys`] finds rows it cannot group.
pub(crate) fn row_keys(
    grouping: &AppendGrouping,
    schema: &SchemaRef,
    data: &[RecordBatch],
) -> RowKeys {
    let rows: usize = data.iter().map(RecordBatch::num_rows).sum();
    let bytes: usize = data.iter().map(RecordBatch::get_array_memory_size).sum();
    if rows > MEMBER_MAX_ROWS || bytes > MEMBER_MAX_BYTES {
        return RowKeys::Alone;
    }
    member_keys(grouping, schema, data).unwrap_or(RowKeys::Alone)
}

/// The encoded value of `grouping.key` in each row of `data`, to keep appends
/// that share one out of a group, or `None` when the append cannot join a group
/// and is written alone: a column of `grouping.non_null` is missing or holds a
/// null, which the accelerator refuses, and in a group that one append's row
/// would fail every member.
fn member_keys(
    grouping: &AppendGrouping,
    schema: &SchemaRef,
    data: &[RecordBatch],
) -> Option<RowKeys> {
    let non_null = grouping
        .non_null
        .iter()
        .map(|column| schema.index_of(column).ok())
        .collect::<Option<Vec<usize>>>()?;
    if data.iter().any(|batch| {
        non_null
            .iter()
            .any(|&index| batch.column(index).null_count() > 0)
    }) {
        return None;
    }
    if grouping.key.is_empty() {
        return Some(RowKeys::Unkeyed);
    }
    let indices = grouping
        .key
        .iter()
        .map(|column| schema.index_of(column).ok())
        .collect::<Option<Vec<usize>>>()?;
    let converter = RowConverter::new(
        indices
            .iter()
            .map(|&index| SortField::new(schema.field(index).data_type().clone()))
            .collect(),
    )
    .ok()?;
    let mut keys = HashSet::with_capacity(data.iter().map(RecordBatch::num_rows).sum());
    for batch in data {
        let columns: Vec<ArrayRef> = indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect();
        if columns.iter().any(|column| column.null_count() > 0) {
            return None;
        }
        let rows = converter.convert_columns(&columns).ok()?;
        keys.extend(rows.iter().map(|row| Box::<[u8]>::from(row.as_ref())));
    }
    Some(RowKeys::Keyed(keys))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::TableProvider;
    use datafusion::common::TableReference;
    use datafusion::datasource::MemTable;
    use datafusion::error::DataFusionError;

    use super::{
        AppendQueue, FreeTurn, InsertFailure, InsertStage, QueuedAppend, RowKeys, row_keys,
    };
    use crate::accelerated::AppendGrouping;

    fn grouping(key: &[&str], non_null: &[&str]) -> AppendGrouping {
        AppendGrouping {
            key: key.iter().map(ToString::to_string).collect(),
            non_null: non_null.iter().map(ToString::to_string).collect(),
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn batch(rows: &[(Option<i64>, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    fn provider() -> Arc<dyn TableProvider> {
        Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("mem table"))
    }

    fn values(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("value column")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    async fn enqueue(
        queue: &Arc<AppendQueue>,
        provider: &Arc<dyn TableProvider>,
        rows: &[(Option<i64>, i64)],
    ) -> QueuedAppend {
        let data = vec![batch(rows)];
        let keys = row_keys(&grouping(&["id"], &["id"]), &schema(), &data);
        let guard = Arc::new(tokio::sync::RwLock::new(())).read_owned().await;
        queue.enqueue(
            TableReference::bare("t"),
            Arc::clone(provider),
            schema(),
            Arc::new(data),
            keys,
            guard,
        )
    }

    #[tokio::test]
    async fn a_group_stops_before_an_append_sharing_a_key_with_a_member() {
        let queue = Arc::new(AppendQueue::default());
        let table = provider();
        let first = enqueue(&queue, &table, &[(Some(1), 10), (Some(2), 20)]).await;
        let second = enqueue(&queue, &table, &[(Some(3), 30)]).await;
        let third = enqueue(&queue, &table, &[(Some(2), 21)]).await;
        let fourth = enqueue(&queue, &table, &[(Some(4), 40)]).await;

        let group = queue.claim(&second).expect("second is queued");
        assert_eq!(values(&group.batches), vec![10, 20, 30]);
        assert_eq!(group.members.len(), 2);
        assert!(queue.claim(&first).is_none(), "first was claimed");

        let group = queue.claim(&fourth).expect("fourth is queued");
        assert_eq!(values(&group.batches), vec![21, 40]);
        assert!(queue.claim(&third).is_none(), "third was claimed");
        assert_eq!(queue.claimed(), (2, 4));
    }

    #[tokio::test]
    async fn a_group_stops_at_another_provider() {
        let queue = Arc::new(AppendQueue::default());
        let (one, other) = (provider(), provider());
        let first = enqueue(&queue, &one, &[(Some(1), 10)]).await;
        let _second = enqueue(&queue, &other, &[(Some(2), 20)]).await;
        let group = queue.claim(&first).expect("first is queued");
        assert_eq!(values(&group.batches), vec![10]);
        assert_eq!(queue.queued(), 1);
    }

    #[tokio::test]
    async fn dropping_a_queued_append_withdraws_its_rows() {
        let queue = Arc::new(AppendQueue::default());
        let table = provider();
        let kept = enqueue(&queue, &table, &[(Some(1), 10)]).await;
        let dropped = enqueue(&queue, &table, &[(Some(2), 20)]).await;
        drop(dropped);
        assert_eq!(queue.queued(), 1);
        let group = queue.claim(&kept).expect("kept is queued");
        assert_eq!(values(&group.batches), vec![10]);
    }

    #[tokio::test]
    async fn a_failed_group_gives_every_member_the_same_error() {
        let queue = Arc::new(AppendQueue::default());
        let table = provider();
        let mut first = enqueue(&queue, &table, &[(Some(1), 10)]).await;
        let mut second = enqueue(&queue, &table, &[(Some(2), 20)]).await;
        let group = queue.claim(&first).expect("first is queued");
        group.completion.complete(&Err(InsertFailure::new(
            InsertStage::Execute,
            DataFusionError::Execution("disk full".to_string()),
        )));
        for queued in [&mut first, &mut second] {
            let failure = queued
                .outcome
                .try_recv()
                .expect("outcome delivered")
                .expect_err("a failed group fails every member");
            assert_eq!(failure.stage, InsertStage::Execute);
            assert_eq!(
                failure.into_error().to_string(),
                "Execution error: disk full"
            );
        }
    }

    #[tokio::test]
    async fn a_turn_free_while_appends_wait_puts_the_newcomer_behind_them() {
        let queue = Arc::new(AppendQueue::default());
        let table = provider();
        assert!(
            matches!(queue.try_take_turn(), Some(FreeTurn::Idle(_))),
            "with nothing queued the newcomer is written at once"
        );

        // Queued, and not yet waiting for the turn when the turn comes free.
        let older = enqueue(&queue, &table, &[(Some(1), 10)]).await;
        let Some(FreeTurn::Behind(turn)) = queue.try_take_turn() else {
            panic!("a newcomer must not be written ahead of an append queued before it");
        };
        let newer = enqueue(&queue, &table, &[(Some(1), 20)]).await;
        let group = queue.claim(&newer).expect("the newcomer is queued");
        assert_eq!(
            values(&group.batches),
            vec![10],
            "the older append is written first"
        );
        let group = queue.claim(&newer).expect("the newcomer is still queued");
        assert_eq!(
            values(&group.batches),
            vec![20],
            "then the newer copy of key 1"
        );
        assert!(queue.claim(&older).is_none());
        drop(turn);
    }

    #[tokio::test]
    async fn an_append_written_alone_keeps_its_place_and_shares_no_group() {
        let queue = Arc::new(AppendQueue::default());
        let table = provider();
        let first = enqueue(&queue, &table, &[(Some(1), 10)]).await;
        let data = vec![batch(&[(None, 20)])];
        let alone = queue.enqueue(
            TableReference::bare("t"),
            Arc::clone(&table),
            schema(),
            Arc::new(data),
            RowKeys::Alone,
            Arc::new(tokio::sync::RwLock::new(())).read_owned().await,
        );
        let third = enqueue(&queue, &table, &[(Some(3), 30)]).await;

        let group = queue.claim(&third).expect("third is queued");
        assert_eq!(values(&group.batches), vec![10]);
        assert!(queue.claim(&first).is_none());
        let group = queue.claim(&third).expect("third is still queued");
        assert_eq!(values(&group.batches), vec![20], "written by itself");
        assert!(queue.claim(&alone).is_none());
        let group = queue.claim(&third).expect("third is still queued");
        assert_eq!(values(&group.batches), vec![30]);
    }

    #[test]
    fn rows_with_a_null_key_are_written_alone() {
        let data = vec![batch(&[(Some(1), 10), (None, 20)])];
        assert_eq!(
            row_keys(&grouping(&["id"], &["id"]), &schema(), &data),
            RowKeys::Alone
        );
        assert_eq!(
            row_keys(&grouping(&["missing"], &["missing"]), &schema(), &data),
            RowKeys::Alone,
            "a key column the rows lack keeps them out of groups too"
        );
        assert_eq!(
            row_keys(&grouping(&[], &[]), &schema(), &data),
            RowKeys::Unkeyed,
            "a dataset without a key groups any rows"
        );
        assert_eq!(
            row_keys(&grouping(&["value"], &["value", "id"]), &schema(), &data),
            RowKeys::Alone,
            "a null in a column another possible key names keeps the rows out too"
        );
        assert_eq!(
            row_keys(&grouping(&[], &["id"]), &schema(), &data),
            RowKeys::Alone,
            "even when the grouping key is empty"
        );
    }
}
