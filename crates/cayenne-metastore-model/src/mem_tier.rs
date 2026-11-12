// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Int64 memory CDC visibility, pending publication, and source redelivery.
//! Citations are relative to `crates/cayenne/src` at 43bee9dd8b plus the
//! working-tree changes read for this model; see the README for scope limits.
use stateright::{Model, Property};

type Rows = Vec<(usize, usize)>;
type Index = [(usize, usize); 2]; // (delete, insert), zero = absent

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Row {
    key: usize,
    value: usize,
    seq: usize,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Segment {
    row: Option<Row>,
    deletes: Index,
    position: usize,
    sealed: bool,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Snapshot {
    sequence: usize,
    min_sequence: usize,
    rows: Vec<Row>,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Change {
    High(usize),
    Inline(Vec<Row>),
    Deletes(Index),
    Snapshot(Snapshot),
    Inserts(Vec<usize>, usize),
    ClearInline,
    Bake(Snapshot),
    Position(usize),
    FullRewrite(Rewrite),
}
// Audit history is observable through steps(), but no transition or property
// consults its grouping. States with equal durable catalog, live catalog, pending
// actions and protocol state have identical successors and verdicts regardless
// of earlier record boundaries. Retain the actual path's records for trace replay.
#[derive(Clone, Debug, Default)]
struct AuditRecords(Vec<Vec<Change>>);
impl PartialEq for AuditRecords {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for AuditRecords {}
impl std::hash::Hash for AuditRecords {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq)]
struct Catalog {
    high: usize,
    inline: Vec<Row>,
    deletes: Index,
    vectors: Vec<Index>,
    inserts: [usize; 2],
    current: Vec<Row>,
    current_min: Option<usize>,
    snapshots: Vec<Snapshot>,
    has_delete_file: bool,
    position: usize,
}
impl Catalog {
    fn apply(&mut self, change: &Change) {
        match change {
            Change::High(n) => self.high = self.high.max(*n),
            Change::Inline(rows) => self.inline.extend(rows.clone()),
            Change::Deletes(index) => {
                merge(&mut self.deletes, *index);
                if index.iter().any(|&(d, _)| d > 0) {
                    self.vectors.push(*index);
                    self.has_delete_file = true;
                }
            }
            Change::Snapshot(snapshot) => self.snapshots.push(snapshot.clone()),
            Change::Inserts(keys, seq) => {
                for &key in keys {
                    self.inserts[key] = self.inserts[key].max(*seq);
                    self.deletes[key].1 = self.deletes[key].1.max(*seq);
                }
            }
            Change::ClearInline => self.inline.clear(),
            Change::Bake(snapshot) => self.snapshots = vec![snapshot.clone()],
            Change::Position(n) => self.position = self.position.max(*n),
            Change::FullRewrite(rewrite) => {
                self.current.clone_from(&rewrite.rows);
                self.current_min = rewrite.min_sequence;
                self.snapshots.retain(|s| !rewrite.folded.contains(s));
                // commit_compaction_fenced removes vector FILES by delete seq,
                // including their reinsert stamp, and insert RECORDS by insert seq.
                for vector in &mut self.vectors {
                    for entry in vector {
                        if entry.0 <= rewrite.cutoff {
                            *entry = (0, 0);
                        }
                    }
                }
                self.vectors.retain(|v| v.iter().any(|&(d, _)| d > 0));
                self.has_delete_file = !self.vectors.is_empty();
                self.deletes = [(0, 0); 2];
                for vector in &self.vectors {
                    merge(&mut self.deletes, *vector);
                }
                for (key, insert) in self.inserts.iter_mut().enumerate() {
                    if *insert <= rewrite.cutoff {
                        *insert = 0;
                    }
                    self.deletes[key].1 = self.deletes[key].1.max(*insert);
                }
            }
        }
    }
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Rewrite {
    cutoff: usize,
    folded: Vec<Snapshot>,
    rows: Vec<Row>,
    min_sequence: Option<usize>,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Capture {
    checkpoint: bool,
    phase: usize,
    sequence: usize,
    through: usize,
    position: usize,
    corpus: Vec<Row>,
    deletes: Index,
    inserts: Vec<usize>,
    clear_inline: bool,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent bounded events and reachability witnesses"
)]
pub(crate) struct State {
    catalog: Catalog,
    immediate: bool,
    records: AuditRecords,
    durable_catalog: Catalog,
    pending: Vec<Change>,
    high: usize,
    watermark: usize,
    index: Index,
    pk_cache: Option<[bool; 2]>,
    inline: Vec<Row>,
    current: Vec<Row>,
    current_min: Option<usize>,
    snapshots: Vec<Snapshot>,
    tier: Vec<Segment>,
    source: Vec<(usize, Option<usize>)>,
    ack: usize,
    capture: Option<Capture>,
    seals: usize,
    checkpoints: usize,
    baked: bool,
    rewrite: Option<Rewrite>,
    rewrite_used: bool,
    rewritten: bool,
    rewrite_interleaved: bool,
    rewrite_preserved: bool,
    crashed: bool,
    crash_used: bool,
    replay: Option<usize>,
    flushed_checkpoint: bool,
    crashed_stage: bool,
    recovered_shadow: bool,
    pruned: [bool; 2],
    reinserted: [bool; 2],
    checkpointed_reinsert: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Upsert(usize, usize),
    Delete(usize),
    SealCapture,
    SealReserve,
    SealStage,
    SealStageRest,
    SealEnd,
    CkCapture,
    CkPrecompute,
    CkStage,
    CkStageRest,
    CkPublish,
    CkEnd,
    Flush,
    Bake,
    FullRewriteCapture,
    FullRewrite,
    Crash,
    Recover,
}
fn merge(into: &mut Index, from: Index) {
    // provider/deletion_index.rs::merged_entry: independent per-side maxima.
    for (entry, other) in into.iter_mut().zip(from) {
        entry.0 = entry.0.max(other.0);
        entry.1 = entry.1.max(other.1);
    }
}
fn prune_index(index: &mut Index, cutoff: usize) {
    // table.rs::prune_deletion_index_at_or_below returns early for an
    // insert-only index; otherwise deletion_index.rs prunes each side.
    if !index.iter().any(|&(delete, _)| delete > 0) {
        return;
    }
    for entry in index {
        if entry.0 <= cutoff {
            entry.0 = 0;
        }
        if entry.1 <= cutoff {
            entry.1 = 0;
        }
    }
}
fn tombstones(tier: &[Segment]) -> Index {
    let mut index = [(0, 0); 2];
    for segment in tier {
        merge(&mut index, segment.deletes);
    }
    index
}
fn memory_rows(rows: &[Row], file: Index, tier: Index) -> Vec<Row> {
    // table.rs::filter_inlined_batch_against / visible_mem_tier_batches.
    rows.iter()
        .filter(|r| r.seq > file[r.key].0.max(tier[r.key].0))
        .cloned()
        .collect()
}
fn tier_rows(tier: &[Segment], file: Index) -> Vec<Row> {
    let rows: Vec<_> = tier.iter().filter_map(|s| s.row.clone()).collect();
    memory_rows(&rows, file, tombstones(tier))
}
fn initial_rows() -> Vec<Row> {
    (0..2)
        .map(|key| Row {
            key,
            value: key + 1,
            seq: 1,
        })
        .collect()
}
fn initial_catalog() -> Catalog {
    Catalog {
        high: 1,
        snapshots: vec![Snapshot {
            sequence: 1,
            min_sequence: 1,
            rows: initial_rows(),
        }],
        ..Catalog::default()
    }
}
fn scan_rows(state: &State, pushdown: bool) -> Vec<Row> {
    let mut merged = state.index;
    let tier = tombstones(&state.tier);
    merge(&mut merged, tier);
    // table.rs:42327,43025 and file_pruning.rs::tombstone_exclusion_filter:
    // sequence-blind exclusion is shared by main and protected scans.
    let pushed = |key: usize| !pushdown || merged[key].0 == 0 || merged[key].1 > merged[key].0;
    let mut rows: Vec<Row> = state
        .current
        .iter()
        .filter(|r| {
            let (delete, insert) = merged[r.key];
            pushed(r.key) && (delete == 0 || (state.snapshots.is_empty() && insert > delete))
        })
        .cloned()
        .collect();
    // table.rs::apply_partial_deletion_filter ignores reinserts after pushdown.
    for snapshot in &state.snapshots {
        rows.extend(
            snapshot
                .rows
                .iter()
                .filter(|r| pushed(r.key) && merged[r.key].0 <= snapshot.sequence)
                .cloned(),
        );
    }
    rows.extend(
        memory_rows(&state.inline, state.index, tier)
            .into_iter()
            .filter(|r| r.seq <= state.watermark),
    );
    rows.extend(tier_rows(&state.tier, state.index));
    rows
}
fn visible(state: &State) -> Rows {
    let mut out: Rows = scan_rows(state, true)
        .iter()
        .map(|r| (r.key, r.value))
        .collect();
    out.sort_unstable(); // Deliberately preserve identical physical duplicates.
    out
}
fn reference(source: &[(usize, Option<usize>)]) -> Rows {
    // Independent logical source oracle: no physical sequences or filters.
    let mut values = [Some(1), Some(2)];
    for &(key, value) in source {
        values[key] = value;
    }
    values
        .into_iter()
        .enumerate()
        .filter_map(|(k, v)| Some((k, v?)))
        .collect()
}
// The replay configures keep_recent=0. The real bake still needs two
// eligible inputs and a physically clean current snapshot before it can merge.
fn can_bake(state: &State) -> bool {
    let durable_max = state.index.iter().map(|&(d, _)| d).max().unwrap_or(0);
    let pending_min = tombstones(&state.tier)
        .into_iter()
        .filter_map(|(d, _)| (d > 0).then_some(d))
        .min();
    let fence = pending_min.map_or(durable_max, |d| durable_max.min(d - 1));
    let prefix_cutoff = state
        .snapshots
        .iter()
        .map(|s| s.sequence)
        .max()
        .unwrap_or(0);
    state.snapshots.len() >= 2
        && state.current_min.is_none_or(|min| min > prefix_cutoff)
        && (pending_min.is_none() || state.snapshots.iter().all(|s| s.sequence <= fence))
}

impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
    fn has(&self, flag: &str) -> bool {
        self.mode.contains(flag)
    }
    fn stage(state: &mut State, changes: Vec<Change>) {
        // object_native.rs::commit/buffer: catalog-live before durable publication.
        for change in &changes {
            state.catalog.apply(change);
        }
        state.pending.extend(changes);
        if state.immediate {
            Self::flush(state);
        }
    }
    fn flush(state: &mut State) {
        if !state.pending.is_empty() {
            let record = std::mem::take(&mut state.pending);
            for change in &record {
                state.durable_catalog.apply(change);
            }
            state.records.0.push(record);
        }
    }
    fn reserve(state: &mut State, count: usize) -> usize {
        let first = state.high + 1;
        state.high += count;
        // Logical handouts inside an already reserved engine block do not flush
        // another operation's pending metadata. High is retained with the next
        // publication; physical reserve-ahead gaps are normalized by replay.
        Self::stage(state, vec![Change::High(state.high)]);
        first
    }
    fn apply(state: &mut State, key: usize, value: Option<usize>, position: usize) {
        if value.is_some() && state.pk_cache.is_none() {
            // load_existing_pk_index filters durable files by the file index,
            // not pending tier deletes, and folds ALL tier keys. A cached key is
            // retained across an absorbed delete and a bake (a safe superset).
            let mut keys = [false; 2];
            for row in &state.current {
                if state.index[row.key].0 == 0 {
                    keys[row.key] = true;
                }
            }
            for snapshot in &state.snapshots {
                for row in &snapshot.rows {
                    if state.index[row.key].0 <= snapshot.sequence {
                        keys[row.key] = true;
                    }
                }
            }
            for row in memory_rows(&state.inline, state.index, [(0, 0); 2]) {
                if row.seq <= state.watermark {
                    keys[row.key] = true;
                }
            }
            for row in state.tier.iter().filter_map(|s| s.row.as_ref()) {
                keys[row.key] = true;
            }
            state.pk_cache = Some(keys);
        }
        let exists = state.pk_cache.is_some_and(|keys| keys[key]);
        if value.is_some()
            && let Some(keys) = &mut state.pk_cache
        {
            keys[key] = true;
        }
        let delete = Self::reserve(state, 2);
        let mut deletes = [(0, 0); 2];
        // table.rs::probe_reinsert_over_tombstone and mem-tier append path:
        // insert-only index entries do not count as tombstones.
        if value.is_none() || exists || state.index[key].0 > 0 {
            deletes[key].0 = delete;
        }
        state.tier.push(Segment {
            row: value.map(|value| Row {
                key,
                value,
                seq: delete + 1,
            }),
            deletes,
            position,
            sealed: false,
        });
    }
    fn durable(state: &State) -> Catalog {
        state.durable_catalog.clone()
    }
    fn recover(state: &mut State) {
        let durable = Self::durable(state);
        state.high = durable.high;
        state.watermark = durable.high;
        state.inline.clone_from(&durable.inline);
        state.index = durable.deletes;
        state.current.clone_from(&durable.current);
        state.current_min = durable.current_min;
        state.snapshots.clone_from(&durable.snapshots);
        // load_deletion_vectors_all unions per-file reinsert stamps and insert
        // records. Insert-only entries survive in this two-sided representation.
        state.catalog = durable;
        state.ack = state.ack.max(state.catalog.position);
        state.pending.clear();
        state.tier.clear();
        state.pk_cache = None;
        state.capture = None;
        state.rewrite = None;
        state.crashed = false;
        state.replay = (state.ack < state.source.len()).then_some(state.ack);
    }
    fn recovered_rows(state: &State) -> Rows {
        let mut recovered = state.clone();
        Self::recover(&mut recovered);
        for position in recovered.ack..recovered.source.len() {
            let (key, value) = recovered.source[position];
            Self::apply(&mut recovered, key, value, position + 1);
        }
        visible(&recovered)
    }
    fn capture(&self, state: &mut State, checkpoint: bool) {
        let captured: Vec<_> = state
            .tier
            .iter()
            .filter(|s| checkpoint || !s.sealed)
            .cloned()
            .collect();
        let mut corpus = tier_rows(&captured, state.index);
        if checkpoint {
            corpus.extend(
                memory_rows(&state.inline, state.index, tombstones(&captured))
                    .into_iter()
                    .filter(|r| r.seq <= state.watermark),
            );
        }
        let sequence = if !checkpoint && self.has("no-capture-reservation") {
            0
        } else {
            Self::reserve(state, 1)
        };
        state.capture = Some(Capture {
            checkpoint,
            phase: 0,
            sequence,
            through: state.tier.len(),
            position: state
                .tier
                .iter()
                .map(|s| s.position)
                .max()
                .unwrap_or(state.ack),
            corpus,
            deletes: tombstones(&captured),
            inserts: vec![],
            clear_inline: checkpoint && !state.catalog.inline.is_empty(),
        });
    }
    fn deletion_change(&self, capture: &Capture) -> Change {
        let mut index = capture.deletes;
        for row in &capture.corpus {
            if index[row.key].0 > 0 && (capture.checkpoint || !self.has("seal-delete-only")) {
                index[row.key].1 = capture.sequence;
            }
        }
        Change::Deletes(index)
    }
    fn checkpoint_changes(&self, capture: &Capture) -> Vec<Change> {
        let mut changes = vec![self.deletion_change(capture)];
        if !capture.corpus.is_empty() {
            changes.push(Change::Snapshot(Snapshot {
                sequence: capture.sequence,
                min_sequence: capture.sequence,
                rows: capture.corpus.clone(),
            }));
        }
        if capture.clear_inline {
            changes.push(Change::ClearInline);
        }
        changes
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            catalog: initial_catalog(),
            immediate: self.has("sqlite"),
            records: AuditRecords::default(),
            durable_catalog: initial_catalog(),
            pending: vec![],
            high: 1,
            watermark: 1,
            index: [(0, 0); 2],
            pk_cache: None,
            inline: vec![],
            current: vec![],
            current_min: None,
            snapshots: initial_catalog().snapshots,
            tier: vec![],
            source: vec![],
            ack: 0,
            capture: None,
            seals: 0,
            checkpoints: 0,
            baked: false,
            rewrite: None,
            rewrite_used: false,
            rewritten: false,
            rewrite_interleaved: false,
            rewrite_preserved: false,
            crashed: false,
            crash_used: false,
            replay: None,
            flushed_checkpoint: false,
            crashed_stage: false,
            recovered_shadow: false,
            pruned: [false; 2],
            reinserted: [false; 2],
            checkpointed_reinsert: false,
        }]
    }
    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.crashed {
            actions.push(Action::Recover);
            return;
        }
        if let Some(position) = state.replay {
            let (key, value) = state.source[position];
            actions.push(value.map_or(Action::Delete(key), |v| Action::Upsert(key, v)));
        } else if state.source.len() < self.bound {
            for key in 0..2 {
                actions.extend([
                    Action::Upsert(key, 10 + state.source.len()),
                    Action::Delete(key),
                ]);
            }
        }
        // A table flush is idempotent, including after an early publication.
        actions.push(Action::Flush);
        if !state.crash_used {
            actions.push(Action::Crash);
        }
        if state.rewrite.is_some() {
            actions.push(Action::FullRewrite);
        }
        if state.rewrite.is_none()
            && !state.rewrite_used
            && state.capture.is_none()
            && state.replay.is_none()
            && state.tier.is_empty()
            && state.inline.is_empty()
            && state.catalog.inline.is_empty()
        {
            // The engine drains memory and inline data before capturing. Explicit
            // checkpoints in this model supply that prerequisite, within their budget.
            actions.push(Action::FullRewriteCapture);
        }
        if let Some(c) = &state.capture {
            actions.push(match (c.checkpoint, c.phase) {
                (false, 0) if c.sequence == 0 => Action::SealReserve,
                (false, 0) => Action::SealStage,
                (false, 1) => Action::SealStageRest,
                (false, _) => Action::SealEnd,
                (true, 0) => Action::CkPrecompute,
                (true, 1) => Action::CkStage,
                (true, 2) => Action::CkStageRest,
                (true, 3) => Action::CkPublish,
                (true, _) => Action::CkEnd,
            });
        } else if state.replay.is_none() {
            if state.seals < 1 && state.tier.iter().any(|s| !s.sealed) {
                actions.push(Action::SealCapture);
            }
            if state.checkpoints < 2 && !state.tier.is_empty() {
                actions.push(Action::CkCapture);
            }
            if !state.baked && state.rewrite.is_none() && can_bake(state) {
                actions.push(Action::Bake);
            }
        }
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut enabled = vec![];
        self.actions(state, &mut enabled);
        if !enabled.contains(&action) {
            return None;
        }
        let mut next = state.clone();
        match action {
            Action::Upsert(key, value) => {
                if state.replay.is_none() {
                    next.source.push((key, Some(value)));
                    next.reinserted[key] |= next.pruned[key];
                }
                let position = state.replay.map_or(next.source.len(), |p| p + 1);
                next.rewrite_interleaved |= next.rewrite.is_some();
                Self::apply(&mut next, key, Some(value), position);
                next.replay = (position < next.source.len()).then_some(position);
            }
            Action::Delete(key) => {
                if state.replay.is_none() {
                    next.source.push((key, None));
                }
                let position = state.replay.map_or(next.source.len(), |p| p + 1);
                next.rewrite_interleaved |= next.rewrite.is_some();
                Self::apply(&mut next, key, None, position);
                next.replay = (position < next.source.len()).then_some(position);
            }
            Action::SealCapture => {
                next.seals += 1;
                self.capture(&mut next, false);
            }
            Action::CkCapture => {
                next.checkpoints += 1;
                self.capture(&mut next, true);
            }
            Action::SealReserve => {
                let sequence = Self::reserve(&mut next, 1);
                next.capture.as_mut()?.sequence = sequence;
            }
            Action::SealStage => {
                let c = next.capture.as_ref()?;
                let rows = c
                    .corpus
                    .iter()
                    .cloned()
                    .map(|mut r| {
                        r.seq = c.sequence;
                        r
                    })
                    .collect();
                let mut changes = vec![Change::Inline(rows)];
                if !self.has("no-one-call") {
                    changes.push(self.deletion_change(c));
                }
                Self::stage(&mut next, changes);
                next.capture.as_mut()?.phase = if self.has("no-one-call") { 1 } else { 2 };
            }
            Action::SealStageRest => {
                let changes = vec![self.deletion_change(next.capture.as_ref()?)];
                Self::stage(&mut next, changes);
                next.capture.as_mut()?.phase = 2;
            }
            Action::SealEnd | Action::CkEnd => {
                let c = next.capture.take()?;
                if !c.checkpoint {
                    for s in &mut next.tier[..c.through] {
                        s.sealed = true;
                    }
                }
                Self::stage(&mut next, vec![Change::Position(c.position)]);
                Self::flush(&mut next); // fire_slot_advancer commits BEFORE acknowledgement.
                next.ack = next.ack.max(c.position);
            }
            Action::CkPrecompute => {
                let c = next.capture.as_mut()?;
                if !self.has("no-insert-records") {
                    c.inserts = c
                        .corpus
                        .iter()
                        .filter(|r| {
                            state.catalog.has_delete_file
                                || c.deletes.iter().any(|&(d, _)| d > 0)
                                || (state.index[r.key].0 > 0
                                    && state.index[r.key].1 < state.index[r.key].0)
                        })
                        .map(|r| r.key)
                        .collect();
                    c.inserts.sort_unstable();
                    c.inserts.dedup();
                }
                c.phase = 1;
            }
            Action::CkStage => {
                let c = next.capture.as_ref()?;
                let mut changes = self.checkpoint_changes(c);
                let keys = c.inserts.clone();
                let mut stamp = c.sequence;
                if self.has("no-early-stamp") {
                    stamp = Self::reserve(&mut next, 1);
                }
                if !self.has("no-one-call") && !keys.is_empty() {
                    changes.push(Change::Inserts(keys, stamp));
                }
                Self::stage(&mut next, changes);
                next.capture.as_mut()?.phase = if self.has("no-one-call") { 2 } else { 3 };
            }
            Action::CkStageRest => {
                let c = next.capture.as_ref()?;
                let changes = if c.inserts.is_empty() {
                    vec![]
                } else {
                    vec![Change::Inserts(c.inserts.clone(), c.sequence)]
                };
                Self::stage(&mut next, changes);
                next.capture.as_mut()?.phase = 3;
            }
            Action::CkPublish => {
                let c = next.capture.as_ref()?;
                let Change::Deletes(index) = self.deletion_change(c) else {
                    return None;
                };
                merge(&mut next.index, index);
                if !c.corpus.is_empty() {
                    next.snapshots.push(Snapshot {
                        sequence: c.sequence,
                        min_sequence: c.sequence,
                        rows: c.corpus.clone(),
                    });
                    if !self.has("no-insert-records") {
                        for r in &c.corpus {
                            let entry = &mut next.index[r.key];
                            if entry.0 > 0 && entry.1 < entry.0 {
                                entry.1 = entry.1.max(c.sequence);
                            }
                        }
                    }
                }
                next.checkpointed_reinsert |= c.corpus.iter().any(|r| next.reinserted[r.key]);
                if c.clear_inline {
                    next.inline.clear();
                }
                next.tier.drain(..c.through);
                next.capture.as_mut()?.phase = 4;
            }
            Action::Flush => {
                next.flushed_checkpoint |= state
                    .capture
                    .as_ref()
                    .is_some_and(|c| c.checkpoint && c.phase >= 3);
                Self::flush(&mut next);
            }
            Action::Bake => {
                next.baked = true;
                // table.rs::bake_clean_prefix_holds checks PHYSICAL current-file
                // ranges, even when deletions hide every row of those files.
                let durable_max = next.index.iter().map(|&(d, _)| d).max()?;
                let pending_min = tombstones(&next.tier)
                    .into_iter()
                    .filter_map(|(d, _)| (d > 0).then_some(d))
                    .min();
                let fence = pending_min.map_or(durable_max, |d| durable_max.min(d - 1));
                let clean = next.current_min.is_none_or(|min| min > fence);
                let eligible =
                    pending_min.is_none() || next.snapshots.iter().all(|s| s.sequence <= fence);
                if clean && eligible {
                    // Bake scans apply partial deletions WITHOUT the query-only
                    // sequence-blind pushdown (table.rs:29210).
                    let rows = next
                        .snapshots
                        .iter()
                        .flat_map(|s| {
                            s.rows
                                .iter()
                                .filter(|r| next.index[r.key].0 <= s.sequence)
                                .cloned()
                        })
                        .collect();
                    let snapshot = Snapshot {
                        sequence: fence,
                        min_sequence: next.snapshots.iter().map(|s| s.min_sequence).min()?,
                        rows,
                    };
                    Self::stage(&mut next, vec![Change::Bake(snapshot.clone())]);
                    Self::flush(&mut next);
                    next.snapshots = vec![snapshot];
                    for (key, entry) in next.index.iter().enumerate() {
                        next.pruned[key] |= entry.0 > 0 && entry.0 <= fence;
                    }
                    prune_index(&mut next.index, fence);
                }
            }
            Action::FullRewriteCapture => {
                next.rewrite_used = true;
                let min_sequence = next
                    .snapshots
                    .iter()
                    .map(|s| s.min_sequence)
                    .chain(next.current_min)
                    .min();
                next.rewrite = Some(Rewrite {
                    cutoff: state.high,
                    folded: state.snapshots.clone(),
                    rows: scan_rows(state, false),
                    min_sequence,
                });
            }
            Action::FullRewrite => {
                let mut rewrite = next.rewrite.take()?;
                if rewrite.rows.is_empty() {
                    rewrite.min_sequence = None;
                }
                Self::stage(&mut next, vec![Change::FullRewrite(rewrite.clone())]);
                // A rewrite is outside the deferred CDC scope: it also flushes
                // any pending same-table changes (object_native.rs::buffer).
                Self::flush(&mut next);
                next.current = rewrite.rows;
                next.current_min = rewrite.min_sequence;
                next.rewrite_preserved |=
                    next.snapshots.iter().any(|s| !rewrite.folded.contains(s));
                next.snapshots.retain(|s| !rewrite.folded.contains(s));
                prune_index(&mut next.index, rewrite.cutoff);
                next.pk_cache = None;
                next.rewritten = true;
            }
            Action::Crash => {
                next.crashed_stage |= state
                    .capture
                    .as_ref()
                    .is_some_and(|c| c.checkpoint && c.phase == 3);
                next.crashed = true;
                next.crash_used = true;
                next.pending.clear();
                next.tier.clear();
                next.capture = None;
                next.rewrite = None;
                next.current.clear();
                next.current_min = None;
                next.index = [(0, 0); 2];
                next.inline.clear();
                next.snapshots.clear();
            }
            Action::Recover => {
                Self::recover(&mut next);
                next.recovered_shadow |= !next.inline.is_empty();
            }
        }
        Some(next)
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Upsert(k, v) => format!("CDC upsert k{k} = v{v} (or its redelivery)"),
            Action::Delete(k) => format!("CDC delete k{k} (or its redelivery)"),
            Action::SealCapture => "seal: capture the memory tier".into(),
            Action::SealReserve => "seal: reserve its sequence number".into(),
            Action::SealStage => "seal: stage inline shadow rows (and tombstones)".into(),
            Action::SealStageRest => "seal: stage the tombstones separately".into(),
            Action::SealEnd => "seal: publish, then acknowledge the source".into(),
            Action::CkCapture => "checkpoint: capture the memory tier".into(),
            Action::CkPrecompute => "checkpoint: choose the keys that need insert records".into(),
            Action::CkStage => "checkpoint: stage its snapshot, deletes and insert records".into(),
            Action::CkStageRest => "checkpoint: stage insert records separately".into(),
            Action::CkPublish => "checkpoint: apply to the in-memory index".into(),
            Action::CkEnd => "checkpoint: publish, then acknowledge the source".into(),
            Action::Flush => "flush pending changes to the metastore".into(),
            Action::Bake => "bake protected snapshots into one".into(),
            Action::FullRewriteCapture => "full rewrite: capture rows to rewrite".into(),
            Action::FullRewrite => "full rewrite: publish the new current file".into(),
            Action::Crash => "the process crashes (memory lost)".into(),
            Action::Recover => "the process recovers from the metastore".into(),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        Some(crate::explorer::summary(&[
            format!("live rows:      {}", rows_text(&visible(&s))),
            format!("after restart:  {}", rows_text(&Self::recovered_rows(&s))),
            format!("source says:    {}", rows_text(&reference(&s.source))),
            format!(
                "sequence high {} (watermark {}); acknowledged position {} of {}",
                s.high,
                s.watermark,
                s.ack,
                s.source.len()
            ),
            format!("seals {}, checkpoints {}", s.seals, s.checkpoints),
            crate::explorer::flags(&[
                ("crashed", s.crashed),
                ("baked", s.baked),
                ("full rewrite committed", s.rewritten),
                ("rewrite interleaved with apply", s.rewrite_interleaved),
                (
                    "rewrite kept a later protected snapshot",
                    s.rewrite_preserved,
                ),
                (
                    "flush between checkpoint stage and end",
                    s.flushed_checkpoint,
                ),
                (
                    "crash between checkpoint stage and publish",
                    s.crashed_stage,
                ),
                ("seal shadow recovered", s.recovered_shadow),
                ("reinsert checkpointed", s.checkpointed_reinsert),
            ]),
        ]))
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Svg, Timeline, Tone, row, steps};
        let s = path.last_state().clone();
        let mut svg = Svg::new();
        let live = visible(&s);
        let recovered = Self::recovered_rows(&s);
        let expected = reference(&s.source);
        let source: Vec<String> = s
            .source
            .iter()
            .enumerate()
            .map(|(i, (k, v))| {
                format!(
                    "{}. k{k} {}{}",
                    i + 1,
                    v.map_or("delete".into(), |v| format!("= v{v}")),
                    if i < s.ack { " (acknowledged)" } else { "" }
                )
            })
            .collect();
        let mut outcome = vec![
            (
                "SOURCE (CDC changes)".to_string(),
                if source.is_empty() {
                    vec!["no changes".into()]
                } else {
                    source
                },
                Tone::Actor,
            ),
            (
                if s.crashed {
                    "LIVE (process CRASHED)".into()
                } else {
                    "LIVE (what a query sees)".into()
                },
                vec![rows_text(&live)],
                if s.crashed {
                    Tone::Gone
                } else if live == expected {
                    Tone::Live
                } else {
                    Tone::Bad
                },
            ),
            (
                "AFTER A RESTART".into(),
                vec![
                    rows_text(&recovered),
                    format!("source says {}", rows_text(&expected)),
                ],
                if recovered == expected {
                    Tone::Live
                } else {
                    Tone::Bad
                },
            ),
        ];
        let segments: Vec<String> = s
            .tier
            .iter()
            .map(|seg| {
                let mut parts = vec![format!("pos {}:", seg.position)];
                if let Some(r) = &seg.row {
                    parts.push(format!("k{} = v{} seq {}", r.key, r.value, r.seq));
                }
                for (k, &(d, i)) in seg.deletes.iter().enumerate() {
                    if d > 0 {
                        parts.push(format!("delete k{k} <= {d} (insert {i})"));
                    }
                }
                if seg.sealed {
                    parts.push("[sealed]".into());
                }
                parts.join(" ")
            })
            .collect();
        outcome.push((
            "MEMORY TIER (lost on crash)".into(),
            if segments.is_empty() {
                vec!["empty".into()]
            } else {
                segments
            },
            Tone::Staged,
        ));
        if let Some(c) = &s.capture {
            outcome.push((
                format!(
                    "{} IN FLIGHT (phase {})",
                    if c.checkpoint { "CHECKPOINT" } else { "SEAL" },
                    c.phase
                ),
                vec![
                    format!(
                        "sequence {}, through segment {}, position {}",
                        c.sequence, c.through, c.position
                    ),
                    format!("corpus {}", rows_seq_text(&c.corpus)),
                ],
                Tone::Staged,
            ));
        }
        if !s.pending.is_empty() {
            outcome.push((
                "STAGED, NOT PUBLISHED".into(),
                s.pending.iter().map(change_text).collect(),
                Tone::Staged,
            ));
        }
        let mut y = row(&mut svg, 0, &outcome) + 30;
        y = draw_catalog(&mut svg, y, &s.durable_catalog) + 30;
        let (src, tier, capture, store, process) = (0, 1, 2, 3, 4);
        let lane_names = [
            "source",
            "memory tier",
            "seal / checkpoint",
            "metastore",
            "process",
        ]
        .map(String::from)
        .to_vec();
        let mut events = vec![];
        for (before, action, after) in steps(path) {
            let label = self.format_action(&action);
            let published = after.durable_catalog != before.durable_catalog;
            events.push(match action {
                Action::Upsert(..) | Action::Delete(_) => {
                    let replay =
                        before.replay.is_some() || before.source.len() == after.source.len();
                    Event::on(
                        src,
                        if replay {
                            format!("redeliver: {label}")
                        } else {
                            label
                        },
                    )
                    .to(tier)
                }
                Action::SealCapture | Action::CkCapture | Action::FullRewriteCapture => {
                    Event::on(capture, label).to(tier).tone(Tone::Actor)
                }
                Action::SealReserve | Action::CkPrecompute | Action::CkPublish => {
                    Event::on(capture, label)
                }
                Action::SealStage
                | Action::SealStageRest
                | Action::CkStage
                | Action::CkStageRest => Event::on(capture, label).to(store).tone(Tone::Staged),
                Action::SealEnd | Action::CkEnd | Action::FullRewrite => Event::on(capture, label)
                    .to(store)
                    .tone(if published { Tone::Live } else { Tone::Gone }),
                Action::Flush | Action::Bake => Event::on(tier, label)
                    .to(store)
                    .tone(if published { Tone::Live } else { Tone::Gone }),
                Action::Crash => Event::on(process, "CRASH").tone(Tone::Bad),
                Action::Recover => Event::on(store, label).to(process),
            });
        }
        Timeline {
            lanes: lane_names,
            events,
        }
        .draw(&mut svg, 0, y);
        Some(svg.finish())
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("live reads match the source", |_, s: &State| {
                s.crashed || s.replay.is_some() || visible(s) == reference(&s.source)
            }),
            Property::always("recovered reads match the source", |_, s: &State| {
                Self::recovered_rows(s) == reference(&s.source)
            }),
            Property::always("acknowledged applies are durable", |_, s: &State| {
                let rows = Self::recovered_rows(s);
                let expected = reference(&s.source);
                (0..2).all(|key| {
                    let latest = s.source.iter().rposition(|&(k, _)| k == key);
                    latest.is_none_or(|p| p >= s.ack)
                        || rows
                            .iter()
                            .filter(|&&(k, _)| k == key)
                            .copied()
                            .collect::<Rows>()
                            == expected
                                .iter()
                                .filter(|&&(k, _)| k == key)
                                .copied()
                                .collect::<Rows>()
                })
            }),
            Property::sometimes("flush between checkpoint stage and end", |_, s: &State| {
                s.flushed_checkpoint
            }),
            Property::sometimes(
                "crash between checkpoint stage and publish",
                |_, s: &State| s.crashed_stage,
            ),
            Property::sometimes("bake prunes then reinsert checkpoints", |_, s: &State| {
                s.checkpointed_reinsert
            }),
            Property::sometimes("full rewrite committed", |_, s: &State| s.rewritten),
            Property::sometimes("full rewrite interleaves apply", |_, s: &State| {
                s.rewritten && s.rewrite_interleaved
            }),
            Property::sometimes(
                "full rewrite retains later protected snapshot",
                |_, s: &State| s.rewrite_preserved,
            ),
            Property::sometimes("seal shadow recovered", |_, s: &State| s.recovered_shadow),
            Property::sometimes("protected survivor excluded by pushdown", |_, s: &State| {
                let mut index = s.index;
                merge(&mut index, tombstones(&s.tier));
                s.snapshots.iter().any(|snapshot| {
                    snapshot.rows.iter().any(|r| {
                        let (delete, insert) = index[r.key];
                        delete > 0 && delete <= snapshot.sequence && insert <= delete
                    })
                })
            }),
        ]
    }
}

fn rows_text(rows: &Rows) -> String {
    if rows.is_empty() {
        return "(no rows)".into();
    }
    rows.iter()
        .map(|(k, v)| format!("k{k}=v{v}"))
        .collect::<Vec<_>>()
        .join(" ")
}
fn rows_seq_text(rows: &[Row]) -> String {
    rows.iter()
        .map(|r| format!("k{}=v{}@{}", r.key, r.value, r.seq))
        .collect::<Vec<_>>()
        .join(" ")
}
fn index_text(index: &Index) -> String {
    index
        .iter()
        .enumerate()
        .filter(|(_, (d, _))| *d > 0)
        .map(|(k, (d, i))| format!("k{k}: delete <= {d}, insert stamp {i}"))
        .collect::<Vec<_>>()
        .join("; ")
}
fn change_text(change: &Change) -> String {
    match change {
        Change::High(n) => format!("sequence high {n}"),
        Change::Inline(rows) => format!("inline shadow rows {}", rows_seq_text(rows)),
        Change::Deletes(index) => format!("delete vector {}", index_text(index)),
        Change::Snapshot(snap) => format!(
            "protected snapshot @{} {}",
            snap.sequence,
            rows_seq_text(&snap.rows)
        ),
        Change::Inserts(keys, seq) => format!("insert records {keys:?} at {seq}"),
        Change::ClearInline => "clear inline rows".into(),
        Change::Bake(snap) => format!("bake into snapshot @{}", snap.sequence),
        Change::Position(n) => format!("source position {n}"),
        Change::FullRewrite(r) => format!(
            "full rewrite through {} -> current {}",
            r.cutoff,
            rows_seq_text(&r.rows)
        ),
    }
}
/// Draws the durable catalog: the head's fields, then every row-holding part
/// (current file, protected snapshots, inline shadow) and the delete vectors,
/// each delete pointing at the rows of its key at or below its sequence.
fn draw_catalog(svg: &mut crate::explorer::Svg, y: usize, catalog: &Catalog) -> usize {
    use crate::explorer::Tone;
    svg.heading(0, y, "DURABLE CATALOG (survives a restart)");
    let y = y + 22;
    let mut lines = vec![
        format!("sequence high {}", catalog.high),
        format!("source position {}", catalog.position),
        format!(
            "insert records: k0 {}, k1 {}",
            catalog.inserts[0], catalog.inserts[1]
        ),
    ];
    let vectors_at = lines.len();
    for v in &catalog.vectors {
        lines.push(format!("delete vector {}", index_text(v)));
    }
    let head = svg.node(0, y, 0, "CATALOG HEAD", &lines, Tone::Live);
    let col = head.right() + 90;
    let mut cy = y;
    let mut parts: Vec<(String, Vec<Row>)> = vec![];
    if !catalog.current.is_empty() {
        parts.push((
            format!("current file (min seq {:?})", catalog.current_min),
            catalog.current.clone(),
        ));
    }
    for snap in &catalog.snapshots {
        parts.push((
            format!(
                "protected snapshot @{} (min {})",
                snap.sequence, snap.min_sequence
            ),
            snap.rows.clone(),
        ));
    }
    if !catalog.inline.is_empty() {
        parts.push(("inline shadow rows".into(), catalog.inline.clone()));
    }
    let mut placed = vec![];
    for (title, rows) in parts {
        let lines: Vec<String> = rows
            .iter()
            .map(|r| format!("k{} = v{} seq {}", r.key, r.value, r.seq))
            .collect();
        let node = svg.node(col, cy, 0, &title, &lines, Tone::Live);
        svg.arrow(
            (head.right(), head.line_y(0)),
            (node.x, node.mid_y()),
            Tone::Live,
            None,
        );
        cy = node.bottom() + 10;
        placed.push((rows, node));
    }
    for (i, v) in catalog.vectors.iter().enumerate() {
        for (key, &(d, _)) in v.iter().enumerate() {
            if d == 0 {
                continue;
            }
            for (rows, node) in &placed {
                for (j, r) in rows.iter().enumerate() {
                    if r.key == key && r.seq <= d {
                        svg.arrow(
                            (head.right(), head.line_y(vectors_at + i)),
                            (node.x, node.line_y(j)),
                            Tone::Gone,
                            Some("covers"),
                        );
                    }
                }
            }
        }
    }
    head.bottom().max(cy)
}
/// Modeled data-content projection, with physical row multiplicity retained.
/// Vector entries are flattened per key so physical vector-file grouping is immaterial.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogView {
    pub current_rows: Rows,
    pub protected_rows: Vec<(usize, Rows)>,
    /// (key, value, sequence), sorted.
    pub inline_rows: Vec<(usize, usize, usize)>,
    /// (key, delete sequence, reinsert sequence), sorted, preserving multiplicity.
    pub delete_entries: Vec<(usize, usize, usize)>,
    pub insert_records: [usize; 2],
}
impl Catalog {
    fn view(&self) -> CatalogView {
        let rows = |rows: &[Row]| {
            let mut out: Rows = rows.iter().map(|r| (r.key, r.value)).collect();
            out.sort_unstable();
            out
        };
        let mut protected_rows: Vec<_> = self
            .snapshots
            .iter()
            .map(|s| (s.sequence, rows(&s.rows)))
            .collect();
        protected_rows.sort_unstable();
        let mut inline_rows: Vec<_> = self
            .inline
            .iter()
            .map(|r| (r.key, r.value, r.seq))
            .collect();
        inline_rows.sort_unstable();
        let mut delete_entries: Vec<_> = self
            .vectors
            .iter()
            .flat_map(|v| {
                v.iter()
                    .enumerate()
                    .filter_map(|(key, &(d, i))| (d > 0).then_some((key, d, i)))
            })
            .collect();
        delete_entries.sort_unstable();
        CatalogView {
            current_rows: rows(&self.current),
            protected_rows,
            inline_rows,
            delete_entries,
            insert_records: self.inserts,
        }
    }
}

/// One action's live rows, committed records, and hypothetical restart result.
/// Rows retain multiplicity; an empty live view while crashed means unavailable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub pending_changes: Vec<String>,
    pub durable: CatalogView,
    pub durable_plus_pending: CatalogView,
    pub recovered_rows_plus_pending: Rows,
    pub sequence_high_water: usize,
    pub bake_enabled: bool,
    pub action: String,
    pub available: bool,
    pub live_rows: Rows,
    pub durable_records: Vec<Vec<String>>,
    pub recovered_rows: Rows,
    pub reference_rows: Rows,
    pub acknowledged_position: usize,
    pub durable_current_rows: Rows,
    pub durable_protected_rows: Vec<(usize, Rows)>,
    pub durable_delete_vectors: Vec<Index>,
    pub durable_insert_records: [usize; 2],
}
/// Replay exact `Debug` action strings, rejecting unknown or disabled actions.
pub(crate) fn steps(mode: &str, bound: usize, actions: &[String]) -> Option<Vec<Step>> {
    let protocol = Protocol::new(mode, bound);
    let mut state = protocol.init_states().pop()?;
    let mut out = Vec::with_capacity(actions.len());
    for text in actions {
        // Same numeric Debug grammar as transactions::steps; next_state also
        // validates enablement and the source-assigned value of each upsert.
        let (name, arguments) = match text.split_once('(') {
            Some((name, rest)) => (
                name,
                rest.strip_suffix(')')?
                    .split(',')
                    .map(|part| part.trim().parse::<usize>().ok())
                    .collect::<Option<Vec<_>>>()?,
            ),
            None => (text.as_str(), vec![]),
        };
        let action = match (name, arguments.as_slice()) {
            ("Upsert", &[key, value]) => Action::Upsert(key, value),
            ("Delete", &[key]) => Action::Delete(key),
            ("SealCapture", &[]) => Action::SealCapture,
            ("SealReserve", &[]) => Action::SealReserve,
            ("SealStage", &[]) => Action::SealStage,
            ("SealStageRest", &[]) => Action::SealStageRest,
            ("SealEnd", &[]) => Action::SealEnd,
            ("CkCapture", &[]) => Action::CkCapture,
            ("CkPrecompute", &[]) => Action::CkPrecompute,
            ("CkStage", &[]) => Action::CkStage,
            ("CkStageRest", &[]) => Action::CkStageRest,
            ("CkPublish", &[]) => Action::CkPublish,
            ("CkEnd", &[]) => Action::CkEnd,
            ("Flush", &[]) => Action::Flush,
            ("Bake", &[]) => Action::Bake,
            ("FullRewriteCapture", &[]) => Action::FullRewriteCapture,
            ("FullRewrite", &[]) => Action::FullRewrite,
            ("Crash", &[]) => Action::Crash,
            ("Recover", &[]) => Action::Recover,
            _ => return None,
        };
        state = protocol.next_state(&state, action)?;
        let durable = Protocol::durable(&state);
        // The trace API independently folds the audit log to guard the cached
        // durable catalog used by exhaustive exploration.
        let mut folded = initial_catalog();
        for record in &state.records.0 {
            for change in record {
                folded.apply(change);
            }
        }
        if folded != durable {
            return None;
        }
        let mut early = state.clone();
        Protocol::flush(&mut early);
        out.push(Step {
            pending_changes: state.pending.iter().map(|c| format!("{c:?}")).collect(),
            durable: durable.view(),
            durable_plus_pending: early.durable_catalog.view(),
            recovered_rows_plus_pending: Protocol::recovered_rows(&early),
            sequence_high_water: state.high,
            bake_enabled: !state.crashed
                && !state.baked
                && state.capture.is_none()
                && state.rewrite.is_none()
                && state.replay.is_none()
                && can_bake(&state),
            durable_current_rows: durable.current.iter().map(|r| (r.key, r.value)).collect(),
            durable_protected_rows: durable
                .snapshots
                .iter()
                .map(|s| {
                    (
                        s.sequence,
                        s.rows.iter().map(|r| (r.key, r.value)).collect(),
                    )
                })
                .collect(),
            durable_delete_vectors: durable.vectors,
            durable_insert_records: durable.inserts,
            action: text.clone(),
            available: !state.crashed && state.replay.is_none(),
            live_rows: if state.crashed {
                vec![]
            } else {
                visible(&state)
            },
            durable_records: state
                .records
                .0
                .iter()
                .map(|r| r.iter().map(|a| format!("{a:?}")).collect())
                .collect(),
            recovered_rows: Protocol::recovered_rows(&state),
            reference_rows: reference(&state.source),
            acknowledged_position: state.ack,
        });
    }
    Some(out)
}
