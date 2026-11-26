// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Persisted lookup-index runs, whose file names are derived from their
//! content. A run indexes a set of data files, and its file is named for that
//! set (a digest of the files' names and the run's row count), so a run built
//! twice over the same files gets the same name: the second build finds its
//! file already written, and registering it again reuses that file. A
//! registered run's file is loaded when the table opens; a registered run
//! whose file is missing is unregistered and its files read in full, so a lost
//! run file costs time, never rows. The property checked here is the stronger
//! one, that a registered run's file exists.
//!
//! Data files go from absent to manifested (written, and named by the
//! manifest of a snapshot that is not live yet: a compaction records it, and
//! stops holding its files as uncommitted uploads, before the snapshot goes
//! live), then live (the snapshot goes live) or gone (the snapshot is
//! abandoned), then retired (a commit removes them; old snapshots still read
//! them), then gone (deleted and forgotten). A run is built over files that can
//! be read (manifested, live or retired), possibly slowly: it can finish after
//! one of its files is gone.
//! The writer holding the dataset's lease commits; a writer another's
//! takeover has fenced keeps running unaware, and its uploads and deletes
//! still land, but its commits are refused (and tell it it was fenced).
//!
//! The protocol: a file is gone once no manifest, live set or retired set
//! names it. A run file is written, then registered by a commit that accepts
//! it only while none of its data files is gone and its name is not retired. A run no longer wanted is unregistered and its file kept, since
//! the same run may be built again. A run file is retired, by a commit that
//! also unregisters it, only once one of its data files is gone: no run over
//! that set can be built again, so the name is never registered again. The
//! retirement sweep then deletes it, and forgets it once the delete returned.
//! A delete can be in flight for long, or sent twice (a retry, or the sweep of
//! a process that was fenced meanwhile), and land after the forget. A run file
//! can be retired at any time its rule allows, including between another
//! step's writing and registering it: the orphan sweep retires unregistered
//! run files concurrently with the index's syncs.
//!
//! Controls: `delete-while-live` retires a run file while its data files are
//! live, as a cleanup that deletes runs merged into a larger one or left
//! unregistered by a crash would, and registers without checking; `skip-retired`
//! retires while the files are live but refuses to register a name while it is
//! retired; `persist-forgotten` keeps the protocol's retirement rule but
//! registers a run whose build finished after one of its files was gone;
//! `manifest-not-readable` counts a file named only by a not-yet-live
//! snapshot's manifest as gone.
use stateright::{Model, Property};

/// Data files.
const DATA: usize = 2;
/// Run names: each non-empty set of data files, as a bit set over them.
const NAMES: u8 = (1 << DATA) - 1;
const WRITERS: usize = 2;
/// Builds started across writers.
const BUILDS: u8 = 3;
/// Delete requests sent across writers.
const DELETES: u8 = 2;
const TAKES: u8 = 1;
const CRASHES: u8 = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
enum Data {
    #[default]
    Absent,
    Manifested,
    Live,
    Retired,
    Gone,
}
impl Data {
    /// Whether the file exists to be read, so a run can be built over it.
    fn readable(self) -> bool {
        matches!(self, Data::Manifested | Data::Live | Data::Retired)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rule {
    Protocol,
    DeleteWhileLive,
    SkipRetired,
    PersistForgotten,
    ManifestNotReadable,
}

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    rule: Rule,
}

/// A run name: the bit set of the data files the run covers, `1..=NAMES`.
type Name = u8;
/// The bit a name has in a set of names.
fn bit(name: Name) -> u8 {
    1 << (name - 1)
}
fn covers(name: Name, file: usize) -> bool {
    name & (1 << file) != 0
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Writer {
    /// The lease generation this writer believes it holds.
    generation: Option<u8>,
    /// A build in progress: the run it will produce.
    build: Option<Name>,
    /// A run file written and not yet registered.
    put: Option<Name>,
    /// The retired run names as this writer last knew them, once another's
    /// takeover fenced it unaware.
    retired_view: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    data: [Data; DATA],
    lease: u8,
    writers: [Writer; WRITERS],
    /// Registered run names, as a bit set.
    registered: u8,
    /// Run files in the store.
    store: u8,
    /// Retired run names not yet forgotten.
    retired: u8,
    /// Delete requests in flight, by name.
    deletes_in_flight: Vec<Name>,
    builds: u8,
    deletes: u8,
    takes: u8,
    crashes: u8,
    /// Names registered once and unregistered since (a ghost).
    unwanted: u8,
    /// A name registered again after it was unregistered (reuse).
    reused: bool,
    /// A retired run file was deleted and forgotten.
    reclaimed: bool,
    /// A fenced writer's commit was refused.
    fenced_refused: bool,
    /// A delete landed after its name was forgotten.
    late_delete: bool,
    /// A build finished after one of its files was gone.
    late_build: bool,
    /// A run was built over a file named only by a not-yet-live manifest.
    built_over_manifested: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    DataWrite(usize),
    DataPublish(usize),
    DataAbandon(usize),
    DataRetire(usize),
    DataForget(usize),
    BuildStart(usize, Name),
    Put(usize),
    Register(usize),
    Unwant(Name),
    RetireRun(usize, Name),
    SweepDelete(usize, Name),
    DeleteLand(usize),
    ForgetRun(Name),
    Takeover(usize),
    Crash(usize),
}

impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        let rule = if mode.contains("delete-while-live") {
            Rule::DeleteWhileLive
        } else if mode.contains("skip-retired") {
            Rule::SkipRetired
        } else if mode.contains("persist-forgotten") {
            Rule::PersistForgotten
        } else if mode.contains("manifest-not-readable") {
            Rule::ManifestNotReadable
        } else {
            Rule::Protocol
        };
        Self { rule }
    }
    fn holds_lease(s: &State, w: usize) -> bool {
        s.writers[w].generation == Some(s.lease)
    }
    fn holder(s: &State) -> Option<usize> {
        (0..WRITERS).find(|&w| Self::holds_lease(s, w))
    }
    /// Whether one of `name`'s files is gone, as the rule judges it from the
    /// manifests, live set and retired set.
    fn any_gone(&self, s: &State, name: Name) -> bool {
        (0..DATA).any(|f| {
            covers(name, f)
                && match s.data[f] {
                    Data::Gone => true,
                    Data::Manifested => self.rule == Rule::ManifestNotReadable,
                    Data::Absent | Data::Live | Data::Retired => false,
                }
        })
    }
    fn all_readable(s: &State, name: Name) -> bool {
        (0..DATA).all(|f| !covers(name, f) || s.data[f].readable())
    }
    /// Whether a register commit of `name` is accepted under the lease.
    fn may_register(&self, s: &State, name: Name) -> bool {
        let retired = s.retired & bit(name) != 0;
        match self.rule {
            Rule::Protocol | Rule::ManifestNotReadable => !retired && !self.any_gone(s, name),
            Rule::SkipRetired | Rule::PersistForgotten => !retired,
            Rule::DeleteWhileLive => true,
        }
    }
    /// Whether a retirement commit of `name` is accepted under the lease.
    fn may_retire(&self, s: &State, name: Name) -> bool {
        match self.rule {
            Rule::Protocol | Rule::PersistForgotten | Rule::ManifestNotReadable => {
                self.any_gone(s, name)
            }
            Rule::DeleteWhileLive | Rule::SkipRetired => true,
        }
    }
    /// A fenced writer's refused commit: it learns it was fenced and stops.
    fn refuse(next: &mut State, w: usize) {
        next.fenced_refused = true;
        next.writers[w] = Writer::default();
    }
}

impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let mut writers = [Writer::default(); WRITERS];
        writers[0].generation = Some(1);
        vec![State {
            data: [Data::Absent; DATA],
            lease: 1,
            writers,
            registered: 0,
            store: 0,
            retired: 0,
            deletes_in_flight: vec![],
            builds: 0,
            deletes: 0,
            takes: 0,
            crashes: 0,
            unwanted: 0,
            reused: false,
            reclaimed: false,
            fenced_refused: false,
            late_delete: false,
            late_build: false,
            built_over_manifested: false,
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for f in 0..DATA {
            a.extend([
                Action::DataWrite(f),
                Action::DataPublish(f),
                Action::DataAbandon(f),
                Action::DataRetire(f),
                Action::DataForget(f),
            ]);
        }
        for name in 1..=NAMES {
            a.extend([Action::Unwant(name), Action::ForgetRun(name)]);
        }
        for w in 0..WRITERS {
            for name in 1..=NAMES {
                a.extend([
                    Action::BuildStart(w, name),
                    Action::RetireRun(w, name),
                    Action::SweepDelete(w, name),
                ]);
            }
            a.extend([
                Action::Put(w),
                Action::Register(w),
                Action::Takeover(w),
                Action::Crash(w),
            ]);
        }
        a.extend((0..s.deletes_in_flight.len()).map(Action::DeleteLand));
        a.retain(|action| self.enabled(s, action));
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        if !self.enabled(s, &action) {
            return None;
        }
        let mut next = s.clone();
        match action {
            Action::DataWrite(f) => next.data[f] = Data::Manifested,
            Action::DataPublish(f) => next.data[f] = Data::Live,
            Action::DataRetire(f) => next.data[f] = Data::Retired,
            Action::DataAbandon(f) | Action::DataForget(f) => next.data[f] = Data::Gone,
            Action::BuildStart(w, name) => {
                next.writers[w].build = Some(name);
                next.builds += 1;
                next.built_over_manifested |=
                    (0..DATA).any(|f| covers(name, f) && s.data[f] == Data::Manifested);
            }
            Action::Put(w) => {
                let name = s.writers[w].build?;
                next.late_build |= (0..DATA).any(|f| covers(name, f) && s.data[f] == Data::Gone);
                next.store |= bit(name);
                next.writers[w].build = None;
                next.writers[w].put = Some(name);
            }
            Action::Register(w) => {
                let name = s.writers[w].put?;
                next.writers[w].put = None;
                if !Self::holds_lease(s, w) {
                    Self::refuse(&mut next, w);
                } else if self.may_register(s, name) {
                    next.reused |= s.unwanted & bit(name) != 0;
                    next.registered |= bit(name);
                }
            }
            Action::Unwant(name) => {
                next.registered &= !bit(name);
                next.unwanted |= bit(name);
            }
            Action::RetireRun(w, name) => {
                if Self::holds_lease(s, w) {
                    if self.may_retire(s, name) {
                        if s.registered & bit(name) != 0 {
                            next.unwanted |= bit(name);
                        }
                        next.registered &= !bit(name);
                        next.retired |= bit(name);
                    }
                } else {
                    Self::refuse(&mut next, w);
                }
            }
            Action::SweepDelete(_, name) => {
                next.deletes_in_flight.push(name);
                next.deletes_in_flight.sort_unstable();
                next.deletes += 1;
            }
            Action::DeleteLand(i) => {
                let name = next.deletes_in_flight.remove(i);
                next.late_delete |= s.retired & bit(name) == 0;
                next.store &= !bit(name);
            }
            Action::ForgetRun(name) => {
                next.retired &= !bit(name);
                next.reclaimed = true;
            }
            Action::Takeover(w) => {
                if let Some(h) = Self::holder(s)
                    && h != w
                {
                    next.writers[h].retired_view = s.retired;
                }
                next.takes += 1;
                next.lease += 1;
                next.writers[w] = Writer {
                    generation: Some(next.lease),
                    ..Writer::default()
                };
            }
            Action::Crash(w) => {
                next.crashes += 1;
                next.writers[w] = Writer::default();
            }
        }
        (next != *s).then_some(next)
    }
    fn format_action(&self, action: &Action) -> String {
        let files = |name: Name| {
            (0..DATA)
                .filter(|&f| covers(name, f))
                .map(|f| format!("d{f}"))
                .collect::<Vec<_>>()
                .join("+")
        };
        match *action {
            Action::DataWrite(f) => {
                format!("data file d{f} is written and named by a manifest not yet live")
            }
            Action::DataPublish(f) => format!("the snapshot naming d{f} goes live"),
            Action::DataAbandon(f) => format!("the snapshot naming d{f} is abandoned"),
            Action::DataRetire(f) => format!("a commit retires data file d{f}"),
            Action::DataForget(f) => format!("data file d{f} is deleted and forgotten"),
            Action::BuildStart(w, n) => {
                format!("writer {w} starts building the run over {}", files(n))
            }
            Action::Put(w) => format!("writer {w} writes its run's file"),
            Action::Register(w) => format!("writer {w} commits its run's registration"),
            Action::Unwant(n) => format!(
                "the holder unregisters the run over {}, keeping its file",
                files(n)
            ),
            Action::RetireRun(w, n) => format!(
                "writer {w} commits the retirement of the run file over {}",
                files(n)
            ),
            Action::SweepDelete(w, n) => format!(
                "writer {w}'s sweep sends a delete of the run file over {}",
                files(n)
            ),
            Action::DeleteLand(i) => format!("delete {i} lands"),
            Action::ForgetRun(n) => format!("the holder forgets the run file over {}", files(n)),
            Action::Takeover(w) => format!("writer {w} takes over the lease"),
            Action::Crash(w) => format!("writer {w} crashes"),
        }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("registered run's file exists", |_, s: &State| {
                s.registered & !s.store == 0
            }),
            Property::sometimes("run file deleted and forgotten", |_, s: &State| s.reclaimed),
            Property::sometimes(
                "delete lands after its name was forgotten",
                |_, s: &State| s.late_delete,
            ),
            Property::sometimes("fenced writer's commit refused", |_, s: &State| {
                s.fenced_refused
            }),
        ];
        if self.rule == Rule::Protocol {
            properties.extend([
                Property::sometimes(
                    "run registered again under its name and file",
                    |_, s: &State| s.reused,
                ),
                Property::sometimes(
                    "build finishes after one of its files is gone",
                    |_, s: &State| s.late_build,
                ),
                Property::sometimes(
                    "run built over a file of a snapshot not yet live",
                    |_, s: &State| s.built_over_manifested,
                ),
            ]);
        }
        properties
    }
}

impl Protocol {
    fn enabled(&self, s: &State, action: &Action) -> bool {
        let writer = |w: usize| &s.writers[w];
        let holder = Self::holder(s).is_some();
        match *action {
            Action::DataWrite(f) => holder && s.data[f] == Data::Absent,
            Action::DataPublish(f) | Action::DataAbandon(f) => {
                holder && s.data[f] == Data::Manifested
            }
            Action::DataRetire(f) => holder && s.data[f] == Data::Live,
            Action::DataForget(f) => holder && s.data[f] == Data::Retired,
            Action::BuildStart(w, name) => {
                writer(w).generation.is_some()
                    && writer(w).build.is_none()
                    && writer(w).put.is_none()
                    && s.builds < BUILDS
                    && Self::all_readable(s, name)
            }
            Action::Put(w) => writer(w).build.is_some(),
            Action::Register(w) => writer(w).put.is_some(),
            Action::Unwant(name) => holder && s.registered & bit(name) != 0,
            Action::RetireRun(w, name) => {
                writer(w).generation.is_some()
                    && (s.store | s.registered) & bit(name) != 0
                    && s.retired & bit(name) == 0
                    && (!Self::holds_lease(s, w) || self.may_retire(s, name))
            }
            Action::SweepDelete(w, name) => {
                let known = if Self::holds_lease(s, w) {
                    s.retired
                } else {
                    writer(w).retired_view
                };
                writer(w).generation.is_some() && s.deletes < DELETES && known & bit(name) != 0
            }
            Action::DeleteLand(i) => i < s.deletes_in_flight.len(),
            // Once a delete returned: another delete of the name, a retry or a
            // fenced process's, may still be in flight.
            Action::ForgetRun(name) => {
                holder && s.retired & bit(name) != 0 && s.store & bit(name) == 0
            }
            Action::Takeover(w) => s.takes < TAKES && writer(w).generation != Some(s.lease),
            Action::Crash(w) => s.crashes < CRASHES && *writer(w) != Writer::default(),
        }
    }
}
