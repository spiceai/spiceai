// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Source schema changes on a shared state location.
//!
//! Three instances serve one dataset copy. Instance 0 runs a Spicepod whose
//! `on_schema_change` evolves the schema and that recreates the acceleration
//! for a change it cannot apply in place (`drop_and_recreate`); instance 1 runs
//! the previous Spicepod (`block`); instance 2 evolves like instance 0 but never
//! recreates. The copy has one writer, and the others serve it as replicas. The
//! source starts at schema 0, may widen it to 1 (a column added, a type
//! widened), and may make a change no policy applies in place (2).
//!
//! The copy records its committed schema, which schemas its data was written
//! under, a folder generation that only a drop and recreate changes, and its
//! writer. Each instance serves the copy at a schema (its provider's), from a
//! folder generation.
//!
//! The design: only the writer commits a schema, under its own policy; a
//! replica opens the copy at the stored schema and commits nothing, whatever
//! its own Spicepod says; a replica takes up a committed widening when it
//! follows the copy's log, so a query never reads data written under a wider
//! schema than it serves; a takeover registers again at the stored schema, so a
//! writer with the previous Spicepod keeps the copy's schema and never narrows
//! it; and on a shared location a recreate never drops the copy: the writer
//! fails the dataset instead. Each control removes one part of it.
use stateright::{Model, Property};

const INSTANCES: usize = 3;
/// How many source schema changes a run explores.
const SOURCE_CHANGES: u8 = 2;
/// The schema of a change no policy applies in place.
const INCOMPATIBLE: u8 = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum Status {
    #[default]
    Stopped,
    Serving,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Copy {
    /// The schema the copy's log commits.
    schema: u8,
    /// The schemas data was written under, as a bit set.
    data: u8,
    /// Bumped by a drop and recreate.
    folder: u8,
    /// The instance holding the writer lease.
    writer: Option<usize>,
    /// The writer committed a schema and has written no data under it yet.
    unwritten_commit: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct Instance {
    status: Status,
    /// The schema the instance's provider serves.
    bound: u8,
    /// The copy's folder generation the instance serves.
    folder: u8,
    /// The instance has followed the log since the copy last committed a schema.
    followed: bool,
    /// The instance took up a committed widening without reopening.
    adopted: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct State {
    source: u8,
    changes: u8,
    copy: Option<Copy>,
    instances: [Instance; INSTANCES],
    /// A query read data written under a wider schema than it serves.
    misaligned: bool,
    /// A copy an instance was serving was dropped.
    dropped_served: bool,
    /// The previous Spicepod's instance wrote data under the widened schema.
    previous_wrote_wide: bool,
    /// A writer was killed with a committed schema it had written no data under.
    killed_after_commit: bool,
    /// Another instance then took the copy over and wrote under that schema.
    took_over_after_commit: bool,
    /// The recreating writer refused to drop a copy another instance served.
    refused_recreate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// The source widens its schema.
    SourceWidens,
    /// The source makes a change no policy applies in place.
    SourceChangesIncompatibly,
    /// An instance loads the dataset.
    Open(usize),
    /// An instance takes the copy's free writer lease.
    TakeLease(usize),
    /// The writer commits the source's wider schema under its policy.
    CommitSchema(usize),
    /// The writer writes rows under the schema it serves.
    Write(usize),
    /// A replica follows the copy's log.
    Follow(usize),
    /// An instance answers a query over the copy's data.
    Read(usize),
    /// The recreating writer faces a change it cannot apply in place.
    Recreate(usize),
    /// An instance is killed; a writer lease it held lapses.
    Kill(usize),
}

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
}

impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self { mode: mode.into() }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    /// Whether instance `i`'s Spicepod evolves the schema.
    fn evolves(i: usize) -> bool {
        i != 1
    }
    /// Whether instance `i`'s Spicepod recreates for an incompatible change.
    fn recreates(i: usize) -> bool {
        i == 0
    }
    fn writes(s: &State, i: usize) -> bool {
        s.copy.as_ref().is_some_and(|copy| copy.writer == Some(i))
    }
    /// Whether another instance than `i` serves folder `folder`.
    fn others_serve(s: &State, i: usize, folder: u8) -> bool {
        (0..INSTANCES).any(|j| {
            j != i
                && s.instances[j].status == Status::Serving
                && s.instances[j].folder == folder
        })
    }
}

impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State::default()]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if s.changes < SOURCE_CHANGES {
            if s.source == 0 {
                a.push(Action::SourceWidens);
            }
            if s.source != INCOMPATIBLE {
                a.push(Action::SourceChangesIncompatibly);
            }
        }
        for i in 0..INSTANCES {
            let instance = &s.instances[i];
            match instance.status {
                Status::Stopped => a.push(Action::Open(i)),
                Status::Serving | Status::Failed => a.push(Action::Kill(i)),
            }
            if instance.status != Status::Serving {
                continue;
            }
            let Some(copy) = &s.copy else {
                continue;
            };
            a.push(Action::Read(i));
            if copy.writer.is_none() {
                a.push(Action::TakeLease(i));
            }
            if copy.writer == Some(i) {
                a.push(Action::Write(i));
                if Self::evolves(i) && s.source == 1 && copy.schema == 0 {
                    a.push(Action::CommitSchema(i));
                }
                if Self::recreates(i) && s.source == INCOMPATIBLE {
                    a.push(Action::Recreate(i));
                }
            } else if !instance.followed {
                a.push(Action::Follow(i));
            }
        }
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        match action {
            Action::SourceWidens => {
                n.source = 1;
                n.changes += 1;
            }
            Action::SourceChangesIncompatibly => {
                n.source = INCOMPATIBLE;
                n.changes += 1;
            }
            Action::Open(i) => {
                let Some(copy) = n.copy.as_mut() else {
                    // The first instance creates the copy at the source's schema,
                    // and writes it.
                    let schema = n.source.min(1);
                    n.copy = Some(Copy {
                        schema,
                        data: 0,
                        folder: 0,
                        writer: Some(i),
                        unwritten_commit: false,
                    });
                    n.instances[i] = Instance {
                        status: Status::Serving,
                        bound: schema,
                        folder: 0,
                        followed: true,
                        adopted: false,
                    };
                    return Some(n);
                };
                if copy.writer.is_none() {
                    copy.writer = Some(i);
                }
                let writes = copy.writer == Some(i);
                // A replica whose own policy would change the stored schema
                // tries to commit that change, and is refused.
                if self.has("replica-commits")
                    && !writes
                    && Self::evolves(i)
                    && s.source == 1
                    && copy.schema == 0
                {
                    n.instances[i].status = Status::Failed;
                    return Some(n);
                }
                n.instances[i] = Instance {
                    status: Status::Serving,
                    bound: copy.schema,
                    folder: copy.folder,
                    followed: true,
                    adopted: false,
                };
            }
            Action::TakeLease(i) => {
                let copy = n.copy.as_mut()?;
                copy.writer = Some(i);
                if self.has("takeover-narrows") {
                    // The new writer commits the schema it registered with.
                    copy.schema = s.instances[i].bound;
                } else {
                    // A takeover reloads the dataset at the stored schema.
                    n.instances[i].bound = copy.schema;
                    n.instances[i].followed = true;
                }
            }
            Action::CommitSchema(i) => {
                let copy = n.copy.as_mut()?;
                copy.schema = 1;
                copy.unwritten_commit = true;
                n.instances[i].bound = 1;
                for (j, instance) in n.instances.iter_mut().enumerate() {
                    if j != i {
                        instance.followed = false;
                    }
                }
            }
            Action::Write(i) => {
                let bound = s.instances[i].bound;
                let copy = n.copy.as_mut()?;
                copy.data |= 1 << bound;
                if bound == 1 {
                    copy.unwritten_commit = false;
                    if i == 1 {
                        n.previous_wrote_wide = true;
                    }
                    if s.killed_after_commit {
                        n.took_over_after_commit = true;
                    }
                }
            }
            Action::Follow(i) => {
                let schema = n.copy.as_ref()?.schema;
                let instance = &mut n.instances[i];
                instance.followed = true;
                if !self.has("no-adopt") && schema > instance.bound {
                    instance.bound = schema;
                    instance.adopted = true;
                }
            }
            Action::Read(i) => {
                let copy = n.copy.as_ref()?;
                let mut bound = s.instances[i].bound;
                // A query confirms the head first, taking up a widening the
                // writer committed; reading by position skips that.
                if !self.has("positional") && !Self::writes(s, i) && copy.schema > bound {
                    bound = copy.schema;
                    n.instances[i].bound = bound;
                    n.instances[i].followed = true;
                    n.instances[i].adopted = true;
                }
                if copy.data >> (bound + 1) != 0 {
                    n.misaligned = true;
                }
            }
            Action::Recreate(i) => {
                let folder = s.copy.as_ref()?.folder;
                if self.has("recreate-drops") {
                    if Self::others_serve(s, i, folder) {
                        n.dropped_served = true;
                    }
                    n.copy = Some(Copy {
                        schema: INCOMPATIBLE,
                        data: 0,
                        folder: folder + 1,
                        writer: Some(i),
                        unwritten_commit: false,
                    });
                    n.instances[i].bound = INCOMPATIBLE;
                    n.instances[i].folder = folder + 1;
                } else {
                    // The writer fails the dataset rather than drop the copy.
                    if Self::others_serve(s, i, folder) {
                        n.refused_recreate = true;
                    }
                    n.instances[i].status = Status::Failed;
                    n.copy.as_mut()?.writer = None;
                }
            }
            Action::Kill(i) => {
                n.instances[i] = Instance::default();
                if let Some(copy) = n.copy.as_mut()
                    && copy.writer == Some(i)
                {
                    if copy.unwritten_commit {
                        n.killed_after_commit = true;
                    }
                    copy.writer = None;
                }
            }
        }
        if n == *s { None } else { Some(n) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "a replica never serves rows misaligned with its schema",
                |_, s: &State| !s.misaligned,
            ),
            Property::always(
                "no instance drops a copy another serves",
                |_, s: &State| !s.dropped_served,
            ),
            Property::always(
                "a copy's schema never narrows below its data",
                |_, s: &State| {
                    s.copy
                        .as_ref()
                        .is_none_or(|copy| copy.schema == INCOMPATIBLE || copy.data >> (copy.schema + 1) == 0)
                },
            ),
            Property::always(
                "an instance fails only for a change it cannot apply",
                |_, s: &State| {
                    (0..INSTANCES).all(|i| {
                        s.instances[i].status != Status::Failed
                            || (Protocol::recreates(i) && s.source == INCOMPATIBLE)
                    })
                },
            ),
            Property::always(
                "a replica that followed the log serves the committed schema",
                |_, s: &State| {
                    s.copy.as_ref().is_none_or(|copy| {
                        (0..INSTANCES).all(|i| {
                            let instance = &s.instances[i];
                            instance.status != Status::Serving
                                || copy.writer == Some(i)
                                || !instance.followed
                                || instance.folder != copy.folder
                                || instance.bound >= copy.schema
                        })
                    })
                },
            ),
            Property::sometimes(
                "a replica serves the writer's new schema without a restart",
                |_, s: &State| {
                    (0..INSTANCES).any(|i| {
                        let instance = &s.instances[i];
                        instance.status == Status::Serving
                            && instance.adopted
                            && instance.bound == 1
                            && !Protocol::writes(s, i)
                    })
                },
            ),
            Property::sometimes(
                "an instance with the previous Spicepod writes the widened copy",
                |_, s: &State| s.previous_wrote_wide,
            ),
            Property::sometimes(
                "a writer killed after committing a schema change is taken over and the copy serves on",
                |_, s: &State| s.took_over_after_commit,
            ),
            Property::sometimes(
                "the recreating writer leaves a copy other instances serve",
                |_, s: &State| s.refused_recreate,
            ),
        ]
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::SourceWidens => "the source widens its schema".into(),
            Action::SourceChangesIncompatibly => {
                "the source changes its schema incompatibly".into()
            }
            Action::Open(i) => format!("instance {i} loads the dataset"),
            Action::TakeLease(i) => format!("instance {i} takes the copy's writer lease"),
            Action::CommitSchema(i) => format!("instance {i} commits the wider schema"),
            Action::Write(i) => format!("instance {i} writes rows"),
            Action::Follow(i) => format!("instance {i} follows the copy's log"),
            Action::Read(i) => format!("instance {i} answers a query"),
            Action::Recreate(i) => format!("instance {i} meets a change it would recreate for"),
            Action::Kill(i) => format!("instance {i} is killed"),
        }
    }
}
