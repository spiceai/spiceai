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
//! widened), may make a change no policy applies in place (2), and may revert
//! that change.
//!
//! The copy records the schema its log commits, the runtime's checkpoint of the
//! schema (a separate commit, so a kill between the two leaves the checkpoint
//! behind), which schemas its data was written under, a folder generation that
//! only a drop and recreate changes, and its writer. Each instance serves the
//! copy at a schema (its provider's), from a folder generation, and keeps a
//! cached plan of one query, planned at the schema it served then.
//!
//! A query runs in the order the engine runs it: the plan cache is checked
//! against the tables' schemas, then the scan confirms the dataset head (on a
//! replica, taking up a widening the writer committed), then the plan executes
//! in the schema it was planned at, reading the data by column name with
//! checked casts.
//!
//! The design: only the writer commits a schema, under its own policy; a
//! replica opens the copy at the runtime's checkpoint of the schema and commits
//! nothing, whatever its own Spicepod says; a replica takes up a committed
//! widening when it follows the copy's log; a cached plan is used only while
//! its table still has the schema it was planned at, and a plan that outlives
//! one within a query reads by name with checked casts; a takeover registers
//! again at the stored schema, so a writer with the previous Spicepod keeps the
//! copy's schema and never narrows it; and on a shared location a recreate
//! never drops the copy: the writer fails the dataset instead, and loads it
//! again once the source change is reverted. Each control removes one part.
use stateright::{Model, Property};

/// The instances a run models.
pub const INSTANCES: usize = 3;
/// How many source schema changes a run explores.
const SOURCE_CHANGES: u8 = 3;
/// The schema of a change no policy applies in place.
pub const INCOMPATIBLE: u8 = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Status {
    #[default]
    Stopped,
    Serving,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Copy {
    /// The schema the copy's log commits.
    pub schema: u8,
    /// The runtime's checkpoint of the schema, committed after `schema`.
    pub checkpoint: u8,
    /// The schemas data was written under, as a bit set.
    pub data: u8,
    /// Bumped by a drop and recreate.
    pub folder: u8,
    /// The instance holding the writer lease.
    pub writer: Option<usize>,
    /// The writer committed a schema and has written no data under it yet.
    pub unwritten_commit: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Instance {
    pub status: Status,
    /// The schema the instance's provider serves.
    pub bound: u8,
    /// The copy's folder generation the instance serves.
    pub folder: u8,
    /// The schema the instance's cached plan was planned at.
    pub plan: Option<u8>,
    /// The instance has followed the log since the copy last committed a schema.
    pub followed: bool,
    /// The instance took up a committed widening without reopening.
    pub adopted: bool,
    /// The instance failed the dataset rather than drop a shared copy.
    pub refused: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct State {
    pub source: u8,
    /// The source's schema before its incompatible change, which a revert restores.
    pub prior: u8,
    pub changes: u8,
    pub copy: Option<Copy>,
    pub instances: [Instance; INSTANCES],
    /// What the run has seen, as [`Seen`] bits.
    pub seen: u16,
}

/// The events a run records, one bit each in [`State::seen`].
struct Seen;

impl Seen {
    /// A query read data written under a wider schema than it answers in,
    /// other than by name with checked casts.
    const MISALIGNED: u16 = 1;
    /// A copy an instance was serving was dropped.
    const DROPPED_SERVED: u16 = 1 << 1;
    /// The previous Spicepod's instance wrote data under the widened schema.
    const PREVIOUS_WROTE_WIDE: u16 = 1 << 2;
    /// A writer was killed with a committed schema it had written no data under.
    const KILLED_AFTER_COMMIT: u16 = 1 << 3;
    /// Another instance then took the copy over and wrote under that schema.
    const TOOK_OVER_AFTER_COMMIT: u16 = 1 << 4;
    /// The recreating writer refused to drop a copy another instance served.
    const REFUSED_RECREATE: u16 = 1 << 5;
    /// A query planned after its instance served the committed schema answered
    /// in an older one.
    const STALE_ANSWER: u16 = 1 << 6;
    /// A replica's query answered in the writer's new schema without a restart.
    const ANSWERED_NEW: u16 = 1 << 7;
    /// A replica opened while the checkpoint lagged the committed schema.
    const OPENED_LAGGING: u16 = 1 << 8;
    /// A replica took up the committed schema while the checkpoint lagged it.
    const ADOPTED_PAST_CHECKPOINT: u16 = 1 << 9;
    /// A writer that refused a rebuild loaded the dataset again after a revert.
    const RECOVERED: u16 = 1 << 10;
}

impl State {
    fn has_seen(&self, event: u16) -> bool {
        self.seen & event != 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// The source widens its schema.
    SourceWidens,
    /// The source makes a change no policy applies in place.
    SourceChangesIncompatibly,
    /// The source reverts its incompatible change.
    SourceReverts,
    /// An instance loads the dataset.
    Open(usize),
    /// An instance takes the copy's free writer lease.
    TakeLease(usize),
    /// The writer commits the source's wider schema under its policy.
    CommitSchema(usize),
    /// The writer records the committed schema in the runtime's checkpoint.
    CommitCheckpoint(usize),
    /// The writer writes rows under the schema it serves.
    Write(usize),
    /// A replica follows the copy's log.
    Follow(usize),
    /// An instance plans a query and caches the plan.
    Plan(usize),
    /// An instance runs its cached query.
    Run(usize),
    /// The recreating writer faces a change it cannot apply in place.
    Recreate(usize),
    /// A dataset that failed is loaded again.
    Retry(usize),
    /// An instance is killed; a writer lease it held lapses.
    Kill(usize),
}

#[derive(Clone, Debug)]
pub struct Protocol {
    mode: String,
}

impl Protocol {
    #[must_use]
    pub fn new(mode: &str) -> Self {
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
            j != i && s.instances[j].status == Status::Serving && s.instances[j].folder == folder
        })
    }
    /// A replica takes up the committed schema, when it widens the one it serves.
    fn adopt(&self, n: &mut State, i: usize) {
        let Some(copy) = n.copy.as_ref() else {
            return;
        };
        let (schema, lagging) = (copy.schema, copy.checkpoint < copy.schema);
        let instance = &mut n.instances[i];
        instance.followed = true;
        if self.has("no-adopt") || schema <= instance.bound {
            return;
        }
        instance.bound = schema;
        instance.adopted = true;
        if lagging {
            n.seen |= Seen::ADOPTED_PAST_CHECKPOINT;
        }
    }
}

impl Action {
    /// The action a trace names, as its `Debug` form prints it (`Open(0)`).
    /// # Errors
    /// Returns an error for a name this family has no action for.
    pub fn parse(text: &str) -> Result<Self, String> {
        match text {
            "SourceWidens" => return Ok(Self::SourceWidens),
            "SourceChangesIncompatibly" => return Ok(Self::SourceChangesIncompatibly),
            "SourceReverts" => return Ok(Self::SourceReverts),
            _ => {}
        }
        let (name, rest) = text
            .split_once('(')
            .ok_or_else(|| format!("unknown action {text}"))?;
        let i: usize = rest
            .strip_suffix(')')
            .and_then(|n| n.parse().ok())
            .filter(|i| *i < INSTANCES)
            .ok_or_else(|| format!("bad instance in {text}"))?;
        Ok(match name {
            "Open" => Self::Open(i),
            "TakeLease" => Self::TakeLease(i),
            "CommitSchema" => Self::CommitSchema(i),
            "CommitCheckpoint" => Self::CommitCheckpoint(i),
            "Write" => Self::Write(i),
            "Follow" => Self::Follow(i),
            "Plan" => Self::Plan(i),
            "Run" => Self::Run(i),
            "Recreate" => Self::Recreate(i),
            "Retry" => Self::Retry(i),
            "Kill" => Self::Kill(i),
            _ => return Err(format!("unknown action {text}")),
        })
    }
}

/// The model as a replay through the real code steps it, without the checker.
impl Protocol {
    /// The actions enabled in `s`.
    #[must_use]
    pub fn enabled(&self, s: &State) -> Vec<Action> {
        let mut actions = Vec::new();
        self.actions(s, &mut actions);
        actions
    }
    /// The state after `action`, or `None` when it changes nothing.
    #[must_use]
    pub fn step(&self, s: &State, action: Action) -> Option<State> {
        self.next_state(s, action)
    }
    /// The names of the `always` properties `s` breaks.
    #[must_use]
    pub fn violated(&self, s: &State) -> Vec<&'static str> {
        self.properties()
            .into_iter()
            .filter(|p| p.expectation == stateright::Expectation::Always && !(p.condition)(self, s))
            .map(|p| p.name)
            .collect()
    }
    /// Whether instance `i`'s Spicepod evolves the schema.
    #[must_use]
    pub fn evolves_schema(i: usize) -> bool {
        Self::evolves(i)
    }
    /// Whether instance `i` holds the copy's writer lease in `s`.
    #[must_use]
    pub fn writer_is(s: &State, i: usize) -> bool {
        Self::writes(s, i)
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
            if s.source == INCOMPATIBLE {
                a.push(Action::SourceReverts);
            } else {
                a.push(Action::SourceChangesIncompatibly);
            }
        }
        for i in 0..INSTANCES {
            let instance = &s.instances[i];
            match instance.status {
                Status::Stopped => a.push(Action::Open(i)),
                Status::Failed => {
                    a.push(Action::Kill(i));
                    if instance.refused && s.source != INCOMPATIBLE {
                        a.push(Action::Retry(i));
                    }
                }
                Status::Serving => a.push(Action::Kill(i)),
            }
            if instance.status != Status::Serving {
                continue;
            }
            let Some(copy) = &s.copy else {
                continue;
            };
            match instance.plan {
                None => a.push(Action::Plan(i)),
                Some(_) => a.push(Action::Run(i)),
            }
            if copy.writer.is_none() {
                a.push(Action::TakeLease(i));
            }
            if copy.writer == Some(i) {
                a.push(Action::Write(i));
                if Self::evolves(i) && s.source == 1 && copy.schema == 0 {
                    a.push(Action::CommitSchema(i));
                }
                if copy.checkpoint < copy.schema {
                    a.push(Action::CommitCheckpoint(i));
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
                n.prior = s.source;
                n.source = INCOMPATIBLE;
                n.changes += 1;
            }
            Action::SourceReverts => {
                n.source = s.prior;
                n.changes += 1;
            }
            Action::Open(i) => {
                let Some(copy) = n.copy.as_mut() else {
                    // The first instance creates the copy at the source's schema,
                    // and writes it.
                    let schema = n.source.min(1);
                    n.copy = Some(Copy {
                        schema,
                        checkpoint: schema,
                        data: 0,
                        folder: 0,
                        writer: Some(i),
                        unwritten_commit: false,
                    });
                    n.instances[i] = Instance {
                        status: Status::Serving,
                        bound: schema,
                        followed: true,
                        ..Instance::default()
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
                let bound = if writes {
                    // The writer's registration evolves again to the stored
                    // schema, which repairs a lagging checkpoint.
                    copy.checkpoint = copy.schema;
                    copy.schema
                } else {
                    // A replica registers at the runtime's checkpoint.
                    copy.checkpoint
                };
                let lagging = bound < copy.schema;
                n.instances[i] = Instance {
                    status: Status::Serving,
                    bound,
                    folder: copy.folder,
                    followed: !lagging,
                    ..Instance::default()
                };
                if lagging {
                    n.seen |= Seen::OPENED_LAGGING;
                }
            }
            Action::TakeLease(i) => {
                let copy = n.copy.as_mut()?;
                copy.writer = Some(i);
                if self.has("takeover-narrows") {
                    // The new writer commits the schema it registered with.
                    copy.schema = s.instances[i].bound;
                } else if self.has("takeover-trusts-checkpoint") {
                    // The new writer takes the runtime's checkpoint as the schema.
                    copy.schema = copy.checkpoint;
                    n.instances[i].bound = copy.checkpoint;
                } else {
                    // A takeover reloads the dataset at the stored schema.
                    copy.checkpoint = copy.schema;
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
            Action::CommitCheckpoint(_) => {
                let copy = n.copy.as_mut()?;
                copy.checkpoint = copy.schema;
            }
            Action::Write(i) => {
                let bound = s.instances[i].bound;
                let copy = n.copy.as_mut()?;
                copy.data |= 1 << bound;
                if bound == 1 {
                    copy.unwritten_commit = false;
                    if i == 1 {
                        n.seen |= Seen::PREVIOUS_WROTE_WIDE;
                    }
                    if s.has_seen(Seen::KILLED_AFTER_COMMIT) {
                        n.seen |= Seen::TOOK_OVER_AFTER_COMMIT;
                    }
                }
            }
            Action::Follow(i) => self.adopt(&mut n, i),
            Action::Plan(i) => {
                n.instances[i].plan = Some(s.instances[i].bound);
            }
            Action::Run(i) => {
                let writes = Self::writes(s, i);
                let mut planned = s.instances[i].plan?;
                // The plan cache: a plan is used only while its table has the
                // schema it was planned at.
                let served = n.instances[i].bound;
                if planned != served {
                    if self.has("stale-plan") {
                        if served == n.copy.as_ref()?.schema {
                            n.seen |= Seen::STALE_ANSWER;
                        }
                    } else {
                        planned = served;
                    }
                }
                // The scan confirms the head: a replica takes up a widening the
                // writer committed, which reading by position skips.
                if !writes && !self.has("positional") {
                    self.adopt(&mut n, i);
                }
                // The plan executes in its planned schema; data written under a
                // wider one is read by name with checked casts.
                let copy = n.copy.as_ref()?;
                let by_name = !self.has("positional") && !self.has("no-conform");
                if !by_name && copy.data >> (planned + 1) != 0 {
                    n.seen |= Seen::MISALIGNED;
                }
                if !writes && n.instances[i].adopted && planned == 1 {
                    n.seen |= Seen::ANSWERED_NEW;
                }
                n.instances[i].plan = Some(planned);
            }
            Action::Recreate(i) => {
                let folder = s.copy.as_ref()?.folder;
                if self.has("recreate-drops") {
                    if Self::others_serve(s, i, folder) {
                        n.seen |= Seen::DROPPED_SERVED;
                    }
                    n.copy = Some(Copy {
                        schema: INCOMPATIBLE,
                        checkpoint: INCOMPATIBLE,
                        data: 0,
                        folder: folder + 1,
                        writer: Some(i),
                        unwritten_commit: false,
                    });
                    n.instances[i].bound = INCOMPATIBLE;
                    n.instances[i].folder = folder + 1;
                    n.instances[i].plan = None;
                } else {
                    // The writer fails the dataset rather than drop the copy.
                    if Self::others_serve(s, i, folder) {
                        n.seen |= Seen::REFUSED_RECREATE;
                    }
                    n.instances[i] = Instance {
                        status: Status::Failed,
                        refused: true,
                        ..Instance::default()
                    };
                    n.copy.as_mut()?.writer = None;
                }
            }
            Action::Retry(i) => {
                // The load retry finds the source reverted and loads the
                // dataset, writing it again if no other instance took it over.
                let copy = n.copy.as_mut()?;
                if copy.writer.is_none() {
                    copy.writer = Some(i);
                    copy.checkpoint = copy.schema;
                }
                n.instances[i] = Instance {
                    status: Status::Serving,
                    bound: copy.schema,
                    folder: copy.folder,
                    followed: true,
                    ..Instance::default()
                };
                n.seen |= Seen::RECOVERED;
            }
            Action::Kill(i) => {
                n.instances[i] = Instance::default();
                if let Some(copy) = n.copy.as_mut()
                    && copy.writer == Some(i)
                {
                    if copy.unwritten_commit {
                        n.seen |= Seen::KILLED_AFTER_COMMIT;
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
                |_, s: &State| !s.has_seen(Seen::MISALIGNED),
            ),
            Property::always("no instance drops a copy another serves", |_, s: &State| {
                !s.has_seen(Seen::DROPPED_SERVED)
            }),
            Property::always(
                "a copy's schema never narrows below its data",
                |_, s: &State| {
                    s.copy.as_ref().is_none_or(|copy| {
                        copy.schema == INCOMPATIBLE || copy.data >> (copy.schema + 1) == 0
                    })
                },
            ),
            Property::always(
                "an instance fails only for a change it cannot apply",
                |_, s: &State| {
                    s.instances
                        .iter()
                        .all(|instance| instance.status != Status::Failed || instance.refused)
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
            Property::always(
                "a query answers in the schema its instance serves",
                |_, s: &State| !s.has_seen(Seen::STALE_ANSWER),
            ),
            Property::sometimes(
                "a replica answers in the writer's new schema without a restart",
                |_, s: &State| s.has_seen(Seen::ANSWERED_NEW),
            ),
            Property::sometimes(
                "an instance with the previous Spicepod writes the widened copy",
                |_, s: &State| s.has_seen(Seen::PREVIOUS_WROTE_WIDE),
            ),
            Property::sometimes(
                "a writer killed after committing a schema change is taken over and the copy serves on",
                |_, s: &State| s.has_seen(Seen::TOOK_OVER_AFTER_COMMIT),
            ),
            Property::sometimes(
                "a replica opened at a lagging checkpoint takes up the committed schema",
                |_, s: &State| {
                    s.has_seen(Seen::OPENED_LAGGING) && s.has_seen(Seen::ADOPTED_PAST_CHECKPOINT)
                },
            ),
            Property::sometimes(
                "the recreating writer leaves a copy other instances serve",
                |_, s: &State| s.has_seen(Seen::REFUSED_RECREATE),
            ),
            Property::sometimes(
                "a writer that refused a rebuild loads the dataset again after a revert",
                |_, s: &State| s.has_seen(Seen::RECOVERED),
            ),
        ]
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::SourceWidens => "the source widens its schema".into(),
            Action::SourceChangesIncompatibly => {
                "the source changes its schema incompatibly".into()
            }
            Action::SourceReverts => "the source reverts its incompatible change".into(),
            Action::Open(i) => format!("instance {i} loads the dataset"),
            Action::TakeLease(i) => format!("instance {i} takes the copy's writer lease"),
            Action::CommitSchema(i) => format!("instance {i} commits the wider schema"),
            Action::CommitCheckpoint(i) => {
                format!("instance {i} records the schema in the runtime's checkpoint")
            }
            Action::Write(i) => format!("instance {i} writes rows"),
            Action::Follow(i) => format!("instance {i} follows the copy's log"),
            Action::Plan(i) => format!("instance {i} plans and caches a query"),
            Action::Run(i) => format!("instance {i} runs its cached query"),
            Action::Recreate(i) => format!("instance {i} meets a change it would recreate for"),
            Action::Retry(i) => format!("instance {i} loads the dataset again"),
            Action::Kill(i) => format!("instance {i} is killed"),
        }
    }
}
