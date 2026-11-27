// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Dataset definitions on a shared state location.
//!
//! Two Spicepods run on one location, and both declare a dataset `orders` with
//! the same `from`, read from two different source hosts. Instance 0 is app
//! `shop` on host 0; instance 1 is app `store` on host 1. Instance 0 may also
//! restart renamed, with the same definition. The location keeps one copy of
//! the dataset, which records the definition and the app it was built for, the
//! source hosts whose rows it holds, and which instance holds its writer lease.
//!
//! An instance starts (opening the location, where it already contends for
//! writer leases) and later loads the dataset. A lease is taken in two steps,
//! a request sent and its answer landing, so a request can land after the
//! instance that sent it was refused the copy. `mode: file_create` replaces the
//! copy with a new generation; an instance still serving the previous
//! generation keeps its rows until it reloads.
//!
//! The design: a copy records its definition, including its source's
//! connection settings, and an instance serves a copy only when its own
//! definition matches. A mismatch fails the dataset on that instance, which
//! stops taking the copy's writer lease and hands back one a request in flight
//! wins. A copy with no recorded definition (built with none, as by DDL) is
//! refused like a different one: its source is unknown. `file_create` replaces the copy only
//! under its writer lease, and a reload checks the definition again. The app
//! name is recorded and logged, never compared. Each control removes one part
//! of the rule.
use stateright::{Model, Property};

const INSTANCES: usize = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum Status {
    #[default]
    Stopped,
    /// Started: the location is open, the dataset not loaded yet.
    Loading,
    Serving,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Copy {
    /// Bumped by `mode: file_create`, which replaces the copy.
    generation: u8,
    /// The definition the copy was built from; `None` for a copy built with
    /// no definition.
    definition: Option<u8>,
    /// The app that last served the copy as its writer.
    app: u8,
    /// The app that built the copy.
    built_by: u8,
    /// The source hosts whose rows the copy holds, as a bit set.
    rows: u8,
    /// The instance holding the copy's writer lease.
    writer: Option<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct State {
    copy: Option<Copy>,
    /// The rows of the generation `file_create` replaced, which an instance
    /// still serving it reads.
    previous_rows: u8,
    status: [Status; INSTANCES],
    /// The copy generation each serving instance reads.
    serves: [Option<u8>; INSTANCES],
    /// A lease request each instance has in flight.
    pending: [bool; INSTANCES],
    /// Each instance has been refused the copy and stopped taking its lease.
    declined: [bool; INSTANCES],
    /// Each instance's app: 0 `shop`, 1 `store`, 2 `shop` renamed.
    app: [u8; INSTANCES],
    /// Witness: an instance opened the copy as a different definition.
    refused: bool,
    /// Witness: a lease request landed after its instance was refused.
    landed_after_refusal: bool,
    /// Witness: `file_create` replaced the copy.
    recreated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// An instance starts and opens the location.
    Start(usize),
    /// A started instance loads the dataset.
    Open(usize),
    /// A started instance loads the dataset with `mode: file_create`.
    Recreate(usize),
    /// A serving instance reloads the dataset (a role change or a new revision).
    Reload(usize),
    /// The copy's writer commits rows from its own source host.
    Write(usize),
    /// An instance sends a request for the copy's free writer lease.
    SendAcquire(usize),
    /// The answer to that request lands.
    LandAcquire(usize),
    /// An instance stops; a writer lease it held lapses.
    Stop(usize),
    /// Instance 0, stopped, restarts as a renamed app with the same definition.
    Rename,
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
    /// The source host instance `i` reads.
    fn host(i: usize) -> u8 {
        u8::try_from(i).unwrap_or(u8::MAX)
    }
    /// Instance `i`'s definition: one `from`, and the connection settings of its
    /// host, unless the control leaves the connection out.
    fn definition(&self, i: usize) -> u8 {
        if self.has("no-connection") {
            0
        } else {
            Self::host(i)
        }
    }
    /// Whether instance `i` serves a copy whose definition is recorded.
    fn matches(&self, s: &State, i: usize, copy: &Copy, recorded: u8) -> bool {
        if self.has("no-check") || self.has("rebuild") {
            true
        } else if self.has("app-name") {
            copy.app == s.app[i]
        } else {
            recorded == self.definition(i)
        }
    }
    /// The rows instance `i` reads: the copy's, or the replaced generation's.
    fn rows_read(s: &State, i: usize) -> u8 {
        match (&s.copy, s.serves[i]) {
            (Some(copy), Some(generation)) if generation == copy.generation => copy.rows,
            (_, Some(_)) => s.previous_rows,
            _ => 0,
        }
    }
    /// Fail the dataset on instance `i`, which stops taking the copy's lease and,
    /// unless the control keeps it, hands back one it holds.
    fn refuse(mut n: State, i: usize, hand_back: bool) -> State {
        n.status[i] = Status::Failed;
        n.serves[i] = None;
        n.declined[i] = true;
        n.refused = true;
        if let Some(copy) = n.copy.as_mut()
            && copy.writer == Some(i)
            && hand_back
        {
            copy.writer = None;
        }
        n
    }
    /// Load the dataset on instance `i`: serve the copy, create it, or fail.
    /// `None` when the load waits.
    fn open(&self, s: &State, i: usize) -> Option<State> {
        let mut n = s.clone();
        let new = Copy {
            generation: 0,
            definition: Some(self.definition(i)),
            app: s.app[i],
            built_by: s.app[i],
            rows: 0,
            writer: Some(i),
        };
        match &s.copy {
            None => n.copy = Some(new),
            Some(copy) => match copy.definition {
                // A copy built with no definition: its source is unknown, so it is
                // refused as a different definition is. The control adopts it, as
                // the first writer to load it once did.
                None if self.has("unrecorded-admits") => {
                    let copy = n.copy.as_mut()?;
                    if copy.writer.is_none_or(|w| w == i) {
                        copy.writer = Some(i);
                        copy.definition = Some(self.definition(i));
                        copy.app = s.app[i];
                    }
                }
                None => return Some(Self::refuse(n, i, !self.has("failed-takes-lease"))),
                Some(recorded) => {
                    if self.has("rebuild") && recorded != self.definition(i) {
                        // The single-instance rule: drop the stored copy and
                        // rebuild it in place from this instance's source.
                        n.copy = Some(Copy {
                            generation: copy.generation,
                            ..new
                        });
                    } else if self.matches(s, i, copy, recorded) {
                        n.declined[i] = false;
                        if copy.writer == Some(i) {
                            n.copy.as_mut()?.app = s.app[i];
                        }
                    } else {
                        return Some(Self::refuse(n, i, !self.has("failed-takes-lease")));
                    }
                }
            },
        }
        n.status[i] = Status::Serving;
        n.serves[i] = n.copy.as_ref().map(|copy| copy.generation);
        Some(n)
    }
}

impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let mut state = State {
            app: [0, 1],
            ..State::default()
        };
        if self.has("unrecorded") {
            // A copy built with no definition, holding host 0's rows. Instance 0
            // has started and holds its writer lease, and has not loaded the
            // dataset yet.
            state.copy = Some(Copy {
                generation: 0,
                definition: None,
                app: 0,
                built_by: 0,
                rows: 1,
                writer: Some(0),
            });
            state.status[0] = Status::Loading;
        }
        vec![state]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for i in 0..INSTANCES {
            match s.status[i] {
                Status::Stopped => a.push(Action::Start(i)),
                Status::Loading => {
                    a.push(Action::Open(i));
                    if !self.has("unrecorded") {
                        a.push(Action::Recreate(i));
                    }
                    a.push(Action::Stop(i));
                }
                Status::Serving => {
                    a.push(Action::Reload(i));
                    a.push(Action::Stop(i));
                }
                Status::Failed => a.push(Action::Stop(i)),
            }
            let Some(copy) = &s.copy else {
                continue;
            };
            if copy.writer == Some(i)
                && s.status[i] == Status::Serving
                && s.serves[i] == Some(copy.generation)
            {
                a.push(Action::Write(i));
            }
            let contends = matches!(s.status[i], Status::Loading | Status::Serving)
                || (s.status[i] == Status::Failed && self.has("failed-takes-lease"));
            if copy.writer.is_none() && contends && !s.pending[i] && !s.declined[i] {
                a.push(Action::SendAcquire(i));
            }
            if s.pending[i] {
                a.push(Action::LandAcquire(i));
            }
        }
        if s.status[0] == Status::Stopped && s.app[0] == 0 {
            a.push(Action::Rename);
        }
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        match action {
            Action::Start(i) => n.status[i] = Status::Loading,
            Action::Open(i) => return self.open(s, i),
            Action::Recreate(i) => {
                // The drop takes the lease back first; it replaces the copy only
                // when no other instance writes it.
                let copy = s.copy.as_ref()?;
                if copy.generation > 0 || copy.writer.is_some_and(|w| w != i) {
                    return None;
                }
                n.previous_rows = copy.rows;
                n.copy = Some(Copy {
                    generation: copy.generation + 1,
                    definition: Some(self.definition(i)),
                    app: s.app[i],
                    built_by: s.app[i],
                    rows: 0,
                    writer: Some(i),
                });
                n.status[i] = Status::Serving;
                n.serves[i] = Some(copy.generation + 1);
                n.declined[i] = false;
                n.recreated = true;
            }
            Action::Reload(i) => {
                let copy = s.copy.as_ref()?;
                if s.serves[i] == Some(copy.generation) {
                    return None;
                }
                if self.has("reload-without-check") {
                    n.serves[i] = Some(copy.generation);
                } else {
                    n.status[i] = Status::Loading;
                    n.serves[i] = None;
                    return self.open(&n, i);
                }
            }
            Action::Write(i) => {
                n.copy.as_mut()?.rows |= 1 << Self::host(i);
            }
            Action::SendAcquire(i) => n.pending[i] = true,
            Action::LandAcquire(i) => {
                n.pending[i] = false;
                let copy = n.copy.as_mut()?;
                if copy.writer.is_none() {
                    let hand_back = s.declined[i]
                        && !self.has("no-recheck-after-acquire")
                        && !self.has("failed-takes-lease");
                    if hand_back {
                        n.landed_after_refusal = true;
                    } else {
                        copy.writer = Some(i);
                        if s.status[i] == Status::Serving {
                            copy.app = s.app[i];
                        }
                    }
                }
            }
            Action::Stop(i) => {
                n.status[i] = Status::Stopped;
                n.serves[i] = None;
                n.pending[i] = false;
                n.declined[i] = false;
                if let Some(copy) = n.copy.as_mut()
                    && copy.writer == Some(i)
                {
                    copy.writer = None;
                }
            }
            Action::Rename => n.app[0] = 2,
        }
        if n == *s { None } else { Some(n) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "an instance reads only its own source's rows",
                |_, s: &State| {
                    (0..INSTANCES).all(|i| {
                        s.status[i] != Status::Serving
                            || (Protocol::rows_read(s, i) & !(1 << Protocol::host(i))) == 0
                    })
                },
            ),
            Property::always(
                "an instance fails only for a different definition",
                |_, s: &State| {
                    (0..INSTANCES).all(|i| {
                        s.status[i] != Status::Failed
                            || s.copy.as_ref().is_some_and(|copy| {
                                copy.definition
                                    .is_none_or(|recorded| recorded != Protocol::host(i))
                            })
                    })
                },
            ),
            Property::always("a copy's writer serves it", |_, s: &State| {
                s.copy.as_ref().is_none_or(|copy| {
                    copy.writer.is_none_or(|writer| {
                        matches!(s.status[writer], Status::Loading | Status::Serving)
                    })
                })
            }),
            Property::sometimes(
                "the other Spicepod's instance is refused the copy",
                |_, s: &State| s.refused && s.copy.as_ref().is_some_and(|copy| copy.rows != 0),
            ),
            Property::sometimes(
                "a renamed app serves and writes the copy it built",
                |_, s: &State| {
                    s.app[0] == 2
                        && s.status[0] == Status::Serving
                        && s.copy.as_ref().is_some_and(|copy| {
                            copy.built_by == 0 && copy.app == 2 && copy.rows == 1
                        })
                },
            ),
            Property::sometimes(
                "a refused instance leaves the copy to a writer that serves it",
                |_, s: &State| {
                    s.refused
                        && s.copy.as_ref().is_some_and(|copy| {
                            copy.writer.is_some_and(|w| s.status[w] == Status::Serving)
                        })
                },
            ),
            Property::sometimes(
                "a lease request lands after its instance was refused",
                |_, s: &State| s.landed_after_refusal,
            ),
            Property::sometimes(
                "file_create replaces the copy while another instance serves the previous one",
                |_, s: &State| {
                    s.recreated
                        && s.copy.as_ref().is_some_and(|copy| {
                            (0..INSTANCES).any(|i| {
                                s.status[i] == Status::Serving
                                    && s.serves[i] != Some(copy.generation)
                            })
                        })
                },
            ),
        ]
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Timeline, Tone, state_and_trace, steps};
        let state = path.last_state().clone();
        let app = |app: u8| match app {
            0 => "shop",
            1 => "store",
            _ => "shop (renamed)",
        };
        let hosts = |rows: u8| {
            (0..INSTANCES)
                .filter(|i| rows & (1 << Self::host(*i)) != 0)
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut boxes = vec![];
        boxes.push(match &state.copy {
            None => (
                "COPY".to_string(),
                vec!["not created".to_string()],
                Tone::Gone,
            ),
            Some(copy) => (
                format!("COPY (generation {})", copy.generation),
                vec![
                    copy.definition.map_or_else(
                        || "no definition recorded".to_string(),
                        |d| format!("built from host {d}"),
                    ),
                    format!("app recorded: {}", app(copy.app)),
                    format!("rows from hosts: {}", hosts(copy.rows)),
                    format!(
                        "writer: {}",
                        copy.writer
                            .map_or_else(|| "none".to_string(), |w| format!("instance {w}"))
                    ),
                ],
                Tone::Live,
            ),
        });
        for i in 0..INSTANCES {
            let reads_other = state.status[i] == Status::Serving
                && Self::rows_read(&state, i) & !(1 << Self::host(i)) != 0;
            let mut lines = vec![
                format!("app {}, host {}", app(state.app[i]), Self::host(i)),
                format!("{:?}", state.status[i]),
            ];
            if let Some(generation) = state.serves[i] {
                lines.push(format!(
                    "serves generation {generation}: rows from hosts {}",
                    hosts(Self::rows_read(&state, i))
                ));
            }
            if state.pending[i] {
                lines.push("lease request in flight".to_string());
            }
            if state.declined[i] {
                lines.push("takes no writer lease".to_string());
            }
            boxes.push((
                format!("INSTANCE {i}"),
                lines,
                if reads_other { Tone::Bad } else { Tone::Actor },
            ));
        }
        let lanes = vec![
            "instance 0".to_string(),
            "instance 1".to_string(),
            "copy".to_string(),
        ];
        let copy_lane = INSTANCES;
        let mut events = vec![];
        for (before, action, after) in steps(path) {
            let label = self.format_action(&action);
            let event = match action {
                Action::Open(i) | Action::Reload(i) => {
                    let tone = match after.status[i] {
                        Status::Failed => Tone::Gone,
                        _ => Tone::Live,
                    };
                    Event::on(i, label).to(copy_lane).tone(tone)
                }
                Action::Recreate(i) | Action::Write(i) => {
                    Event::on(i, label).to(copy_lane).tone(Tone::Live)
                }
                Action::SendAcquire(i) | Action::LandAcquire(i) => {
                    Event::on(i, label).to(copy_lane).tone(Tone::Staged)
                }
                Action::Start(i) => Event::on(i, label).tone(Tone::Actor),
                Action::Stop(i) => Event::on(i, label).tone(Tone::Gone),
                Action::Rename => Event::on(0, label).tone(Tone::Actor),
            };
            events.push(event.breaking(&crate::explorer::broken(self, &before, &after)));
        }
        Some(state_and_trace(&boxes, &Timeline { lanes, events }))
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Start(i) => format!("instance {i} starts and opens the location"),
            Action::Open(i) => format!("instance {i} loads the dataset"),
            Action::Recreate(i) => {
                format!("instance {i} loads the dataset with mode: file_create")
            }
            Action::Reload(i) => format!("instance {i} reloads the dataset"),
            Action::Write(i) => format!("instance {i} writes rows from its source host"),
            Action::SendAcquire(i) => format!("instance {i} requests the copy's writer lease"),
            Action::LandAcquire(i) => format!("instance {i}'s lease request lands"),
            Action::Stop(i) => format!("instance {i} stops"),
            Action::Rename => "instance 0 restarts as a renamed app".into(),
        }
    }
}
