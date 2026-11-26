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
//! The design: a copy records its definition, including a digest of every
//! connection setting, and an instance serves a copy only when its own
//! definition matches. A mismatch fails the dataset on that instance, which
//! then never takes the copy's writer lease. The app name is recorded and
//! logged, never compared. Each control removes one part of the rule.
use stateright::{Model, Property};

const INSTANCES: usize = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum Status {
    #[default]
    Stopped,
    Serving,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Copy {
    /// The definition the copy was built from.
    definition: u8,
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
    status: [Status; INSTANCES],
    /// Each instance's app: 0 `shop`, 1 `store`, 2 `shop` renamed.
    app: [u8; INSTANCES],
    /// Witness: an instance opened the copy as a different definition.
    refused: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// An instance loads the dataset.
    Open(usize),
    /// The copy's writer commits rows from its own source host.
    Write(usize),
    /// An instance takes the copy's free writer lease.
    TakeLease(usize),
    /// An instance stops; a writer lease it held lapses.
    Stop(usize),
    /// Instance 0 restarts as a renamed app with the same definition.
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
    /// Whether instance `i` serves a copy recorded as `copy`.
    fn matches(&self, s: &State, i: usize, copy: &Copy) -> bool {
        if self.has("no-check") || self.has("rebuild") {
            true
        } else if self.has("app-name") {
            copy.app == s.app[i]
        } else {
            copy.definition == self.definition(i)
        }
    }
}

impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            app: [0, 1],
            ..State::default()
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for i in 0..INSTANCES {
            match s.status[i] {
                Status::Stopped => a.push(Action::Open(i)),
                Status::Serving | Status::Failed => a.push(Action::Stop(i)),
            }
            let Some(copy) = &s.copy else {
                continue;
            };
            if copy.writer == Some(i) && s.status[i] == Status::Serving {
                a.push(Action::Write(i));
            }
            let contends = s.status[i] == Status::Serving
                || (s.status[i] == Status::Failed && self.has("failed-takes-lease"));
            if copy.writer.is_none() && contends {
                a.push(Action::TakeLease(i));
            }
        }
        if s.status[0] == Status::Stopped && s.app[0] == 0 {
            a.push(Action::Rename);
        }
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        match action {
            Action::Open(i) => {
                let new = Copy {
                    definition: self.definition(i),
                    app: s.app[i],
                    built_by: s.app[i],
                    rows: 0,
                    writer: Some(i),
                };
                match &s.copy {
                    None => n.copy = Some(new),
                    Some(copy) if self.has("rebuild") && copy.definition != new.definition => {
                        // The single-instance rule: drop the stored copy and
                        // rebuild it from this instance's source.
                        n.copy = Some(new);
                    }
                    Some(copy) if self.matches(s, i, copy) => {
                        if copy.writer == Some(i) {
                            n.copy.as_mut()?.app = s.app[i];
                        }
                    }
                    Some(_) => {
                        n.status[i] = Status::Failed;
                        n.refused = true;
                        let copy = n.copy.as_mut()?;
                        if copy.writer == Some(i) && !self.has("failed-takes-lease") {
                            copy.writer = None;
                        }
                        return Some(n);
                    }
                }
                n.status[i] = Status::Serving;
            }
            Action::Write(i) => {
                n.copy.as_mut()?.rows |= 1 << Self::host(i);
            }
            Action::TakeLease(i) => {
                let copy = n.copy.as_mut()?;
                copy.writer = Some(i);
                if s.status[i] == Status::Serving {
                    copy.app = s.app[i];
                }
            }
            Action::Stop(i) => {
                n.status[i] = Status::Stopped;
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
                            || s.copy
                                .as_ref()
                                .is_none_or(|copy| (copy.rows & !(1 << Protocol::host(i))) == 0)
                    })
                },
            ),
            Property::always(
                "an instance fails only for a different definition",
                |_, s: &State| {
                    (0..INSTANCES).all(|i| {
                        s.status[i] != Status::Failed
                            || s.copy
                                .as_ref()
                                .is_some_and(|copy| copy.definition != Protocol::host(i))
                    })
                },
            ),
            Property::always("a copy's writer serves it", |_, s: &State| {
                s.copy.as_ref().is_none_or(|copy| {
                    copy.writer
                        .is_none_or(|writer| s.status[writer] == Status::Serving)
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
        let mut boxes = vec![];
        boxes.push(match &state.copy {
            None => (
                "COPY".to_string(),
                vec!["not created".to_string()],
                Tone::Gone,
            ),
            Some(copy) => (
                "COPY".to_string(),
                vec![
                    format!("built from host {}", copy.definition),
                    format!("app recorded: {}", app(copy.app)),
                    format!(
                        "rows from hosts: {}",
                        (0..INSTANCES)
                            .filter(|i| copy.rows & (1 << Self::host(*i)) != 0)
                            .map(|i| i.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
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
                && state
                    .copy
                    .as_ref()
                    .is_some_and(|copy| copy.rows & !(1 << Self::host(i)) != 0);
            boxes.push((
                format!("INSTANCE {i}"),
                vec![
                    format!("app {}, host {}", app(state.app[i]), Self::host(i)),
                    format!("{:?}", state.status[i]),
                ],
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
                Action::Open(i) => {
                    let tone = match after.status[i] {
                        Status::Failed => Tone::Gone,
                        _ => Tone::Live,
                    };
                    Event::on(i, label).to(copy_lane).tone(tone)
                }
                Action::Write(i) => Event::on(i, label).to(copy_lane).tone(Tone::Live),
                Action::TakeLease(i) => Event::on(i, label).to(copy_lane).tone(Tone::Staged),
                Action::Stop(i) => Event::on(i, label).tone(Tone::Gone),
                Action::Rename => Event::on(0, label).tone(Tone::Actor),
            };
            events.push(event.breaking(&crate::explorer::broken(self, &before, &after)));
        }
        Some(state_and_trace(&boxes, &Timeline { lanes, events }))
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Open(i) => format!("instance {i} loads the dataset"),
            Action::Write(i) => format!("instance {i} writes rows from its source host"),
            Action::TakeLease(i) => format!("instance {i} takes the copy's writer lease"),
            Action::Stop(i) => format!("instance {i} stops"),
            Action::Rename => "instance 0 restarts as a renamed app".into(),
        }
    }
}
