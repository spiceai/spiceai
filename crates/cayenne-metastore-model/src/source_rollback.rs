// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Two durable acceleration copies consume one append source across an upgrade,
//! rollback and upgrade again. Default consumers belong to each copy; an
//! explicitly shared consumer requires rebuilding a copy behind its acknowledgement.
//! Rows are the source's numbered events, retained independently of positions.
use stateright::{Model, Property};

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    shared: bool,
    rebuild: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct State {
    source: u8,
    rows: [Vec<u8>; 2],
    acknowledged: [Option<u8>; 2],
    cursor: Option<u8>,
    switches: u8,
    rebuilt: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Append,
    Open,
    Apply,
    Acknowledge,
    Switch,
}

impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self {
            shared: mode.contains("shared"),
            rebuild: !mode.contains("no-rebuild"),
        }
    }

    fn slot(&self, s: &State) -> usize {
        if self.shared { 0 } else { s.copy() }
    }
}

impl State {
    fn copy(&self) -> usize {
        usize::from(self.switches % 2)
    }

    fn position(&self) -> u8 {
        self.rows[self.copy()].last().copied().unwrap_or(0)
    }
}

impl Model for Protocol {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State::default()]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.source < 2 {
            actions.push(Action::Append);
        }
        if let Some(cursor) = s.cursor {
            if cursor < s.source {
                actions.push(Action::Apply);
            }
            if s.acknowledged[self.slot(s)].is_none_or(|ack| ack < s.position()) {
                actions.push(Action::Acknowledge);
            }
        } else {
            actions.push(Action::Open);
        }
        if s.switches < 3 && s.cursor.is_some() {
            actions.push(Action::Switch);
        }
    }

    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        match action {
            Action::Append => n.source += 1,
            Action::Open => {
                let ack = s.acknowledged[self.slot(s)];
                // A new consumer snapshots the source; an existing consumer
                // rebuilds when its acknowledgement exceeds this copy's position.
                if ack.is_none() || self.rebuild && ack.is_some_and(|ack| ack > s.position()) {
                    n.rows[s.copy()] = (1..=s.source).collect();
                    n.cursor = Some(s.source);
                    n.rebuilt |= ack.is_some_and(|ack| ack > s.position());
                } else {
                    n.cursor = ack;
                }
            }
            Action::Apply => {
                let next = s.cursor? + 1;
                n.cursor = Some(next);
                if next > s.position() {
                    n.rows[s.copy()].push(next);
                }
            }
            Action::Acknowledge => n.acknowledged[self.slot(s)] = Some(s.position()),
            Action::Switch => {
                n.switches += 1;
                n.cursor = None;
            }
        }
        Some(n)
    }

    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Append => "the source appends a numbered event",
            Action::Open if self.rebuild => {
                "open this copy from its consumer, rebuilding if it is behind"
            }
            Action::Open => "open this copy from its consumer without rebuilding a gap",
            Action::Apply => "commit the next event, skipping durable redelivery",
            Action::Acknowledge => "acknowledge this copy's durable position",
            Action::Switch => "stop and switch to the other durable copy",
        }
        .into()
    }

    fn format_step(&self, last: &State, action: Action) -> Option<String> {
        let s = self.next_state(last, action)?;
        Some(crate::explorer::summary(&[
            format!("source rows {:?}", (1..=s.source).collect::<Vec<_>>()),
            format!(
                "release copy {:?}; upgraded copy {:?}",
                s.rows[0], s.rows[1]
            ),
            format!(
                "acknowledged {:?}; active copy {}; cursor {:?}",
                s.acknowledged,
                s.copy(),
                s.cursor
            ),
        ]))
    }

    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Timeline, Tone, state_and_trace, steps};
        let s = path.last_state();
        let mut boxes = vec![(
            "SOURCE".into(),
            vec![
                format!("rows {:?}", (1..=s.source).collect::<Vec<_>>()),
                format!("acknowledged {:?}", s.acknowledged),
            ],
            Tone::Actor,
        )];
        for (copy, name) in ["RELEASE COPY", "UPGRADED COPY"].into_iter().enumerate() {
            boxes.push((
                name.into(),
                vec![format!("rows {:?}", s.rows[copy])],
                if copy == s.copy() {
                    Tone::Live
                } else {
                    Tone::Gone
                },
            ));
        }
        let events = steps(path)
            .into_iter()
            .map(|(before, action, after)| {
                Event::on(after.copy() + 1, self.format_action(&action))
                    .breaking(&crate::explorer::broken(self, &before, &after))
            })
            .collect();
        Some(state_and_trace(
            &boxes,
            &Timeline {
                lanes: ["source", "release copy", "upgraded copy"]
                    .map(String::from)
                    .to_vec(),
                events,
            },
        ))
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("caught-up reads match the source", |_, s: &State| {
                s.cursor != Some(s.source) || s.rows[s.copy()] == (1..=s.source).collect::<Vec<_>>()
            }),
            Property::sometimes("rollback resumes after upgrade", |_, s: &State| {
                s.switches == 2 && s.source == 2 && s.cursor == Some(2)
            }),
            Property::sometimes("upgrade again resumes after rollback", |_, s: &State| {
                s.switches == 3 && s.source == 2 && s.cursor == Some(2)
            }),
            Property::sometimes(
                "consumer isolation or gap rebuild reached",
                |m, s: &State| {
                    if m.shared {
                        s.rebuilt && s.switches >= 2
                    } else {
                        s.acknowledged[0]
                            .is_some_and(|old| s.acknowledged[1].is_some_and(|new| new > old))
                    }
                },
            ),
        ]
    }
}
