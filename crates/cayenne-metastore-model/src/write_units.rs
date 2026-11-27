// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Datasets that share a change source's consumer: two datasets on one
//! PostgreSQL replication slot, and two instances. The slot has one consumer at
//! a time, as PostgreSQL enforces, and its change stream interleaves the two
//! datasets' changes. The consumer applies a change only to a dataset it
//! writes; at a change for a dataset it does not write, it holds the slot's
//! position, as the shared pump holds the floor of a table whose dataset has
//! not joined, and so stalls. Writing moves between the instances by releasing
//! and acquiring leases at a commit boundary.
//!
//! The datasets are one write unit, leased together: the instance holding the
//! unit's lease writes both, and only it consumes the slot. The
//! `per-dataset` control leases each dataset on its own, as before write units,
//! so two instances can each write one of them, and the slot's consumer then
//! meets a change it cannot apply.
use stateright::{Model, Property};

/// The dataset each change of the slot's stream belongs to, in stream order.
const STREAM: [usize; 4] = [0, 1, 0, 1];
const INSTANCES: usize = 2;
const DATASETS: usize = 2;
/// Lease acquisitions, bounded so the runs stay finite.
const ACQUIRES: u8 = 3;

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct State {
    /// Each lease's holder: one lease for the unit, or one per dataset in the
    /// `per-dataset` control.
    leases: Vec<Option<usize>>,
    /// The instance consuming the slot.
    consumer: Option<usize>,
    /// How far the slot is acknowledged: every change before it was applied.
    pos: usize,
    /// The changes committed to each dataset, by stream index.
    committed: [Vec<usize>; DATASETS],
    acquires: u8,
    /// A lease changed holder between two changes of the stream.
    moved: bool,
    /// The consumer met a change for a dataset it does not write.
    stalled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    /// Instance `.0` acquires lease `.1`, which no one holds.
    Acquire(usize, usize),
    /// Instance `.0` releases lease `.1` at a commit boundary.
    Release(usize, usize),
    /// Instance `.0` starts consuming the slot, which no one consumes.
    Attach(usize),
    /// The consumer `.0` takes the next change from the slot.
    Apply(usize),
}

impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self { mode: mode.into() }
    }

    fn per_dataset(&self) -> bool {
        self.mode.contains("per-dataset")
    }

    fn lease_count(&self) -> usize {
        if self.per_dataset() { DATASETS } else { 1 }
    }

    /// Whether instance `i` writes dataset `d`.
    fn writes(&self, s: &State, i: usize, d: usize) -> bool {
        let lease = if self.per_dataset() { d } else { 0 };
        s.leases[lease] == Some(i)
    }

    /// Whether instance `i` runs the slot's consumer: it writes every dataset
    /// on the slot (one unit), or, in the control, any one of them, since each
    /// dataset's refresh joins the slot on its own.
    fn runs_consumer(&self, s: &State, i: usize) -> bool {
        if self.per_dataset() {
            (0..DATASETS).any(|d| self.writes(s, i, d))
        } else {
            self.writes(s, i, 0)
        }
    }
}

impl Model for Protocol {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            leases: vec![None; self.lease_count()],
            ..State::default()
        }]
    }

    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for i in 0..INSTANCES {
            for l in 0..self.lease_count() {
                a.push(Action::Acquire(i, l));
                a.push(Action::Release(i, l));
            }
            a.push(Action::Attach(i));
            a.push(Action::Apply(i));
        }
        crate::prune_for_simulation(self, s, a);
    }

    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut next = s.clone();
        match action {
            Action::Acquire(i, l) => {
                if s.leases[l].is_some() || s.acquires >= ACQUIRES {
                    return None;
                }
                next.leases[l] = Some(i);
                next.acquires += 1;
                // A move mid-stream: some changes applied, some still to come.
                next.moved |= s.pos > 0 && s.pos < STREAM.len();
            }
            Action::Release(i, l) => {
                if s.leases[l] != Some(i) {
                    return None;
                }
                next.leases[l] = None;
                // An instance that no longer runs the consumer stops it, and
                // the slot is free for the next one.
                if s.consumer == Some(i) && !self.runs_consumer(&next, i) {
                    next.consumer = None;
                }
            }
            Action::Attach(i) => {
                if s.consumer.is_some() || !self.runs_consumer(s, i) {
                    return None;
                }
                next.consumer = Some(i);
            }
            Action::Apply(i) => {
                if s.consumer != Some(i) || s.pos >= STREAM.len() || s.stalled {
                    return None;
                }
                let dataset = STREAM[s.pos];
                if self.writes(s, i, dataset) {
                    next.committed[dataset].push(s.pos);
                    next.pos += 1;
                } else {
                    // The change cannot be applied here, and the slot holds
                    // its position: no one else may consume it meanwhile.
                    next.stalled = true;
                }
            }
        }
        (next != *s).then_some(next)
    }

    fn format_action(&self, action: &Action) -> String {
        let lease = |l: usize| {
            if self.per_dataset() {
                format!("dataset {l}'s lease")
            } else {
                "the unit's lease".to_string()
            }
        };
        match action {
            Action::Acquire(i, l) => format!("instance {i} acquires {}", lease(*l)),
            Action::Release(i, l) => format!("instance {i} releases {}", lease(*l)),
            Action::Attach(i) => format!("instance {i} starts consuming the slot"),
            Action::Apply(i) => format!("instance {i} takes the slot's next change"),
        }
    }

    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        Some(crate::explorer::summary(&[
            format!(
                "lease holders {:?}; slot consumer {:?}",
                s.leases, s.consumer
            ),
            format!(
                "slot acknowledged through change {} of {}; committed {:?}",
                s.pos,
                STREAM.len(),
                s.committed
            ),
            crate::explorer::flags(&[("moved mid-stream", s.moved), ("STALLED", s.stalled)]),
        ]))
    }

    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Timeline, Tone, state_and_trace, steps};
        let last = path.last_state().clone();
        let slot = INSTANCES;
        let mut lanes: Vec<String> = (0..INSTANCES).map(|i| format!("instance {i}")).collect();
        lanes.push("slot".into());
        let mut events = vec![];
        for (before, action, after) in steps(path) {
            let ev = match action {
                Action::Acquire(i, l) => Event::on(i, format!("acquire lease {l}")),
                Action::Release(i, l) => {
                    Event::on(i, format!("release lease {l}")).tone(Tone::Gone)
                }
                Action::Attach(i) => Event::on(i, "consume the slot").to(slot),
                Action::Apply(i) => Event::on(
                    slot,
                    if after.stalled {
                        format!(
                            "change {} for a dataset instance {i} does not write: STALL",
                            before.pos
                        )
                    } else {
                        format!("change {} applied", before.pos)
                    },
                )
                .to(i)
                .tone(if after.stalled { Tone::Bad } else { Tone::Live }),
            };
            events.push(ev.breaking(&crate::explorer::broken(self, &before, &after)));
        }
        let boxes = vec![
            (
                "LEASES".into(),
                vec![format!("holders {:?}", last.leases)],
                Tone::Actor,
            ),
            (
                "SLOT".into(),
                vec![
                    format!("consumer {:?}", last.consumer),
                    format!("acknowledged through change {}", last.pos),
                ],
                if last.stalled { Tone::Bad } else { Tone::Live },
            ),
            (
                "DATASETS".into(),
                (0..DATASETS)
                    .map(|d| format!("dataset {d} committed {:?}", last.committed[d]))
                    .collect(),
                Tone::Live,
            ),
        ];
        Some(state_and_trace(&boxes, &Timeline { lanes, events }))
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "the slot never stalls on a change its consumer cannot apply",
                |_, s: &State| !s.stalled,
            ),
            Property::always("every change committed once, in order", |_, s: &State| {
                (0..DATASETS).all(|d| {
                    let expected: Vec<usize> = (0..s.pos).filter(|&p| STREAM[p] == d).collect();
                    s.committed[d] == expected
                })
            }),
            Property::always("only a holder consumes the slot", |m: &Self, s: &State| {
                s.consumer.is_none_or(|i| m.runs_consumer(s, i))
            }),
            Property::sometimes(
                "every change committed across a move of writing mid-stream",
                |_, s: &State| s.pos == STREAM.len() && s.moved,
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::{Action, Protocol, State};
    use stateright::Model;

    fn run(model: &Protocol, actions: &[Action]) -> State {
        let mut state = model.init_states().remove(0);
        for action in actions {
            state = model
                .next_state(&state, action.clone())
                .unwrap_or_else(|| panic!("{action:?} is enabled"));
        }
        state
    }

    /// With per-dataset leases two instances each write one dataset, and the
    /// slot's consumer stalls at the other's first change.
    #[test]
    fn a_split_consumer_stalls_and_a_unit_cannot_split() {
        let control = Protocol::new("write-units-per-dataset");
        let split = run(
            &control,
            &[
                Action::Acquire(0, 0),
                Action::Acquire(1, 1),
                Action::Attach(0),
                Action::Apply(0),
                Action::Apply(0),
            ],
        );
        assert!(split.stalled);
        assert_eq!(split.pos, 1);

        // One lease for both: the second instance cannot take either dataset.
        let unit = Protocol::new("write-units");
        let held = run(&unit, &[Action::Acquire(0, 0)]);
        assert_eq!(unit.next_state(&held, Action::Acquire(1, 0)), None);
    }
}
