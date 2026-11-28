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

/// How instances that may disagree on a write unit's members decide who
/// writes each dataset: two datasets that one Spicepod names on one
/// replication slot, the second of which may never open in the object-native
/// metastore (accelerated by another engine, say), and two instances, each
/// of which groups the datasets on the slot or leases each on its own, as two
/// Spicepods mid-rollout do. Every dataset has its own lease, and the slot's
/// datasets share a unit lease. A renewal is one instance's `keep`: it takes
/// what it may, and an instance writes a dataset while its leases allow it.
/// Releases (a handoff, a shutdown, a lapse) are bounded so runs end.
///
/// The fixes: a unit groups only the datasets that open, and the holder of a
/// unit takes each member's own lease as well, all or none, releasing those
/// it holds while another instance holds any. The controls remove each
/// (`-partial-own-lease` takes members' own leases one by one and keeps them,
/// so two instances each wait for a lease the other holds):
/// `-groups-unopened` groups the dataset that never opens, so a unit can
/// never complete, and `-no-own-lease` writes a unit's members under the
/// unit lease alone, so two instances write one dataset.
pub(crate) mod grouping {
    use stateright::{Model, Property};

    const INSTANCES: usize = 2;
    const DATASETS: usize = 2;
    /// Lease releases, bounded so the runs stay finite.
    const RELEASES: u8 = 2;

    #[derive(Clone, Debug)]
    pub(crate) struct Protocol {
        mode: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub(crate) struct State {
        /// Whether each instance's Spicepod puts both datasets on the slot.
        grouped: [bool; INSTANCES],
        /// Whether dataset 1 opens in the metastore; dataset 0 always does.
        opens: bool,
        /// The unit lease's holder.
        unit: Option<usize>,
        /// Each dataset's own lease's holder.
        own: [Option<usize>; DATASETS],
        releases: u8,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub(crate) enum Action {
        /// Instance `.0` renews: a grouping instance its unit lease and the
        /// members' own leases; any other the own lease of dataset `.1`,
        /// which it keeps under that dataset's own unit, renewed on its own.
        Renew(usize, usize),
        /// Instance `.0` gives up every lease it holds.
        Release(usize),
    }

    impl Protocol {
        pub(crate) fn new(mode: &str) -> Self {
            Self { mode: mode.into() }
        }

        fn opened(s: &State, d: usize) -> bool {
            d == 0 || s.opens
        }

        /// The datasets a grouping instance's unit holds.
        fn members(&self, s: &State) -> Vec<usize> {
            (0..DATASETS)
                .filter(|&d| self.mode.contains("groups-unopened") || Self::opened(s, d))
                .collect()
        }

        fn own_lease(&self) -> bool {
            !self.mode.contains("no-own-lease")
        }

        /// Whether instance `i` has data writes open on dataset `d`.
        pub(crate) fn writes(&self, s: &State, i: usize, d: usize) -> bool {
            if !Self::opened(s, d) {
                return false;
            }
            if !s.grouped[i] {
                return s.own[d] == Some(i);
            }
            let members = self.members(s);
            s.unit == Some(i)
                && members.contains(&d)
                // A unit opens its writes once every member is open.
                && members.iter().all(|&m| Self::opened(s, m))
                && (!self.own_lease() || members.iter().all(|&m| s.own[m] == Some(i)))
        }
    }

    impl Model for Protocol {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            let mut states = vec![];
            for grouped in [[true, true], [true, false], [false, true]] {
                for opens in [true, false] {
                    states.push(State {
                        grouped,
                        opens,
                        unit: None,
                        own: [None; DATASETS],
                        releases: 0,
                    });
                }
            }
            states
        }

        fn actions(&self, s: &State, a: &mut Vec<Action>) {
            for i in 0..INSTANCES {
                for d in 0..DATASETS {
                    a.push(Action::Renew(i, d));
                }
                a.push(Action::Release(i));
            }
            crate::prune_for_simulation(self, s, a);
        }

        fn next_state(&self, s: &State, action: Action) -> Option<State> {
            let mut next = s.clone();
            match action {
                Action::Renew(i, dataset) => {
                    if s.grouped[i] {
                        // One renewal per unit, not per dataset.
                        if dataset != 0 {
                            return None;
                        }
                        if next.unit.is_none() {
                            next.unit = Some(i);
                        }
                        if next.unit == Some(i) && self.own_lease() {
                            let members: Vec<usize> = self
                                .members(s)
                                .into_iter()
                                .filter(|&d| Self::opened(s, d))
                                .collect();
                            if self.mode.contains("partial-own-lease") {
                                // Take each free member lease and keep it.
                                for d in members {
                                    if next.own[d].is_none() {
                                        next.own[d] = Some(i);
                                    }
                                }
                            } else if members
                                .iter()
                                .any(|&d| next.own[d].is_some_and(|holder| holder != i))
                            {
                                for d in 0..DATASETS {
                                    if next.own[d] == Some(i) {
                                        next.own[d] = None;
                                    }
                                }
                            } else {
                                for d in members {
                                    next.own[d] = Some(i);
                                }
                            }
                        }
                    } else if Self::opened(s, dataset) && next.own[dataset].is_none() {
                        next.own[dataset] = Some(i);
                    }
                }
                Action::Release(i) => {
                    let holds = s.unit == Some(i) || s.own.contains(&Some(i));
                    if !holds || s.releases >= RELEASES {
                        return None;
                    }
                    if next.unit == Some(i) {
                        next.unit = None;
                    }
                    for holder in &mut next.own {
                        if *holder == Some(i) {
                            *holder = None;
                        }
                    }
                    next.releases += 1;
                }
            }
            (next != *s).then_some(next)
        }

        fn format_action(&self, action: &Action) -> String {
            match action {
                Action::Renew(i, d) => {
                    format!("instance {i} renews (its unit, or dataset {d}'s own lease)")
                }
                Action::Release(i) => format!("instance {i} releases every lease it holds"),
            }
        }

        fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
            use crate::explorer::{Event, Timeline, Tone, state_and_trace, steps};
            let last = path.last_state().clone();
            let lanes: Vec<String> = (0..INSTANCES)
                .map(|i| {
                    let grouping = if last.grouped[i] {
                        "groups the slot"
                    } else {
                        "leases each dataset"
                    };
                    format!("instance {i} ({grouping})")
                })
                .collect();
            let mut events = vec![];
            for (before, action, after) in steps(path) {
                let writing = |i: usize| {
                    (0..DATASETS)
                        .filter(|&d| self.writes(&after, i, d))
                        .map(|d| d.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let ev = match action {
                    Action::Renew(i, _) => {
                        Event::on(i, format!("renew; writes datasets [{}]", writing(i)))
                    }
                    Action::Release(i) => Event::on(i, "release every lease").tone(Tone::Gone),
                };
                events.push(ev.breaking(&crate::explorer::broken(self, &before, &after)));
            }
            let boxes = vec![
                (
                    "LEASES".into(),
                    vec![
                        format!("unit {:?}", last.unit),
                        format!("own {:?}", last.own),
                        format!("dataset 1 opens: {}", last.opens),
                    ],
                    Tone::Actor,
                ),
                (
                    "WRITES".into(),
                    (0..DATASETS)
                        .map(|d| {
                            let writers: Vec<usize> = (0..INSTANCES)
                                .filter(|&i| self.writes(&last, i, d))
                                .collect();
                            format!("dataset {d} written by {writers:?}")
                        })
                        .collect(),
                    Tone::Live,
                ),
            ];
            Some(state_and_trace(&boxes, &Timeline { lanes, events }))
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![
                Property::always(
                    "at most one instance has data writes open on each dataset",
                    |m: &Self, s: &State| {
                        (0..DATASETS)
                            .all(|d| (0..INSTANCES).filter(|&i| m.writes(s, i, d)).count() <= 1)
                    },
                ),
                // Every path ends (releases are bounded, and a renewal that
                // changes nothing is no step), so this is checked where it
                // ends: once the instances stop releasing, every dataset that
                // opens has one writer.
                Property::eventually(
                    "every dataset that opens is written once the leases settle",
                    |m: &Self, s: &State| {
                        (0..DATASETS)
                            .filter(|&d| Self::opened(s, d))
                            .all(|d| (0..INSTANCES).any(|i| m.writes(s, i, d)))
                    },
                ),
            ]
        }
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
