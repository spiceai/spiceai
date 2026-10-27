// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Position identities survive a racing rewrite through an explicit row mapping.
use stateright::{Model, Property};

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct File {
    id: usize,
    rows: Vec<usize>,
    seq: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Rewrite {
    base: usize,
    output: File,
    // One entry for every input position; None proves the row was deleted.
    mapping: Vec<Option<usize>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Delete {
    base: usize,
    target: (usize, usize),
    key: usize,
    seq: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Record {
    Replace(Rewrite),
    Delete {
        original: usize,
        target: Option<(usize, usize)>,
        seq: usize,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol witnesses"
)]
pub(crate) struct State {
    files: Vec<File>,
    deletes: Vec<(usize, usize, usize)>,
    expected: Vec<usize>,
    versions: Vec<(Vec<usize>, Vec<usize>)>,
    log: Vec<Record>,
    delete: Option<Delete>,
    rewrite: Option<Rewrite>,
    planned_delete: bool,
    planned_rewrite: bool,
    remapped: bool,
    consumed: bool,
    error: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    PlanDelete(usize),
    PlanRewrite(bool),
    PublishRewrite,
    RebaseDelete,
    PublishDelete,
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self { mode: mode.into() }
    }
    fn has(&self, word: &str) -> bool {
        self.mode.contains(word)
    }
    fn visible(files: &[File], deletes: &[(usize, usize, usize)]) -> Vec<usize> {
        let mut rows = Vec::new();
        for file in files {
            for (position, key) in file.rows.iter().enumerate() {
                if !deletes
                    .iter()
                    .any(|&(f, p, q)| f == file.id && p == position && q >= file.seq)
                {
                    rows.push(*key);
                }
            }
        }
        rows.sort_unstable();
        rows
    }
    fn save(state: &mut State) {
        state.versions.push((
            Self::visible(&state.files, &state.deletes),
            state.expected.clone(),
        ));
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let deletes = if self.has("already-dropped") {
            vec![(0, 0, 2)]
        } else {
            vec![]
        };
        let files = vec![File {
            id: 0,
            rows: vec![0, 1, 2],
            seq: 1,
        }];
        let expected = Self::visible(&files, &deletes);
        vec![State {
            files,
            deletes,
            expected: expected.clone(),
            versions: vec![(expected.clone(), expected)],
            log: vec![],
            delete: None,
            rewrite: None,
            planned_delete: false,
            planned_rewrite: false,
            remapped: false,
            consumed: false,
            error: false,
        }]
    }
    fn actions(&self, state: &State, action: &mut Vec<Action>) {
        if !state.planned_delete && state.files.iter().any(|f| f.id == 0) {
            for position in 0..if self.has("minimal") || self.has("already-dropped") {
                1
            } else {
                3
            } {
                action.push(Action::PlanDelete(position));
            }
        }
        if !state.planned_rewrite && state.files.iter().any(|f| f.id == 0) {
            action.extend([Action::PlanRewrite(false), Action::PlanRewrite(true)]);
        }
        if state.rewrite.is_some() {
            action.push(Action::PublishRewrite);
        }
        if let Some(d) = &state.delete {
            if d.base == state.log.len() {
                action.push(Action::PublishDelete);
            } else {
                action.push(Action::RebaseDelete);
            }
        }
        action.retain(|action| self.next_state(state, action.clone()).is_some());
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::PlanDelete(position) => {
                let key = *state.files.iter().find(|f| f.id == 0)?.rows.get(position)?;
                next.delete = Some(Delete {
                    base: state.log.len(),
                    target: (0, position),
                    key,
                    seq: 3,
                });
                next.planned_delete = true;
            }
            Action::PlanRewrite(reverse) => {
                let input = state.files.iter().find(|f| f.id == 0)?;
                let mut rows = Self::visible(std::slice::from_ref(input), &state.deletes);
                if reverse {
                    rows.reverse();
                } else {
                    rows.rotate_left(1);
                }
                let mapping = input
                    .rows
                    .iter()
                    .map(|key| rows.iter().position(|k| k == key))
                    .collect();
                next.rewrite = Some(Rewrite {
                    base: state.log.len(),
                    output: File {
                        id: 1,
                        rows,
                        seq: 1,
                    },
                    mapping,
                });
                next.planned_rewrite = true;
            }
            Action::PublishRewrite => {
                let mut r = next.rewrite.take()?;
                if r.base != state.log.len() {
                    // A winning position delete is mapped onto the planned output before retry.
                    for &(file, p, seq) in &state.deletes {
                        if file == 0
                            && let Some(Some(out)) = r.mapping.get(p)
                        {
                            next.deletes.push((1, *out, seq));
                        }
                    }
                    r.base = state.log.len();
                }
                next.files.retain(|f| f.id != 0);
                next.files.push(r.output.clone());
                next.log.push(Record::Replace(r));
                Self::save(&mut next);
            }
            Action::RebaseDelete => {
                let d = next.delete.as_mut()?;
                let r = state.log[d.base..].iter().find_map(|r| {
                    if let Record::Replace(r) = r {
                        Some(r)
                    } else {
                        None
                    }
                })?;
                if self.has("no-position-remap") {
                    // Both controls silently finish the logical delete without changing output rows.
                    d.target = (0, d.target.1);
                } else if let Some(out) = r.mapping[d.target.1] {
                    let position = if self.has("no-row-identity-mapping") {
                        (out + 1) % r.output.rows.len()
                    } else {
                        out
                    };
                    d.target = (r.output.id, position);
                    next.remapped = true;
                } else if self.has("reject-dropped-target") {
                    next.error = true;
                    next.delete = None;
                    return Some(next);
                } else if self.has("no-row-identity-mapping") {
                    d.target = (r.output.id, 0);
                    next.remapped = true;
                } else {
                    // Mapping evidence establishes that a prior committed delete already hid it.
                    next.consumed = true;
                }
                if let Some(d) = &mut next.delete {
                    d.base = state.log.len();
                }
            }
            Action::PublishDelete => {
                let d = next.delete.take()?;
                let target = if state.consumed
                    || self.has("no-position-remap-silent-reject")
                        && d.target.0 == 0
                        && !state.files.iter().any(|f| f.id == 0)
                {
                    None
                } else {
                    Some(d.target)
                };
                if let Some((file, position)) = target {
                    next.deletes.push((file, position, d.seq));
                }
                next.expected.retain(|key| *key != d.key);
                next.log.push(Record::Delete {
                    original: d.key,
                    target,
                    seq: d.seq,
                });
                Self::save(&mut next);
            }
        }
        (next != *state).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("I9 position delete exact target", |_, state: &State| {
                state
                    .versions
                    .iter()
                    .all(|(actual, expected)| actual == expected)
            }),
            Property::always("at most one live row per table key", |_, state: &State| {
                state
                    .versions
                    .iter()
                    .all(|(rows, _)| rows.windows(2).all(|pair| pair[0] != pair[1]))
            }),
            Property::always(
                "remapped delete hides exactly targeted row",
                |_, state: &State| {
                    state.log.iter().enumerate().all(|(i, r)| {
                        if let Record::Delete { original, .. } = r {
                            let expected: Vec<_> = state.versions[i]
                                .0
                                .iter()
                                .filter(|key| *key != original)
                                .copied()
                                .collect();
                            state.versions[i + 1].0 == expected
                        } else {
                            true
                        }
                    })
                },
            ),
            Property::sometimes(
                "position delete remapped after compaction",
                |_, state: &State| {
                    state.remapped && state.log.iter().any(|r| matches!(r, Record::Delete { .. }))
                },
            ),
            Property::sometimes("already deleted target consumed", |_, state: &State| {
                state.consumed && state.log.iter().any(|r| matches!(r, Record::Delete { .. }))
            }),
            Property::sometimes("dropped target reports conflict", |_, state: &State| {
                state.error
            }),
            Property::sometimes(
                "compaction remaps winning position delete",
                |_, state: &State| {
                    matches!(
                        state.log.as_slice(),
                        [Record::Delete { .. }, Record::Replace(_)]
                    )
                },
            ),
        ]
    }
}
