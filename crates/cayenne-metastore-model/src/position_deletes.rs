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
    fn keys(keys: &[usize]) -> String {
        let keys: Vec<_> = keys.iter().map(|k| format!("k{k}")).collect();
        format!("[{}]", keys.join(", "))
    }
    /// The Explorer's view of a state: the files a reader scans, which rows are
    /// visible against the rows the serial history expects, the commit log and
    /// the operations still in flight. "Complete State?" shows the raw state.
    fn render(state: &State) -> Result<String, std::fmt::Error> {
        use std::fmt::Write as _;
        let mut out = String::new();
        writeln!(out, "FILES (position: key)")?;
        for file in &state.files {
            write!(out, "  file {} (seq {}):", file.id, file.seq)?;
            for (position, key) in file.rows.iter().enumerate() {
                let deleted = state
                    .deletes
                    .iter()
                    .find(|&&(f, p, q)| f == file.id && p == position && q >= file.seq);
                match deleted {
                    Some((_, _, seq)) => write!(out, "  {position}:k{key} [deleted @{seq}]")?,
                    None => write!(out, "  {position}:k{key}")?,
                }
            }
            writeln!(out)?;
        }
        let visible = Self::visible(&state.files, &state.deletes);
        writeln!(out)?;
        writeln!(
            out,
            "READ NOW   visible {}  expected {}  {}",
            Self::keys(&visible),
            Self::keys(&state.expected),
            if visible == state.expected {
                "ok"
            } else {
                "MISMATCH"
            }
        )?;
        writeln!(out)?;
        writeln!(out, "COMMITTED VERSIONS (visible vs expected)")?;
        for (version, (actual, expected)) in state.versions.iter().enumerate() {
            writeln!(
                out,
                "  v{version}: {} vs {}  {}",
                Self::keys(actual),
                Self::keys(expected),
                if actual == expected { "ok" } else { "MISMATCH" }
            )?;
        }
        writeln!(out)?;
        writeln!(out, "COMMIT LOG")?;
        if state.log.is_empty() {
            writeln!(out, "  (empty)")?;
        }
        for (index, record) in state.log.iter().enumerate() {
            match record {
                Record::Replace(r) => {
                    let mapping: Vec<_> = r
                        .mapping
                        .iter()
                        .enumerate()
                        .map(|(from, to)| match to {
                            Some(to) => format!("{from}->{to}"),
                            None => format!("{from}->gone"),
                        })
                        .collect();
                    writeln!(
                        out,
                        "  {}. compaction: file 0 -> file {} {}, positions {}",
                        index + 1,
                        r.output.id,
                        Self::keys(&r.output.rows),
                        mapping.join(" ")
                    )?;
                }
                Record::Delete {
                    original,
                    target,
                    seq,
                } => match target {
                    Some((file, position)) => writeln!(
                        out,
                        "  {}. delete k{original} at file {file} position {position} (seq {seq})",
                        index + 1
                    )?,
                    None => writeln!(
                        out,
                        "  {}. delete k{original} with no position (seq {seq})",
                        index + 1
                    )?,
                },
            }
        }
        writeln!(out)?;
        writeln!(out, "IN FLIGHT")?;
        match &state.delete {
            Some(d) => writeln!(
                out,
                "  delete k{} at file {} position {}, planned at log {}",
                d.key, d.target.0, d.target.1, d.base
            )?,
            None if state.planned_delete => writeln!(out, "  delete: published")?,
            None => writeln!(out, "  delete: not planned")?,
        }
        match &state.rewrite {
            Some(r) => writeln!(
                out,
                "  compaction to file {} {}, planned at log {}",
                r.output.id,
                Self::keys(&r.output.rows),
                r.base
            )?,
            None if state.planned_rewrite => writeln!(out, "  compaction: published")?,
            None => writeln!(out, "  compaction: not planned")?,
        }
        Ok(out)
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
        crate::prune_for_simulation(self, state, action);
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
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::PlanDelete(position) => {
                format!("plan delete of the row at file 0 position {position}")
            }
            Action::PlanRewrite(false) => "plan compaction of file 0 (rotate rows)".into(),
            Action::PlanRewrite(true) => "plan compaction of file 0 (reverse rows)".into(),
            Action::PublishRewrite => "publish compaction".into(),
            Action::RebaseDelete => "rebase delete over the compaction".into(),
            Action::PublishDelete => "publish delete".into(),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        self.next_state(last_state, action)
            .and_then(|next| Self::render(&next).ok())
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
