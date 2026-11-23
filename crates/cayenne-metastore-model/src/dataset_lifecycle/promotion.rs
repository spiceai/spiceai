// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Warm inputs replaced by a cold output in one conditional head record.
use super::{Data, Protocol as Local, Record, record_lines, rows_text};
use stateright::{Model, Property};

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    remove_inputs: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct State {
    initial: Data,
    data: Data,
    log: Vec<Record>,
    request: Option<(usize, Record)>,
    delivered: u8,
    checkpoint: Option<(usize, Data)>,
    recovered: Option<Data>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Stage,
    Deliver(u8),
    Checkpoint,
    Recover,
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self {
            remove_inputs: !mode.contains("no-input-removal"),
        }
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        Local::new("dataset-lifecycle", 1)
            .init_states()
            .into_iter()
            .map(|s| State {
                initial: s.data.clone(),
                data: s.data,
                log: vec![],
                request: None,
                delivered: 0,
                checkpoint: None,
                recovered: None,
            })
            .collect()
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if s.request.is_none() {
            a.push(Action::Stage);
        }
        if s.request.is_some() {
            for copy in 0..2 {
                if s.delivered & (1 << copy) == 0 {
                    a.push(Action::Deliver(copy));
                }
            }
        }
        if s.checkpoint.is_none() {
            a.push(Action::Checkpoint);
        }
        a.push(Action::Recover);
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        n.recovered = None;
        match action {
            Action::Stage => {
                let mut r = Record {
                    add: vec![],
                    remove: vec![],
                    deletes: vec![],
                    clear: vec![],
                    high: s.data.high,
                    page: s.data.page,
                    prepare: None,
                    resolve: false,
                    decision: None,
                    ops: vec![],
                };
                for file in s.data.files.iter().filter(|f| !f.cold) {
                    let mut cold = file.clone();
                    cold.id += 100;
                    cold.cold = true;
                    cold.applied = Some(s.data.high[file.table]);
                    r.add.push(cold);
                    if self.remove_inputs {
                        r.remove.push(file.id);
                    }
                }
                n.request = Some((s.log.len(), r));
            }
            Action::Deliver(copy) => {
                n.delivered |= 1 << copy;
                let (base, r) = s.request.as_ref()?;
                // The unchanged condition and live-input guard apply to original and retry.
                if *base == s.log.len()
                    && r.remove
                        .iter()
                        .all(|id| s.data.files.iter().any(|f| f.id == *id))
                {
                    n.data.apply(r);
                    n.log.push(r.clone());
                }
            }
            Action::Checkpoint => n.checkpoint = Some((s.log.len(), s.data.clone())),
            Action::Recover => {
                let (base, mut data) = s.checkpoint.clone().unwrap_or((0, s.initial.clone()));
                for record in &s.log[base..] {
                    data.apply(record);
                }
                n.recovered = Some(data);
            }
        }
        (n != *s).then_some(n)
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Stage => "plan promotion of every warm file to cold".into(),
            Action::Deliver(0) => "the promotion CAS arrives".into(),
            Action::Deliver(_) => "a delayed duplicate of the promotion CAS arrives".into(),
            Action::Checkpoint => "write a checkpoint".into(),
            Action::Recover => "recover from checkpoint plus log tail".into(),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        let mut lines = vec![format!("reader sees: {}", rows_text(&s.data.rows()))];
        if let Some(data) = &s.recovered {
            lines.push(format!("recovered:   {}", rows_text(&data.rows())));
        }
        lines.push(format!("copies delivered: {:02b}", s.delivered));
        Some(crate::explorer::summary(&lines))
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Svg, Tone, row};
        let s = path.last_state();
        let mut svg = Svg::new();
        let head = vec![format!("commit {}", s.log.len())];
        let bottom = s
            .data
            .draw(&mut svg, 0, 0, "DATASET HEAD AND FILES", &head, None);
        let mut boxes = vec![];
        if let Some((base, r)) = &s.request {
            boxes.push((
                format!("PROMOTION CAS (on commit {base})"),
                record_lines(r),
                Tone::Staged,
            ));
        }
        if let Some((c, data)) = &s.checkpoint {
            boxes.push((
                format!("CHECKPOINT at commit {c}"),
                vec![rows_text(&data.rows())],
                Tone::Live,
            ));
        }
        if !boxes.is_empty() {
            row(&mut svg, bottom + 24, &boxes);
        }
        Some(svg.finish())
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("at most one live row per table key", |_, s: &State| {
                s.data.unique_keys() && s.recovered.as_ref().is_none_or(Data::unique_keys)
            }),
            Property::always("I9 cold promotion preserves rows", |_, s: &State| {
                s.data.rows() == s.initial.rows()
            }),
            Property::always("I8 checkpoint plus tail equals state", |_, s: &State| {
                s.recovered.as_ref().is_none_or(|d| *d == s.data)
            }),
            Property::sometimes("warm inputs replaced by cold outputs", |_, s: &State| {
                !s.log.is_empty() && s.data.files.iter().all(|f| f.cold)
            }),
            Property::sometimes("identical retry after acceptance", |_, s: &State| {
                s.delivered == 3 && s.log.len() == 1
            }),
            Property::sometimes("checkpoint followed by tail recovery", |_, s: &State| {
                s.checkpoint
                    .as_ref()
                    .is_some_and(|(base, _)| *base < s.log.len())
                    && s.recovered.is_some()
            }),
        ]
    }
}
