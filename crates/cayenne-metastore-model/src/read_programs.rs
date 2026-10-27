// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Snapshot programs, conditional publication, and commit-order program replay.
use stateright::{Model, Property};
use std::collections::BTreeSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) mod delivery;

type Image = [u8; 2];
type Rows = [[Option<Image>; 3]; 2];
type Key = (usize, usize);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Predicate {
    table: usize,
    column: usize,
    equals: u8,
}
impl Predicate {
    fn matches(&self, table: usize, row: Image) -> bool {
        table == self.table && (self.equals == 4 || row[self.column] == self.equals)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Program {
    Transaction(Vec<Program>),
    Update {
        predicate: Predicate,
        column: usize,
        limit: u8,
    },
    Delete(Predicate),
    InsertSelect {
        predicate: Predicate,
        target: usize,
        key_offset: usize,
    },
    Aggregate {
        table: usize,
        target: Key,
        cap: u8,
    },
    Absent {
        lookup: Key,
        target: Key,
    },
    Blind(Vec<(Key, Option<Image>)>),
    Insert {
        target: Key,
        value: Image,
    },
    Append(Vec<(Key, Image)>),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Mutation {
    key: Key,
    value: Option<Image>,
    replace: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Plan {
    actions: Vec<Mutation>,
    looked_up: Vec<Key>,
    predicates: Vec<Predicate>,
}
impl Program {
    fn dependent(&self) -> bool {
        !matches!(self, Self::Blind(_) | Self::Insert { .. } | Self::Append(_))
    }
    fn tables(&self, writes_only: bool) -> Vec<usize> {
        let mut tables = match self {
            Self::Transaction(statements) => statements
                .iter()
                .flat_map(|p| p.tables(writes_only))
                .collect(),
            Self::Update { predicate, .. } | Self::Delete(predicate) => vec![predicate.table],
            Self::InsertSelect {
                predicate, target, ..
            } => {
                if writes_only {
                    vec![*target]
                } else {
                    vec![predicate.table, *target]
                }
            }
            Self::Aggregate { table, target, .. } => {
                if writes_only {
                    vec![target.0]
                } else {
                    vec![*table, target.0]
                }
            }
            Self::Absent { lookup, target } => {
                if writes_only {
                    vec![target.0]
                } else {
                    vec![lookup.0, target.0]
                }
            }
            Self::Blind(ops) => ops.iter().map(|(k, _)| k.0).collect(),
            Self::Insert { target, .. } => vec![target.0],
            Self::Append(ops) => ops.iter().map(|(k, _)| k.0).collect(),
        };
        tables.sort_unstable();
        tables.dedup();
        tables
    }
    // Statement execution builds actions and dependencies from one immutable snapshot.
    fn plan(&self, rows: &Rows) -> Option<Plan> {
        let mut plan = Plan {
            actions: vec![],
            looked_up: vec![],
            predicates: vec![],
        };
        match self {
            Self::Transaction(statements) => {
                let mut overlay = *rows;
                for statement in statements {
                    let next = statement.plan(&overlay)?;
                    for mutation in &next.actions {
                        overlay[mutation.key.0][mutation.key.1] = mutation.value;
                    }
                    plan.actions.extend(next.actions);
                    plan.looked_up.extend(next.looked_up);
                    plan.predicates.extend(next.predicates);
                }
            }
            Self::Update { predicate, .. }
            | Self::Delete(predicate)
            | Self::InsertSelect { predicate, .. } => {
                plan.predicates.push(predicate.clone());
                for (key, row) in rows[predicate.table].iter().enumerate() {
                    if let Some(mut row) = row.filter(|r| predicate.matches(predicate.table, *r)) {
                        let (target, value, replace) = match self {
                            Self::Update { column, limit, .. } => {
                                row[*column] =
                                    row[*column].checked_add(1).filter(|v| *v <= *limit)?;
                                ((predicate.table, key), Some(row), true)
                            }
                            Self::Delete(_) => ((predicate.table, key), None, true),
                            Self::InsertSelect {
                                target, key_offset, ..
                            } => {
                                let destination = (key + key_offset) % 3;
                                if rows[*target][destination].is_some() {
                                    return None;
                                }
                                ((*target, destination), Some(row), false)
                            }
                            _ => return None,
                        };
                        plan.actions.push(Mutation {
                            key: target,
                            value,
                            replace,
                        });
                    }
                }
            }
            Self::Aggregate { table, target, cap } => {
                // SUM over a non-key predicate; all possible values are represented.
                plan.predicates.push(Predicate {
                    table: *table,
                    column: 0,
                    equals: 4,
                });
                let sum: u8 = rows[*table].iter().flatten().map(|r| r[0]).sum();
                if sum + 1 > *cap || rows[target.0][target.1].is_some() {
                    return None;
                }
                plan.actions.push(Mutation {
                    key: *target,
                    value: Some([1, 0]),
                    replace: false,
                });
            }
            Self::Absent { lookup, target } => {
                plan.looked_up.push(*lookup);
                if rows[lookup.0][lookup.1].is_none() {
                    plan.actions.push(Mutation {
                        key: *target,
                        value: Some([1, 1]),
                        replace: true,
                    });
                }
            }
            Self::Blind(ops) => plan.actions.extend(ops.iter().map(|(key, value)| Mutation {
                key: *key,
                value: *value,
                replace: true,
            })),
            Self::Append(ops) if ops.iter().any(|(key, _)| rows[key.0][key.1].is_some()) => {
                return None;
            }
            Self::Append(ops) => plan.actions.extend(ops.iter().map(|(key, value)| Mutation {
                key: *key,
                value: Some(*value),
                replace: false,
            })),
            Self::Insert { target, value } => {
                if rows[target.0][target.1].is_some() {
                    return None;
                }
                plan.actions.push(Mutation {
                    key: *target,
                    value: Some(*value),
                    replace: false,
                });
            }
        }
        Some(plan)
    }
}

// The oracle interprets the program directly, without consuming planned actions.
fn execute(program: &Program, rows: &Rows) -> Option<Rows> {
    let mut out = *rows;
    match program {
        Program::Transaction(statements) => {
            for statement in statements {
                out = execute(statement, &out)?;
            }
        }
        Program::Update {
            predicate,
            column,
            limit,
        } => {
            for k in 0..3 {
                if let Some(mut r) = rows[predicate.table][k]
                    && r[predicate.column] == predicate.equals
                {
                    r[*column] = r[*column].checked_add(1).filter(|v| *v <= *limit)?;
                    out[predicate.table][k] = Some(r);
                }
            }
        }
        Program::Delete(p) => {
            for k in 0..3 {
                if rows[p.table][k].is_some_and(|r| r[p.column] == p.equals) {
                    out[p.table][k] = None;
                }
            }
        }
        Program::InsertSelect {
            predicate: p,
            target,
            key_offset,
        } => {
            for (key, row) in rows[p.table].iter().enumerate() {
                if let Some(r) = row
                    && r[p.column] == p.equals
                {
                    let destination = (key + key_offset) % 3;
                    if out[*target][destination].is_some() {
                        return None;
                    }
                    out[*target][destination] = Some(*r);
                }
            }
        }
        Program::Aggregate { table, target, cap } => {
            let mut total = 1;
            for row in rows[*table] {
                total += row.map_or(0, |value| value[0]);
            }
            if total > *cap || rows[target.0][target.1].is_some() {
                return None;
            }
            out[target.0][target.1] = Some([1, 0]);
        }
        Program::Absent { lookup, target } => {
            if rows[lookup.0][lookup.1].is_none() {
                out[target.0][target.1] = Some([1, 1]);
            }
        }
        Program::Blind(ops) => {
            for &(key, value) in ops {
                out[key.0][key.1] = value;
            }
        }
        Program::Append(ops) => {
            for &(key, value) in ops {
                if out[key.0][key.1].is_some() {
                    return None;
                }
                out[key.0][key.1] = Some(value);
            }
        }
        Program::Insert { target, value } => {
            if out[target.0][target.1].is_some() {
                return None;
            }
            out[target.0][target.1] = Some(*value);
        }
    }
    Some(out)
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Row {
    key: Key,
    value: Image,
    seq: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Stamped {
    mutation: Mutation,
    delete: usize,
    insert: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TransactionState {
    overlay: Rows,
    next_statement: usize,
    provisional_high: [usize; 2],
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Writer {
    program: Program,
    rule: u8,
    phase: u8,
    snapshot: usize,
    base: usize,
    plan: Option<Plan>,
    stamped: Vec<Stamped>,
    attempts: usize,
    examined: usize,
    evaluated: usize,
    statements: usize,
    chosen: bool,
    transaction: Option<TransactionState>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Record {
    writer: usize,
    statement: usize,
    program: Program,
    snapshot: usize,
    plan: Plan,
    footprint: Vec<Key>,
    stamped: Vec<Stamped>,
    examined: usize,
    evaluated: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct State {
    writers: Vec<Writer>,
    rows: Vec<Row>,
    deletes: Vec<(Key, usize)>,
    high: [usize; 2],
    versions: Vec<Rows>,
    log: Vec<Record>,
    replanned: bool,
    retried: bool,
    transaction_interleaved: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    Start(usize),
    TransactionStatement(usize),
    Choose(usize, usize),
    Next(usize),
    Cas(usize),
    Retry(usize),
    Crash(usize),
    Expire(usize),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CommitCost {
    writer: usize,
    commit: usize,
    snapshot: usize,
    winners_examined: usize,
    rows_evaluated: usize,
}
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
    counts: Arc<[AtomicUsize; 2]>,
    profiles: Arc<Mutex<BTreeSet<CommitCost>>>,
    choices: Arc<Vec<Vec<Program>>>,
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
            counts: Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]),
            profiles: Arc::default(),
            choices: Arc::new(
                (0..3)
                    .map(|writer| {
                        if mode.contains("transaction") {
                            Self::transaction_choices(writer)
                        } else {
                            Self::choices(writer)
                        }
                    })
                    .collect(),
            ),
        }
    }
    pub(crate) fn profiles(&self) -> Vec<CommitCost> {
        self.profiles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
            .collect()
    }
    fn has(&self, name: &str) -> bool {
        self.mode.contains(name)
    }
    pub(crate) fn counts(&self) -> [usize; 2] {
        std::array::from_fn(|i| self.counts[i].load(Ordering::Relaxed))
    }
    fn initial() -> Rows {
        [[Some([0, 1]), Some([1, 0]), None], [None; 3]]
    }
    fn transaction_choices(writer: usize) -> Vec<Program> {
        if writer == 2 {
            return Self::choices(writer);
        }
        let choices = Self::choices(writer);
        choices
            .iter()
            .flat_map(|first| {
                choices
                    .iter()
                    .map(|second| Program::Transaction(vec![first.clone(), second.clone()]))
            })
            .collect()
    }
    fn selected_choices(&self, writer: usize) -> &[Program] {
        &self.choices[writer]
    }
    fn choices(writer: usize) -> Vec<Program> {
        let mut choices = Vec::new();
        for value in 0..3 {
            if writer == 2 {
                for key in 0..3 {
                    choices.push(Program::Blind(vec![
                        ((0, key), Some([value, value])),
                        ((1, key), Some([value, value])),
                    ]));
                    if value == 0 {
                        choices.push(Program::Blind(vec![((0, key), None), ((1, key), None)]));
                    }
                }
                choices.push(Program::Append(vec![
                    ((0, 2), [value, value]),
                    ((1, 2), [value, value]),
                ]));
            } else {
                for column in 0..2 {
                    let predicate = Predicate {
                        table: 0,
                        column,
                        equals: value,
                    };
                    choices.push(Program::Update {
                        predicate: predicate.clone(),
                        column: 1 - column,
                        limit: 2,
                    });
                    choices.push(Program::Delete(predicate.clone()));
                    for target in 0..2 {
                        choices.push(Program::InsertSelect {
                            predicate: predicate.clone(),
                            target,
                            key_offset: usize::from(target == 0),
                        });
                    }
                }
                choices.push(Program::Aggregate {
                    table: 0,
                    target: (writer, 2),
                    cap: value + 1,
                });
            }
        }
        if writer != 2 {
            choices.push(Program::Absent {
                lookup: (0, 2),
                target: (writer, 2),
            });
        }
        choices
    }
    fn programs(&self) -> Vec<Program> {
        if self.has("transaction") && !self.has("widened") {
            let first = if self.has("overlay-read-keys") {
                Program::Delete(Predicate {
                    table: 0,
                    column: 0,
                    equals: 0,
                })
            } else {
                Program::Update {
                    predicate: Predicate {
                        table: 0,
                        column: 0,
                        equals: 1,
                    },
                    column: 1,
                    limit: 2,
                }
            };
            return vec![
                Program::Transaction(vec![
                    first,
                    Program::Absent {
                        lookup: (0, 0),
                        target: (1, 2),
                    },
                ]),
                Program::Blind(if self.has("overlay-read-keys") {
                    vec![((0, 0), Some([0, 0]))]
                } else {
                    vec![((0, 1), Some([1, 1]))]
                }),
            ];
        }
        let mut programs = if self.has("widened") {
            (0..3)
                .map(|i| self.selected_choices(i)[0].clone())
                .collect()
        } else {
            self.scenario_programs()
        };
        if self.has("single-writer") {
            programs.truncate(1);
        }
        if self.has("full") {
            if programs.iter().filter(|p| p.dependent()).count() < 2 {
                programs.push(Program::Absent {
                    lookup: (1, 2),
                    target: (1, 2),
                });
            }
            if programs.iter().all(Program::dependent) {
                programs.push(Program::Blind(vec![
                    ((0, 2), Some([0, 0])),
                    ((1, 2), Some([0, 0])),
                ]));
            }
            for program in &mut programs {
                if let Program::Blind(ops) = program
                    && !ops.iter().any(|(key, _)| key.0 == 1)
                {
                    ops.push(((1, 0), Some([0, 0])));
                }
            }
        }
        programs
    }
    fn scenario_programs(&self) -> Vec<Program> {
        let p = |column, equals| Predicate {
            table: 0,
            column,
            equals,
        };
        let update = Program::Update {
            predicate: p(0, 1),
            column: 1,
            limit: 3,
        };
        let other = Program::Update {
            predicate: p(1, 1),
            column: 0,
            limit: 3,
        };
        let blind = Program::Blind(vec![((0, 0), Some([1, 0])), ((1, 2), Some([0, 0]))]);
        if self.has("append") {
            return vec![
                update,
                other,
                Program::Append(vec![((0, 2), [0, 0]), ((1, 2), [0, 0])]),
            ];
        }
        if self.has("same-key") {
            return vec![
                Program::Insert {
                    target: (1, 2),
                    value: [1, 0],
                },
                Program::Insert {
                    target: (1, 2),
                    value: [0, 1],
                },
            ];
        }
        if self.has("absent") {
            return vec![
                Program::Absent {
                    lookup: (0, 2),
                    target: (1, 2),
                },
                Program::Blind(vec![((0, 2), Some([0, 0]))]),
            ];
        }
        if self.has("delete-keys") {
            return vec![update, Program::Delete(p(0, 1))];
        }
        if self.has("overlap-blind") {
            return vec![
                update,
                Program::Blind(vec![((0, 1), Some([1, 1])), ((1, 2), Some([0, 0]))]),
            ];
        }
        if self.has("cross-table") {
            return vec![
                Program::InsertSelect {
                    predicate: p(0, 1),
                    target: 1,
                    key_offset: 0,
                },
                if self.has("insert-guard") {
                    Program::InsertSelect {
                        predicate: Predicate {
                            table: 1,
                            column: 0,
                            equals: 1,
                        },
                        target: 0,
                        key_offset: 0,
                    }
                } else {
                    Program::Absent {
                        lookup: (1, 1),
                        target: (0, 0),
                    }
                },
                Program::Blind(vec![((0, 2), Some([0, 0])), ((1, 2), Some([0, 0]))]),
            ];
        }
        if self.has("select") {
            return vec![
                Program::InsertSelect {
                    predicate: p(0, 1),
                    target: usize::from(!self.has("same-table")),
                    key_offset: usize::from(self.has("same-table")),
                },
                Program::Blind(vec![((0, 1), None)]),
            ];
        }
        if self.has("gate") {
            return vec![
                Program::Aggregate {
                    table: 0,
                    target: (0, 2),
                    cap: 2,
                },
                Program::Aggregate {
                    table: 0,
                    target: (1, 2),
                    cap: 2,
                },
                blind,
            ];
        }
        if self.has("history") {
            return vec![update, blind, Program::Blind(vec![((1, 1), Some([0, 0]))])];
        }
        if self.has("minimal") {
            return vec![update, if self.has("blind") { blind } else { other }];
        }
        if self.has("delete") {
            return vec![Program::Delete(p(0, 1)), other, blind];
        }
        vec![update, other, blind]
    }
    fn token_tables(&self, w: &Writer) -> Vec<usize> {
        w.program.tables(self.has("write-table-token"))
    }
    fn admitted(&self, s: &State, i: usize) -> bool {
        let w = &s.writers[i];
        !w.program.dependent()
            || w.rule != 1
            || self.has("no-exclusive-token")
            || !s.writers.iter().enumerate().any(|(j, other)| {
                i != j
                    && other.rule == 1
                    && other.program.dependent()
                    && matches!(other.phase, 1 | 4 | 5 | 6)
                    && self
                        .token_tables(w)
                        .iter()
                        .any(|t| self.token_tables(other).contains(t))
            })
    }
    fn footprint(&self, program: &Program, plan: &Plan) -> Vec<Key> {
        plan.actions
            .iter()
            .filter(|m| {
                !(self.has("no-blind-key-footprint") && !program.dependent()
                    || self.has("no-delete-keys") && m.value.is_none()
                    || self.has("no-inserted-keys") && !m.replace)
            })
            .map(|m| m.key)
            .collect()
    }
    fn stamp(plan: &Plan, high: [usize; 2]) -> Vec<Stamped> {
        let mut seq = high;
        plan.actions
            .iter()
            .map(|m| {
                seq[m.key.0] += 1;
                let delete = seq[m.key.0];
                if m.value.is_some() && m.replace {
                    seq[m.key.0] += 1;
                }
                Stamped {
                    mutation: m.clone(),
                    delete,
                    insert: seq[m.key.0],
                }
            })
            .collect()
    }
    fn observe(s: &State) -> Rows {
        std::array::from_fn(|t| {
            std::array::from_fn(|k| {
                s.rows
                    .iter()
                    .filter(|r| {
                        r.key == (t, k)
                            && !s
                                .deletes
                                .iter()
                                .any(|&(key, seq)| key == r.key && seq >= r.seq)
                    })
                    .max_by_key(|r| r.seq)
                    .map(|r| r.value)
            })
        })
    }
    fn conflicts(&self, s: &State, w: &Writer) -> (bool, usize, usize) {
        let Some(plan) = &w.plan else {
            return (false, 0, 0);
        };
        let mut validation = plan.clone();
        if let Program::Transaction(statements) = &w.program {
            let mut overlay = s.versions[w.snapshot];
            let mut written_so_far = Vec::new();
            let mut combined = Plan {
                actions: vec![],
                looked_up: vec![],
                predicates: vec![],
            };
            for statement in statements {
                let Some(mut part) = statement.plan(&overlay) else {
                    return (true, 0, 0);
                };
                if self.has("no-overlay-read-keys") {
                    part.looked_up.retain(|key| !written_so_far.contains(key));
                }
                for mutation in &part.actions {
                    overlay[mutation.key.0][mutation.key.1] = mutation.value;
                    written_so_far.push(mutation.key);
                }
                if self.has("no-union-footprint") {
                    combined = part;
                } else {
                    combined.actions.extend(part.actions);
                    combined.looked_up.extend(part.looked_up);
                    combined.predicates.extend(part.predicates);
                }
            }
            validation = combined;
        }
        let plan = &validation;
        let written = self.footprint(&w.program, plan);
        let mut conflict = false;
        let mut examined = 0;
        let mut evaluated = 0;
        let start = if self.has("previous-winner-only") {
            s.log.len().saturating_sub(1).max(w.snapshot)
        } else {
            w.snapshot
        };
        for winner in &s.log[start..] {
            examined += 1;
            for key in &winner.footprint {
                conflict |= written.contains(key)
                    || plan.looked_up.iter().any(|lookup| {
                        lookup == key
                            && !(self.has("no-absent-lookup")
                                && s.versions[w.snapshot][lookup.0][lookup.1].is_none())
                    });
                if w.rule == 2 || self.has("predicate-validation") {
                    let before = s.versions[w.snapshot][key.0][key.1];
                    let after = winner
                        .plan
                        .actions
                        .iter()
                        .filter(|m| m.key == *key)
                        .filter_map(|m| m.value);
                    for predicate in &plan.predicates {
                        if predicate.table != key.0 {
                            continue;
                        }
                        if !self.has("after-image-only")
                            && let Some(row) = before
                        {
                            evaluated += 1;
                            conflict |= predicate.matches(key.0, row);
                        }
                        if !self.has("before-image-only") {
                            for row in after.clone() {
                                evaluated += 1;
                                conflict |= predicate.matches(key.0, row);
                            }
                        }
                    }
                }
            }
            if self.has("table-validation") && w.program.dependent() {
                conflict |= winner
                    .plan
                    .actions
                    .iter()
                    .any(|m| w.program.tables(false).contains(&m.key.0));
            }
        }
        (conflict, examined, evaluated)
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let rows = Self::initial();
        vec![State {
            writers: self
                .programs()
                .into_iter()
                .enumerate()
                .map(|(i, program)| Writer {
                    program,
                    rule: if self.has("rule1") || self.has("mixed") && i == 0 {
                        1
                    } else {
                        2
                    },
                    phase: 0,
                    snapshot: 0,
                    base: 0,
                    plan: None,
                    stamped: vec![],
                    attempts: 0,
                    examined: 0,
                    evaluated: 0,
                    statements: 0,
                    chosen: !self.has("widened"),
                    transaction: None,
                })
                .collect(),
            rows: rows
                .iter()
                .enumerate()
                .flat_map(|(t, table)| {
                    table.iter().enumerate().filter_map(move |(k, r)| {
                        r.map(|value| Row {
                            key: (t, k),
                            value,
                            seq: k + 1,
                        })
                    })
                })
                .collect(),
            deletes: vec![],
            high: [2, 0],
            versions: vec![rows],
            log: vec![],
            replanned: false,
            retried: false,
            transaction_interleaved: false,
        }]
    }
    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        for (i, w) in s.writers.iter().enumerate() {
            match w.phase {
                0 if self.has("widened") && !w.chosen => {
                    if w.statements < if self.has("transaction") { 1 } else { 2 }
                        && s.writers.iter().map(|w| w.statements).sum::<usize>() < self.bound
                    {
                        for (choice, program) in self.selected_choices(i).iter().enumerate() {
                            let mut candidate = s.clone();
                            candidate.writers[i].program = program.clone();
                            if self.admitted(&candidate, i) {
                                actions.push(Action::Choose(i, choice));
                            }
                        }
                    }
                }
                0 if self.admitted(s, i) => actions.push(Action::Start(i)),
                2 | 3 if self.has("widened") && !self.has("transaction") && w.statements < 2 => {
                    actions.push(Action::Next(i));
                }
                6 => actions.push(Action::TransactionStatement(i)),
                1 => {
                    actions.push(if w.base == s.log.len() {
                        Action::Cas(i)
                    } else {
                        Action::Retry(i)
                    });
                    if self.has("crash") && w.rule == 1 {
                        actions.push(Action::Crash(i));
                    }
                }
                4 if !self.has("no-token-expiry") => actions.push(Action::Expire(i)),
                _ => (),
            }
        }
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Choose(index, choice) => {
                next.writers[index].program = self.selected_choices(index).get(choice)?.clone();
                next.writers[index].chosen = true;
                next.writers[index].statements += 1;
                return self.next_state(&next, Action::Start(index));
            }
            Action::Next(index) => {
                let w = &mut next.writers[index];
                w.phase = 0;
                w.chosen = false;
                w.plan = None;
                w.stamped.clear();
                w.attempts = 0;
                w.examined = 0;
                w.evaluated = 0;
            }
            Action::Start(index) => {
                let writer = &mut next.writers[index];
                writer.snapshot = state.log.len();
                writer.base = state.log.len();
                writer.attempts += 1;
                if matches!(writer.program, Program::Transaction(_)) {
                    writer.transaction = Some(TransactionState {
                        overlay: *state.versions.last()?,
                        next_statement: 0,
                        provisional_high: state.high,
                    });
                    writer.plan = Some(Plan {
                        actions: vec![],
                        looked_up: vec![],
                        predicates: vec![],
                    });
                    writer.phase = 6;
                } else {
                    writer.plan = writer.program.plan(state.versions.last()?);
                    writer.phase = if writer.plan.is_some() { 1 } else { 3 };
                }
                if let Some(plan) = &writer.plan
                    && !self.has("stamp-at-commit")
                {
                    writer.stamped = Self::stamp(plan, state.high);
                }
            }
            Action::TransactionStatement(index) => {
                let writer = &mut next.writers[index];
                let Program::Transaction(statements) = &writer.program else {
                    return None;
                };
                let transaction = writer.transaction.as_mut()?;
                let statement = statements.get(transaction.next_statement)?;
                if let Some(part) = statement.plan(&transaction.overlay) {
                    for mutation in &part.actions {
                        transaction.overlay[mutation.key.0][mutation.key.1] = mutation.value;
                    }
                    let plan = writer.plan.as_mut()?;
                    plan.actions.extend(part.actions);
                    plan.looked_up.extend(part.looked_up);
                    plan.predicates.extend(part.predicates);
                    transaction.next_statement += 1;
                    if transaction.next_statement == statements.len() {
                        writer.phase = 1;
                        if !self.has("stamp-at-commit") {
                            writer.stamped = Self::stamp(plan, transaction.provisional_high);
                        }
                    }
                } else {
                    writer.phase = 3;
                    writer.plan = None;
                    writer.stamped.clear();
                    writer.transaction = None;
                }
            }
            Action::Cas(index) => {
                next.transaction_interleaved |=
                    state.writers.iter().enumerate().any(|(other, writer)| {
                        other != index
                            && writer.phase == 6
                            && writer
                                .transaction
                                .as_ref()
                                .is_some_and(|t| t.next_statement == 1)
                    });
                let writer = &mut next.writers[index];
                if writer.base != state.log.len() {
                    return None;
                }
                let plan = writer.plan.clone()?;
                if self.has("stamp-at-commit") {
                    writer.stamped = Self::stamp(&plan, state.high);
                }
                for stamped in &writer.stamped {
                    let mutation = &stamped.mutation;
                    if mutation.replace || mutation.value.is_none() {
                        next.deletes.push((mutation.key, stamped.delete));
                    }
                    if let Some(value) = mutation.value {
                        next.rows.push(Row {
                            key: mutation.key,
                            value,
                            seq: stamped.insert,
                        });
                    }
                    next.high[mutation.key.0] = next.high[mutation.key.0].max(stamped.insert);
                }
                next.log.push(Record {
                    writer: index,
                    statement: writer.statements,
                    program: writer.program.clone(),
                    snapshot: writer.snapshot,
                    plan: plan.clone(),
                    footprint: self.footprint(&writer.program, &plan),
                    stamped: writer.stamped.clone(),
                    examined: writer.examined,
                    evaluated: writer.evaluated,
                });
                if writer.rule == 2 && writer.program.dependent() {
                    self.counts[0].fetch_max(writer.examined, Ordering::Relaxed);
                    self.counts[1].fetch_max(writer.evaluated, Ordering::Relaxed);
                    self.profiles
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(CommitCost {
                            writer: index,
                            commit: next.log.len(),
                            snapshot: writer.snapshot,
                            winners_examined: writer.examined,
                            rows_evaluated: writer.evaluated,
                        });
                }
                writer.phase = 2;
                next.versions.push(Self::observe(&next));
            }
            Action::Retry(index) => {
                let writer = &state.writers[index];
                let (conflict, examined, evaluated) = self.conflicts(state, writer);
                let out = &mut next.writers[index];
                out.examined += examined;
                out.evaluated += evaluated;
                if conflict && !self.has("no-conflict-replan") {
                    // A failed attempt releases its token before admission of another attempt.
                    out.phase = if out.attempts >= if self.has("widened") { 3 } else { self.bound }
                    {
                        3
                    } else {
                        0
                    };
                    out.plan = None;
                    out.transaction = None;
                    out.stamped.clear();
                    next.replanned = true;
                } else {
                    out.base = state.log.len();
                    for table in 0..2 {
                        if let Some(lo) = out
                            .stamped
                            .iter()
                            .filter(|o| o.mutation.key.0 == table)
                            .map(|o| o.delete)
                            .min()
                        {
                            let delta = (state.high[table] + 1).saturating_sub(lo);
                            for o in &mut out.stamped {
                                if o.mutation.key.0 == table {
                                    o.delete += delta;
                                    o.insert += delta;
                                }
                            }
                        }
                    }
                    next.retried = true;
                }
            }
            Action::Crash(index) => next.writers[index].phase = 4,
            Action::Expire(index) => next.writers[index].phase = 3,
        }
        Some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("commit-order program serializability", |_, s: &State| {
                let mut oracle = Self::initial();
                for (i, r) in s.log.iter().enumerate() {
                    let Some(after) = execute(&r.program, &oracle) else {
                        return false;
                    };
                    oracle = after;
                    if oracle != s.versions[i + 1] {
                        return false;
                    }
                }
                true
            }),
            Property::always(
                "accepted actions equal program at commit",
                |_, s: &State| {
                    s.log.iter().enumerate().all(|(i, r)| {
                        let mut actions = s.versions[i];
                        for m in &r.plan.actions {
                            actions[m.key.0][m.key.1] = m.value;
                        }
                        execute(&r.program, &s.versions[i]) == Some(actions)
                    })
                },
            ),
            Property::always("accepted actions equal snapshot program", |_, s: &State| {
                s.log
                    .iter()
                    .all(|r| r.program.plan(&s.versions[r.snapshot]).as_ref() == Some(&r.plan))
            }),
            Property::always("I9 keyed actions follow commit order", |_, s: &State| {
                let mut rows = Self::initial();
                for (i, r) in s.log.iter().enumerate() {
                    for m in &r.plan.actions {
                        rows[m.key.0][m.key.1] = m.value;
                    }
                    if rows != s.versions[i + 1] {
                        return false;
                    }
                }
                true
            }),
            Property::always("at most one live row per table key", |_, s: &State| {
                (0..2).all(|t| {
                    (0..3).all(|k| {
                        s.rows
                            .iter()
                            .filter(|r| {
                                r.key == (t, k)
                                    && !s
                                        .deletes
                                        .iter()
                                        .any(|&(key, seq)| key == r.key && seq >= r.seq)
                            })
                            .count()
                            <= 1
                    })
                })
            }),
            Property::always("per-table publication sequence order", |_, s: &State| {
                let mut high = [2, 0];
                for r in &s.log {
                    if r.stamped.iter().any(|o| o.delete <= high[o.mutation.key.0]) {
                        return false;
                    }
                    for o in &r.stamped {
                        high[o.mutation.key.0] = high[o.mutation.key.0].max(o.insert);
                    }
                }
                true
            }),
            Property::sometimes(
                "two read-dependent writers and blind commit",
                |_, s: &State| {
                    s.log.len() == 3 && s.log.iter().filter(|r| r.program.dependent()).count() == 2
                },
            ),
            Property::sometimes("all three writers commit", |_, s: &State| {
                (0..3).all(|writer| {
                    s.log
                        .iter()
                        .any(|r| r.writer == writer && !r.plan.actions.is_empty())
                })
            }),
            Property::sometimes("writer commits two statements", |_, s: &State| {
                (0..s.writers.len()).any(|writer| {
                    s.log
                        .iter()
                        .filter(|r| r.writer == writer && !r.plan.actions.is_empty())
                        .count()
                        == 2
                })
            }),
            Property::sometimes(
                "winner commits between transaction statements",
                |_, s: &State| s.transaction_interleaved,
            ),
            Property::sometimes("transaction reads its own writes", |_, s: &State| {
                s.log.iter().any(|r| {
                    let Program::Transaction(statements) = &r.program else {
                        return false;
                    };
                    let before = s.versions[r.snapshot];
                    let Some(first) = statements.first().and_then(|p| execute(p, &before)) else {
                        return false;
                    };
                    statements
                        .get(1)
                        .is_some_and(|p| p.plan(&before) != p.plan(&first))
                })
            }),
            Property::sometimes(
                "transaction commits two nonempty statements",
                |_, s: &State| {
                    s.log.iter().any(|r| {
                        let Program::Transaction(statements) = &r.program else {
                            return false;
                        };
                        let mut overlay = s.versions[r.snapshot];
                        statements.iter().all(|p| {
                            let Some(plan) = p.plan(&overlay) else {
                                return false;
                            };
                            for mutation in &plan.actions {
                                overlay[mutation.key.0][mutation.key.1] = mutation.value;
                            }
                            !plan.actions.is_empty()
                        })
                    })
                },
            ),
            Property::sometimes("conflict replans statement", |_, s: &State| s.replanned),
            Property::sometimes("independent loser retries", |_, s: &State| s.retried),
        ]
    }
}

/// Finite fair servicing of every admitted token, including crashed holders.
#[derive(Clone, Debug)]
pub(crate) struct TokenProgress {
    protocol: Protocol,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TokenState {
    inner: State,
    history: Vec<Action>,
}
impl TokenProgress {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            protocol: Protocol::new(mode, bound),
        }
    }
}
impl Model for TokenProgress {
    type State = TokenState;
    type Action = Action;
    fn init_states(&self) -> Vec<TokenState> {
        self.protocol
            .init_states()
            .into_iter()
            .map(|inner| TokenState {
                inner,
                history: vec![],
            })
            .collect()
    }
    fn actions(&self, s: &TokenState, a: &mut Vec<Action>) {
        self.protocol.actions(&s.inner, a);
    }
    fn next_state(&self, s: &TokenState, action: Action) -> Option<TokenState> {
        let mut history = s.history.clone();
        history.push(action.clone());
        Some(TokenState {
            inner: self.protocol.next_state(&s.inner, action)?,
            history,
        })
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::eventually(
                "exclusive tokens eventually released",
                |_, s: &TokenState| s.inner.writers.iter().all(|w| matches!(w.phase, 2 | 3)),
            ),
            Property::sometimes("crashed token expires", |_, s: &TokenState| {
                s.history.iter().any(|a| matches!(a, Action::Expire(_)))
            }),
            Property::sometimes("conflict releases token", |_, s: &TokenState| {
                s.inner.replanned
            }),
            Property::sometimes("commit releases token", |_, s: &TokenState| {
                !s.inner.log.is_empty()
            }),
        ]
    }
}

/// A bounded fair round: ingestion wins once or twice, then validation and a
/// successful rebased CAS are serviced before the next ingestion round.
#[derive(Clone, Debug)]
pub(crate) struct PredicateProgress {
    whole_table: bool,
    bound: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProgressState {
    rows: Rows,
    snapshot: Rows,
    phase: u8,
    attempt: usize,
    ingested: usize,
    committed: bool,
    exhausted: bool,
    history: Vec<ProgressAction>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ProgressAction {
    Ingest,
    Validate,
    Publish,
}
impl PredicateProgress {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            whole_table: mode.contains("whole-table"),
            bound,
        }
    }
}
impl Model for PredicateProgress {
    type State = ProgressState;
    type Action = ProgressAction;
    fn init_states(&self) -> Vec<ProgressState> {
        vec![ProgressState {
            rows: Protocol::initial(),
            snapshot: Protocol::initial(),
            phase: 0,
            attempt: 1,
            ingested: 0,
            committed: false,
            exhausted: false,
            history: vec![],
        }]
    }
    fn actions(&self, s: &ProgressState, a: &mut Vec<ProgressAction>) {
        if s.committed || s.exhausted {
            return;
        }
        if s.phase == 0 {
            if s.ingested < 2 {
                a.push(ProgressAction::Ingest);
            }
            if s.ingested > 0 {
                a.push(ProgressAction::Validate);
            }
        } else {
            a.push(ProgressAction::Publish);
        }
    }
    fn next_state(&self, s: &ProgressState, action: ProgressAction) -> Option<ProgressState> {
        let mut n = s.clone();
        n.history.push(action.clone());
        match action {
            ProgressAction::Ingest => {
                n.rows[0][0] = Some([0, u8::try_from(s.ingested).ok()?]);
                n.ingested += 1;
            }
            ProgressAction::Validate => {
                let p = Program::Update {
                    predicate: Predicate {
                        table: 0,
                        column: 0,
                        equals: 1,
                    },
                    column: 1,
                    limit: 3,
                };
                let before_after_match = [s.snapshot[0][0], s.rows[0][0]]
                    .into_iter()
                    .flatten()
                    .any(|r| r[0] == 1);
                if self.whole_table || before_after_match {
                    n.exhausted = s.attempt == self.bound;
                    n.attempt += usize::from(!n.exhausted);
                    n.snapshot = s.rows;
                    n.ingested = 0;
                } else {
                    // The independent action computation is retained; the new CAS base is fresh.
                    p.plan(&s.snapshot)?;
                    n.phase = 1;
                }
            }
            ProgressAction::Publish => {
                let p = Program::Update {
                    predicate: Predicate {
                        table: 0,
                        column: 0,
                        equals: 1,
                    },
                    column: 1,
                    limit: 3,
                };
                let actions = p.plan(&s.snapshot)?;
                for m in actions.actions {
                    n.rows[m.key.0][m.key.1] = m.value;
                }
                n.committed = true;
            }
        }
        Some(n)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::eventually(
                "unrelated ingestion statement commits within attempts",
                |_, s: &ProgressState| s.committed,
            ),
            Property::always("publication agrees with program", |_, s: &ProgressState| {
                !s.committed || {
                    let p = Program::Update {
                        predicate: Predicate {
                            table: 0,
                            column: 0,
                            equals: 1,
                        },
                        column: 1,
                        limit: 3,
                    };
                    let mut prior = s.rows;
                    prior[0][1] = Some([1, 0]);
                    execute(&p, &prior) == Some(s.rows)
                }
            }),
            Property::always(
                "whole-table terminal schedules exhaust attempts",
                |model, state: &ProgressState| {
                    !model.whole_table
                        || (!state.committed && (!state.exhausted || state.attempt == model.bound))
                },
            ),
            Property::sometimes("all attempts exhausted", |_, s: &ProgressState| s.exhausted),
        ]
    }
}
