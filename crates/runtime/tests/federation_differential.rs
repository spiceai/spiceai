/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Helpers shared by the federation differential tests, which run one query
//! corpus against a source database (the oracle) and against Spice's federated
//! and accelerated datasets over the same rows: rendering answers as text,
//! comparing them as a multiset or in order, and reading an `EXPLAIN` plan to
//! check which sources a Spice side actually read.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use runtime::Runtime;

use crate::utils::run_query;

/// One query, with `{items}` and `{regions}` standing for the tables.
pub(crate) struct Case {
    pub(crate) sql: &'static str,
    /// The oracle's spelling of `sql`, where the source database defines an
    /// operator in `sql` differently from Spice. `None` sends `sql` unchanged.
    pub(crate) oracle: Option<&'static str>,
    /// Whether the rows' order is part of the answer: the query's top-level
    /// `ORDER BY` covers a unique key, so the order is fixed.
    pub(crate) ordered: bool,
    /// Whether no row is the right answer. Every other case must return rows,
    /// so two empty answers cannot pass for agreement.
    pub(crate) empty: bool,
    /// Text of an expression Spice must evaluate itself rather than send to
    /// the source, because the source would give it another meaning. `None`
    /// means federation must send the whole query.
    pub(crate) kept_local: Option<&'static str>,
}

impl Case {
    /// `sql` with its placeholders bound to the tables `items` and `regions`.
    pub(crate) fn bind(sql: &str, items: &str, regions: &str) -> String {
        sql.replace("{items}", items).replace("{regions}", regions)
    }

    /// The SQL sent to the oracle, over its tables `items` and `regions`.
    pub(crate) fn oracle_sql(&self, items: &str, regions: &str) -> String {
        Self::bind(self.oracle.unwrap_or(self.sql), items, regions)
    }
}

pub(crate) const fn rows(sql: &'static str) -> Case {
    Case {
        sql,
        oracle: None,
        ordered: false,
        empty: false,
        kept_local: None,
    }
}

pub(crate) const fn ordered(sql: &'static str) -> Case {
    Case {
        ordered: true,
        ..rows(sql)
    }
}

pub(crate) const fn empty(sql: &'static str) -> Case {
    Case {
        empty: true,
        ..rows(sql)
    }
}

/// A case the oracle spells `oracle` for the same semantics.
pub(crate) const fn rewritten(sql: &'static str, oracle: &'static str) -> Case {
    Case {
        oracle: Some(oracle),
        ..rows(sql)
    }
}

/// A row as its cells, each rendered, or `None` for NULL.
pub(crate) type Row = Vec<Option<String>>;

/// One operator of a physical plan: its name, and its depth in the tree.
pub(crate) struct Operator<'a> {
    pub(crate) name: &'a str,
    depth: usize,
    /// The operator's whole line, with its details.
    pub(crate) line: &'a str,
}

/// The physical plan text in `plan`, an `EXPLAIN` answer.
pub(crate) fn physical_plan(plan: &[Row]) -> Result<String, String> {
    plan.iter()
        .find(|row| row.first().and_then(Option::as_deref) == Some("physical_plan"))
        .and_then(|row| row.get(1).cloned().flatten())
        .ok_or_else(|| format!("the plan has no physical plan: {plan:?}"))
}

/// The operators of `physical`, in plan order, each with its indentation as
/// its depth.
pub(crate) fn operators(physical: &str) -> Vec<Operator<'_>> {
    physical
        .lines()
        .filter_map(|line| {
            let name = line.split([':', ' ']).find(|token| !token.is_empty())?;
            Some(Operator {
                name,
                depth: line.len() - line.trim_start().len(),
                line,
            })
        })
        .collect()
}

/// Each operator of `operators` with no child, a source its rows come from,
/// with the names of the operators above it.
fn leaves<'o, 'a>(operators: &'o [Operator<'a>]) -> Vec<(&'o Operator<'a>, Vec<&'a str>)> {
    let mut above: Vec<&Operator<'a>> = Vec::new();
    let mut leaves = Vec::new();
    for (i, op) in operators.iter().enumerate() {
        while above.last().is_some_and(|parent| parent.depth >= op.depth) {
            above.pop();
        }
        if operators
            .get(i + 1)
            .is_none_or(|next| next.depth <= op.depth)
        {
            leaves.push((op, above.iter().map(|parent| parent.name).collect()));
        }
        above.push(op);
    }
    leaves
}

pub(crate) fn render(batches: &[RecordBatch]) -> Result<Vec<Row>, String> {
    let options = FormatOptions::default();
    let mut rendered = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        for row in 0..batch.num_rows() {
            rendered.push(
                batch
                    .columns()
                    .iter()
                    .zip(&formatters)
                    .map(|(column, formatter)| {
                        (!column.is_null(row)).then(|| formatter.value(row).to_string())
                    })
                    .collect(),
            );
        }
    }
    Ok(rendered)
}

pub(crate) async fn spice_rows(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<Row>, String> {
    let batches = run_query(rt, sql).await.map_err(|e| e.to_string())?;
    render(&batches)
}

pub(crate) fn agreement(
    subject: &[Row],
    oracle: &[Row],
    ordered: bool,
) -> Result<(), &'static str> {
    if ordered {
        return (subject == oracle)
            .then_some(())
            .ok_or("rows or their order differ");
    }
    let (mut subject, mut oracle) = (subject.to_vec(), oracle.to_vec());
    subject.sort();
    oracle.sort();
    (subject == oracle).then_some(()).ok_or("rows differ")
}

/// The operator that scans a Cayenne acceleration.
const CAYENNE_SCAN: &str = "CayenneAccelerationExec";

/// Sources that read no table: `DataFusion` plans one when it answers from the
/// table's statistics, such as `count(*) ... WHERE grp > 100` folded to `0`
/// over one placeholder row because Cayenne reports `grp` at most 6. The
/// constant then comes from Cayenne's statistics, so it is still Cayenne's
/// answer to get right.
const CONSTANT_SOURCES: [&str; 2] = ["PlaceholderRowExec", "EmptyExec"];

/// Checks that every source of the physical plan in `plan`, an `EXPLAIN`
/// answer, is one `accepts` admits, given the operators above it, or is one of
/// [`CONSTANT_SOURCES`]. A side with any other source answered from somewhere
/// other than `what`, and its agreement says nothing about it.
pub(crate) fn check_sources(
    plan: &[Row],
    accepts: impl Fn(&Operator, &[&str]) -> bool,
    what: &str,
) -> Result<(), String> {
    let physical = physical_plan(plan)?;
    let operators = operators(&physical);
    let other: Vec<&str> = leaves(&operators)
        .into_iter()
        .filter(|(leaf, above)| !accepts(leaf, above) && !CONSTANT_SOURCES.contains(&leaf.name))
        .map(|(leaf, _)| leaf.name)
        .collect();
    if other.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the plan reads {other:?}, not only {what}:\n{physical}"
        ))
    }
}

/// Checks that every source of the physical plan in `plan` is read through a
/// [`CAYENNE_SCAN`], so a Cayenne side answers from what Cayenne stored.
pub(crate) fn check_cayenne_sources(plan: &[Row]) -> Result<(), String> {
    check_sources(
        plan,
        |leaf, above| leaf.name == CAYENNE_SCAN || above.contains(&CAYENNE_SCAN),
        "the Cayenne acceleration (CayenneAccelerationExec)",
    )
}

/// The operators a federated plan may run in Spice: wrappers that pass rows
/// through. Both tables of a corpus come from one database, so federation
/// sends the whole query there. Any other operator means Spice evaluated part
/// of the query itself, over a wider scan: a filter, an aggregate, a join, a
/// sort, a window, or a computed column. The answer can still match, but then
/// it compares `DataFusion` with the source, which the accelerated sides
/// already do, and not the translation the federated side exists to check.
const PASS_THROUGH: [&str; 3] = [
    "SchemaCastScanExec",
    "CooperativeExec",
    "BytesProcessedExec",
];

/// Operators `DataFusion` adds above an operator Spice runs itself, which
/// move rows between partitions and compute nothing.
const PLUMBING: [&str; 2] = ["RepartitionExec", "CoalesceBatchesExec"];

/// The SQL a remote scan's plan line sends to the source.
fn sent_sql(line: &str) -> &str {
    line.find("sql=")
        .map_or("", |at| &line[at + "sql=".len()..])
}

/// Checks that the physical plan in `plan`, an `EXPLAIN` answer, sends the
/// query to `source` through one of `remote_scans` and runs nothing in Spice
/// but [`PASS_THROUGH`] operators, except `kept_local`: an expression Spice
/// must evaluate itself, which then must appear in an operator Spice runs
/// (with only [`PLUMBING`] around it) and in none of the SQL sent to `source`.
pub(crate) fn check_federated(
    plan: &[Row],
    remote_scans: &[&str],
    source: &str,
    kept_local: Option<&str>,
) -> Result<(), String> {
    let physical = physical_plan(plan)?;
    let operators = operators(&physical);
    let remote: Vec<&Operator> = operators
        .iter()
        .filter(|op| remote_scans.contains(&op.name))
        .collect();
    if remote.is_empty() {
        return Err(format!("the plan sends no SQL to {source}:\n{physical}"));
    }
    let local: Vec<&Operator> = operators
        .iter()
        .filter(|op| !PASS_THROUGH.contains(&op.name) && !remote_scans.contains(&op.name))
        .collect();
    let Some(expr) = kept_local else {
        if local.is_empty() {
            return Ok(());
        }
        let names: Vec<&str> = local.iter().map(|op| op.name).collect();
        return Err(format!(
            "Spice evaluates {names:?} itself instead of sending it to {source}:\n{physical}"
        ));
    };
    let keeps = |op: &&Operator| op.line.to_uppercase().contains(&expr.to_uppercase());
    let other: Vec<&str> = local
        .iter()
        .filter(|op| !keeps(op) && !PLUMBING.contains(&op.name))
        .map(|op| op.name)
        .collect();
    let sent = remote.iter().any(|op| {
        sent_sql(op.line)
            .to_uppercase()
            .contains(&expr.to_uppercase())
    });
    if local.iter().any(keeps) && other.is_empty() && !sent {
        Ok(())
    } else {
        Err(format!(
            "Spice must evaluate `{expr}` itself and send the rest of the query to {source}:\n{physical}"
        ))
    }
}
