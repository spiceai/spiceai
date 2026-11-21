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

use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};

pub(crate) type Row = Vec<Option<String>>;

/// A check of an `EXPLAIN` answer, which says what the plan does wrong.
pub(crate) type PlanCheck = fn(&[Row]) -> Result<(), String>;

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
