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

//! Differential tests of a connector's filter, sort and limit pushdown.
//!
//! Each query runs twice: against the federated dataset, whose filters, sorts
//! and limits the connector pushes to the source, and against an Arrow
//! acceleration of the same source, refreshed in full, which `DataFusion`
//! evaluates without asking the source anything. The accelerated copy holds
//! exactly the rows the connector's conversion produces, so the two must return
//! the same rows for every query. A check that each query expected to push down
//! did keeps a translation that pushes nothing from passing vacuously.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use runtime::Runtime;

use crate::utils::run_query;

/// Whether a case must show a pushed-down filter in its federated plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pushed {
    Yes,
    /// Pushed down or not, the rows must match.
    Either,
}

/// One query, with `{t}` standing for the table.
#[derive(Debug, Clone)]
pub(crate) struct Case {
    sql: String,
    pushed: Pushed,
    /// For a `LIMIT` without an `ORDER BY`, which rows come back is not fixed:
    /// the federated rows must be that many of the rows `sql` keeps without it.
    limit: Option<usize>,
}

impl Case {
    pub(crate) fn pushed(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            pushed: Pushed::Yes,
            limit: None,
        }
    }

    pub(crate) fn either(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            pushed: Pushed::Either,
            limit: None,
        }
    }

    /// `sql` with `LIMIT limit` and no `ORDER BY`.
    pub(crate) fn limited(sql: impl Into<String>, limit: usize, pushed: Pushed) -> Self {
        Self {
            sql: sql.into(),
            pushed,
            limit: Some(limit),
        }
    }
}

/// The datasets a suite compares, and how a plan shows a filter pushed down.
pub(crate) struct Tables<'a> {
    pub(crate) federated: &'a str,
    pub(crate) local: &'a str,
    pub(crate) shows_pushdown: fn(&str) -> bool,
}

/// Runs every case over both tables and fails with each disagreement.
pub(crate) async fn assert_round_trips(
    rt: &Arc<Runtime>,
    tables: &Tables<'_>,
    cases: &[Case],
) -> Result<(), anyhow::Error> {
    let mut failures = Vec::new();
    let mut pushed = 0_usize;
    for case in cases {
        let federated_sql = case.sql.replace("{t}", tables.federated);
        let local_sql = case.sql.replace("{t}", tables.local);
        let limited_sql = case
            .limit
            .map(|limit| format!("{federated_sql} LIMIT {limit}"));

        let federated = rows(rt, limited_sql.as_deref().unwrap_or(&federated_sql)).await;
        let local = rows(rt, &local_sql).await;
        let agreement = match (&federated, &local, case.limit) {
            (Ok(federated), Ok(local), None) => {
                let ordered = case.sql.to_ascii_uppercase().contains("ORDER BY");
                same_rows(federated, local, ordered)
            }
            (Ok(federated), Ok(local), Some(limit)) => limited_subset(federated, local, limit),
            (Err(e), _, _) => Err(format!("federated query failed: {e}")),
            (_, Err(e), _) => Err(format!("local query failed: {e}")),
        };
        if let Err(reason) = agreement {
            failures.push(format!(
                "{federated_sql}\n    {reason}\n    federated: {federated:?}\n    local:     {local:?}"
            ));
        }

        let plan = explain(rt, limited_sql.as_deref().unwrap_or(&federated_sql)).await;
        match (case.pushed, plan) {
            (_, Err(e)) => failures.push(format!("{federated_sql}\n    EXPLAIN failed: {e}")),
            (Pushed::Yes, Ok(plan)) if !(tables.shows_pushdown)(&plan) => {
                failures.push(format!(
                    "{federated_sql}\n    expected the filter pushed down, plan:\n{plan}"
                ));
            }
            (_, Ok(plan)) => {
                if (tables.shows_pushdown)(&plan) {
                    pushed += 1;
                }
            }
        }
    }
    tracing::info!(
        "{} of {} round-trip cases pushed a filter down to {}",
        pushed,
        cases.len(),
        tables.federated
    );
    anyhow::ensure!(
        failures.is_empty(),
        "{} of {} pushdown round-trip cases failed:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
    Ok(())
}

fn same_rows(federated: &[Row], local: &[Row], ordered: bool) -> Result<(), String> {
    if ordered {
        return (federated == local)
            .then_some(())
            .ok_or_else(|| "rows or their order differ".to_string());
    }
    let (mut federated, mut local) = (federated.to_vec(), local.to_vec());
    federated.sort();
    local.sort();
    (federated == local)
        .then_some(())
        .ok_or_else(|| "rows differ".to_string())
}

fn limited_subset(federated: &[Row], local: &[Row], limit: usize) -> Result<(), String> {
    let expected = limit.min(local.len());
    if federated.len() != expected {
        return Err(format!("expected {expected} rows, got {}", federated.len()));
    }
    let mut available = local.to_vec();
    for row in federated {
        match available.iter().position(|r| r == row) {
            Some(i) => {
                available.swap_remove(i);
            }
            None => return Err(format!("row {row:?} is not one the query keeps")),
        }
    }
    Ok(())
}

/// A row as its cells, each rendered, or `None` for NULL. The cells stay apart
/// and a NULL stays apart from the string "NULL", so two results render alike
/// only when their values do.
type Row = Vec<Option<String>>;

/// The rows of `sql`.
async fn rows(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<Row>, String> {
    let batches = run_query(rt, sql).await.map_err(|e| e.to_string())?;
    render(&batches)
}

fn render(batches: &[RecordBatch]) -> Result<Vec<Row>, String> {
    let options = FormatOptions::default().with_null("NULL");
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

/// The plan of `sql` as text, one line per row, for the checks that search it.
async fn explain(rt: &Arc<Runtime>, sql: &str) -> Result<String, String> {
    let batches = run_query(rt, &format!("EXPLAIN {sql}"))
        .await
        .map_err(|e| e.to_string())?;
    let lines: Vec<String> = render(&batches)?
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(Option::unwrap_or_default)
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect();
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, RecordBatch, StringArray};

    use super::render;

    fn batch(columns: &[&[Option<&str>]]) -> RecordBatch {
        RecordBatch::try_from_iter(columns.iter().enumerate().map(|(i, values)| {
            let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
            (format!("c{i}"), array)
        }))
        .expect("a batch")
    }

    #[test]
    fn cells_render_apart() {
        // Joined into one string, both of these read `a | b | c`.
        let split_early = render(&[batch(&[&[Some("a | b")], &[Some("c")]])]).expect("rendered");
        let split_late = render(&[batch(&[&[Some("a")], &[Some("b | c")]])]).expect("rendered");
        assert_ne!(split_early, split_late);
    }

    #[test]
    fn a_null_renders_apart_from_the_string_null() {
        let null = render(&[batch(&[&[None]])]).expect("rendered");
        let word = render(&[batch(&[&[Some("NULL")]])]).expect("rendered");
        assert_ne!(null, word);
        assert_eq!(null, vec![vec![None]]);
    }
}
