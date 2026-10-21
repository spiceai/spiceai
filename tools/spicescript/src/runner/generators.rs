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

//! Generated queries: ternary logic partitioning, random queries, and
//! sqllogictest files.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::array::RecordBatch;

use crate::compare;
use crate::script::{Script, SltStep, TlpStep};

use super::{Instance, QueryFailure, QueryFailures, Sources, namespace, one_line, sanitize};

pub(super) async fn table_columns(
    instance: &mut Instance,
    table: &str,
    skip: &[String],
) -> Result<Vec<crate::tlp::Column>> {
    use crate::tlp::{Column, Family};
    let batches = instance
        .spiced
        .query(&format!(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = '{table}' ORDER BY ordinal_position"
        ))
        .await?;
    let mut columns = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cell = |c: usize| {
                result_compare::array_value_to_string(batch.column(c).as_ref(), row)
                    .map(Option::unwrap_or_default)
            };
            let (name, data_type) = (cell(0)?, cell(1)?);
            if let Some(family) = Family::of(&data_type)
                && !skip.contains(&name)
            {
                columns.push(Column { name, family });
            }
        }
    }
    Ok(columns)
}

/// Random queries over nested subqueries: each answer from Spice must be the
/// oracle's answer to the same SQL. A query the oracle cannot answer is not
/// counted.
pub(super) async fn random_queries(
    instance: &mut Instance,
    step: &crate::script::RandomQueriesStep,
    sources: &Sources,
) -> Result<String> {
    use crate::rqg::{Generator, Join, Table};
    let mut tables = Vec::new();
    for name in &step.tables {
        let columns = table_columns(instance, name, &step.skip_columns).await?;
        ensure!(
            !columns.is_empty(),
            "table '{name}' has no columns the generator can use"
        );
        let key = columns
            .iter()
            .any(|c| c.name == "id")
            .then(|| "id".to_string());
        tables.push(Table {
            name: name.clone(),
            columns,
            key,
        });
    }
    let joins = step
        .joins
        .iter()
        .map(|j| {
            let (l, r) = j
                .split_once('=')
                .with_context(|| format!("a join is `a.x = b.y`, not '{j}'"))?;
            let (left, left_column) = l
                .trim()
                .split_once('.')
                .with_context(|| format!("'{l}' is not `table.column`"))?;
            let (right, right_column) = r
                .trim()
                .split_once('.')
                .with_context(|| format!("'{r}' is not `table.column`"))?;
            Ok(Join {
                left: left.into(),
                left_column: left_column.into(),
                right: right.into(),
                right_column: right_column.into(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let source = sources.get(&step.oracle)?;
    let namespace = namespace(instance, &step.oracle)?;
    let mut generator = Generator::new(step.seed, &tables, &joins, step.avoid.clone());
    let (mut failures, mut compared, mut unanswerable, mut empty) = (Vec::new(), 0u64, 0u64, 0u64);
    for i in 0..step.queries {
        let sql = generator.query(step.depth);
        let labels = generator.take_touched();
        let Ok(expected) = source.query(&namespace, &sql).await else {
            unanswerable += 1;
            continue;
        };
        let problem = match instance.spiced.query(&sql).await {
            Err(e) => Some(format!(
                "Spice rejected it: {}",
                one_line(&format!("{e:#}"), 300)
            )),
            Ok(answer) => match answer.first().map(RecordBatch::schema) {
                None if expected.rows.is_empty() => {
                    empty += 1;
                    None
                }
                None => Some(format!(
                    "Spice returned no rows, the oracle {}",
                    expected.rows.len()
                )),
                Some(schema) => {
                    if expected.rows.is_empty() && answer.iter().all(|b| b.num_rows() == 0) {
                        empty += 1;
                        None
                    } else {
                        let typed = compare::type_oracle(&expected, &schema)?;
                        compare::compare("random", &sql, &answer, &[typed], false)?.map(|diff| {
                            format!(
                                "the answer differs from the oracle's: {}",
                                one_line(&diff, 300)
                            )
                        })
                    }
                }
            },
        };
        compared += 1;
        if let Some(problem) = problem {
            failures.push(QueryFailure {
                number: i,
                text: format!("seed {} #{i}: {sql}", step.seed),
                problem,
                labels,
            });
        }
    }
    ensure!(
        unanswerable * 4 <= step.queries,
        "the oracle could not answer {unanswerable} of {} queries; the generator is producing SQL it does not accept",
        step.queries
    );
    if !failures.is_empty() {
        let path = instance
            .spiced
            .log_path()
            .with_file_name(format!("random-queries-seed{}.txt", step.seed));
        let all: Vec<String> = failures
            .iter()
            .map(|f| format!("{}\n      {}", f.text, f.problem))
            .collect();
        tokio::fs::write(&path, all.join("\n\n")).await?;
        return Err(QueryFailures {
            summary: format!(
                "{} of {compared} queries answered differently from the oracle (all in {}):\n{}",
                failures.len(),
                path.display(),
                all.iter().take(8).cloned().collect::<Vec<_>>().join("\n")
            ),
            failed: failures,
        }
        .into());
    }
    Ok(format!(
        "{compared} queries matched ({empty} empty on both sides, {unanswerable} the oracle refused)"
    ))
}

/// Ternary logic partitioning: for each random predicate `p`, the table's
/// rows (and, with `aggregate`, its count, sum, min and max) must equal those
/// of `WHERE p`, `WHERE NOT (p)` and `WHERE (p) IS NULL` together; with
/// `oracle`, the partitions must also match the oracle answering the same SQL.
pub(super) async fn tlp(
    instance: &mut Instance,
    step: &TlpStep,
    sources: &Sources,
) -> Result<String> {
    use crate::tlp::Generator;
    let table = &step.table;
    let columns = table_columns(instance, table, &step.skip_columns).await?;
    ensure!(
        !columns.is_empty(),
        "table '{table}' has no columns TLP can use"
    );
    // Every column the table has, predicates or not.
    let projection = "*";
    let whole_sql = format!("SELECT {projection} FROM {table}");
    let whole = instance
        .spiced
        .query(&whole_sql)
        .await
        .context("Spice could not scan the table")?;
    let aggregate = |x: &str, filter: &str| {
        format!(
            "SELECT count(*) AS n, sum({x}) AS s, min({x}) AS mn, max({x}) AS mx FROM {table}{filter}"
        )
    };
    let whole_agg = match &step.aggregate {
        Some(x) => Some(instance.spiced.query(&aggregate(x, "")).await?),
        None => None,
    };
    let oracle = match &step.oracle {
        Some(name) => Some((sources.get(name)?, namespace(instance, name)?)),
        None => None,
    };
    let mut generator = Generator::new(step.seed, &columns, step.avoid.clone());
    let mut failures = Vec::new();
    for i in 0..step.queries {
        let predicate = generator.predicate(step.depth);
        let labels = generator.take_touched();
        let parts = format!(
            "SELECT {projection} FROM {table} WHERE {predicate} UNION ALL SELECT {projection} FROM {table} WHERE NOT ({predicate}) \
             UNION ALL SELECT {projection} FROM {table} WHERE ({predicate}) IS NULL"
        );
        let mut problem = None;
        match instance.spiced.query(&parts).await {
            Err(e) => {
                problem = Some(format!(
                    "Spice rejected the partitions: {}",
                    one_line(&format!("{e:#}"), 300)
                ));
            }
            Ok(answer) => {
                if let Some(diff) = compare::compare("tlp", &parts, &answer, &whole, false)? {
                    problem = Some(format!(
                        "the partitions are not the table: {}",
                        one_line(&diff, 300)
                    ));
                } else if let Some((source, namespace)) = &oracle {
                    let schema = whole.first().map(RecordBatch::schema);
                    match (source.query(namespace, &parts).await, schema) {
                        (Ok(text), Some(schema)) => {
                            let expected = compare::type_oracle(&text, &schema)?;
                            if let Some(diff) =
                                compare::compare("tlp", &parts, &answer, &[expected], false)?
                            {
                                problem = Some(format!(
                                    "the partitions differ from the oracle: {}",
                                    one_line(&diff, 300)
                                ));
                            }
                        }
                        (Err(e), _) => {
                            problem = Some(format!(
                                "the oracle rejected the partitions: {}",
                                one_line(&format!("{e:#}"), 200)
                            ));
                        }
                        (Ok(_), None) => {}
                    }
                }
            }
        }
        if problem.is_none()
            && let (Some(x), Some(whole_agg)) = (&step.aggregate, &whole_agg)
        {
            let parts_agg = format!(
                "SELECT sum(n), sum(s), min(mn), max(mx) FROM ({} UNION ALL {} UNION ALL {}) AS parts",
                aggregate(x, &format!(" WHERE {predicate}")),
                aggregate(x, &format!(" WHERE NOT ({predicate})")),
                aggregate(x, &format!(" WHERE ({predicate}) IS NULL"))
            );
            match instance.spiced.query(&parts_agg).await {
                Err(e) => {
                    problem = Some(format!(
                        "Spice rejected the aggregate partitions: {}",
                        one_line(&format!("{e:#}"), 300)
                    ));
                }
                Ok(answer) => {
                    // Compare as text: the parts' sums are wider types than the whole's.
                    let render = |b: &[RecordBatch]| -> Result<Vec<Option<String>>> {
                        let batch = b.first().context("no aggregate row")?;
                        (0..batch.num_columns())
                            .map(|c| {
                                result_compare::array_value_to_string(batch.column(c).as_ref(), 0)
                            })
                            .collect()
                    };
                    let (got, want) = (render(&answer)?, render(whole_agg)?);
                    // Floating sums depend on the order they are added in.
                    let close = |left: &Option<String>, right: &Option<String>| match (left, right)
                    {
                        (Some(lhs), Some(rhs)) => match (lhs.parse::<f64>(), rhs.parse::<f64>()) {
                            (Ok(lv), Ok(rv)) => {
                                (lv - rv).abs() <= 1e-9 * lv.abs().max(rv.abs()).max(1.0)
                            }
                            _ => lhs == rhs,
                        },
                        (lhs, rhs) => lhs == rhs,
                    };
                    if got.len() != want.len()
                        || !got.iter().zip(&want).all(|(lhs, rhs)| close(lhs, rhs))
                    {
                        problem = Some(format!(
                            "the aggregate partitions {got:?} are not the table's {want:?}"
                        ));
                    }
                }
            }
        }
        if let Some(problem) = problem {
            failures.push(QueryFailure {
                number: i,
                text: format!("seed {} #{i}: WHERE {predicate}", step.seed),
                problem,
                labels,
            });
        }
    }
    if !failures.is_empty() {
        let shown: Vec<String> = failures
            .iter()
            .take(8)
            .map(|f| format!("{}\n      {}", f.text, f.problem))
            .collect();
        return Err(QueryFailures {
            summary: format!(
                "{} of {} predicates broke TLP over {table}:\n{}",
                failures.len(),
                step.queries,
                shown.join("\n")
            ),
            failed: failures,
        }
        .into());
    }
    Ok(format!("{} predicates held", step.queries))
}

/// Runs a sqllogictest file. With `setup`, its statements run on the source;
/// otherwise its queries run through Spice and each answer that differs from
/// the recorded one is classified by asking the source the same query.
pub(super) async fn run_slt(
    script: &Script,
    instance: &mut Instance,
    step: &SltStep,
    sources: &Sources,
) -> Result<String> {
    use crate::slt::{self, Item, Outcome};
    let path = script.dir.join(instance.sql(&step.file)?);
    let kind = sources
        .kinds
        .get(&step.source)
        .ok_or_else(|| anyhow!("unknown source '{}'", step.source))?;
    let labels = step.labels.clone().unwrap_or_else(|| {
        vec![match kind.as_str() {
            "postgres" => "postgresql".to_string(),
            other => other.to_string(),
        }]
    });
    let loaded = slt::load(&path, &labels)?;
    let namespace = namespace(instance, &step.source)?;
    let src = sources.get(&step.source)?;

    if step.setup {
        let mut ran = 0;
        for item in &loaded.items {
            if let Item::Statement { loc, sql, may_fail } = item {
                match src.exec(&namespace, sql).await {
                    Ok(()) => ran += 1,
                    Err(_) if *may_fail => {}
                    Err(e) => return Err(e.context(format!("statement at {loc}"))),
                }
            }
        }
        return Ok(format!("{ran} statements on '{}'", step.source));
    }

    let mut seen_query = false;
    let (mut pass, mut wrong, mut errors, mut dialect, mut accepted) = (0, 0, 0, 0, 0);
    let mut known: BTreeMap<&str, usize> = BTreeMap::new();
    let mut report = String::new();
    for item in &loaded.items {
        let query = match item {
            Item::Statement { loc, .. } if seen_query => {
                bail!("{loc}: a statement after a query is not supported yet; split the file")
            }
            Item::Statement { .. } => continue,
            Item::Query(q) => q,
        };
        seen_query = true;
        let spice =
            tokio::time::timeout(Duration::from_secs(60), instance.spiced.query(&query.sql))
                .await
                .unwrap_or_else(|_| Err(anyhow!("timed out after 60s")));
        if query.expect_error {
            if spice.is_ok() {
                accepted += 1;
            } else {
                pass += 1;
            }
            continue;
        }
        let spice_rows = spice.and_then(|batches| slt::rows_from_batches(&batches, &query.types));
        if let Ok(rows) = &spice_rows
            && slt::matches(query, rows.clone())
        {
            pass += 1;
            continue;
        }
        let source_agrees = match src.query(&namespace, &query.sql).await {
            Ok(text) => {
                slt::rows_from_text(&text, &query.types).is_ok_and(|rows| slt::matches(query, rows))
            }
            Err(_) => false,
        };
        let outcome = match (spice_rows, source_agrees) {
            (Ok(rows), true) => Outcome::SpiceWrong {
                got: slt::summarize(&rows),
            },
            (Err(e), true) => Outcome::SpiceError {
                error: format!("{e:#}"),
            },
            (Ok(rows), false) => Outcome::Dialect {
                got: slt::summarize(&rows),
            },
            (Err(e), false) => Outcome::Dialect {
                got: format!("error: {e:#}"),
            },
        };
        let (tag, detail) = match outcome {
            Outcome::SpiceWrong { got } => {
                wrong += 1;
                ("SPICE-WRONG", got)
            }
            Outcome::SpiceError { error } => {
                if let Some(k) = step
                    .known_errors
                    .iter()
                    .find(|k| error.contains(k.as_str()))
                {
                    *known.entry(k.as_str()).or_default() += 1;
                    ("KNOWN-ERROR", error)
                } else {
                    errors += 1;
                    ("SPICE-ERROR", error)
                }
            }
            Outcome::Dialect { got } => {
                dialect += 1;
                ("DIALECT", got)
            }
        };
        let _ = writeln!(
            report,
            "{tag} {}
  sql: {}
  expected: {}
  spice: {}",
            query.loc,
            one_line(&query.sql, 400),
            one_line(&query.expected.join(" "), 200),
            one_line(&detail, 400)
        );
    }
    let report_path = instance
        .spiced
        .dir
        .join(format!("slt-{}.txt", sanitize(&step.file)));
    std::fs::write(&report_path, &report)?;
    let known_total: usize = known.values().sum();
    let summary = format!(
        "{pass} pass, {wrong} spice-wrong, {errors} spice-error, {known_total} known-error, {dialect} dialect, {accepted} accepted-invalid, {} skipped; report: {}",
        loaded.skipped,
        report_path.display()
    );
    ensure!(wrong == 0 && errors == 0, "{summary}");
    Ok(summary)
}
