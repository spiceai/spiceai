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

//! `query` and `explain` steps: running a query, checking its answer, the
//! SQL it sends to its source, and that HTTP and `/v1/queries` give the same
//! answer.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::array::RecordBatch;

use crate::compare;
use crate::script::{Defaults, Expect, ExplainStep, QueryStep, Script, Timing};

use super::{Instance, POLL, Sources, at_place, max_wait, namespace, one_line};

/// The SQL each federated scan in an `EXPLAIN` sends to its source: the last
/// SQL a `VirtualExecutionPlan` line shows (its rewrites follow `base_sql`).
pub(super) fn sent_sql(plan: &str) -> Vec<String> {
    const FIELDS: [&str; 5] = [
        " base_sql=",
        " rewritten_logical_sql=",
        " rewritten_executor_sql=",
        " rewritten_ast_analyzer=",
        " rewritten_sql=",
    ];
    plan.lines()
        .filter(|l| l.contains("VirtualExecutionPlan"))
        .filter_map(|line| {
            let line = line.trim().trim_end_matches('|').trim_end();
            let (start, field) = FIELDS
                .iter()
                .filter_map(|f| line.rfind(f).map(|at| (at, *f)))
                .max_by_key(|(at, _)| *at)?;
            Some(line[start + field.len()..].trim().to_string())
        })
        .collect()
}

/// Checks the text of a query's plan.
pub(super) async fn explain(instance: &mut Instance, step: &ExplainStep) -> Result<()> {
    let keyword = if step.analyze {
        "EXPLAIN ANALYZE"
    } else {
        "EXPLAIN"
    };
    let sql = instance.sql(&step.sql)?;
    let batches = instance
        .spiced
        .query(&format!("{keyword} {sql}"))
        .await
        .context("Spice rejected the EXPLAIN")?;
    let plan = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    let missing: Vec<&str> = step
        .contains
        .iter()
        .filter(|s| !plan.contains(s.as_str()))
        .map(String::as_str)
        .collect();
    let present: Vec<&str> = step
        .omits
        .iter()
        .filter(|s| plan.contains(s.as_str()))
        .map(String::as_str)
        .collect();
    ensure!(
        missing.is_empty() && present.is_empty(),
        "the plan {}{}{}:\n{plan}",
        if missing.is_empty() {
            String::new()
        } else {
            format!("lacks {missing:?}")
        },
        if missing.is_empty() || present.is_empty() {
            ""
        } else {
            " and "
        },
        if present.is_empty() {
            String::new()
        } else {
            format!("contains {present:?}")
        },
    );
    Ok(())
}

/// Runs one query step until its timing is satisfied. Returns the last answer.
pub(super) async fn run_query(
    script: &Script,
    instance: &mut Instance,
    q: &QueryStep,
    corpus: Option<usize>,
    sources: &Sources,
) -> Result<Option<Vec<RecordBatch>>> {
    let expect = q.expect.as_ref().or(script.defaults.expect.as_ref());
    let timing = match q.timing {
        Timing::Once => script
            .defaults
            .eventually
            .map_or(Timing::Once, Timing::Eventually),
        t => t,
    };
    let started = retry_since(timing, corpus, &mut instance.corpus_started, Instant::now());
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        let limit = script
            .defaults
            .query_timeout
            .unwrap_or(Duration::from_mins(2));
        let attempt = query_attempt(instance, q, expect, sources, limit, &script.defaults).await;
        match (timing, attempt) {
            (Timing::Once, result) => return result,
            (Timing::Eventually(_), Ok(answer)) => return Ok(answer),
            (Timing::Eventually(limit), Err(e)) if started.elapsed() >= max_wait(limit) => {
                return Err(e.context(format!(
                    "still failing after {attempts} attempts over {limit:?}"
                )));
            }
            (Timing::Consistently(_), Err(e)) => {
                return Err(e.context(format!(
                    "stopped matching after {:.1}s ({attempts} attempts)",
                    started.elapsed().as_secs_f64()
                )));
            }
            (Timing::Consistently(limit), Ok(answer)) if started.elapsed() >= limit => {
                return Ok(answer);
            }
            _ => tokio::time::sleep(POLL).await,
        }
    }
}

/// When a query's `eventually` limit started: for a query from a corpus, when
/// this instance began that corpus's first query, so the corpus waits out one
/// limit rather than one per query and a query reached after the limit gets a
/// single attempt; otherwise `now`.
pub(super) fn retry_since(
    timing: Timing,
    corpus: Option<usize>,
    corpus_started: &mut BTreeMap<usize, Instant>,
    now: Instant,
) -> Instant {
    match (timing, corpus) {
        (Timing::Eventually(_), Some(corpus)) => *corpus_started.entry(corpus).or_insert(now),
        _ => now,
    }
}

/// An expected error must contain the text the step names, if it names one.
pub(super) fn check_error_text(error: &anyhow::Error, contains: Option<&str>) -> Result<()> {
    let Some(text) = contains else {
        return Ok(());
    };
    let message = format!("{error:#}");
    ensure!(
        message.contains(text),
        "rejected, but the error lacks `{text}`: {}",
        one_line(&message, 400)
    );
    Ok(())
}

/// One attempt at a query step; an answer that meets its expectation must
/// also have reached its source as the step's `sends`/`keeps` checks say.
pub(super) async fn query_attempt(
    instance: &mut Instance,
    q: &QueryStep,
    expect: Option<&Expect>,
    sources: &Sources,
    limit: Duration,
    defaults: &Defaults,
) -> Result<Option<Vec<RecordBatch>>> {
    let answer = answer_attempt(instance, q, expect, sources, limit, defaults).await?;
    if answer.is_some() && !q.sent.is_empty() {
        let sql = instance.sql(&q.sql)?;
        check_sent(instance, q, &sql).await?;
    }
    Ok(answer)
}

pub(super) async fn answer_attempt(
    instance: &mut Instance,
    q: &QueryStep,
    expect: Option<&Expect>,
    sources: &Sources,
    limit: Duration,
    defaults: &Defaults,
) -> Result<Option<Vec<RecordBatch>>> {
    let spice_sql = instance.sql(&q.sql)?;
    let oracle_sql = instance.sql(q.oracle_sql.as_deref().unwrap_or(&q.sql))?;
    let run = async {
        if q.parameters.is_some() || q.session.is_some() {
            let parameters = q.parameters.clone().unwrap_or_default();
            instance
                .spiced
                .query_prepared(
                    &spice_sql,
                    &parameters,
                    q.session.as_deref(),
                    &q.key,
                    q.protocols.is_some(),
                )
                .await
        } else if let Some(protocols) = &q.protocols {
            super::generators::protocol_answers(instance, &spice_sql, protocols, &q.key).await
        } else {
            instance.spiced.query_as(&spice_sql, &q.key).await
        }
    };
    let actual = match tokio::time::timeout(limit, run)
        .await
        .unwrap_or_else(|_| Err(anyhow!("the query did not return within {limit:?}")))
    {
        Ok(batches) => batches,
        Err(e) if q.expect_error => {
            check_error_text(&e, q.error_contains.as_deref())?;
            return Ok(None);
        }
        Err(e) if q.protocols.is_some() => {
            return Err(anyhow!(
                "Spice rejected the query: {e:#}\nSQL:\n{spice_sql}"
            ));
        }
        Err(e) => return Err(e.context("Spice rejected the query")),
    };
    if q.expect_error {
        bail!(
            "the query succeeded, but the script expected an error:\n{}",
            compare::preview(&actual)
        );
    }
    let Some(schema) = actual.first().map(RecordBatch::schema) else {
        // Flight may send no batch at all for an empty answer, leaving no
        // schema to type the expectation with; only the row count can be
        // compared.
        let expected_rows = match expect {
            None
            | Some(Expect {
                oracle: None,
                rows: None,
                same_as: None,
            }) => return Ok(Some(actual)),
            Some(Expect {
                oracle: Some(oracle),
                ..
            }) => {
                let namespace = namespace(instance, oracle)?;
                sources
                    .get(oracle)?
                    .query(&namespace, &oracle_sql)
                    .await?
                    .rows
                    .len()
            }
            Some(Expect {
                rows: Some(rows), ..
            }) => rows.len(),
            Some(Expect {
                same_as: Some(id), ..
            }) => instance
                .named
                .get(id)
                .map_or(0, |b| b.iter().map(RecordBatch::num_rows).sum()),
        };
        return match (expected_rows, q.expect_empty) {
            (0, true) => Ok(Some(actual)),
            (0, false) => bail!(
                "both answers are empty, which compares nothing; set `expect-empty: true` if an empty answer is the point"
            ),
            (n, _) => bail!("Spice returned no rows, expected {n}"),
        };
    };
    let expected = match expect {
        // `expect: {}` records the answer without checking it, overriding a
        // default expectation (for a query a later one is `same-as`).
        None
        | Some(Expect {
            oracle: None,
            rows: None,
            same_as: None,
        }) => {
            ensure!(
                !q.expect_empty || actual.iter().all(|batch| batch.num_rows() == 0),
                "expected an empty answer, got {} rows:\n{}",
                actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
                compare::preview(&actual)
            );
            if defaults.also_async {
                async_agrees(
                    instance,
                    &spice_sql,
                    q.parameters.as_deref(),
                    &actual,
                    &schema,
                    q.expect_empty,
                    limit,
                )
                .await?;
            }
            return Ok(Some(actual));
        }
        Some(Expect {
            oracle: Some(oracle),
            rows: None,
            same_as: None,
        }) => {
            let namespace = namespace(instance, oracle)?;
            let text = sources.get(oracle)?.query(&namespace, &oracle_sql).await?;
            compare::type_oracle(&text, &schema)?
        }
        Some(Expect {
            oracle: None,
            rows: Some(rows),
            same_as: None,
        }) => compare::literal_rows(rows, &schema)?,
        Some(Expect {
            oracle: None,
            rows: None,
            same_as: Some(id),
        }) => {
            let earlier = instance.named.get(id).with_context(|| {
                format!("no answer was recorded for query '{id}' on this instance")
            })?;
            arrow::compute::concat_batches(&schema, earlier)
                .context("the answer's schema changed since the earlier query")?
        }
        Some(_) => bail!("`expect` takes exactly one of `oracle`, `rows` or `same-as`"),
    };
    if let Some(report) = compare::compare(
        &instance.label,
        &spice_sql,
        &actual,
        &[expected],
        q.expect_empty,
    )? {
        bail!("answer differs from the expected one: {report}");
    }
    if defaults.also_http {
        http_agrees(instance, &spice_sql, &actual, &schema, q.expect_empty).await?;
    }
    if defaults.also_async {
        async_agrees(
            instance,
            &spice_sql,
            q.parameters.as_deref(),
            &actual,
            &schema,
            q.expect_empty,
            limit,
        )
        .await?;
    }
    Ok(Some(actual))
}

/// The query's `sends`/`keeps` checks at this instance's place, against the
/// SQL its plan sends to the source.
pub(super) async fn check_sent(instance: &mut Instance, q: &QueryStep, sql: &str) -> Result<()> {
    let here: Vec<&crate::script::SentCheck> = q
        .sent
        .iter()
        .filter(|c| at_place(&c.place, instance))
        .collect();
    if here.is_empty() {
        return Ok(());
    }
    let plan = arrow::util::pretty::pretty_format_batches(
        &instance.spiced.query(&format!("EXPLAIN {sql}")).await?,
    )?
    .to_string();
    let sent = sent_sql(&plan).join("\n");
    for check in here {
        let (text, what) = if check.whole_plan {
            (plan.as_str(), "the plan")
        } else {
            ensure!(
                !sent.is_empty(),
                "nothing is sent to a source, so no SQL to check; plan:\n{plan}"
            );
            (sent.as_str(), "the SQL sent")
        };
        let found = text.matches(check.fragment.as_str()).count();
        if let Some(count) = check.count {
            ensure!(
                found == count,
                "{what} has `{}` {found} times, not {count}: {text}",
                check.fragment
            );
        } else {
            ensure!(
                (found > 0) == check.sent,
                "{what} {} `{}`: {text}",
                if check.sent { "lacks" } else { "contains" },
                check.fragment
            );
        }
    }
    Ok(())
}

/// The elements of a top-level JSON array, as slices of its text.
pub(super) fn json_array_elements(text: &str) -> Result<Vec<&str>> {
    let inner = text
        .trim()
        .strip_prefix('[')
        .and_then(|t| t.strip_suffix(']'))
        .context("expected a JSON array")?;
    let mut elements = Vec::new();
    let (mut depth, mut in_string, mut escaped, mut start) = (0usize, false, false, 0usize);
    for (i, c) in inner.char_indices() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '{' | '[' => depth += 1,
            '}' | ']' => depth = depth.checked_sub(1).context("unbalanced JSON")?,
            ',' if depth == 0 => {
                elements.push(inner[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = inner[start..].trim();
    if !last.is_empty() {
        elements.push(last);
    }
    Ok(elements)
}

/// Asks the same SQL over HTTP (`/v1/sql`, JSON) and requires the answer
/// Flight gave. JSON objects key cells by column name, so an answer with
/// repeated column names is not compared.
pub(super) async fn http_agrees(
    instance: &Instance,
    sql: &str,
    flight: &[RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
    expect_empty: bool,
) -> Result<()> {
    let names: std::collections::BTreeSet<_> = schema.fields().iter().map(|f| f.name()).collect();
    if names.len() != schema.fields().len() {
        return Ok(());
    }
    let typed = http_answer(instance, sql, schema, &crate::spiced::Key::Default).await?;
    if let Some(report) = compare::compare(&instance.label, sql, flight, &typed, expect_empty)? {
        bail!("the HTTP JSON answer differs from Flight's: {report}");
    }
    Ok(())
}

pub(super) async fn http_agrees_as(
    instance: &Instance,
    sql: &str,
    flight: &[RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
    expect_empty: bool,
    key: &crate::spiced::Key,
) -> Result<()> {
    let typed = http_answer(instance, sql, schema, key).await?;
    if let Some(report) =
        compare::compare_protocol(&instance.label, sql, flight, &typed, expect_empty)?
    {
        bail!("the HTTP JSON answer differs from Flight's: {report}");
    }
    Ok(())
}

async fn http_answer(
    instance: &Instance,
    sql: &str,
    schema: &arrow::datatypes::SchemaRef,
    key: &crate::spiced::Key,
) -> Result<Vec<RecordBatch>> {
    let (status, _, body) = instance
        .spiced
        .http(
            "POST",
            "/v1/sql",
            &[
                ("content-type".into(), "text/plain".into()),
                ("accept".into(), "application/json".into()),
            ],
            Some(sql),
            key,
        )
        .await?;
    ensure!(
        status == 200,
        "HTTP answered {status}: {}",
        one_line(&body, 300)
    );
    // Decoded from the body's own text, not through serde_json values, which
    // would round a 38-digit decimal to a double.
    let rows = json_array_elements(&body)
        .with_context(|| format!("HTTP answered with no JSON rows: {}", one_line(&body, 200)))?;
    let typed = decode_json_rows(&rows, schema).with_context(|| {
        format!(
            "could not decode the HTTP JSON answer as Flight's schema; body: {}",
            one_line(&body, 4000)
        )
    })?;
    Ok(typed)
}

/// Runs the same SQL as a `/v1/queries` job and requires the answer Flight
/// gave, all within `limit`. On a cluster this is a distributed job whose
/// stages exchange data between executors, a path Flight queries do not take.
async fn async_agrees(
    instance: &Instance,
    sql: &str,
    parameters: Option<&[serde_json::Value]>,
    flight: &[RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
    expect_empty: bool,
    limit: Duration,
) -> Result<()> {
    let names: Vec<&String> = schema.fields().iter().map(|f| f.name()).collect();
    let unique: std::collections::BTreeSet<&&String> = names.iter().collect();
    if unique.len() != names.len() {
        return Ok(());
    }
    let typed = tokio::time::timeout(limit, async_answer(instance, sql, parameters, schema))
        .await
        .unwrap_or_else(|_| {
            Err(anyhow!(
                "the /v1/queries job did not answer within {limit:?}"
            ))
        })?;
    if let Some(report) = compare::compare(&instance.label, sql, flight, &typed, expect_empty)? {
        bail!("the /v1/queries answer differs from Flight's: {report}");
    }
    Ok(())
}

/// The answer to `sql`, with its positional `parameters`, as a `/v1/queries`
/// job, typed with `schema`.
async fn async_answer(
    instance: &Instance,
    sql: &str,
    parameters: Option<&[serde_json::Value]>,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let json = [("content-type".to_string(), "application/json".to_string())];
    let key = crate::spiced::Key::Default;
    let request = serde_json::json!({ "sql": sql, "parameters": parameters }).to_string();
    let (status, _, body) = instance
        .spiced
        .http("POST", "/v1/queries", &json, Some(&request), &key)
        .await?;
    ensure!(
        (200..300).contains(&status),
        "/v1/queries answered {status}: {}",
        one_line(&body, 300)
    );
    let submitted: serde_json::Value = serde_json::from_str(&body)?;
    let id = submitted["query_id"]
        .as_str()
        .with_context(|| format!("/v1/queries returned no query_id: {}", one_line(&body, 200)))?;
    loop {
        let path = format!("/v1/queries/{id}/status");
        let (_, _, body) = instance.spiced.http("GET", &path, &[], None, &key).await?;
        let state: serde_json::Value = serde_json::from_str(&body)
            .with_context(|| format!("{path} answered {}", one_line(&body, 200)))?;
        match state["status"].as_str() {
            Some("SUCCEEDED") => break,
            Some("PENDING" | "RUNNING") => {}
            _ => bail!(
                "the /v1/queries job did not succeed: {}",
                one_line(&body, 600)
            ),
        }
        tokio::time::sleep(POLL).await;
    }
    let mut rows_text = Vec::new();
    let mut path = Some(format!("/v1/queries/{id}/results"));
    while let Some(p) = path.take() {
        let (status, _, body) = instance.spiced.http("GET", &p, &[], None, &key).await?;
        ensure!(
            status == 200,
            "{p} answered {status}: {}",
            one_line(&body, 300)
        );
        let chunk: serde_json::Value = serde_json::from_str(&body)?;
        path = chunk["next_chunk_url"].as_str().map(str::to_string);
        // The rows come from the body's own text, as for `/v1/sql`.
        let rows = json_field_array(&body, "data_array")
            .with_context(|| format!("{p} answered with no rows: {}", one_line(&body, 200)))?;
        rows_text.extend(json_array_elements(rows)?.into_iter().map(str::to_string));
    }
    let rows: Vec<&str> = rows_text.iter().map(String::as_str).collect();
    decode_json_rows(&rows, schema)
        .context("could not decode the /v1/queries answer as Flight's schema")
}

/// The text of the JSON array a top-level field of `text` holds.
fn json_field_array<'a>(text: &'a str, field: &str) -> Result<&'a str> {
    let key = format!("\"{field}\":");
    let at = text.find(&key).context("no such field")? + key.len();
    let rest = text[at..].trim_start();
    ensure!(rest.starts_with('['), "the field is not an array");
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    for (i, c) in rest.char_indices() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '[' | '{' => depth += 1,
            ']' | '}' => {
                depth = depth.checked_sub(1).context("unbalanced JSON")?;
                if depth == 0 {
                    return Ok(&rest[..=i]);
                }
            }
            _ => {}
        }
    }
    bail!("unterminated JSON array")
}

/// JSON objects, one per row, as batches of `schema`.
fn decode_json_rows(
    rows: &[&str],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut decoder = arrow::json::ReaderBuilder::new(Arc::clone(schema))
        .with_strict_mode(true)
        .build_decoder()?;
    let mut rows_text = rows.join("\n").into_bytes();
    rows_text.push(b'\n');
    let mut input = rows_text.as_slice();
    let mut typed = Vec::new();
    while !input.is_empty() {
        let read = decoder.decode(input)?;
        input = &input[read..];
        if read == 0 || input.is_empty() {
            typed.extend(decoder.flush()?);
        }
    }
    if typed.is_empty() {
        typed.push(RecordBatch::new_empty(Arc::clone(schema)));
    }
    Ok(typed)
}

#[cfg(test)]
mod pushdown_tests {
    use super::sent_sql;

    #[test]
    fn sent_sql_takes_each_scans_final_rewrite() {
        let plan = r"+---------------+------------------------------------------------------------------+
| plan_type     | plan                                                             |
+---------------+------------------------------------------------------------------+
| physical_plan | SchemaCastScanExec                                               |
|               |   VirtualExecutionPlan name=postgres compute_context=a base_sql=SELECT t.id FROM t WHERE (t.i > 40) |
|               |   VirtualExecutionPlan name=mysql compute_context=b base_sql=SELECT `t`.`id` FROM `t` rewritten_ast_analyzer=SELECT `t`.`id` FROM `t` LIMIT 5 |
|               |   VirtualExecutionPlan name=x compute_context=c base_sql=SELECT 1 rewritten_logical_sql=SELECT 2 rewritten_sql=SELECT 3 |
|               |   ProjectionExec: expr=[id@0 as id]                              |
+---------------+------------------------------------------------------------------+";
        assert_eq!(
            sent_sql(plan),
            vec![
                "SELECT t.id FROM t WHERE (t.i > 40)".to_string(),
                "SELECT `t`.`id` FROM `t` LIMIT 5".to_string(),
                "SELECT 3".to_string(),
            ]
        );
        assert!(
            sent_sql("| ProjectionExec: expr=[1] |").is_empty(),
            "no federated scan sends no SQL"
        );
    }
}
