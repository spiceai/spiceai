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

//! Running one step: the dispatcher for every action, and how a step is
//! described in reports.

use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde_json::Value;

use crate::script::{Action, KillSignal, Script, Step};
use crate::{compare, template};

use super::{
    DEFAULT_READY_TIMEOUT, Instance, POLL, STOP_TIMEOUT, SourceState, Sources, apply_patch,
    assert_files, assert_log, assert_status, check_error_text, explain, http_step, max_wait,
    namespace, random_queries, refresh, run_query, run_slt, source_query, spice_cli,
    start_background, start_executors, stop_background, tlp, wait_metric,
};

pub(super) fn describe(action: &Action) -> String {
    match action {
        Action::Start { .. } => "start".into(),
        Action::Stop => "stop".into(),
        Action::Kill { signal } => format!("kill-spiced ({signal:?})"),
        Action::Restart { kill, upgrade, .. } => {
            let name = if *upgrade { "upgrade" } else { "restart" };
            match kill {
                Some(signal) => format!("{name} (kill {signal:?})"),
                None => name.into(),
            }
        }
        Action::WaitReady { .. } => "wait-ready".into(),
        Action::KillExecutor(n) => format!("kill-executor {n}"),
        Action::StartExecutors => "start-executors".into(),
        Action::AddExecutor => "add-executor".into(),
        Action::EditSpicepod(_) => "edit-spicepod".into(),
        Action::FlightPut { dataset, rows, .. } => {
            format!("flight-put {dataset}: {} rows", rows.len())
        }
        Action::FlightExec { sql, .. } => format!("flight-exec: {}", one_line(sql, 76)),
        Action::StartSource(s) => format!("start-source {s}"),
        Action::StopSource(s) => format!("stop-source {s}"),
        Action::PauseSource(s) => format!("pause-source {s}"),
        Action::UnpauseSource(s) => format!("unpause-source {s}"),
        Action::SourceExec { source, sql } => {
            format!("source-exec {source}: {}", one_line(&sql.join("; "), 70))
        }
        Action::Refresh { dataset, .. } => format!("refresh {dataset}"),
        Action::Query(q) => format!("query: {}", one_line(&q.sql, 80)),
        Action::SpiceExec { sql, .. } => format!("spice-exec: {}", one_line(sql, 76)),
        Action::Explain(e) => format!("explain: {}", one_line(&e.sql, 78)),
        Action::AssertStatus(s) => format!("assert-status {}", s.dataset),
        Action::AssertLog(_) => "assert-log".into(),
        Action::Background(b) => {
            format!("background {}: {}", b.id, one_line(&b.work.describe(), 70))
        }
        Action::StopBackground { id, .. } => format!("stop-background {id}"),
        Action::AssertFiles { .. } => "assert-files".into(),
        Action::Tlp(t) => format!(
            "tlp {}: {} predicates (seed {})",
            t.table, t.queries, t.seed
        ),
        Action::RandomQueries(r) => format!(
            "random-queries {}: {} queries (seed {})",
            r.tables.join(", "),
            r.queries,
            r.seed
        ),
        Action::Dbgen { scale, sources } => format!("dbgen sf={scale} into {}", sources.join(", ")),
        Action::SourceQuery { source, sql, .. } => {
            format!("source-query {source}: {}", one_line(sql, 70))
        }
        Action::Http(h) => format!(
            "http {} {}{}",
            h.method,
            h.path,
            h.body
                .as_deref()
                .map(|b| format!(": {}", one_line(b, 60)))
                .unwrap_or_default()
        ),
        Action::SpiceCli { args, .. } => format!("spice {}", one_line(&args.join(" "), 80)),
        Action::MarkMetrics(m) => format!("mark-metrics {m}"),
        Action::AssertObjects {
            source,
            prefix,
            min,
            ..
        } => format!("assert-objects {source}/{prefix} >= {min}"),
        Action::Generate {
            source,
            spec,
            mirror,
        } => format!(
            "generate {source}.{}: {} rows (seed {}){}",
            spec.get("table").and_then(Value::as_str).unwrap_or("?"),
            spec.get("rows")
                .map_or_else(String::new, ToString::to_string),
            spec.get("seed")
                .map_or_else(String::new, ToString::to_string),
            mirror
                .as_ref()
                .map_or_else(String::new, |m| format!(", mirrored to {m}"))
        ),
        Action::WaitMetric(w) => format!("wait-metric {} {:?}", w.name, w.condition),
        Action::Slt(s) if s.setup => format!("slt-setup {} on {}", s.file, s.source),
        Action::Slt(s) => format!("slt {} over {}", s.file, s.source),
        Action::Sleep(d) => format!("sleep {d:?}"),
    }
}

pub(super) fn one_line(s: &str, max: usize) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() > max {
        format!("{}…", flat.chars().take(max).collect::<String>())
    } else {
        flat
    }
}

pub(super) async fn run_source_step(action: &Action, sources: &mut Sources) -> Result<String> {
    match action {
        Action::StartSource(name) | Action::UnpauseSource(name) => {
            sources.set(name, SourceState::Running).await
        }
        Action::StopSource(name) => sources.set(name, SourceState::Stopped).await,
        Action::PauseSource(name) => sources.set(name, SourceState::Paused).await,
        Action::Sleep(d) => {
            tokio::time::sleep(*d).await;
            Ok(())
        }
        _ => unreachable!("only source steps reach here"),
    }
    .map(|()| String::new())
}

pub(super) async fn run_instance_step(
    script: &Script,
    instance: &mut Instance,
    step: &Step,
    sources: &Sources,
) -> Result<String> {
    let ready_default = max_wait(
        script
            .defaults
            .ready_timeout
            .unwrap_or(DEFAULT_READY_TIMEOUT),
    );
    let started = Instant::now();
    match &step.action {
        Action::Start {
            ready_timeout,
            wait_ready,
            patch,
        } => {
            apply_patch(instance, patch.as_ref())?;
            instance.spiced.start(&instance.spicepod_yaml)?;
            let timeout = max_wait(ready_timeout.unwrap_or(ready_default));
            // A scheduler with accelerations is ready only once an executor
            // has joined and loaded them, so the executor starts first.
            instance.spiced.wait_http(timeout).await?;
            start_executors(instance, timeout, !*wait_ready).await?;
            if *wait_ready {
                instance.spiced.wait_ready(timeout).await?;
            }
        }
        Action::Stop => {
            instance.spiced.stop(STOP_TIMEOUT).await?;
            for executor in &mut instance.executors {
                executor.stop(STOP_TIMEOUT).await?;
            }
        }
        Action::Kill { signal } => {
            instance
                .spiced
                .kill(*signal == KillSignal::StopThenKill)
                .await?;
        }
        Action::Restart {
            kill,
            ready_timeout,
            wait_ready,
            patch,
            upgrade,
        } => {
            match kill {
                Some(signal) => {
                    instance
                        .spiced
                        .kill(*signal == KillSignal::StopThenKill)
                        .await?;
                }
                None => instance.spiced.stop(STOP_TIMEOUT).await?,
            }
            if *upgrade {
                instance.spiced.binary.clone_from(&instance.target_binary);
                for executor in &mut instance.executors {
                    executor.stop(STOP_TIMEOUT).await?;
                    executor.binary.clone_from(&instance.target_binary);
                }
            }
            apply_patch(instance, patch.as_ref())?;
            instance.spiced.start(&instance.spicepod_yaml)?;
            let timeout = max_wait(ready_timeout.unwrap_or(ready_default));
            // A scheduler with accelerations is ready only once an executor
            // has joined and loaded them, so the executor starts first.
            instance.spiced.wait_http(timeout).await?;
            start_executors(instance, timeout, !*wait_ready).await?;
            if *wait_ready {
                instance.spiced.wait_ready(timeout).await?;
            }
        }
        Action::WaitReady { timeout } => instance.spiced.wait_ready(*timeout).await?,
        Action::KillExecutor(n) => {
            let count = instance.executors.len();
            instance
                .executors
                .get_mut(*n)
                .ok_or_else(|| anyhow!("no executor {n}; this instance has {count}"))?
                .kill(false)
                .await?;
        }
        Action::StartExecutors => start_executors(instance, ready_default, false).await?,
        Action::AddExecutor => {
            ensure!(
                instance.active_executors < instance.executors.len(),
                "no spare executor left to add; set defaults.cluster.spare"
            );
            instance.active_executors += 1;
            start_executors(instance, ready_default, true).await?;
        }
        Action::FlightPut {
            dataset,
            rows,
            from_source,
            times,
            key,
            expect_error,
            error_contains,
        } => {
            let rows = match from_source {
                None => rows.clone(),
                Some((source, sql)) => {
                    let namespace = namespace(instance, source)?;
                    let answer = sources
                        .get(source)?
                        .query(&namespace, &instance.sql(sql)?)
                        .await?;
                    answer
                        .rows
                        .into_iter()
                        .map(|row| {
                            row.into_iter()
                                .map(|v| v.map_or(Value::Null, Value::String))
                                .collect()
                        })
                        .collect()
                }
            };
            let mut outcome = Ok(());
            for _ in 0..*times {
                outcome = instance.spiced.flight_put(dataset, &rows, key).await;
                if outcome.is_err() {
                    break;
                }
            }
            match (outcome, expect_error) {
                (Ok(()), false) => {}
                (Ok(()), true) => bail!("the DoPut succeeded, but the script expected an error"),
                (Err(e), true) => check_error_text(&e, error_contains.as_deref())?,
                (Err(e), false) => return Err(e.context("Spice rejected the DoPut")),
            }
        }
        Action::FlightExec {
            sql,
            count,
            key,
            expect_error,
            error_contains,
        } => {
            let sql = instance.sql(sql)?;
            match (instance.spiced.flight_update(&sql, key).await, expect_error) {
                (Ok(n), false) => {
                    if let Some(count) = count {
                        ensure!(
                            n == *count,
                            "ExecuteUpdate reported {n} rows affected, not {count}"
                        );
                    }
                }
                (Ok(_), true) => bail!("the statement succeeded, but the script expected an error"),
                (Err(e), true) => check_error_text(&e, error_contains.as_deref())?,
                (Err(e), false) => return Err(e.context("Spice rejected the statement")),
            }
        }
        Action::EditSpicepod(patch) => {
            apply_patch(instance, Some(patch))?;
            instance.spiced.write_spicepod(&instance.spicepod_yaml)?;
        }
        Action::SourceExec { source, sql } => {
            let namespace = namespace(instance, source)?;
            let src = sources.get(source)?;
            for statement in sql {
                src.exec(&namespace, &instance.sql(statement)?).await?;
            }
        }
        Action::Refresh {
            dataset,
            expect_error,
            wait: true,
            refresh_sql,
        } => {
            let refresh_sql = refresh_sql
                .as_deref()
                .map(|sql| instance.sql(sql))
                .transpose()?;
            refresh(instance, dataset, *expect_error, refresh_sql.as_deref()).await?;
        }
        Action::Refresh {
            dataset,
            wait: false,
            refresh_sql,
            ..
        } => {
            let refresh_sql = refresh_sql
                .as_deref()
                .map(|sql| instance.sql(sql))
                .transpose()?;
            instance
                .spiced
                .refresh(dataset, refresh_sql.as_deref())
                .await?;
        }
        Action::Query(q) => {
            let answer = run_query(script, instance, q, sources).await?;
            if q.show
                && let Some(answer) = &answer
            {
                for line in compare::preview(answer).lines() {
                    say!("        {line}");
                }
            }
            if let Some(answer) = answer {
                if let Some(id) = &q.id {
                    instance.named.insert(id.clone(), answer.clone());
                }
                instance.answers.insert(step.index, answer);
            }
        }
        Action::SpiceExec {
            sql,
            expect_error,
            error_contains,
        } => {
            let sql = instance.sql(sql)?;
            match (instance.spiced.exec(&sql).await, expect_error) {
                (Ok(_), false) => {}
                (Err(e), true) => {
                    check_error_text(&e, error_contains.as_deref())?;
                    return Ok(" (rejected, as expected)".into());
                }
                (Ok(answer), true) => bail!(
                    "the statement succeeded, but the script expected an error: {}",
                    one_line(&answer, 300)
                ),
                (Err(e), false) => return Err(e.context("Spice rejected the statement")),
            }
        }
        Action::Explain(e) => explain(instance, e).await?,
        Action::Generate {
            source,
            spec,
            mirror,
        } => {
            let spec: crate::generate::TableSpec =
                serde_json::from_value(template::substitute(spec, &instance.context)?)
                    .context("invalid table spec")?;
            let spec = &spec;
            let loaded = sources
                .get(source)?
                .generate(&namespace(instance, source)?, spec)
                .await?;
            if let Some(mirror) = mirror {
                // The same rows, in a database that answers for the files.
                sources
                    .get(mirror)?
                    .generate(&namespace(instance, mirror)?, spec)
                    .await?;
            }
            return Ok(format!(
                " ({:.1}s) {loaded} rows",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::AssertObjects {
            source,
            prefix,
            min,
            within,
        } => {
            let namespace = namespace(instance, source)?;
            let deadline = Instant::now() + within.unwrap_or_default();
            let found = loop {
                let found = sources.get(source)?.list(&namespace, prefix).await?;
                if found.len() >= *min || Instant::now() >= deadline {
                    break found;
                }
                tokio::time::sleep(POLL).await;
            };
            ensure!(
                found.len() >= *min,
                "expected at least {min} objects under '{prefix}' in '{source}', found {}: {found:?}",
                found.len()
            );
            return Ok(format!(" ({} objects)", found.len()));
        }
        Action::MarkMetrics(id) => {
            let text = instance.spiced.metrics().await?;
            instance.metric_marks.insert(id.clone(), text);
        }
        Action::WaitMetric(w) => {
            let shown = wait_metric(instance, w, sources).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::Slt(s) => {
            let shown = run_slt(script, instance, s, sources).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::AssertStatus(s) => {
            let shown = assert_status(instance, s).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::SpiceCli {
            args,
            output_contains,
            expect_error,
        } => {
            let shown =
                spice_cli(instance, args, output_contains.as_deref(), *expect_error).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::Http(h) => {
            let shown = http_step(instance, h).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::Background(b) => start_background(instance, b, sources)?,
        Action::Tlp(t) => {
            let shown = tlp(instance, t, sources).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::RandomQueries(r) => {
            let shown = random_queries(instance, r, sources).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::Dbgen {
            scale,
            sources: into,
        } => {
            let mut rows = 0;
            for source in into {
                let namespace = namespace(instance, source)?;
                rows = crate::tpch::load(
                    sources.get(source)?,
                    &namespace,
                    &instance.cache_dir,
                    *scale,
                )
                .await?;
            }
            return Ok(format!(
                " ({:.1}s) {rows} rows per source",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::SourceQuery {
            source,
            sql,
            rows,
            timing,
        } => {
            let shown = source_query(instance, sources, source, sql, rows, timing).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::AssertFiles { contains, lacks } => {
            let shown = assert_files(&instance.spiced.dir, contains, lacks)?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::StopBackground { id, allow } => {
            let shown = stop_background(instance, id, allow).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::AssertLog(l) => {
            let shown = assert_log(instance, l).await?;
            return Ok(format!(
                " ({:.1}s) {shown}",
                started.elapsed().as_secs_f64()
            ));
        }
        Action::StartSource(_)
        | Action::StopSource(_)
        | Action::PauseSource(_)
        | Action::UnpauseSource(_)
        | Action::Sleep(_) => unreachable!("source steps run once, not per instance"),
    }
    Ok(format!(" ({:.1}s)", started.elapsed().as_secs_f64()))
}
