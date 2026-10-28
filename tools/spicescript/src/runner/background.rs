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

//! Work that runs in the background until a `stop-background` step.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde_json::Value;

use crate::script::{BackgroundStep, BackgroundWork};

use super::{Background, BackgroundStats, Instance, Sources, namespace, one_line};

/// Starts `step`'s work in the background: `concurrency` loops, each running
/// the work once per `every` (back to back when unset) until a
/// `stop-background` with the same id. `${round}` in the SQL is each loop's
/// iteration number, counted across the loops.
pub(super) fn start_background(
    instance: &mut Instance,
    spec: &BackgroundStep,
    sources: &Sources,
) -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    const ROUND: &str = "\u{0}round\u{0}";
    ensure!(
        !instance.background.contains_key(&spec.id),
        "background '{}' is already running",
        spec.id
    );
    let halt = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(parking_lot::Mutex::new(BackgroundStats::default()));
    let round = Arc::new(AtomicU64::new(0));
    let (sql, target) = match &spec.work {
        BackgroundWork::Query(sql) => (sql.clone(), None),
        BackgroundWork::SourceExec { source, sql } => (
            sql.clone(),
            Some((
                Arc::clone(
                    sources
                        .by_name
                        .get(source)
                        .ok_or_else(|| anyhow!("unknown source '{source}'"))?,
                ),
                namespace(instance, source)?,
            )),
        ),
    };
    let sql = instance.sql(&sql.replace("${round}", ROUND))?;
    let endpoint = format!("{}/v1/sql", instance.spiced.http_endpoint());
    let mut tasks = Vec::new();
    for _ in 0..spec.concurrency {
        let (halt, stats, round, sql, target, endpoint, every, expect) = (
            Arc::clone(&halt),
            Arc::clone(&stats),
            Arc::clone(&round),
            sql.clone(),
            target.clone(),
            endpoint.clone(),
            spec.every,
            spec.expect.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let client = reqwest::Client::new();
            while !halt.load(Ordering::Relaxed) {
                let n = round.fetch_add(1, Ordering::Relaxed) + 1;
                let sql = sql.replace(ROUND, &n.to_string());
                let began = Instant::now();
                let outcome: Result<()> = match &target {
                    Some((source, namespace)) => source.exec(namespace, &sql).await,
                    None => {
                        async {
                            let response = client
                                .post(&endpoint)
                                .header("Content-Type", "text/plain")
                                .body(sql.clone())
                                .send()
                                .await?;
                            let code = response.status();
                            // A body cut off by a kill is a lost connection, not an
                            // empty answer.
                            let body = response.text().await.map_err(|e| {
                                anyhow::anyhow!("connection lost while reading the answer: {e:#}")
                            })?;
                            ensure!(code.is_success(), "{code}: {}", one_line(&body, 200));
                            if let Some(expect) = &expect {
                                check_invariant(&body, expect)?;
                            }
                            Ok(())
                        }
                        .await
                    }
                };
                {
                    let mut stats = stats.lock();
                    stats.iterations += 1;
                    stats.latencies.push(began.elapsed());
                    if let Err(e) = outcome {
                        stats.errors.push((chrono::Utc::now(), format!("{e:#}")));
                    }
                }
                if let Some(every) = every {
                    tokio::time::sleep(every.saturating_sub(began.elapsed())).await;
                }
            }
        }));
    }
    instance.background.insert(
        spec.id.clone(),
        Background {
            stop: halt,
            tasks,
            stats,
        },
    );
    Ok(())
}

/// Fails unless a `/v1/sql` JSON answer is exactly one row holding `expect`.
pub(super) fn check_invariant(body: &str, expect: &BTreeMap<String, Option<String>>) -> Result<()> {
    let rows: Vec<serde_json::Map<String, Value>> = serde_json::from_str(body)
        .with_context(|| format!("not a JSON row array: {}", one_line(body, 200)))?;
    let [row] = rows.as_slice() else {
        bail!(
            "expected one row, got {}: {}",
            rows.len(),
            one_line(body, 200)
        );
    };
    for (column, want) in expect {
        let got = match row.get(column) {
            None => bail!("no column '{column}' in {}", one_line(body, 200)),
            Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(other) => Some(other.to_string()),
        };
        ensure!(
            &got == want,
            "invariant broken: {column} = {got:?}, expected {want:?}"
        );
    }
    Ok(())
}

/// Stops a background task, waiting for each loop's iteration in flight, and
/// fails if any iteration failed with an error no `allow` pattern matches.
pub(super) async fn stop_background(
    instance: &mut Instance,
    id: &str,
    allow: &[regex::Regex],
) -> Result<String> {
    let background = instance
        .background
        .remove(id)
        .ok_or_else(|| anyhow!("no background '{id}' is running"))?;
    background
        .stop
        .store(true, std::sync::atomic::Ordering::Relaxed);
    for task in background.tasks {
        task.await.context("background task panicked")?;
    }
    let stats = background.stats.lock();
    let mut latencies = stats.latencies.clone();
    latencies.sort_unstable();
    let p99 = latencies
        .get((latencies.len() * 99).div_ceil(100).saturating_sub(1))
        .copied()
        .unwrap_or_default();
    let unexpected: Vec<&(chrono::DateTime<chrono::Utc>, String)> = stats
        .errors
        .iter()
        .filter(|(_, e)| !allow.iter().any(|a| a.is_match(e)))
        .collect();
    // When, in the clock `spiced.log` uses, so the failures can be lined up
    // with what the instance logged then. On a line of its own: a ledger
    // signature is the first line, and the times differ from run to run.
    let at =
        |t: &chrono::DateTime<chrono::Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    ensure!(
        unexpected.is_empty(),
        "{} of {} iterations failed; first: {}\nfailed from {} to {}",
        unexpected.len(),
        stats.iterations,
        unexpected
            .first()
            .map_or(String::new(), |(_, e)| one_line(e, 300)),
        unexpected.first().map_or(String::new(), |(t, _)| at(t)),
        unexpected.last().map_or(String::new(), |(t, _)| at(t)),
    );
    ensure!(
        stats.iterations > 0,
        "background '{id}' never ran an iteration"
    );
    // The commonest allowed errors, numbers aside, so that what was allowed
    // is seen rather than only counted.
    let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
    for (_, e) in &stats.errors {
        let kind: String = one_line(e, 160)
            .chars()
            .map(|c| if c.is_ascii_digit() { '#' } else { c })
            .collect();
        *kinds.entry(kind).or_default() += 1;
    }
    let mut kinds: Vec<(String, usize)> = kinds.into_iter().collect();
    kinds.sort_by_key(|k| std::cmp::Reverse(k.1));
    let mut common = String::new();
    for (kind, n) in kinds.iter().take(2) {
        let _ = write!(common, "; {n}× {kind}");
    }
    Ok(format!(
        "{} iterations, {} allowed errors, p99 {:.0}ms{common}",
        stats.iterations,
        stats.errors.len(),
        p99.as_secs_f64() * 1000.0
    ))
}
