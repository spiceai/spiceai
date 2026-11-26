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

//! `load` steps: many clients querying at once, every answer checked, with
//! faults applied while they run.
//!
//! Each client holds a Flight connection of its own and runs the load's
//! queries in turn. Every answer is compared with the query's expected one,
//! which is asked of the oracle once, before the load starts, and typed with
//! the schema of the first answer. A wrong answer always fails the step. An
//! error fails it unless the query was in flight within a fault's window:
//! from when the fault began until `recover-within` after it ended.

use std::fmt::Write as _;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::RecordBatch;
use flight_client::FlightClient;
use serde_json::Value;

use crate::compare;
use crate::script::{Expect, LoadEnd, LoadStep, Script, Step, Tier};
use crate::source::TextResult;
use crate::spiced::{FlightTarget, answer_with_schema};

use super::{Instance, Sources, applies, namespace, note_broken, one_line, when_matches};

/// How long a client waits after an error before its next query, as a
/// retrying application would. Without it a client of a `spiced` that fails
/// fast spins, and its errors crowd out the answers being measured.
const RETRY_PAUSE: Duration = Duration::from_millis(100);

/// Answers needed beyond a percentile for it to be reported: P99 needs
/// 1,000 answers, P99.9 10,000.
const BEYOND_PERCENTILE: usize = 10;

/// What a query's answers are compared with, before it is typed.
enum Expected {
    /// The oracle's text answer.
    Oracle(TextResult),
    Rows(Vec<Vec<Value>>),
    /// An earlier answer of the same instance (`same-as`).
    SameAs(Vec<RecordBatch>),
}

/// One query of a running load.
struct Query {
    sql: String,
    expected: Expected,
    expect_empty: bool,
    /// The expected answer typed with the schema of the first answer, or why
    /// it could not be.
    typed: OnceLock<std::result::Result<RecordBatch, String>>,
}

impl Query {
    /// `None` when `actual` is the expected answer, else what differs.
    fn mismatch(&self, label: &str, actual: &[RecordBatch]) -> Option<String> {
        let schema = actual.first().map(RecordBatch::schema)?;
        let typed = self.typed.get_or_init(|| {
            match &self.expected {
                Expected::Oracle(text) => compare::type_oracle(text, &schema),
                Expected::Rows(rows) => compare::literal_rows(rows, &schema),
                Expected::SameAs(batches) => arrow::compute::concat_batches(&schema, batches)
                    .context("the answer's schema changed since the earlier query"),
            }
            .map_err(|e| format!("{e:#}"))
        });
        let expected = match typed {
            Ok(expected) => expected,
            Err(e) => return Some(e.clone()),
        };
        match compare::compare(
            label,
            &self.sql,
            actual,
            std::slice::from_ref(expected),
            self.expect_empty,
        ) {
            Ok(None) => None,
            Ok(Some(report)) => Some(report),
            Err(e) => Some(format!("{e:#}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Right,
    /// What differs from the expected answer.
    Wrong(String),
    Failed(String),
}

/// One query a client ran: which, when it began and how long it took, both
/// from the start of the load.
#[derive(Debug, Clone)]
struct Sample {
    query: usize,
    began: Duration,
    took: Duration,
    outcome: Outcome,
}

/// A fault that ran: when it began and how long it took, from the start of
/// the load.
#[derive(Debug, Clone)]
struct FaultRun {
    what: String,
    began: Duration,
    took: Duration,
}

/// What every client of a load shares.
struct Shared {
    target: FlightTarget,
    queries: Arc<[Query]>,
    label: String,
    start: Instant,
    end: LoadEnd,
    timeout: Duration,
    /// Queries started so far, across clients; each client's next query is
    /// the load's `issued`-th, so the mix is the same whatever the timing.
    issued: AtomicU64,
    /// Set when a fault failed: the clients stop at once.
    halt: AtomicBool,
}

/// Runs a load on the `spiced` on `tier`, with its faults on the instance.
pub(super) async fn run_load(
    script: &Script,
    instance: &mut Instance,
    load: &LoadStep,
    tier: Tier,
    sources: &Sources,
) -> Result<String> {
    // Each query's expected answer is asked of the oracle once, here: a
    // load's queries read data that does not change while it runs.
    let mut queries = Vec::with_capacity(load.queries.len());
    for q in &load.queries {
        let sql = instance.sql(&q.sql)?;
        let expect = q
            .expect
            .as_ref()
            .or(load.expect.as_ref())
            .or(script.defaults.expect.as_ref());
        let expected = match expect {
            Some(Expect {
                oracle: Some(oracle),
                rows: None,
                same_as: None,
            }) => {
                let oracle_sql = instance.sql(q.oracle_sql.as_deref().unwrap_or(&q.sql))?;
                let namespace = namespace(instance, oracle)?;
                Expected::Oracle(
                    sources
                        .get(oracle)?
                        .query(&namespace, &oracle_sql)
                        .await
                        .with_context(|| {
                            format!(
                                "the oracle '{oracle}' rejected `{}`",
                                one_line(&oracle_sql, 200)
                            )
                        })?,
                )
            }
            Some(Expect {
                oracle: None,
                rows: Some(rows),
                same_as: None,
            }) => Expected::Rows(rows.clone()),
            Some(Expect {
                oracle: None,
                rows: None,
                same_as: Some(id),
            }) => Expected::SameAs(instance.named.get(id).cloned().with_context(|| {
                format!("no answer was recorded for query '{id}' on this instance")
            })?),
            None
            | Some(Expect {
                oracle: None,
                rows: None,
                same_as: None,
            }) => bail!(
                "load query `{}` has nothing to be checked against; a load checks every answer, so give it an `expect` (oracle, rows or same-as), or set `defaults.expect`",
                one_line(&q.sql, 120)
            ),
            Some(_) => bail!("`expect` takes exactly one of `oracle`, `rows` or `same-as`"),
        };
        queries.push(Query {
            sql,
            expected,
            expect_empty: q.expect_empty,
            typed: OnceLock::new(),
        });
    }
    let shared = Arc::new(Shared {
        target: instance.spiced_on(tier).flight_target(),
        queries: queries.into(),
        label: instance.label.clone(),
        start: Instant::now(),
        end: load.end,
        timeout: load
            .query_timeout
            .or(script.defaults.query_timeout)
            .unwrap_or(Duration::from_mins(2)),
        issued: AtomicU64::new(0),
        halt: AtomicBool::new(false),
    });
    let clients: Vec<_> = (0..load.clients)
        .map(|_| tokio::spawn(client(Arc::clone(&shared))))
        .collect();

    // The faults run on the instance as the steps around the load see it,
    // each at its offset or once the one before has finished.
    let mut faults = Vec::new();
    let mut fault_error = None;
    for fault in &load.faults {
        if !fault_applies(&fault.step, instance) {
            continue;
        }
        tokio::time::sleep_until(tokio::time::Instant::from_std(shared.start + fault.at)).await;
        let began = shared.start.elapsed();
        let mut outcome = Box::pin(super::steps::run_instance_step(
            script,
            instance,
            &fault.step,
            sources,
        ))
        .await;
        note_broken(instance, &fault.step, &mut outcome).await;
        let what = super::steps::describe_step(&fault.step);
        if let Err(e) = outcome {
            fault_error = Some(e.context(format!(
                "the load's fault `{what}` at +{:.1}s failed",
                began.as_secs_f64()
            )));
            shared.halt.store(true, Ordering::Relaxed);
            break;
        }
        faults.push(FaultRun {
            what,
            began,
            took: shared.start.elapsed().saturating_sub(began),
        });
    }
    let mut samples = Vec::new();
    for handle in clients {
        samples.extend(handle.await.context("a load client panicked")?);
    }
    let ran = shared.start.elapsed();
    if let Some(error) = fault_error {
        return Err(error);
    }
    let sqls: Vec<&str> = shared.queries.iter().map(|q| q.sql.as_str()).collect();
    judge(load, &sqls, &samples, &faults, ran)
}

/// Whether a fault runs on this instance: its `only`/`except`, `when` and
/// `cluster` hold.
fn fault_applies(step: &Step, instance: &Instance) -> bool {
    let combo = instance.context["matrix"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    let clustered = !instance.executors.is_empty();
    applies(step, instance)
        && when_matches(&step.when, &combo)
        && step.cluster.is_none_or(|c| c == clustered)
}

/// One client: queries until the load ends, on a connection of its own. The
/// connection is kept across errors, as an application's would be: its
/// channel connects again by itself after the server goes away, while
/// opening a new one per error would run the host out of ports under a
/// long outage.
async fn client(shared: Arc<Shared>) -> Vec<Sample> {
    let mut samples = Vec::new();
    let mut connection: Option<FlightClient> = None;
    let mut reported_wrong = false;
    let count = u64::try_from(shared.queries.len()).unwrap_or(u64::MAX);
    while !shared.halt.load(Ordering::Relaxed) {
        let n = shared.issued.fetch_add(1, Ordering::Relaxed);
        let over = match shared.end {
            LoadEnd::Duration(d) => shared.start.elapsed() >= d,
            LoadEnd::Count(total) => n >= total,
        };
        if over {
            break;
        }
        let index = usize::try_from(n % count).unwrap_or_default();
        let query = &shared.queries[index];
        let began = shared.start.elapsed();
        let timeout = shared.timeout;
        let answer = tokio::time::timeout(timeout, async {
            if connection.is_none() {
                connection = Some(shared.target.connect().await?);
            }
            match &connection {
                Some(client) => answer_with_schema(client, &query.sql).await,
                None => Err(anyhow!("no connection")),
            }
        })
        .await
        .unwrap_or_else(|_| Err(anyhow!("no answer within {timeout:?}")));
        let took = shared.start.elapsed().saturating_sub(began);
        let outcome = match answer {
            Ok(batches) => match query.mismatch(&shared.label, &batches) {
                None => Outcome::Right,
                // The first wrong answer is reported whole; the rest are
                // counted, and their first lines kept.
                Some(report) if !reported_wrong => {
                    reported_wrong = true;
                    Outcome::Wrong(report)
                }
                Some(report) => Outcome::Wrong(one_line(&report, 200)),
            },
            Err(e) => Outcome::Failed(one_line(&format!("{e:#}"), 300)),
        };
        let failed = matches!(outcome, Outcome::Failed(_));
        samples.push(Sample {
            query: index,
            began,
            took,
            outcome,
        });
        if failed {
            tokio::time::sleep(RETRY_PAUSE).await;
        }
    }
    samples
}

/// The latency below which `per_mille`/1000 of `sorted` fall (nearest rank),
/// when there are at least [`BEYOND_PERCENTILE`] samples beyond it.
fn percentile(sorted: &[Duration], per_mille: usize) -> Option<Duration> {
    let beyond = sorted.len() * (1000 - per_mille) / 1000;
    if beyond < BEYOND_PERCENTILE {
        return None;
    }
    let rank = (sorted.len() * per_mille).div_ceil(1000);
    sorted.get(rank.saturating_sub(1)).copied()
}

/// The answers a percentile needs.
fn needed_for(per_mille: usize) -> usize {
    BEYOND_PERCENTILE * 1000 / (1000 - per_mille)
}

fn shown(latency: Option<Duration>, per_mille: usize) -> String {
    match latency {
        Some(d) => format!("{:.1}ms", d.as_secs_f64() * 1000.0),
        None => format!("n/a (needs {} answers)", needed_for(per_mille)),
    }
}

/// Judges a finished load: every answer right, every error within a fault's
/// window, an answer after the last window, and the bounds the step sets.
fn judge(
    load: &LoadStep,
    sqls: &[&str],
    samples: &[Sample],
    faults: &[FaultRun],
    ran: Duration,
) -> Result<String> {
    let recover = load.recover_within.unwrap_or_default();
    let windows: Vec<(Duration, Duration)> = faults
        .iter()
        .map(|f| (f.began, f.began + f.took + recover))
        .collect();
    // A query was in a window if it was in flight at any time within it.
    let in_window = |s: &Sample| {
        windows
            .iter()
            .any(|&(from, to)| s.began <= to && s.began + s.took >= from)
    };
    let mut latencies: Vec<Duration> = samples
        .iter()
        .filter(|s| s.outcome == Outcome::Right)
        .map(|s| s.took)
        .collect();
    latencies.sort_unstable();
    let right = latencies.len();
    // Each client's samples follow one another, so these are put in order of
    // when they began, for the first to be the earliest.
    let mut wrong: Vec<&Sample> = samples
        .iter()
        .filter(|s| matches!(s.outcome, Outcome::Wrong(_)))
        .collect();
    wrong.sort_by_key(|s| s.began);
    let (allowed, mut unallowed): (Vec<&Sample>, Vec<&Sample>) = samples
        .iter()
        .filter(|s| matches!(s.outcome, Outcome::Failed(_)))
        .partition(|s| in_window(s));
    unallowed.sort_by_key(|s| s.began);
    let seconds = ran.as_secs_f64().max(f64::EPSILON);
    #[expect(
        clippy::cast_precision_loss,
        reason = "a count of answers is far below 2^52"
    )]
    let qps = right as f64 / seconds;
    let (p99, p999) = (percentile(&latencies, 990), percentile(&latencies, 999));
    let errors = allowed.len() + unallowed.len();
    let mut stats = format!(
        "{} clients over {:.1}s: {} queries, {qps:.1} right answers/s, p99 {}, p99.9 {} (of {right} right answers), {} wrong, {errors} errors",
        load.clients,
        ran.as_secs_f64(),
        samples.len(),
        shown(p99, 990),
        shown(p999, 999),
        wrong.len(),
    );
    if !faults.is_empty() {
        let _ = write!(stats, " ({} within fault windows)", allowed.len());
    }
    let mut lines = vec![stats];
    let last_window_end = windows.iter().map(|&(_, to)| to).max();
    for (fault, (_, to)) in faults.iter().zip(&windows) {
        lines.push(format!(
            "fault {} at +{:.1}s took {:.1}s; errors allowed until +{:.1}s",
            fault.what,
            fault.began.as_secs_f64(),
            fault.took.as_secs_f64(),
            to.as_secs_f64()
        ));
    }
    if let (Some(first), Some(last)) = (windows.first(), last_window_end) {
        for (name, from, to) in [
            ("before faults", Duration::ZERO, first.0),
            ("fault windows", first.0, last),
            ("after faults", last, ran.max(last)),
        ] {
            let phase: Vec<&Sample> = samples
                .iter()
                .filter(|s| s.began >= from && s.began < to)
                .collect();
            let phase_right = phase.iter().filter(|s| s.outcome == Outcome::Right).count();
            #[expect(
                clippy::cast_precision_loss,
                reason = "a count of answers is far below 2^52"
            )]
            let rate = phase_right as f64 / to.saturating_sub(from).as_secs_f64().max(f64::EPSILON);
            lines.push(format!(
                "{name} +{:.1}s..+{:.1}s: {phase_right} right answers ({rate:.1}/s), {} errors",
                from.as_secs_f64(),
                to.as_secs_f64(),
                phase
                    .iter()
                    .filter(|s| matches!(s.outcome, Outcome::Failed(_)))
                    .count()
            ));
        }
    }

    // The commonest errors, numbers aside, so that what failed is seen
    // rather than only counted.
    let mut kinds: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for s in samples {
        if let Outcome::Failed(error) = &s.outcome {
            let kind: String = one_line(error, 160)
                .chars()
                .map(|c| if c.is_ascii_digit() { '#' } else { c })
                .collect();
            *kinds.entry(kind).or_default() += 1;
        }
    }
    let mut kinds: Vec<(String, usize)> = kinds.into_iter().collect();
    kinds.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    for (kind, n) in kinds.iter().take(3) {
        lines.push(format!("{n}× {kind}"));
    }

    // Each problem's first line is its signature in the ledger, so it names
    // the kind of failure and the query, not counts or times.
    let mut problems = Vec::new();
    if let Some(first) = wrong.first()
        && let Outcome::Wrong(report) = &first.outcome
    {
        let mut report_lines = report.lines();
        problems.push(format!(
            "load query {} answered wrong: {}\n{} of {} answers were wrong, the first at +{:.1}s\nSQL: {}\n{}",
            first.query + 1,
            report_lines.next().unwrap_or_default(),
            wrong.len(),
            samples.len(),
            first.began.as_secs_f64(),
            sqls.get(first.query).copied().unwrap_or_default(),
            report_lines.collect::<Vec<_>>().join("\n")
        ));
    }
    if let Some(first) = unallowed.first()
        && let Outcome::Failed(error) = &first.outcome
    {
        problems.push(format!(
            "load query {} failed{}: {error}\n{} of {} queries failed{}, the first at +{:.1}s and the last at +{:.1}s\nSQL: {}",
            first.query + 1,
            if faults.is_empty() {
                ""
            } else {
                " outside the fault windows"
            },
            unallowed.len(),
            samples.len(),
            if faults.is_empty() {
                ""
            } else {
                " outside them"
            },
            first.began.as_secs_f64(),
            unallowed.last().map_or(0.0, |s| s.began.as_secs_f64()),
            sqls.get(first.query).copied().unwrap_or_default(),
        ));
    }
    if let Some(end) = last_window_end
        && !samples
            .iter()
            .any(|s| s.outcome == Outcome::Right && s.began >= end)
    {
        problems.push(format!(
            "the load shows no recovery: no query that started after the last fault window answered\nthe last window ended at +{:.1}s and the load at +{:.1}s; lengthen `duration` past the window, or look at why the queries after it failed",
            end.as_secs_f64(),
            ran.as_secs_f64()
        ));
    }
    if let Some(min) = load.min_qps
        && qps < min
    {
        problems.push(format!(
            "throughput is below `min-qps`: {qps:.1} right answers/s, under {min}"
        ));
    }
    for (bound, latency, per_mille, name) in [
        (load.max_p99, p99, 990, "p99"),
        (load.max_p999, p999, 999, "p99.9"),
    ] {
        let Some(bound) = bound else {
            continue;
        };
        match latency {
            None => problems.push(format!(
                "`max-{}` cannot be judged: it needs {} right answers, and the load had {right}",
                name.replace('.', ""),
                needed_for(per_mille)
            )),
            Some(latency) if latency > bound => problems.push(format!(
                "{name} latency exceeds `max-{}`: {}, over {bound:?}",
                name.replace('.', ""),
                shown(Some(latency), per_mille)
            )),
            Some(_) => {}
        }
    }
    let detail = lines.join("\n        ");
    if problems.is_empty() {
        return Ok(format!(" ({:.1}s) {detail}", ran.as_secs_f64()));
    }
    bail!("{}\n{detail}", problems.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(query: usize, began_ms: u64, took_ms: u64, outcome: Outcome) -> Sample {
        Sample {
            query,
            began: Duration::from_millis(began_ms),
            took: Duration::from_millis(took_ms),
            outcome,
        }
    }

    fn load(recover_within: Option<Duration>) -> LoadStep {
        LoadStep {
            queries: Vec::new(),
            corpus: None,
            clients: 2,
            end: LoadEnd::Duration(Duration::from_secs(10)),
            expect: None,
            query_timeout: None,
            faults: Vec::new(),
            recover_within,
            min_qps: None,
            max_p99: None,
            max_p999: None,
        }
    }

    #[test]
    fn percentiles_need_ten_samples_beyond_them() {
        let sorted: Vec<Duration> = (1..=1000).map(Duration::from_millis).collect();
        assert_eq!(percentile(&sorted, 990), Some(Duration::from_millis(990)));
        assert_eq!(percentile(&sorted, 999), None, "1 sample beyond p99.9");
        assert_eq!(
            percentile(&sorted[..999], 990),
            None,
            "9 samples beyond p99"
        );
        let many: Vec<Duration> = (1..=10_000).map(Duration::from_millis).collect();
        assert_eq!(percentile(&many, 999), Some(Duration::from_millis(9990)));
        assert_eq!((needed_for(990), needed_for(999)), (1000, 10_000));
    }

    #[test]
    fn errors_pass_only_in_flight_within_a_fault_window_and_recovery_is_required() {
        let fault = FaultRun {
            what: "kill-executor 0".into(),
            began: Duration::from_secs(2),
            took: Duration::from_secs(1),
        };
        // Window: +2s to +3s, plus 1s to recover: +4s.
        let step = load(Some(Duration::from_secs(1)));
        let sqls = ["SELECT 1", "SELECT 2"];
        let right = |at| sample(0, at, 5, Outcome::Right);
        let failed = |at, took| sample(1, at, took, Outcome::Failed("boom".into()));
        // An error in flight across the window's start, one inside, one at
        // its end, and an answer after it.
        let within = [
            right(0),
            failed(1500, 600),
            failed(2500, 10),
            failed(4000, 10),
            right(4100),
        ];
        let shown = judge(
            &step,
            &sqls,
            &within,
            std::slice::from_ref(&fault),
            Duration::from_secs(5),
        )
        .expect("errors within the window pass");
        assert!(
            shown.contains("3 errors (3 within fault windows)"),
            "{shown}"
        );
        assert!(
            shown.contains("fault kill-executor 0 at +2.0s took 1.0s; errors allowed until +4.0s"),
            "{shown}"
        );
        // An error after the window fails the load, naming the query and the
        // earliest and latest such errors, whatever client saw them.
        let after = [
            right(0),
            failed(4900, 10),
            failed(2500, 10),
            failed(4200, 10),
            right(4300),
        ];
        let err = judge(
            &step,
            &sqls,
            &after,
            std::slice::from_ref(&fault),
            Duration::from_secs(5),
        )
        .expect_err("an error after the window fails");
        let text = format!("{err:#}");
        let lines: Vec<&str> = text.lines().take(2).collect();
        assert_eq!(
            lines,
            [
                "load query 2 failed outside the fault windows: boom",
                "2 of 5 queries failed outside them, the first at +4.2s and the last at +4.9s"
            ]
        );
        // No answer after the window: recovery is not shown.
        let unrecovered = [right(0), failed(2500, 10)];
        let err = judge(
            &step,
            &sqls,
            &unrecovered,
            std::slice::from_ref(&fault),
            Duration::from_secs(5),
        )
        .expect_err("no recovery fails");
        assert!(
            format!("{err:#}").starts_with("the load shows no recovery"),
            "{err:#}"
        );
        // Without faults any error fails.
        let err = judge(
            &load(None),
            &sqls,
            &[right(0), failed(100, 10)],
            &[],
            Duration::from_secs(1),
        )
        .expect_err("an error without faults fails");
        assert_eq!(
            format!("{err:#}").lines().next(),
            Some("load query 2 failed: boom")
        );
    }

    #[test]
    fn a_wrong_answer_fails_even_within_a_fault_window() {
        let fault = FaultRun {
            what: "restart".into(),
            began: Duration::from_secs(1),
            took: Duration::from_secs(1),
        };
        let samples = [
            sample(
                0,
                1500,
                5,
                Outcome::Wrong("RowCountMismatch { expected: 3, actual: 2 }\n--- spice".into()),
            ),
            sample(0, 3000, 5, Outcome::Right),
        ];
        let err = judge(
            &load(Some(Duration::from_secs(10))),
            &["SELECT * FROM t"],
            &samples,
            &[fault],
            Duration::from_secs(4),
        )
        .expect_err("a wrong answer fails");
        let text = format!("{err:#}");
        assert_eq!(
            text.lines().next(),
            Some("load query 1 answered wrong: RowCountMismatch { expected: 3, actual: 2 }")
        );
        assert!(
            text.contains("1 of 2 answers were wrong, the first at +1.5s"),
            "{text}"
        );
        assert!(text.contains("SQL: SELECT * FROM t"), "{text}");
    }

    #[test]
    fn bounds_fail_only_when_set_and_judge_only_with_enough_answers() {
        let samples: Vec<Sample> = (0..2000)
            .map(|i| sample(0, i, if i >= 1990 { 900 } else { 10 }, Outcome::Right))
            .collect();
        let mut step = load(None);
        let ok =
            judge(&step, &["SELECT 1"], &samples, &[], Duration::from_secs(2)).expect("no bounds");
        assert!(
            ok.contains("1000.0 right answers/s, p99 10.0ms, p99.9 n/a (needs 10000 answers) (of 2000 right answers)"),
            "{ok}"
        );
        step.min_qps = Some(2000.0);
        step.max_p99 = Some(Duration::from_millis(5));
        step.max_p999 = Some(Duration::from_millis(5));
        let err = judge(&step, &["SELECT 1"], &samples, &[], Duration::from_secs(2))
            .expect_err("bounds missed");
        let problems: Vec<String> = format!("{err:#}")
            .lines()
            .take(3)
            .map(str::to_string)
            .collect();
        assert_eq!(
            problems,
            [
                "throughput is below `min-qps`: 1000.0 right answers/s, under 2000",
                "p99 latency exceeds `max-p99`: 10.0ms, over 5ms",
                "`max-p999` cannot be judged: it needs 10000 right answers, and the load had 2000",
            ]
        );
    }
}
