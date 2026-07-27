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

//! Data-correctness for HTAP benchmarks.
//!
//! Waits for replication to fully drain (every probed table caught up to the source),
//! then asserts that source (Postgres) and Spice row counts match for each table.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::super::spice::SpiceClients;
use super::compare;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use chbench_driver::ChBenchDriver;
use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use datafusion::logical_expr::{
    Expr, LogicalPlanBuilder, TableSource, builder::LogicalTableSource,
};
use datafusion::prelude::{col, lit};
use datafusion::sql::unparser::Unparser;
use test_framework::anyhow;
use test_framework::opentelemetry::KeyValue;
use tokio::time::sleep;

/// Result of the per-column content fingerprint for one table.
#[derive(Debug, Clone)]
pub enum ContentCheck {
    /// Every compared aggregate agreed; `max_rel_delta` is the largest numeric
    /// drift seen (a fraction, e.g. `0.0` for an exact match).
    Match { max_rel_delta: f64 },
    /// At least one per-column aggregate diverged source↔Spice.
    Mismatch { detail: String },
    /// The fingerprint could not be computed (schema unavailable, counts
    /// already differ, or a probe error); the table is judged on counts alone.
    Skipped(String),
}

/// Per-table source-vs-Spice comparison: row counts plus a per-column content
/// fingerprint (non-null counts + numeric MIN/MAX) so a corruption that
/// preserves `COUNT(*)` + `MAX(_bench_ts)` — a wrong upsert value, a
/// stale/missed update, a column swap — is still caught.
#[derive(Debug, Clone)]
pub struct TableCorrectness {
    pub table: String,
    pub source_count: i64,
    pub spice_count: i64,
    pub content: ContentCheck,
}

impl TableCorrectness {
    fn counts_matched(&self) -> bool {
        self.source_count == self.spice_count
    }

    fn matched(&self) -> bool {
        self.counts_matched() && !matches!(self.content, ContentCheck::Mismatch { .. })
    }
}

/// How replication convergence was established for the report.
///
/// The drain-wait loop's per-table probes can be minutes stale at large scale
/// factors (a `COUNT(*)`/`MAX(_bench_ts)` full scan per side per table), so a
/// timed-out loop is not proof of non-convergence — the fresher final snapshot
/// gets the last word (see #11953, where a fully-matched final snapshot was
/// still reported as "replication did not converge").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Convergence {
    /// Every probed table was observed caught-up during the drain wait.
    Within(Duration),
    /// The drain wait timed out, but the final snapshot then showed every
    /// table caught up (row counts and `MAX(_bench_ts)` both match): the
    /// backlog drained later than the in-loop probes could observe. The
    /// duration is when the confirming snapshot ran — an upper bound on the
    /// real convergence time, not a measurement of it.
    ObservedAtFinalSnapshot(Duration),
    /// The final snapshot still shows divergence after the wait timed out.
    No,
}

impl Convergence {
    /// Whether replication is known to have converged (in-loop or at the
    /// final snapshot).
    pub fn converged(self) -> bool {
        !matches!(self, Convergence::No)
    }
}

/// Three-way `MAX(_bench_ts)` check for one table, run once when the drain
/// loop ends — converged or timed out (a timed-out gate is exactly when the
/// three-way comparison helps localize the cause).
///
/// The drain loop's source-side maximum comes from the driver's in-memory
/// watermark, which is a *claim* about what it committed. A bookkeeping bug there
/// would make the gate agree while replication had actually lost data, so once the
/// loop converges each table is checked against an authoritative
/// `SELECT MAX(_bench_ts)`. Reporting all three values localizes a failure:
/// watermark ≠ source means driver bookkeeping, source ≠ Spice means replication.
#[derive(Debug, Clone)]
pub struct BenchTsAudit {
    pub table: String,
    /// The driver's in-memory watermark.
    pub watermark: Option<i64>,
    /// `SELECT MAX(_bench_ts)` against the source.
    pub source_exact: Option<i64>,
    /// `MAX(_bench_ts)` from Spice.
    pub spice: Option<i64>,
    /// Set when a probe failed, in which case the values above prove nothing.
    pub error: Option<String>,
}

impl BenchTsAudit {
    /// Whether the source's real maximum reached Spice.
    fn replicated(&self) -> bool {
        self.error.is_none() && self.source_exact == self.spice
    }

    /// Whether the driver's watermark matched the source's real maximum.
    ///
    /// Delete-bearing tables answer `max_bench_ts` from the server, so their
    /// watermark trivially equals `source_exact`; this is meaningful for the
    /// tables the in-memory watermark actually serves.
    fn watermark_exact(&self) -> bool {
        self.error.is_none() && self.watermark == self.source_exact
    }

    /// A one-line failure description, or `None` if the audit passed.
    fn failure(&self) -> Option<String> {
        if let Some(e) = &self.error {
            return Some(format!(
                "{}: _bench_ts audit probe failed — {e}",
                self.table
            ));
        }
        let fmt = |v: Option<i64>| v.map_or_else(|| "empty".to_string(), |v| v.to_string());
        if !self.watermark_exact() {
            return Some(format!(
                "{}: driver watermark {} != source MAX(_bench_ts) {} — driver bookkeeping bug \
                 (spice={})",
                self.table,
                fmt(self.watermark),
                fmt(self.source_exact),
                fmt(self.spice),
            ));
        }
        if !self.replicated() {
            return Some(format!(
                "{}: source MAX(_bench_ts) {} != spice {} — replication lost data \
                 (watermark={})",
                self.table,
                fmt(self.source_exact),
                fmt(self.spice),
                fmt(self.watermark),
            ));
        }
        None
    }
}

/// Final data-correctness report produced after replication drains.
#[derive(Debug)]
pub struct CorrectnessReport {
    /// Per-table final row-count comparison.
    pub tables: Vec<TableCorrectness>,
    /// Whether (and how) replication converged.
    pub convergence: Convergence,
    /// How long the drain wait took.
    pub wait_duration: Duration,
    /// Post-convergence authoritative `MAX(_bench_ts)` audit, one entry per table.
    pub bench_ts_audit: Vec<BenchTsAudit>,
}

impl CorrectnessReport {
    /// Print a human-readable correctness summary and record OTEL metrics.
    pub fn emit(&self) {
        println!("\nData Correctness");
        match self.convergence {
            Convergence::Within(at) => {
                println!("  replication converged in {}ms", at.as_millis());
            }
            Convergence::ObservedAtFinalSnapshot(at) => {
                println!(
                    "  replication converged late — the drain wait timed out after {}ms, \
                     but the final snapshot (row counts + MAX(_bench_ts)) shows every table \
                     caught up as of {}ms",
                    self.wait_duration.as_millis(),
                    at.as_millis(),
                );
            }
            Convergence::No => {
                println!(
                    "  replication DID NOT converge within {}ms",
                    self.wait_duration.as_millis()
                );
            }
        }
        println!(
            "  {:<14} {:>14} {:>14} {:>8} {:>16}",
            "dataset", "source_count", "spice_count", "count", "content"
        );

        let mut mismatches: u64 = 0;
        for t in &self.tables {
            let matched = t.matched();
            if !matched {
                mismatches += 1;
            }
            let content = match &t.content {
                ContentCheck::Match { max_rel_delta } => format!("Δ{:.4}%", max_rel_delta * 100.0),
                ContentCheck::Mismatch { .. } => "MISMATCH".to_string(),
                ContentCheck::Skipped(_) => "skipped".to_string(),
            };
            println!(
                "  {:<14} {:>14} {:>14} {:>8} {:>16}",
                t.table,
                t.source_count,
                t.spice_count,
                if t.counts_matched() { "ok" } else { "MISMATCH" },
                content,
            );
            match &t.content {
                ContentCheck::Mismatch { detail } => println!("    └─ content: {detail}"),
                ContentCheck::Skipped(reason) => println!("    └─ content: skipped — {reason}"),
                ContentCheck::Match { .. } => {}
            }
            crate::metrics::ROW_COUNT.record(
                t.spice_count.max(0).cast_unsigned(),
                &[KeyValue::new("dataset", t.table.clone())],
            );
        }

        let audit_failures: Vec<String> = self
            .bench_ts_audit
            .iter()
            .filter_map(BenchTsAudit::failure)
            .collect();
        if self.bench_ts_audit.is_empty() {
            println!("  _bench_ts audit: not run");
        } else if audit_failures.is_empty() {
            println!(
                "  _bench_ts audit: {} table(s) — driver watermark == source MAX(_bench_ts) == spice",
                self.bench_ts_audit.len()
            );
        } else {
            println!("  _bench_ts audit: FAILED");
            for failure in &audit_failures {
                println!("    └─ {failure}");
            }
        }

        let failed =
            mismatches + u64::from(!self.convergence.converged()) + audit_failures.len() as u64;
        crate::metrics::CORRECTNESS_ROUNDS_TOTAL.record(1, &[]);
        crate::metrics::CORRECTNESS_ROUNDS_PASSED.record(u64::from(failed == 0), &[]);
        crate::metrics::CORRECTNESS_ROUNDS_FAILED.record(u64::from(failed != 0), &[]);

        if failed == 0 {
            println!("  verdict: PASSED — all {} tables match", self.tables.len());
        } else {
            println!(
                "  verdict: FAILED — {mismatches} table(s) mismatched, {} audit failure(s){}",
                audit_failures.len(),
                if self.convergence.converged() {
                    String::new()
                } else {
                    ", replication did not converge".to_string()
                }
            );
        }
    }

    /// Returns a failure message if replication did not converge or any table's
    /// row counts disagree. `None` means the gate passed.
    pub fn failure_message(&self) -> Option<String> {
        let mut problems = Vec::new();
        if !self.convergence.converged() {
            problems.push(format!(
                "replication did not converge within {}ms",
                self.wait_duration.as_millis()
            ));
        }
        problems.extend(self.bench_ts_audit.iter().filter_map(BenchTsAudit::failure));
        for t in &self.tables {
            if !t.counts_matched() {
                problems.push(format!(
                    "{}: source={} spice={} (diff {})",
                    t.table,
                    t.source_count,
                    t.spice_count,
                    t.source_count - t.spice_count,
                ));
            }
            if let ContentCheck::Mismatch { detail } = &t.content {
                problems.push(format!(
                    "{}: content fingerprint diverged — {detail}",
                    t.table
                ));
            }
        }
        if problems.is_empty() {
            None
        } else {
            Some(format!(
                "HTAP data-correctness gate failed:\n  {}",
                problems.join("\n  ")
            ))
        }
    }
}

/// A probed table is caught up when both row counts and `MAX(_bench_ts)`
/// agree between the source and Spice.
///
/// `None == None` (both sides empty) counts as caught up; an empty side
/// against a non-empty side does not. The single definition of "caught up" —
/// both the drain loop and the final snapshot delegate here so the two
/// verdict sites cannot drift apart.
fn table_caught_up(src_ts: Option<i64>, spice_ts: Option<i64>, src_n: i64, spice_n: i64) -> bool {
    src_ts == spice_ts && src_n == spice_n
}

/// One full probe of a table: source/Spice `MAX(_bench_ts)` and row counts,
/// all four queries issued concurrently. Shared by the drain loop and the
/// final snapshot so the probe wiring exists once.
async fn probe_table(
    driver: &Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
    table: &str,
) -> (
    chbench_driver::Result<Option<i64>>,
    anyhow::Result<Option<i64>>,
    chbench_driver::Result<i64>,
    anyhow::Result<i64>,
) {
    tokio::join!(
        driver.max_bench_ts(table),
        spice.max_bench_ts(table),
        driver.row_count(table),
        spice.count(table),
    )
}

/// Resolve the convergence verdict from the in-loop observation and the final
/// snapshot.
///
/// The in-loop result wins when the loop observed convergence; otherwise the
/// final snapshot — strictly fresher than any in-loop probe — decides. A
/// timed-out loop whose final snapshot shows every table caught up converged
/// *late*, not "not at all" (#11953).
fn resolve_convergence(
    in_loop: Option<Duration>,
    final_snapshot_caught_up: bool,
    observed_at: Duration,
) -> Convergence {
    match in_loop {
        Some(at) => Convergence::Within(at),
        None if final_snapshot_caught_up => Convergence::ObservedAtFinalSnapshot(observed_at),
        None => Convergence::No,
    }
}

/// Wait for replication to fully drain, then snapshot final source/Spice counts.
///
/// Polls each table until both `MAX(_bench_ts)` and `COUNT(*)` agree between the
/// source and Spice, bounded by `max_wait`. A table observed caught-up is not
/// re-probed: the source is static once OLTP stops and CDC applies in commit
/// order, so a caught-up table cannot fall behind again — and skipping it keeps
/// later passes cheap at large scale factors, where a single probe is a
/// full-table `COUNT(*)`/`MAX(_bench_ts)` scan that can take minutes. After the
/// wait (converged or timed out) a final comparison is taken for the report;
/// when the loop timed out, that fresher snapshot decides convergence (a slow
/// probe pass otherwise reports "did not converge" from observations that are
/// minutes stale — #11953).
pub async fn verify_after_drain(
    driver: Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
    tables: &[String],
    max_wait: Duration,
) -> anyhow::Result<CorrectnessReport> {
    let poll = Duration::from_secs(1);
    let start = Instant::now();
    let deadline = start + max_wait;

    println!(
        "\nWaiting up to {}s for replication to drain...",
        max_wait.as_secs()
    );

    // Tables observed caught-up so far, by index into `tables`. Latched tables
    // are not re-probed: the source is static once OLTP stops and CDC applies
    // in commit order, so a caught-up table cannot fall behind again — and a
    // re-probe is a full-table scan that can take minutes at large scale
    // factors.
    let mut latched = vec![false; tables.len()];
    let converged_at = 'wait: loop {
        for (i, table) in tables.iter().enumerate() {
            if latched[i] {
                continue;
            }
            // Re-check the deadline before every table, not once per pass: a
            // single probe can take minutes at large scale factors, and a
            // pass-granular check overshoots `max_wait` by a whole pass.
            if Instant::now() >= deadline {
                break 'wait None;
            }
            let (src_ts, spice_ts, src_n, spice_n) = probe_table(&driver, spice, table).await;
            // Sampled *after* the probe returns, so a slow probe (e.g. an
            // unindexed MAX(_bench_ts) full scan) shows up as a jump in the
            // per-table `+Ns` between adjacent lines.
            let elapsed = start.elapsed().as_secs();

            // A transient error (e.g. a momentary connection blip while replication is still draining) should not fail
            // the whole gate — treat it as "not caught up" and keep polling until max_wait.
            match (src_ts, spice_ts, src_n, spice_n) {
                (Ok(src_ts), Ok(spice_ts), Ok(src_n), Ok(spice_n)) => {
                    let count_ok = src_n == spice_n;
                    let ts_ok = src_ts == spice_ts;
                    // How far the Spice copy trails the source's newest stamped
                    // mutation (MAX(_bench_ts)), in ms; `n/a` if either side is empty.
                    let ts_lag = match (src_ts, spice_ts) {
                        (Some(s), Some(p)) => format!("{}ms", (s - p) / 1000),
                        _ => "n/a".to_string(),
                    };
                    println!(
                        "  drain-probe +{elapsed}s {table:<11} rows src={src_n} spice={spice_n} [{}] | max_bench_ts lag {ts_lag} [{}]",
                        if count_ok { "ok" } else { "behind" },
                        if ts_ok { "ok" } else { "behind" },
                    );
                    if table_caught_up(src_ts, spice_ts, src_n, spice_n) {
                        latched[i] = true;
                    }
                }
                (src_ts, spice_ts, src_n, spice_n) => {
                    eprintln!(
                        "  drain-probe +{elapsed}s {table} PROBE ERROR src_ts={src_ts:?} spice_ts={spice_ts:?} src_count={src_n:?} spice_count={spice_n:?}"
                    );
                }
            }
        }

        if latched.iter().all(|&caught_up| caught_up) {
            break Some(start.elapsed());
        }
        if Instant::now() >= deadline {
            break None;
        }
        sleep(poll).await;
    };
    let wait_duration = start.elapsed();

    // Final snapshot for the report: counts plus a per-column content
    // fingerprint. The fingerprint runs once here (not every poll) — a
    // full-table aggregate scan per probe table is too costly to repeat each
    // second. Counts converging means replication caught up; the fingerprint
    // then answers the distinct question "is the content actually correct?",
    // catching value-level corruption that COUNT(*) + MAX(_bench_ts) miss.
    //
    // When the drain wait timed out, this snapshot decides convergence: a
    // table the loop never latched is re-probed in full (counts AND
    // `MAX(_bench_ts)`), while a latched table only needs its counts to still
    // match — its max ts was already confirmed at latch time and, with the
    // source static, cannot regress (the same argument that lets the loop
    // skip re-probing it). Every table caught up here means replication
    // converged, just later than the loop observed. A probe error, or any
    // table still behind, leaves the timeout verdict standing (fail closed).
    let timed_out = converged_at.is_none();
    let mut final_snapshot_caught_up = true;
    let mut table_results = Vec::with_capacity(tables.len());
    for (i, table) in tables.iter().enumerate() {
        let (source_count, spice_count, caught_up) = if timed_out && !latched[i] {
            let (src_ts, spice_ts, src_n, spice_n) = probe_table(&driver, spice, table).await;
            let (src_n, spice_n) = (src_n?, spice_n?);
            let caught_up = match (src_ts, spice_ts) {
                (Ok(src_ts), Ok(spice_ts)) => table_caught_up(src_ts, spice_ts, src_n, spice_n),
                // A MAX(_bench_ts) probe error cannot confirm convergence.
                _ => false,
            };
            (src_n, spice_n, caught_up)
        } else {
            let (src_n, spice_n) = tokio::join!(driver.row_count(table), spice.count(table));
            let (src_n, spice_n) = (src_n?, spice_n?);
            (src_n, spice_n, src_n == spice_n)
        };
        if !caught_up {
            final_snapshot_caught_up = false;
        }
        // Only fingerprint when counts already agree — comparing content over a
        // differing row set adds no signal (the count mismatch is the finding).
        let content = if source_count == spice_count {
            table_fingerprint(&driver, spice, table).await
        } else {
            ContentCheck::Skipped("row counts differ".to_string())
        };
        table_results.push(TableCorrectness {
            table: table.clone(),
            source_count,
            spice_count,
            content,
        });
    }

    // The authoritative pass: once per run, not once per poll. This is what
    // makes a driver bookkeeping bug fail the gate rather than pass it — the
    // drain loop above compared Spice against an in-memory *claim* about the
    // source. Run for every table, including ones that already latched, and even
    // when the drain timed out: a timed-out gate is exactly when the three-way
    // comparison is most useful for localizing the cause.
    let bench_ts_audit = audit_bench_ts(&driver, spice, tables).await;

    Ok(CorrectnessReport {
        tables: table_results,
        convergence: resolve_convergence(converged_at, final_snapshot_caught_up, start.elapsed()),
        wait_duration,
        bench_ts_audit,
    })
}

/// Compare driver watermark, authoritative source `MAX(_bench_ts)`, and Spice for
/// every table.
///
/// The source-side scan is expensive (a full scan wherever `_bench_ts` is
/// unindexed), so the per-table probes run concurrently — they are independent,
/// and wall-clock is then the slowest table rather than their sum. Each table is
/// timed so a slow one is attributable.
async fn audit_bench_ts(
    driver: &Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
    tables: &[String],
) -> Vec<BenchTsAudit> {
    // Both max_bench_ts and max_bench_ts_exact are fetched even where they
    // resolve to the same scan (Postgres today; MySQL's delete-bearing
    // new_order): the watermark-vs-source comparison is then vacuous by
    // construction, and the duplicate scan is accepted — once per run, in the
    // post-drain tail where it cannot affect any measured number.
    println!("\nAuditing MAX(_bench_ts) against the source (once per run)");
    let probes = tables.iter().map(|table| async move {
        let started = Instant::now();
        let (watermark, source_exact, spice_max) = tokio::join!(
            driver.max_bench_ts(table),
            driver.max_bench_ts_exact(table),
            spice.max_bench_ts(table),
        );
        let mut audit = BenchTsAudit {
            table: table.clone(),
            watermark: None,
            source_exact: None,
            spice: None,
            error: None,
        };
        match (watermark, source_exact, spice_max) {
            (Ok(w), Ok(src), Ok(spc)) => {
                audit.watermark = w;
                audit.source_exact = src;
                audit.spice = spc;
            }
            (w, src, spc) => {
                audit.error = Some(format!(
                    "watermark={w:?} source_exact={src:?} spice={spc:?}"
                ));
            }
        }
        println!(
            "  audit {table:<11} watermark={:?} source={:?} spice={:?} ({:.1}s)",
            audit.watermark,
            audit.source_exact,
            audit.spice,
            started.elapsed().as_secs_f64(),
        );
        audit
    });
    futures::future::join_all(probes).await
}

/// Compute and compare a per-column content fingerprint for `table`.
///
/// The fingerprint is engine-agnostic: `COUNT(*)`, a non-null `COUNT(col)` for
/// every column, `MIN(col)`/`MAX(col)` for numeric columns, and `SUM(col)` for
/// exact (integer/decimal) numeric columns — the `SUM` catches interior value
/// corruption that COUNT/MIN/MAX miss. `SUM` over a *float* column is
/// deliberately excluded (floating sums are order-dependent and legitimately
/// differ across engines); text/temporal `MIN`/`MAX` are excluded (collation
/// and timestamp precision differ across engines). The identical SQL runs
/// against the source and Spice and the single result rows are compared with
/// [`compare::numeric_delta`] (exact for integer/decimal, 0.1% for float).
///
/// Probe errors are retried a bounded number of times before degrading to
/// [`ContentCheck::Skipped`] — a momentary connection/Flight blip after drain
/// must not weaken a value-level check into a false pass.
async fn table_fingerprint(
    driver: &Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
    table: &str,
) -> ContentCheck {
    // Ask Spice for the table schema via the FlightSQL GetSchema RPC rather than
    // running a SELECT against the source purely to recover the column list.
    let schema = match spice.table_schema(table).await {
        Ok(schema) => schema,
        Err(e) => return ContentCheck::Skipped(format!("schema probe failed: {e}")),
    };

    let sql = match build_fingerprint_sql(table, &schema) {
        Ok(sql) => sql,
        Err(e) => return ContentCheck::Skipped(format!("fingerprint SQL build failed: {e}")),
    };

    let (src, spc) = match fetch_fingerprint_rows(driver, spice, &sql).await {
        Ok(rows) => rows,
        Err(e) => return ContentCheck::Skipped(e.to_string()),
    };

    // Arrow/Flight streams can include leading empty batches; the aggregate
    // result is a single row, so take the first non-empty batch on each side.
    let (Some(expected_row), Some(actual_row)) = (first_non_empty(&src), first_non_empty(&spc))
    else {
        return ContentCheck::Skipped("empty fingerprint result".to_string());
    };

    // Identical SQL must project identically on both engines; a differing column
    // count is a real disagreement, so fail rather than letting `numeric_delta`
    // silently compare only the overlapping prefix.
    if expected_row.num_columns() != actual_row.num_columns() {
        return ContentCheck::Mismatch {
            detail: format!(
                "fingerprint column count differs — source {}, spice {}",
                expected_row.num_columns(),
                actual_row.num_columns(),
            ),
        };
    }

    // The fingerprint gate runs identical SQL on both engines (no schema
    // alignment), so the actual row's own types are its pre-alignment types.
    let delta = compare::numeric_delta(
        expected_row,
        actual_row,
        &compare::float_columns(actual_row),
    );
    if delta.exceeded {
        ContentCheck::Mismatch {
            detail: delta
                .worst
                .unwrap_or_else(|| "numeric aggregate diverged".to_string()),
        }
    } else {
        ContentCheck::Match {
            max_rel_delta: delta.max_rel_delta,
        }
    }
}

/// Number of times the fingerprint probe is attempted before degrading to
/// [`ContentCheck::Skipped`].
const FINGERPRINT_MAX_ATTEMPTS: u32 = 3;

/// Run the fingerprint SQL against the source and Spice, retrying transient
/// probe failures a bounded number of times before giving up.
async fn fetch_fingerprint_rows(
    driver: &Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
    sql: &str,
) -> anyhow::Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    let mut last_err = String::new();
    for attempt in 1..=FINGERPRINT_MAX_ATTEMPTS {
        let (src, spc) = tokio::join!(driver.query_arrow(sql), spice.query_arrow(sql));
        last_err = match (src, spc) {
            (Ok(src), Ok(spc)) => return Ok((src, spc)),
            (Err(e), _) => format!("source fingerprint failed: {e}"),
            (_, Err(e)) => format!("spice fingerprint failed: {e}"),
        };
        if attempt < FINGERPRINT_MAX_ATTEMPTS {
            eprintln!("fingerprint probe attempt {attempt} failed ({last_err}); retrying");
            sleep(Duration::from_millis(500)).await;
        }
    }
    anyhow::bail!(last_err)
}

/// First batch with at least one row, or `None` if every batch is empty.
fn first_non_empty(batches: &[RecordBatch]) -> Option<&RecordBatch> {
    batches.iter().find(|b| b.num_rows() > 0)
}

/// Build the engine-agnostic fingerprint aggregate SQL for `table` from its
/// Arrow `schema`, via [`LogicalPlanBuilder`] + the unparser so the generated
/// SQL is well-formed on both the source and Spice.
///
/// A `COUNT(*)`, a non-null `COUNT` for every column (catches column swaps /
/// nulled values), `MIN`/`MAX` for numeric columns, and `SUM` for exact
/// (integer/decimal) numeric columns. Each aggregate is aliased
/// (`count_<col>`, `min_<col>`, `max_<col>`, `sum_<col>`) so a divergence
/// reported by [`compare::numeric_delta`] names the offending column.
fn build_fingerprint_sql(table: &str, schema: &SchemaRef) -> anyhow::Result<String> {
    // The table source only carries the schema — the plan is unparsed, never run.
    let source = Arc::new(LogicalTableSource::new(Arc::clone(schema))) as Arc<dyn TableSource>;

    let mut aggs = vec![count(lit(1_i64)).alias("count_star")];
    for field in schema.fields() {
        let name = field.name();
        aggs.push(count(col(name.as_str())).alias(format!("count_{name}")));
        if compare::is_numeric(field.data_type()) {
            aggs.push(min(col(name.as_str())).alias(format!("min_{name}")));
            aggs.push(max(col(name.as_str())).alias(format!("max_{name}")));
            // SUM only for exact (integer/decimal) columns. It catches interior
            // value corruption — a wrong upsert value, a stale/missed update —
            // that COUNT/MIN/MAX miss (the wrong value need not be a new
            // extreme). A floating SUM is order-dependent and legitimately
            // drifts across engines, so it is never summed; an exact SUM is
            // bit-identical on both sides and compared with zero tolerance. The
            // sum relies on the f64-exactness bound documented in `compare`
            // (magnitudes below 2^53), which holds at the scale factors we run.
            if compare::is_exact_numeric(field.data_type()) {
                aggs.push(sum(col(name.as_str())).alias(format!("sum_{name}")));
            }
        }
    }

    let plan = LogicalPlanBuilder::scan(table, source, None)?
        .aggregate(Vec::<Expr>::new(), aggs)?
        .build()?;
    let sql = Unparser::default().plan_to_sql(&plan)?.to_string();
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn matched_table(name: &str) -> TableCorrectness {
        TableCorrectness {
            table: name.to_string(),
            source_count: 42,
            spice_count: 42,
            content: ContentCheck::Match { max_rel_delta: 0.0 },
        }
    }

    #[test]
    fn table_caught_up_requires_both_counts_and_max_ts() {
        // Fully caught up.
        assert!(table_caught_up(Some(1000), Some(1000), 5, 5));
        // Both sides empty is caught up (a table the workload never touched).
        assert!(table_caught_up(None, None, 0, 0));
        // Spice trailing on MAX(_bench_ts) alone (a missed/stale update
        // preserves counts) is not caught up.
        assert!(!table_caught_up(Some(1000), Some(999), 5, 5));
        // Counts trailing alone is not caught up.
        assert!(!table_caught_up(Some(1000), Some(1000), 5, 4));
        // One side empty against a non-empty side is not caught up.
        assert!(!table_caught_up(Some(1000), None, 5, 0));
        assert!(!table_caught_up(None, Some(1000), 0, 5));
    }

    #[test]
    fn convergence_resolution_prefers_in_loop_then_final_snapshot() {
        let at = Duration::from_secs(10);
        // Observed during the drain wait.
        assert_eq!(
            resolve_convergence(Some(at), false, Duration::from_secs(99)),
            Convergence::Within(at)
        );
        // Regression for #11953: the loop timed out, but the fresher final
        // snapshot shows every table caught up — converged late, not a
        // failure.
        assert_eq!(
            resolve_convergence(None, true, at),
            Convergence::ObservedAtFinalSnapshot(at)
        );
        // Timed out and the final snapshot still diverges — a real failure.
        assert_eq!(resolve_convergence(None, false, at), Convergence::No);
    }

    #[test]
    fn late_convergence_passes_the_gate() {
        // Regression for #11953: a timed-out drain wait whose final snapshot
        // fully matches must not fail the run.
        let report = CorrectnessReport {
            tables: vec![matched_table("customer"), matched_table("order_line")],
            convergence: Convergence::ObservedAtFinalSnapshot(Duration::from_secs(1002)),
            wait_duration: Duration::from_mins(15),
            bench_ts_audit: Vec::new(),
        };
        assert_eq!(report.failure_message(), None);
    }

    #[test]
    fn non_convergence_still_fails_the_gate() {
        let report = CorrectnessReport {
            tables: vec![matched_table("customer")],
            convergence: Convergence::No,
            wait_duration: Duration::from_mins(15),
            bench_ts_audit: Vec::new(),
        };
        let message = report
            .failure_message()
            .expect("non-convergence must fail the gate");
        assert!(
            message.contains("replication did not converge within 900000ms"),
            "unexpected failure message: {message}"
        );
    }

    #[test]
    fn count_mismatch_fails_even_when_converged() {
        let report = CorrectnessReport {
            tables: vec![TableCorrectness {
                table: "stock".to_string(),
                source_count: 10,
                spice_count: 9,
                content: ContentCheck::Skipped("row counts differ".to_string()),
            }],
            convergence: Convergence::Within(Duration::from_secs(5)),
            wait_duration: Duration::from_secs(5),
            bench_ts_audit: Vec::new(),
        };
        let message = report
            .failure_message()
            .expect("count mismatch must fail the gate");
        assert!(
            message.contains("stock: source=10 spice=9 (diff 1)"),
            "unexpected failure message: {message}"
        );
    }

    #[test]
    fn fingerprint_sql_counts_all_columns_minmax_numerics_sum_exact_only() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("ol_amount", DataType::Decimal128(38, 2), true),
            Field::new("ol_quantity", DataType::Int32, true),
            // A genuine float column: MIN/MAX yes, but never SUM (drifts).
            Field::new("ol_ratio", DataType::Float64, true),
            Field::new("ol_dist_info", DataType::Utf8, true),
            Field::new(
                "ol_delivery_d",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let sql = build_fingerprint_sql("order_line", &schema)
            .expect("builds fingerprint SQL")
            .to_lowercase();

        // COUNT(*) and a non-null COUNT alias for every column.
        assert!(sql.contains("count_star"), "missing COUNT(*): {sql}");
        for col in [
            "ol_amount",
            "ol_quantity",
            "ol_ratio",
            "ol_dist_info",
            "ol_delivery_d",
        ] {
            assert!(
                sql.contains(&format!("count_{col}")),
                "missing COUNT for {col}: {sql}"
            );
        }
        // MIN/MAX for every numeric column, including the float.
        for col in ["ol_amount", "ol_quantity", "ol_ratio"] {
            assert!(
                sql.contains(&format!("min_{col}")) && sql.contains(&format!("max_{col}")),
                "missing MIN/MAX for numeric {col}: {sql}"
            );
        }
        // Not for text or temporal (cross-engine collation / precision differ).
        assert!(
            !sql.contains("min_ol_dist_info") && !sql.contains("max_ol_dist_info"),
            "{sql}"
        );
        assert!(
            !sql.contains("min_ol_delivery_d") && !sql.contains("max_ol_delivery_d"),
            "{sql}"
        );
        // SUM only for exact (integer/decimal) numerics — catches interior
        // value corruption without the order-dependent drift of a float sum.
        assert!(
            sql.contains("sum_ol_amount") && sql.contains("sum_ol_quantity"),
            "missing SUM for exact numeric columns: {sql}"
        );
        // Never SUM the float, text, or temporal columns.
        for col in ["ol_ratio", "ol_dist_info", "ol_delivery_d"] {
            assert!(
                !sql.contains(&format!("sum_{col}")),
                "unexpected SUM for {col}: {sql}"
            );
        }
        // The source table is referenced.
        assert!(sql.contains("order_line"), "{sql}");
    }
}
