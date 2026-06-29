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

//! Post-workload query diagnostics for the HTAP command (`--capture-explain`).
//!
//! Runs against the **live** spiced that just served the benchmark — its
//! accelerated tables are still fully loaded in memory — so the captured plans
//! and the eager-aggregation rule's accept/decline decisions reflect the real
//! benchmarked dataset. (Re-launching a fresh spiced after the run instead sees
//! empty, not-yet-replicated accelerated tables, which makes every query plan
//! trivial and the decline map meaningless.)
//!
//! Writes into the capture directory:
//!   * `explain_structure.txt` — `EXPLAIN` (logical + physical plan shape)
//!   * `explain_analyze.txt`   — `EXPLAIN ANALYZE` (per-operator rows/timings)
//!   * `decline_map.txt`       — per-query eager push-site count + the rule's
//!     `eager_aggregation` accept/decline log lines
//!   * `spiced.log`            — the teed spiced log (written by the runtime)
//!
//! Best-effort: any error is surfaced to the caller, which logs it without
//! failing the run.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::Path,
    time::Duration,
};

use test_framework::{
    anyhow,
    arrow::util::pretty::pretty_format_batches,
    queries::{QueryOverrides, get_chbench_test_queries},
    spiced::SpicedInstance,
};

use super::spice::SpiceClients;

/// `EXPLAIN ANALYZE` executes the query. Data is hot so most are quick, but a
/// pathological one shouldn't be able to eat the job timeout — bound each.
const ANALYZE_TIMEOUT: Duration = Duration::from_secs(90);

/// The `log::info!` target the eager-aggregation rule logs its decisions under.
const RULE_TARGET: &str = "eager_aggregation:";

pub(crate) async fn run(
    clients: &SpiceClients,
    spiced: &SpicedInstance,
    out_dir: &Path,
    overrides: Option<QueryOverrides>,
) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    let log_path = spiced.log_capture_path();
    let queries = get_chbench_test_queries(overrides);

    let mut structure = fs::File::create(out_dir.join("explain_structure.txt"))?;
    let mut analyze = fs::File::create(out_dir.join("explain_analyze.txt"))?;
    let mut decline = fs::File::create(out_dir.join("decline_map.txt"))?;
    writeln!(
        decline,
        "capture: live post-run spiced (hot acceleration); log={}",
        log_path
            .as_ref()
            .map_or_else(|| "<none>".to_string(), |p| p.display().to_string())
    )?;

    let total = queries.len();
    let mut fired = 0usize;
    for q in &queries {
        let name = q.name.as_ref();
        let sql = q.sql.as_ref();

        // Where the spiced log currently ends, so we can attribute the rule's
        // accept/decline lines emitted while planning THIS query.
        let off = log_path
            .as_ref()
            .and_then(|p| fs::metadata(p).ok())
            .map_or(0, |m| m.len());

        // EXPLAIN (planning only — cheap, and it's what triggers the rule).
        let plan = match clients.query_arrow(&format!("EXPLAIN {sql}")).await {
            Ok(batches) => pretty_format_batches(&batches)
                .map_or_else(|e| format!("<format error: {e}>"), |d| d.to_string()),
            Err(e) => format!("<EXPLAIN error: {e}>"),
        };
        writeln!(
            structure,
            "===================== {name} ====================="
        )?;
        writeln!(structure, "{plan}\n")?;

        // EXPLAIN ANALYZE (executes — bounded so a slow query can't blow the job).
        let analyze_text = match tokio::time::timeout(
            ANALYZE_TIMEOUT,
            clients.query_arrow(&format!("EXPLAIN ANALYZE {sql}")),
        )
        .await
        {
            Ok(Ok(batches)) => pretty_format_batches(&batches)
                .map_or_else(|e| format!("<format error: {e}>"), |d| d.to_string()),
            Ok(Err(e)) => format!("<EXPLAIN ANALYZE error: {e}>"),
            Err(_) => format!(
                "<EXPLAIN ANALYZE timed out after {}s>",
                ANALYZE_TIMEOUT.as_secs()
            ),
        };
        writeln!(
            analyze,
            "===================== {name} ====================="
        )?;
        writeln!(analyze, "{analyze_text}\n")?;

        // Let the tee thread flush the planner's log lines before we read them.
        tokio::time::sleep(Duration::from_millis(400)).await;

        let pushes = count_eager_push_sites(&plan);
        if pushes > 0 {
            fired += 1;
        }
        let reasons = read_new_reasons(log_path.as_deref(), off);
        writeln!(decline, "===== {name} (eager_push_sites={pushes}) =====")?;
        if reasons.is_empty() {
            writeln!(decline, "    (no rule log lines)")?;
        } else {
            for (reason, n) in reasons {
                writeln!(decline, "    {n:>4} {reason}")?;
            }
        }
    }

    writeln!(decline, "\nfired (eager_push_sites>=1): {fired}/{total}")?;
    println!(
        "EXPLAIN capture written to {} ({fired}/{total} queries with eager push sites)",
        out_dir.display()
    );
    Ok(())
}

/// Count the distinct eager push-site markers (`__eager_p<n>` / `__eager_m<n>`)
/// the rule injects into the physical plan — i.e. how many aggregations it
/// pushed below a join for this query.
fn count_eager_push_sites(plan: &str) -> usize {
    let bytes = plan.as_bytes();
    let pat = b"__eager_";
    let mut sites: BTreeSet<&str> = BTreeSet::new();
    let mut i = 0;
    while i + pat.len() + 1 < bytes.len() {
        if &bytes[i..i + pat.len()] == pat {
            let kind = bytes[i + pat.len()];
            let digit = bytes[i + pat.len() + 1];
            if (kind == b'p' || kind == b'm') && digit.is_ascii_digit() {
                sites.insert(&plan[i..i + pat.len() + 2]);
            }
        }
        i += 1;
    }
    sites.len()
}

/// Read the spiced log bytes appended since `off` and return the rule's
/// accept/decline reasons (the text after the `eager_aggregation:` target),
/// de-duplicated with counts.
fn read_new_reasons(log_path: Option<&Path>, off: u64) -> Vec<(String, usize)> {
    let Some(path) = log_path else {
        return Vec::new();
    };
    let Ok(bytes) = fs::read(path) else {
        return Vec::new();
    };
    let start = usize::try_from(off).unwrap_or(0);
    if start >= bytes.len() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&bytes[start..]);
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for line in text.lines() {
        let line = strip_ansi(line);
        if let Some(idx) = line.find(RULE_TARGET) {
            let reason = line[idx + RULE_TARGET.len()..].trim().to_string();
            if !reason.is_empty() {
                *counts.entry(reason).or_default() += 1;
            }
        }
    }
    counts.into_iter().collect()
}

/// Strip ANSI/SGR escape sequences (spiced's fmt logger colorizes output).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // CSI sequence: consume up to and including the alphabetic terminator.
            for n in chars.by_ref() {
                if n.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}
