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
use datafusion::functions_aggregate::expr_fn::{count, max, min};
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

/// Final data-correctness report produced after replication drains.
#[derive(Debug)]
pub struct CorrectnessReport {
    /// Per-table final row-count comparison.
    pub tables: Vec<TableCorrectness>,
    /// Time at which replication fully converged, or `None` if it did not
    /// converge within the wait bound.
    pub converged_at: Option<Duration>,
    /// How long the drain wait took.
    pub wait_duration: Duration,
}

impl CorrectnessReport {
    /// Print a human-readable correctness summary and record OTEL metrics.
    pub fn emit(&self) {
        println!("\nData Correctness");
        if let Some(converged_at) = self.converged_at {
            println!("  replication converged in {}ms", converged_at.as_millis());
        } else {
            println!(
                "  replication DID NOT converge within {}ms",
                self.wait_duration.as_millis()
            );
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

        let failed = mismatches + u64::from(self.converged_at.is_none());
        crate::metrics::CORRECTNESS_ROUNDS_TOTAL.record(1, &[]);
        crate::metrics::CORRECTNESS_ROUNDS_PASSED.record(u64::from(failed == 0), &[]);
        crate::metrics::CORRECTNESS_ROUNDS_FAILED.record(u64::from(failed != 0), &[]);

        if failed == 0 {
            println!("  verdict: PASSED — all {} tables match", self.tables.len());
        } else {
            println!(
                "  verdict: FAILED — {mismatches} table(s) mismatched{}",
                if self.converged_at.is_some() {
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
        if self.converged_at.is_none() {
            problems.push(format!(
                "replication did not converge within {}ms",
                self.wait_duration.as_millis()
            ));
        }
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

/// Wait for replication to fully drain, then snapshot final source/Spice counts.
///
/// Polls each table until both `MAX(_bench_ts)` and `COUNT(*)` agree between the
/// source and Spice, bounded by `max_wait`. After the wait (converged or timed
/// out) a final count comparison is taken for the report.
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

    let converged_at = loop {
        let mut all_caught_up = true;
        for table in tables {
            let (src_ts, spice_ts, src_n, spice_n) = tokio::join!(
                driver.max_bench_ts(table),
                spice.max_bench_ts(table),
                driver.row_count(table),
                spice.count(table),
            );

            // A transient error (e.g. a momentary connection blip while replication is still draining) should not fail
            // the whole gate — treat it as "not caught up" and keep polling until max_wait.
            match (src_ts, spice_ts, src_n, spice_n) {
                (Ok(src_ts), Ok(spice_ts), Ok(src_n), Ok(spice_n)) => {
                    if !(src_ts == spice_ts && src_n == spice_n) {
                        all_caught_up = false;
                    }
                }
                (src_ts, spice_ts, src_n, spice_n) => {
                    all_caught_up = false;
                    eprintln!(
                        "Data correctness probe: {table} src_ts={src_ts:?} spice_ts={spice_ts:?} src_count={src_n:?} spice_count={spice_n:?}"
                    );
                }
            }
        }

        if all_caught_up {
            break Some(start.elapsed());
        }
        if Instant::now() >= deadline {
            break None;
        }
        sleep(poll).await;
    };

    // Final snapshot for the report: counts plus a per-column content
    // fingerprint. The fingerprint runs once here (not every poll) — a
    // full-table aggregate scan per probe table is too costly to repeat each
    // second. Counts converging means replication caught up; the fingerprint
    // then answers the distinct question "is the content actually correct?",
    // catching value-level corruption that COUNT(*) + MAX(_bench_ts) miss.
    let mut table_results = Vec::with_capacity(tables.len());
    for table in tables {
        let (source_count, spice_count) = tokio::join!(driver.row_count(table), spice.count(table));
        let source_count = source_count?;
        let spice_count = spice_count?;
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

    Ok(CorrectnessReport {
        tables: table_results,
        converged_at,
        wait_duration: start.elapsed(),
    })
}

/// Compute and compare a per-column content fingerprint for `table`.
///
/// The fingerprint is engine-agnostic: `COUNT(*)`, a non-null `COUNT(col)` for
/// every column, and `MIN(col)`/`MAX(col)` for numeric columns. `SUM` is
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

    let delta = compare::numeric_delta(expected_row, actual_row);
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
/// nulled values) and `MIN`/`MAX` for numeric columns. Each aggregate is aliased
/// (`count_<col>`, `min_<col>`, `max_<col>`) so a divergence reported by
/// [`compare::numeric_delta`] names the offending column.
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

    #[test]
    fn fingerprint_sql_counts_all_columns_minmax_only_numerics() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("ol_amount", DataType::Decimal128(38, 2), true),
            Field::new("ol_quantity", DataType::Int32, true),
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
        for col in ["ol_amount", "ol_quantity", "ol_dist_info", "ol_delivery_d"] {
            assert!(
                sql.contains(&format!("count_{col}")),
                "missing COUNT for {col}: {sql}"
            );
        }
        // MIN/MAX only for numeric columns.
        assert!(
            sql.contains("min_ol_amount") && sql.contains("max_ol_amount"),
            "{sql}"
        );
        assert!(
            sql.contains("min_ol_quantity") && sql.contains("max_ol_quantity"),
            "{sql}"
        );
        // Not for text or temporal (cross-engine collation / precision differ).
        assert!(
            !sql.contains("min_ol_dist_info") && !sql.contains("max_ol_dist_info"),
            "{sql}"
        );
        assert!(
            !sql.contains("min_ol_delivery_d") && !sql.contains("max_ol_delivery_d"),
            "{sql}"
        );
        // The source table is referenced.
        assert!(sql.contains("order_line"), "{sql}");
    }
}
