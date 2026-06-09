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

use super::compare;
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use chbench_driver::ChBenchDriver;
use futures::TryStreamExt;
use test_framework::anyhow;
use test_framework::opentelemetry::KeyValue;
use tokio::time::sleep;

use super::super::staleness::query_max_bench_ts_spice;

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
    spice_client: &spiceai::Client,
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
                query_max_bench_ts_spice(spice_client, table),
                driver.row_count(table),
                query_count_spice(spice_client, table),
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
        let (source_count, spice_count) = tokio::join!(
            driver.row_count(table),
            query_count_spice(spice_client, table)
        );
        let source_count = source_count?;
        let spice_count = spice_count?;
        // Only fingerprint when counts already agree — comparing content over a
        // differing row set adds no signal (the count mismatch is the finding).
        let content = if source_count == spice_count {
            table_fingerprint(&driver, spice_client, table).await
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

/// Query `COUNT(*)` from Spice via Flight SQL.
async fn query_count_spice(client: &spiceai::Client, table: &str) -> anyhow::Result<i64> {
    let query = format!("SELECT COUNT(*) FROM {table}");
    let mut stream = client.sql(&query).await?;

    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch
            .column(0)
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| anyhow::anyhow!("unexpected array type for COUNT(*) on {table}"))?;
        if !col.is_null(0) {
            return Ok(col.value(0));
        }
    }

    Ok(0)
}

/// Run a read-only query against Spice via Flight SQL, returning Arrow batches.
async fn query_arrow_spice(
    client: &spiceai::Client,
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let stream = client.sql(sql).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    Ok(batches)
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
/// Probe errors degrade to [`ContentCheck::Skipped`] rather than failing the
/// gate, mirroring the convergence loop's tolerance of transient blips.
async fn table_fingerprint(
    driver: &Arc<dyn ChBenchDriver>,
    spice_client: &spiceai::Client,
    table: &str,
) -> ContentCheck {
    // Derive the column list from the source schema.
    let schema_batches = match driver
        .query_arrow(&format!("SELECT * FROM {table} LIMIT 1"))
        .await
    {
        Ok(b) => b,
        Err(e) => return ContentCheck::Skipped(format!("schema probe failed: {e}")),
    };
    let Some(schema) = schema_batches.first().map(RecordBatch::schema) else {
        return ContentCheck::Skipped("no schema returned".to_string());
    };

    let sql = build_fingerprint_sql(table, &schema);

    let (src, spc) = tokio::join!(
        driver.query_arrow(&sql),
        query_arrow_spice(spice_client, &sql)
    );
    let (src, spc) = match (src, spc) {
        (Ok(s), Ok(p)) => (s, p),
        (Err(e), _) => return ContentCheck::Skipped(format!("source fingerprint failed: {e}")),
        (_, Err(e)) => return ContentCheck::Skipped(format!("spice fingerprint failed: {e}")),
    };

    let (Some(src_row), Some(spc_row)) = (src.first(), spc.first()) else {
        return ContentCheck::Skipped("empty fingerprint result".to_string());
    };

    let delta = compare::numeric_delta(src_row, spc_row);
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

/// Build the engine-agnostic fingerprint aggregate SQL for `table` from its
/// Arrow `schema`. Column names are quoted to preserve the source's casing on
/// both engines.
fn build_fingerprint_sql(table: &str, schema: &arrow::datatypes::Schema) -> String {
    let mut aggs: Vec<String> = vec!["COUNT(*)".to_string()];
    for field in schema.fields() {
        let name = field.name();
        // Non-null count for every column (catches column swaps / nulled values).
        aggs.push(format!("COUNT(\"{name}\")"));
        // Numeric MIN/MAX (exact for int/decimal, tolerant for float).
        if compare::is_numeric_type(field.data_type()) {
            aggs.push(format!("MIN(\"{name}\")"));
            aggs.push(format!("MAX(\"{name}\")"));
        }
    }
    format!("SELECT {} FROM {table}", aggs.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn fingerprint_sql_counts_all_columns_minmax_only_numerics() {
        let schema = Schema::new(vec![
            Field::new("ol_amount", DataType::Decimal128(38, 2), true),
            Field::new("ol_quantity", DataType::Int32, true),
            Field::new("ol_dist_info", DataType::Utf8, true),
            Field::new(
                "ol_delivery_d",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        let sql = build_fingerprint_sql("order_line", &schema);

        // COUNT(*) and a non-null COUNT for every column.
        assert!(sql.contains("COUNT(*)"));
        for col in ["ol_amount", "ol_quantity", "ol_dist_info", "ol_delivery_d"] {
            assert!(sql.contains(&format!("COUNT(\"{col}\")")), "missing COUNT for {col}");
        }
        // MIN/MAX only for numeric columns.
        assert!(sql.contains("MIN(\"ol_amount\")") && sql.contains("MAX(\"ol_amount\")"));
        assert!(sql.contains("MIN(\"ol_quantity\")") && sql.contains("MAX(\"ol_quantity\")"));
        // Not for text or temporal (cross-engine collation / precision differ).
        assert!(!sql.contains("MIN(\"ol_dist_info\")"));
        assert!(!sql.contains("MIN(\"ol_delivery_d\")"));
        assert!(sql.ends_with("FROM order_line"));
    }
}
