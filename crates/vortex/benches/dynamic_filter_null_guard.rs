/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Scan cost of guarding a hash-join dynamic filter's bounds with `IS NULL`,
//! over a Vortex file with two nullable join keys.
//!
//! A dynamic filter's min/max bound is NULL for a NULL key. Adding `IS NULL` as
//! a disjunct of each pushed bound keeps NULL keys in the scan, but a zone that
//! holds a NULL in a guarded column can then no longer be pruned by that bound.
//! This bench times the shapes such a guard produces, on one file and one build:
//!
//! - `static bounds`: `a`/`b` bounds only, the shape the scan pushes for the
//!   hash join's own filter.
//! - `static per-column guard`: each bound ORs `IS NULL` on its own column.
//! - `static full guard`: each bound ORs `a IS NULL OR b IS NULL`.
//! - `hash join`: a real two-key equi-join, so the planner plants the dynamic
//!   filter and the scan pushes whatever this build of the opener produces.
//!   Comparing it across two builds (before and after a change) is the A/B;
//!   the static arms run identical SQL in both and are the noise floor.
//!
//! Each NULL layout is timed separately: `none` (nullable, no NULLs), `sparse`
//! (one NULL every 10,000 rows in each key, so most zones hold one) and
//! `clustered` (the same NULLs packed into the first zone).
//!
//! Arms run interleaved, rotating their order every round, and each query is
//! timed alone, so the report is per-query P50/P99/max rather than a mean.
//!
//! ```text
//! cargo bench -p vortex-datafusion --bench dynamic_filter_null_guard
//! DFNG_ROUNDS=100 cargo bench -p vortex-datafusion --bench dynamic_filter_null_guard
//! ```

#![expect(
    clippy::expect_used,
    reason = "a bench aborts on a setup or query failure, with the failing step named"
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::GetExt;
use object_store::memory::InMemory;
use tokio::runtime::Runtime;
use vortex::VortexSessionDefault;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex_datafusion::{VortexFormatFactory, VortexTableOptions};

/// Rows in the probe file: enough zones that pruning dominates the scan.
const ROW_COUNT: i64 = 4_194_304;
/// The build keys cover 1% of the probe keys, starting in the middle.
const KEY_LO: i64 = ROW_COUNT / 2;
const KEY_HI: i64 = KEY_LO + ROW_COUNT / 100;
/// One NULL every this many rows in each key column for the `sparse` layout.
const SPARSE_NULL_EVERY: i64 = 10_000;
const WARMUP_ROUNDS: usize = 3;
const DEFAULT_ROUNDS: usize = 40;

struct Layout {
    name: &'static str,
    a: String,
    b: String,
}

fn layouts() -> Vec<Layout> {
    let clustered = ROW_COUNT / SPARSE_NULL_EVERY;
    vec![
        // `NULLIF(v, 0)` is never NULL here (`v` starts at 1) but has the
        // nullable type the table declares; the Vortex writer rejects a
        // non-nullable chunk for a nullable column.
        Layout {
            name: "none",
            a: "NULLIF(v, 0)".to_string(),
            b: "NULLIF(v, 0)".to_string(),
        },
        Layout {
            name: "sparse",
            a: format!("CASE WHEN v % {SPARSE_NULL_EVERY} = 0 THEN NULL ELSE v END"),
            b: format!(
                "CASE WHEN v % {SPARSE_NULL_EVERY} = {} THEN NULL ELSE v END",
                SPARSE_NULL_EVERY / 2
            ),
        },
        Layout {
            name: "clustered",
            a: format!("CASE WHEN v <= {clustered} THEN NULL ELSE v END"),
            b: format!(
                "CASE WHEN v > {clustered} AND v <= {} THEN NULL ELSE v END",
                2 * clustered
            ),
        },
    ]
}

struct Arm {
    name: &'static str,
    sql: String,
    /// The physical plan must hold this text, or the arm measured something else.
    expect_in_plan: &'static str,
}

fn arms() -> Vec<Arm> {
    let (lo, hi) = (KEY_LO, KEY_HI);
    let guard = |bound: &str, nulls: &str| format!("({bound} OR {nulls})");
    let bounds = [
        ("a", format!("a >= {lo}")),
        ("a", format!("a <= {hi}")),
        ("b", format!("b >= {lo}")),
        ("b", format!("b <= {hi}")),
    ];
    let per_column = bounds
        .iter()
        .map(|(column, bound)| guard(bound, &format!("{column} IS NULL")))
        .collect::<Vec<_>>()
        .join(" AND ");
    let full = bounds
        .iter()
        .map(|(_, bound)| guard(bound, "a IS NULL OR b IS NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let plain = bounds
        .iter()
        .map(|(_, bound)| bound.clone())
        .collect::<Vec<_>>()
        .join(" AND ");
    let aggregate = "SELECT count(*), sum(payload) FROM probe";
    vec![
        Arm {
            name: "static bounds",
            sql: format!("{aggregate} WHERE {plain}"),
            expect_in_plan: "file_type=vortex, predicate",
        },
        Arm {
            name: "static per-column guard",
            sql: format!("{aggregate} WHERE {per_column}"),
            expect_in_plan: "file_type=vortex, predicate",
        },
        Arm {
            name: "static full guard",
            sql: format!("{aggregate} WHERE {full}"),
            expect_in_plan: "file_type=vortex, predicate",
        },
        Arm {
            name: "hash join",
            sql: "SELECT count(*), sum(p.payload) FROM build k JOIN probe p \
                  ON p.a = k.a AND p.b = k.b"
                .to_string(),
            expect_in_plan: "file_type=vortex, predicate: DynamicFilter",
        },
    ]
}

/// Vortex's write path resolves its executor from the ambient Tokio runtime
/// unless the session is configured with one; a current-thread runtime
/// satisfies that.
fn build_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread tokio runtime")
}

async fn build_context(layout: &Layout) -> SessionContext {
    // `with_tokio` captures `Handle::current()`, so it must run inside `block_on`.
    let session = VortexSession::default().with_tokio();
    let factory = Arc::new(VortexFormatFactory::new_with_options(
        session,
        VortexTableOptions::default(),
    ));
    // One partition keeps the insert in `v` order, so each zone holds a narrow
    // key range and the bounds can prune.
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(1))
        .with_table_factory(
            factory.get_ext().to_uppercase(),
            Arc::new(DefaultTableFactory::new()),
        )
        .with_file_formats(vec![factory])
        .build();
    let ctx = SessionContext::new_with_state(state).enable_url_table();
    ctx.register_object_store(
        &url::Url::try_from("file://").expect("file:// should parse as a URL"),
        Arc::new(InMemory::new()),
    );

    let statements = [
        "CREATE EXTERNAL TABLE probe (a BIGINT, b BIGINT, payload BIGINT NOT NULL) \
         STORED AS vortex LOCATION '/probe/'"
            .to_string(),
        format!(
            "INSERT INTO probe SELECT {} AS a, {} AS b, v * 7 AS payload \
             FROM generate_series(1, {ROW_COUNT}) AS t(v)",
            layout.a, layout.b
        ),
        format!(
            "CREATE TABLE build AS SELECT v AS a, v AS b \
             FROM generate_series({KEY_LO}, {KEY_HI}) AS t(v)"
        ),
    ];
    for sql in statements {
        ctx.sql(&sql)
            .await
            .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("run `{sql}`: {e}"));
    }
    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("run `{sql}`: {e}"))
}

/// `count(*)` of an arm's single result row.
fn count_of(batches: &[RecordBatch]) -> i64 {
    let column = batches
        .first()
        .expect("an aggregate returns one batch")
        .column(0)
        .as_primitive::<Int64Type>();
    assert!(!column.is_null(0), "count(*) is never NULL");
    column.value(0)
}

/// The Vortex scan line of the arm's physical plan.
async fn scan_line(ctx: &SessionContext, arm: &Arm) -> String {
    let plan = ctx
        .sql(&arm.sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let text = displayable(plan.as_ref()).indent(true).to_string();
    let line = text
        .lines()
        .find(|line| line.contains(arm.expect_in_plan))
        .unwrap_or_else(|| {
            panic!(
                "arm `{}` did not reach the scan it measures (no `{}`):\n{text}",
                arm.name, arm.expect_in_plan
            )
        });
    let predicate = line.find("predicate").map_or(line, |start| &line[start..]);
    predicate.chars().take(240).collect()
}

/// Nearest-rank percentile of sorted samples.
fn percentile(sorted: &[Duration], pct: usize) -> Duration {
    let rank = (sorted.len() * pct).div_ceil(100);
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn main() {
    let rounds = std::env::var("DFNG_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ROUNDS);
    let rt = build_runtime();
    let arms = arms();
    println!(
        "dynamic_filter_null_guard: rows={ROW_COUNT} keys=[{KEY_LO}, {KEY_HI}] rounds={rounds} warmup={WARMUP_ROUNDS}"
    );

    for layout in layouts() {
        rt.block_on(async {
            let ctx = build_context(&layout).await;
            let mut counts = Vec::with_capacity(arms.len());
            for arm in &arms {
                println!(
                    "  [{}] {} scan: {}",
                    layout.name,
                    arm.name,
                    scan_line(&ctx, arm).await
                );
                counts.push(count_of(&run(&ctx, &arm.sql).await));
            }

            let mut samples = vec![Vec::with_capacity(rounds); arms.len()];
            for round in 0..WARMUP_ROUNDS + rounds {
                for offset in 0..arms.len() {
                    // Rotate the order every round so no arm always runs last.
                    let index = (round + offset) % arms.len();
                    let started = Instant::now();
                    let batches = run(&ctx, &arms[index].sql).await;
                    let elapsed = started.elapsed();
                    assert_eq!(
                        count_of(&batches),
                        counts[index],
                        "arm `{}` changed its answer between runs",
                        arms[index].name
                    );
                    if round >= WARMUP_ROUNDS {
                        samples[index].push(elapsed);
                    }
                }
            }

            println!(
                "layout={:<9} {:<24} {:>8} {:>9} {:>9} {:>9}",
                layout.name, "arm", "count", "p50_ms", "p99_ms", "max_ms"
            );
            for ((arm, count), mut times) in arms.iter().zip(&counts).zip(samples) {
                times.sort_unstable();
                println!(
                    "layout={:<9} {:<24} {:>8} {:>9.3} {:>9.3} {:>9.3}",
                    layout.name,
                    arm.name,
                    count,
                    millis(percentile(&times, 50)),
                    millis(percentile(&times, 99)),
                    millis(*times.last().expect("at least one round")),
                );
            }
        });
    }
}
