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

//! What running a decision over every row of a table costs, and how many requests a
//! `LIMIT` query sends.
//!
//! The model answers after a delay drawn per row from a seeded long-tailed
//! distribution (5 ms plus an exponential tail with a 15 ms mean), the shape of a
//! remote model's latency. Each query runs repeatedly over a 4-partition table and the
//! wall-time percentiles and request counts are printed. `ARM` labels the output,
//! for comparing builds that differ in how many rows one invocation takes.

#![expect(clippy::expect_used, reason = "benchmark setup")]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::Optimizer;
use datafusion::prelude::{SessionConfig, SessionContext};
use evaluate_api::{Answer, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState};
use runtime_decide::{DecisionFunctions, guard_async_calls};
use runtime_status::RuntimeStatus;
use tokio::sync::RwLock;

const PARTITIONS: usize = 4;
const RUNS: usize = 15;

/// Answers every question with 0.9 when the input mentions a refund, after the input's
/// delay.
#[derive(Debug, Default)]
struct SlowModel {
    requests: AtomicUsize,
}

#[async_trait]
impl Evaluate for SlowModel {
    async fn evaluate(&self, request: EvaluateRequest) -> evaluate_api::Result<EvaluateResponse> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let EvaluateState::String(text) = &request.state else {
            panic!("the bench asks about text");
        };
        tokio::time::sleep(delay(text)).await;
        let noul = if text.contains("refund") { 0.9 } else { 0.1 };
        let answers: BTreeMap<String, Answer> = request
            .questions
            .keys()
            .map(|id| (id.clone(), Answer::Noul { noul }))
            .collect();
        Ok(EvaluateResponse {
            model: request.model,
            answers,
            usage: None,
        })
    }

    async fn health(&self) -> evaluate_api::Result<()> {
        Ok(())
    }

    fn is_decision_model(&self) -> bool {
        true
    }
}

/// 5 ms plus an exponential tail with a 15 ms mean, fixed per input text.
fn delay(text: &str) -> Duration {
    // FNV-1a, then splitmix64, so each row keeps its delay across runs and builds.
    let mut z = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    #[expect(clippy::cast_precision_loss, reason = "53 bits of a uniform draw")]
    let uniform = ((z >> 11) as f64 + 1.0) / (1_u64 << 53) as f64;
    Duration::from_secs_f64(0.005 + 0.015 * -uniform.ln())
}

fn session(rows: usize, model: &Arc<SlowModel>) -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(PARTITIONS)
        .set_str("datafusion.sql_parser.dialect", "PostgreSQL");
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_optimizer_rules(guard_async_calls(Optimizer::new().rules))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let store: HashMap<String, Arc<dyn Evaluate>> =
        HashMap::from([("jev".to_string(), Arc::clone(model) as Arc<dyn Evaluate>)]);
    DecisionFunctions::new(Arc::new(RwLock::new(store)), RuntimeStatus::new()).register(&ctx);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let per_partition = rows / PARTITIONS;
    let partitions: Vec<Vec<RecordBatch>> = (0..PARTITIONS)
        .map(|p| {
            let ids: Vec<i64> = (p * per_partition..(p + 1) * per_partition)
                .map(|i| i64::try_from(i).expect("row id"))
                .collect();
            let bodies: Vec<String> = ids
                .iter()
                .map(|id| {
                    if id % 4 == 0 {
                        format!("ticket {id}: please refund the duplicate charge")
                    } else {
                        format!("ticket {id}: the dashboard does not load")
                    }
                })
                .collect();
            vec![
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(StringArray::from(bodies)),
                    ],
                )
                .expect("batch"),
            ]
        })
        .collect();
    let table = MemTable::try_new(schema, partitions).expect("table");
    ctx.register_table("tickets", Arc::new(table))
        .expect("register tickets");
    ctx
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "a nearest-rank index into a short list"
    )]
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn main() {
    let arm = std::env::var("ARM").unwrap_or_else(|_| "current".to_string());
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    for (name, rows, sql) in [
        (
            "filter_2000_rows",
            2_000,
            "SELECT count(*) FROM tickets WHERE ai_if(body, 'refund')",
        ),
        (
            "filter_8000_rows",
            8_000,
            "SELECT count(*) FROM tickets WHERE ai_if(body, 'refund')",
        ),
        (
            "limit_10_of_8000_rows",
            8_000,
            "SELECT id FROM tickets WHERE ai_if(body, 'refund') LIMIT 10",
        ),
    ] {
        let model = Arc::new(SlowModel::default());
        let ctx = session(rows, &model);
        let mut times = Vec::with_capacity(RUNS);
        let mut requests = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let before = model.requests.load(Ordering::Relaxed);
            let start = Instant::now();
            runtime.block_on(async {
                ctx.sql(sql)
                    .await
                    .expect("plan")
                    .collect()
                    .await
                    .expect("run")
            });
            times.push(start.elapsed());
            requests.push(model.requests.load(Ordering::Relaxed) - before);
        }
        times.sort();
        requests.sort_unstable();
        println!(
            "arm={arm} query={name} runs={RUNS} p50_ms={:.1} p99_ms={:.1} requests_min={} requests_max={}",
            percentile(&times, 0.50).as_secs_f64() * 1000.0,
            percentile(&times, 0.99).as_secs_f64() * 1000.0,
            requests[0],
            requests[RUNS - 1],
        );
    }
}
