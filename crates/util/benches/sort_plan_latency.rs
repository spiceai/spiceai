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

//! Latency of one `sort_plan` call, end to end, for the input shapes its
//! callers sort: a sorted Arrow write (one in-memory batch), a cold-promotion
//! run and a full-refresh replace (one large stream), and a compaction rewrite
//! (many partitions). Prints the P50, P99 and P99.9 of each shape's calls, in
//! milliseconds, so two builds can be compared run against run.
//!
//! `cargo bench -p util --features datafusion --bench sort_plan_latency`
//! Set `SORT_PLAN_LATENCY_SHAPES` to a comma-separated list of shape names to
//! run only those.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
    UnboundedMemoryPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::atomic::{AtomicUsize, Ordering};

const MIB: usize = 1024 * 1024;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// `partitions` partitions of `rows` rows in all, in batches of 8192, with
/// seeded pseudo-random keys and ~`payload_bytes`-byte strings.
/// Columns of a wide row besides its key: this many of each kind, as wide as
/// the widest tables these sorts rewrite (about 33 columns).
const WIDE_STRINGS: usize = 16;
const WIDE_INTEGERS: usize = 8;
const WIDE_FLOATS: usize = 4;
const WIDE_BOOLEANS: usize = 4;

fn wide_schema() -> SchemaRef {
    let mut fields = vec![Field::new("key", DataType::Int64, false)];
    fields.extend((0..WIDE_STRINGS).map(|i| Field::new(format!("s{i}"), DataType::Utf8, true)));
    fields.extend((0..WIDE_INTEGERS).map(|i| Field::new(format!("i{i}"), DataType::Int64, true)));
    fields.extend((0..WIDE_FLOATS).map(|i| Field::new(format!("f{i}"), DataType::Float64, true)));
    fields.extend((0..WIDE_BOOLEANS).map(|i| Field::new(format!("b{i}"), DataType::Boolean, true)));
    Arc::new(Schema::new(fields))
}

/// Wide rows with seeded pseudo-random keys: short strings, integers, floats
/// and booleans, about 280 bytes a row.
fn wide_input(rows: usize, partitions: usize, seed: u64) -> Vec<Vec<RecordBatch>> {
    use arrow::array::{ArrayRef, BooleanArray, Float64Array};
    let mut state = seed;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 17
    };
    let per_partition = rows.div_ceil(partitions);
    (0..partitions)
        .map(|_| {
            (0..per_partition)
                .step_by(8192)
                .map(|start| {
                    let n = 8192.min(per_partition - start);
                    let keys: Vec<i64> = (0..n)
                        .map(|_| i64::try_from(next() % 1_000_000_007).expect("fits"))
                        .collect();
                    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys.clone()))];
                    for c in 0..WIDE_STRINGS {
                        columns.push(Arc::new(StringArray::from_iter_values(
                            keys.iter().map(|k| format!("AC{:010}{c:02}", k ^ c as i64)),
                        )));
                    }
                    for c in 0..WIDE_INTEGERS {
                        columns.push(Arc::new(Int64Array::from_iter_values(
                            keys.iter().map(|k| k.wrapping_mul(c as i64 + 3)),
                        )));
                    }
                    for c in 0..WIDE_FLOATS {
                        columns.push(Arc::new(Float64Array::from_iter_values(
                            keys.iter().map(|k| *k as f64 / (c as f64 + 1.5)),
                        )));
                    }
                    for c in 0..WIDE_BOOLEANS {
                        columns.push(Arc::new(BooleanArray::from_iter(
                            keys.iter().map(|k| Some((k >> c) & 1 == 1)),
                        )));
                    }
                    RecordBatch::try_new(wide_schema(), columns).expect("batch")
                })
                .collect()
        })
        .collect()
}

/// How an input's keys are arranged across its partitions.
#[derive(Clone, Copy)]
enum Layout {
    /// Seeded pseudo-random keys in every partition.
    Random,
    /// Each partition holds its own ascending range of keys, disjoint from every
    /// other partition's, as files range-partitioned and sorted on the key are.
    Ranges,
}

fn input(
    rows: usize,
    partitions: usize,
    payload_bytes: usize,
    seed: u64,
    layout: Layout,
) -> Vec<Vec<RecordBatch>> {
    let mut state = seed;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 17
    };
    let pad = "x".repeat(payload_bytes);
    let per_partition = rows.div_ceil(partitions);
    (0..partitions)
        .map(|partition| {
            (0..per_partition)
                .step_by(8192)
                .map(|start| {
                    let n = 8192.min(per_partition - start);
                    let keys: Vec<i64> = match layout {
                        Layout::Random => (0..n)
                            .map(|_| i64::try_from(next() % 1_000_000_007).expect("fits"))
                            .collect(),
                        Layout::Ranges => (0..n)
                            .map(|i| {
                                i64::try_from(partition * per_partition + start + i).expect("fits")
                            })
                            .collect(),
                    };
                    let payloads: Vec<String> = keys.iter().map(|k| format!("{k}{pad}")).collect();
                    RecordBatch::try_new(
                        schema(),
                        vec![
                            Arc::new(Int64Array::from(keys)),
                            Arc::new(StringArray::from(payloads)),
                        ],
                    )
                    .expect("batch")
                })
                .collect()
        })
        .collect()
}

/// A pool that forwards to `inner` and records the most it ever held.
#[derive(Debug)]
struct PeakPool {
    inner: Arc<dyn MemoryPool>,
    held: AtomicUsize,
    peak: AtomicUsize,
}

impl PeakPool {
    fn add(&self, bytes: usize) {
        let held = self.held.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(held, Ordering::Relaxed);
    }
}

impl std::fmt::Display for PeakPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peak({})", self.inner)
    }
}

impl MemoryPool for PeakPool {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.add(additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.held.fetch_sub(shrink, Ordering::Relaxed);
    }
    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.add(additional);
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

fn context(pool: Option<usize>) -> (Arc<TaskContext>, Arc<PeakPool>) {
    let inner: Arc<dyn MemoryPool> = match pool {
        Some(bytes) => Arc::new(GreedyMemoryPool::new(bytes)),
        None => Arc::new(UnboundedMemoryPool::default()),
    };
    let peak = Arc::new(PeakPool {
        inner,
        held: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&peak) as Arc<dyn MemoryPool>)
        .build_arc()
        .expect("runtime");
    (Arc::new(TaskContext::default().with_runtime(runtime)), peak)
}

struct Shape {
    name: &'static str,
    rows: usize,
    partitions: usize,
    payload_bytes: usize,
    pool: Option<usize>,
    iterations: usize,
    layout: Layout,
    /// Whether rows are wide (about 33 columns) rather than a key and a payload.
    wide: bool,
    /// Whether the caller sorts in a pool set aside for maintenance, as a
    /// compaction rewrite or a cold promotion does, rather than one shared
    /// with queries.
    dedicated: bool,
}

// plan-sort: a build without sort budgets replaces this body with a plain `sort_plan`.
fn plan_sort(
    plan: Arc<dyn ExecutionPlan>,
    columns: &[String],
    ctx: &Arc<TaskContext>,
    dedicated: bool,
) -> Arc<dyn ExecutionPlan> {
    let budget = if dedicated {
        util::sorted_runs::SortBudget::Dedicated
    } else {
        util::sorted_runs::SortBudget::Shared
    };
    util::stream_utils::sort_plan_with_budget(plan, columns, ctx, budget).expect("plan")
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn main() {
    let shapes = [
        Shape {
            name: "arrow_write_1k",
            rows: 1_000,
            partitions: 1,
            payload_bytes: 32,
            pool: None,
            iterations: 2_000,
            layout: Layout::Random,
            dedicated: false,
            wide: false,
        },
        Shape {
            name: "arrow_write_10k",
            rows: 10_000,
            partitions: 1,
            payload_bytes: 32,
            pool: None,
            iterations: 1_000,
            layout: Layout::Random,
            dedicated: false,
            wide: false,
        },
        Shape {
            name: "arrow_write_100k",
            rows: 100_000,
            partitions: 1,
            payload_bytes: 32,
            pool: None,
            iterations: 200,
            layout: Layout::Random,
            dedicated: false,
            wide: false,
        },
        Shape {
            name: "cold_run_750mb",
            rows: 3_000_000,
            partitions: 1,
            payload_bytes: 220,
            pool: Some(8 * 1024 * MIB),
            iterations: 12,
            layout: Layout::Random,
            dedicated: true,
            wide: false,
        },
        Shape {
            name: "cold_run_1gb",
            rows: 4_000_000,
            partitions: 1,
            payload_bytes: 220,
            pool: Some(8 * 1024 * MIB),
            iterations: 12,
            layout: Layout::Random,
            dedicated: true,
            wide: false,
        },
        Shape {
            name: "shared_pool_1p_1gb",
            rows: 4_000_000,
            partitions: 1,
            payload_bytes: 220,
            pool: Some(8 * 1024 * MIB),
            iterations: 12,
            layout: Layout::Random,
            dedicated: false,
            wide: false,
        },
        Shape {
            name: "cold_run_1_5gb",
            rows: 6_000_000,
            partitions: 1,
            payload_bytes: 220,
            pool: Some(8 * 1024 * MIB),
            iterations: 8,
            layout: Layout::Random,
            dedicated: true,
            wide: false,
        },
        Shape {
            name: "cold_run_3gb",
            rows: 12_000_000,
            partitions: 1,
            payload_bytes: 220,
            pool: Some(8 * 1024 * MIB),
            iterations: 6,
            layout: Layout::Random,
            dedicated: true,
            wide: false,
        },
        Shape {
            name: "rewrite_32p_1gb",
            rows: 4_000_000,
            partitions: 32,
            payload_bytes: 220,
            pool: Some(8 * 1024 * MIB),
            iterations: 12,
            layout: Layout::Random,
            dedicated: true,
            wide: false,
        },
        Shape {
            name: "wide_1p_1gb",
            rows: 3_500_000,
            partitions: 1,
            payload_bytes: 0,
            pool: Some(64 * 1024 * MIB),
            iterations: 10,
            layout: Layout::Random,
            dedicated: false,
            wide: true,
        },
        Shape {
            name: "wide_1p_2_5gb",
            rows: 9_000_000,
            partitions: 1,
            payload_bytes: 0,
            pool: Some(64 * 1024 * MIB),
            iterations: 6,
            layout: Layout::Random,
            dedicated: false,
            wide: true,
        },
        // A cold promotion's run: wide rows a quarter the size of a dedicated
        // pool, as an 8 GiB run is of a 31 GB compaction pool.
        Shape {
            name: "wide_cold_run_2_5gb",
            rows: 9_000_000,
            partitions: 1,
            payload_bytes: 0,
            pool: Some(10 * 1024 * MIB),
            iterations: 6,
            layout: Layout::Random,
            dedicated: true,
            wide: true,
        },
        Shape {
            name: "wide_32p_1gb",
            rows: 3_500_000,
            partitions: 32,
            payload_bytes: 0,
            pool: Some(8 * 1024 * MIB),
            iterations: 10,
            layout: Layout::Random,
            dedicated: true,
            wide: true,
        },
        Shape {
            name: "presorted_1p_1gb",
            rows: 4_000_000,
            partitions: 1,
            payload_bytes: 220,
            pool: Some(8 * 1024 * MIB),
            iterations: 12,
            layout: Layout::Ranges,
            dedicated: true,
            wide: false,
        },
        Shape {
            name: "ranges_32p_1gb",
            rows: 4_000_000,
            partitions: 32,
            payload_bytes: 220,
            pool: Some(8 * 1024 * MIB),
            iterations: 12,
            layout: Layout::Ranges,
            dedicated: true,
            wide: false,
        },
    ];
    let only: Option<Vec<String>> = std::env::var("SORT_PLAN_LATENCY_SHAPES")
        .ok()
        .map(|list| list.split(',').map(str::to_string).collect());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    for shape in shapes {
        if only
            .as_ref()
            .is_some_and(|only| !only.iter().any(|name| name == shape.name))
        {
            continue;
        }
        let (data, data_schema) = if shape.wide {
            (wide_input(shape.rows, shape.partitions, 42), wide_schema())
        } else {
            (
                input(
                    shape.rows,
                    shape.partitions,
                    shape.payload_bytes,
                    42,
                    shape.layout,
                ),
                schema(),
            )
        };
        let bytes: usize = data
            .iter()
            .flatten()
            .map(RecordBatch::get_array_memory_size)
            .sum();
        let columns = ["key".to_string()];
        let mut samples = Vec::with_capacity(shape.iterations);
        let mut peak_bytes = 0;
        for iteration in 0..shape.iterations + 1 {
            let (ctx, pool) = context(shape.pool);
            let plan: Arc<dyn ExecutionPlan> =
                MemorySourceConfig::try_new_exec(&data, Arc::clone(&data_schema), None)
                    .expect("source");
            let started = Instant::now();
            let rows = runtime.block_on(async {
                let sorted = plan_sort(plan, &columns, &ctx, shape.dedicated);
                let batches = datafusion::physical_plan::collect(sorted, Arc::clone(&ctx))
                    .await
                    .expect("sort runs");
                batches.iter().map(RecordBatch::num_rows).sum::<usize>()
            });
            let elapsed = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(
                rows,
                shape.rows.div_ceil(shape.partitions) * shape.partitions
            );
            peak_bytes = peak_bytes.max(pool.peak.load(Ordering::Relaxed));
            // The first call warms allocator and caches; it is not counted.
            if iteration > 0 {
                samples.push(elapsed);
            }
        }
        samples.sort_by(f64::total_cmp);
        println!(
            "shape={} rows={} partitions={} input_mib={} n={} p50_ms={:.3} p99_ms={:.3} p999_ms={:.3} max_ms={:.3} peak_pool_mib={:.1}",
            shape.name,
            shape.rows,
            shape.partitions,
            bytes / MIB,
            samples.len(),
            quantile(&samples, 0.5),
            quantile(&samples, 0.99),
            quantile(&samples, 0.999),
            samples.last().copied().unwrap_or(0.0),
            peak_bytes as f64 / MIB as f64,
        );
    }
}
