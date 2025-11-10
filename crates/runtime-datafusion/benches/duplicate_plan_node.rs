/*
Copyright 2025 The Spice.ai OSS Authors

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

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::DFSchema,
    config::ConfigOptions,
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore},
    optimizer::AnalyzerRule,
};
use runtime_datafusion::analyzer_rule::duplicate_plan_node::DuplicateLogicalPlanNode;
use std::{cmp::Ordering, fmt, sync::Arc};

/// A simple extension node for benchmarking
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BenchExtension {
    name: &'static str,
    id: usize,
}

impl PartialOrd for BenchExtension {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BenchExtension {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl UserDefinedLogicalNodeCore for BenchExtension {
    fn name(&self) -> &str {
        self.name
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &Arc<DFSchema> {
        static SCHEMA: std::sync::OnceLock<Arc<DFSchema>> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(|| {
            Arc::new(
                DFSchema::try_from(Schema::new(vec![Field::new(
                    "bench",
                    DataType::Int64,
                    false,
                )]))
                .expect("valid schema"),
            )
        })
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BenchExtension({}, {})", self.name, self.id)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(self.clone())
    }
}

fn create_plan_tree(depth: usize, extension_name: &'static str) -> LogicalPlan {
    let mut plan = LogicalPlanBuilder::empty(false)
        .build()
        .expect("valid base plan");

    // Create a tree with multiple levels, adding extension nodes periodically
    for i in 0..depth {
        // Add extension node every 3 levels to simulate realistic query plans
        if i % 3 == 0 {
            plan = LogicalPlan::Extension(Extension {
                node: Arc::new(BenchExtension {
                    name: extension_name,
                    id: i,
                }),
            });
        }

        // Wrap in another logical plan node
        plan = LogicalPlanBuilder::from(plan)
            .build()
            .expect("valid nested plan");
    }

    plan
}

fn bench_analyze_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("duplicate_plan_node_analyze");

    for size in [10, 50, 100, 200, 500].iter() {
        let plan = create_plan_tree(*size, "bench_ext");
        let rule = DuplicateLogicalPlanNode::extension_nodes("bench_ext");
        let config = ConfigOptions::default();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let result = rule.analyze(black_box(plan.clone()), &config);
                black_box(result)
            });
        });
    }

    group.finish();
}

fn bench_analyze_with_duplicates(c: &mut Criterion) {
    let mut group = c.benchmark_group("duplicate_plan_node_with_duplicates");

    // Create plan with many duplicate extension nodes
    let mut plan = LogicalPlanBuilder::empty(false)
        .build()
        .expect("valid base plan");

    // Add 100 nested duplicate extensions
    for i in 0..100 {
        plan = LogicalPlan::Extension(Extension {
            node: Arc::new(BenchExtension {
                name: "dup_ext",
                id: i,
            }),
        });
    }

    let rule = DuplicateLogicalPlanNode::extension_nodes("dup_ext");
    let config = ConfigOptions::default();

    group.bench_function("100_duplicates", |b| {
        b.iter(|| {
            let result = rule.analyze(black_box(plan.clone()), &config);
            black_box(result)
        });
    });

    group.finish();
}

fn bench_analyze_no_duplicates(c: &mut Criterion) {
    let mut group = c.benchmark_group("duplicate_plan_node_no_duplicates");

    // Create plan with no matching extensions
    let plan = create_plan_tree(100, "other_ext");
    let rule = DuplicateLogicalPlanNode::extension_nodes("target_ext");
    let config = ConfigOptions::default();

    group.bench_function("100_nodes_no_match", |b| {
        b.iter(|| {
            let result = rule.analyze(black_box(plan.clone()), &config);
            black_box(result)
        });
    });

    group.finish();
}

fn bench_xxh3_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("xxh3_hash_operations");

    use std::collections::HashSet;
    use xxhash_rust::xxh3::Xxh3Builder;

    let plan = create_plan_tree(100, "bench");
    let plan_ptr = &plan as *const LogicalPlan;

    group.bench_function("insert_100_pointers", |b| {
        b.iter(|| {
            let mut set: HashSet<*const LogicalPlan, Xxh3Builder> =
                HashSet::with_capacity_and_hasher(100, Xxh3Builder::new());

            for i in 0..100 {
                // Simulate different pointers
                let offset = (i % 10) as isize;
                let ptr = unsafe { plan_ptr.offset(offset) };
                set.insert(ptr);
            }

            black_box(set)
        });
    });

    group.bench_function("lookup_100_pointers", |b| {
        let mut set: HashSet<*const LogicalPlan, Xxh3Builder> =
            HashSet::with_capacity_and_hasher(100, Xxh3Builder::new());

        for i in 0..100 {
            let offset = (i % 10) as isize;
            let ptr = unsafe { plan_ptr.offset(offset) };
            set.insert(ptr);
        }

        b.iter(|| {
            let mut count = 0;
            for i in 0..100 {
                let offset = (i % 10) as isize;
                let ptr = unsafe { plan_ptr.offset(offset) };
                if set.contains(&ptr) {
                    count += 1;
                }
            }
            black_box(count)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_analyze_scaling,
    bench_analyze_with_duplicates,
    bench_analyze_no_duplicates,
    bench_xxh3_performance
);
criterion_main!(benches);
