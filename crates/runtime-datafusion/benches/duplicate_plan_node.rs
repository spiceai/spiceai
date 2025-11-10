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

#![allow(clippy::expect_used)]

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
                .expect("failed to create benchmark DFSchema from Arrow Schema with single non-nullable Int64 field"),
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
        .expect("failed to build base plan for benchmark");

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
            .expect("failed to build nested plan in benchmark");
    }

    plan
}

fn bench_analyze_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("duplicate_plan_node_analyze");

    for size in &[10, 50, 100, 200, 500] {
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
        .expect("failed to build base plan for benchmark");

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

criterion_group!(
    benches,
    bench_analyze_scaling,
    bench_analyze_with_duplicates,
    bench_analyze_no_duplicates
);
criterion_main!(benches);
