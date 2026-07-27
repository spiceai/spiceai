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

//! Capture operator metrics from the plan that already executed, instead of
//! re-running `EXPLAIN ANALYZE`.
//!
//! Local queries emit a `plan` child row from the stream-completion hook in
//! [`super::attach_physical_plan_metrics_to_stream`]; distributed queries emit
//! from [`crate::datafusion::query::handle::QueryHandle::spawn_finalize`] using
//! Ballista stage metrics. The exporter's `ExplainAnalyze` re-run path is
//! disabled; `Explain` (plan-only) still uses the exporter re-plan path.

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use ballista_scheduler::display::DisplayableBallistaExecutionPlan;
use ballista_scheduler::state::execution_graph::ExecutionGraph;
use ballista_scheduler::state::execution_stage::ExecutionStage;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use spicepod::component::runtime::TaskHistoryCapturedPlan;

/// Configuration for capturing executed-plan metrics into `task_history`.
#[derive(Debug, Clone)]
pub struct PlanCaptureConfig {
    pub captured_plan: TaskHistoryCapturedPlan,
    pub min_plan_duration_ms: Option<f64>,
    pub min_sql_duration_ms: Option<f64>,
}

impl PlanCaptureConfig {
    #[must_use]
    pub(crate) fn analyze_enabled(&self) -> bool {
        matches!(self.captured_plan, TaskHistoryCapturedPlan::ExplainAnalyze)
    }
}

/// True when the already-built logical plan is `EXPLAIN` / `EXPLAIN ANALYZE`.
///
/// Prefer this on the query hot path — planning has already classified the
/// statement, so there is no need to re-inspect the SQL text.
#[must_use]
pub(crate) fn logical_plan_is_explain(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Explain(_) | LogicalPlan::Analyze(_))
}

/// Cold-path check used by the exporter when only the SQL text remains.
///
/// Parses with [`DFParser`] (same dialect as the rest of the runtime). A parse
/// failure is treated as non-explain: the background EXPLAIN re-plan either
/// succeeds or fails harmlessly.
#[must_use]
fn sql_is_explain(sql: &str) -> bool {
    let Ok(statements) = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}) else {
        return false;
    };
    matches!(statements.front(), Some(Statement::Explain(_)))
}

/// Emission-side eligibility for capturing a plan row from an executed query.
///
/// Callers must already exclude `EXPLAIN` / `EXPLAIN ANALYZE` plans via
/// [`logical_plan_is_explain`] (or by not attaching capture context).
///
/// Duration thresholds are measured at emission time, which is a hair shorter
/// than the final `sql_query` span duration recorded by the exporter. A query
/// landing exactly on `min_sql_duration_ms` could theoretically produce a plan
/// row whose parent is later filtered — an acceptable orphan.
#[must_use]
pub(crate) fn plan_capture_eligible(elapsed_ms: f64, config: &PlanCaptureConfig) -> bool {
    if !config.analyze_enabled() {
        return false;
    }
    if !config
        .min_plan_duration_ms
        .is_none_or(|min| elapsed_ms >= min)
    {
        return false;
    }
    // Mirror exporter retention: only capture when the parent sql_query row
    // would be retained by min_sql_duration_ms.
    config
        .min_sql_duration_ms
        .is_none_or(|min| elapsed_ms >= min)
}

/// Whether the exporter should spawn a background EXPLAIN re-plan for this
/// retained `sql_query` span. Shared with the `Explain` (non-analyze) path.
#[must_use]
pub(crate) fn should_capture_explain_plan(
    task: &str,
    input: &str,
    execution_duration_ms: f64,
    min_plan_duration_ms: Option<f64>,
) -> bool {
    if !min_plan_duration_ms.is_none_or(|min| execution_duration_ms >= min) {
        return false;
    }
    if task != "sql_query" || input.is_empty() {
        return false;
    }
    !sql_is_explain(input)
}

/// Render a local executed plan in the same `plan_type | plan` pretty-table
/// shape that `DataFusion`'s `EXPLAIN ANALYZE` produces (`Plan with Metrics`).
#[must_use]
pub(crate) fn render_local_plan_with_metrics(plan: &dyn ExecutionPlan) -> String {
    let annotated = DisplayableExecutionPlan::with_metrics(plan)
        .indent(false)
        .to_string();
    wrap_plan_with_metrics_table(&annotated)
}

/// Render a distributed job's stages with aggregated executor metrics when
/// available, falling back to a plain plan tree per stage.
#[must_use]
pub(crate) fn render_distributed_plan_with_metrics(graph: &dyn ExecutionGraph) -> String {
    let stages = graph.stages();
    let mut stage_ids: Vec<usize> = stages.keys().copied().collect();
    stage_ids.sort_unstable();

    let mut sections = Vec::with_capacity(stage_ids.len());
    for stage_id in stage_ids {
        let Some(stage) = stages.get(&stage_id) else {
            continue;
        };
        let plan_str = render_stage_plan(stage);
        sections.push(format!(
            "Stage {stage_id} [{}]:\n{plan_str}",
            stage.variant_name()
        ));
    }

    wrap_plan_with_metrics_table(&sections.join("\n"))
}

fn render_stage_plan(stage: &ExecutionStage) -> String {
    let metrics = stage_metrics(stage);
    match metrics {
        Some(m) if !m.is_empty() => DisplayableBallistaExecutionPlan::new(stage.plan(), m)
            .indent()
            .to_string(),
        _ => displayable(stage.plan()).indent(true).to_string(),
    }
}

fn stage_metrics(stage: &ExecutionStage) -> Option<&Vec<MetricsSet>> {
    match stage {
        ExecutionStage::Successful(s) => Some(&s.stage_metrics),
        ExecutionStage::Running(s) => s.stage_metrics.as_ref(),
        ExecutionStage::Failed(s) => s.stage_metrics.as_ref(),
        ExecutionStage::UnResolved(_) | ExecutionStage::Resolved(_) => None,
    }
}

fn wrap_plan_with_metrics_table(plan_text: &str) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("plan_type", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
    ]));
    let batch = match RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["Plan with Metrics"])),
            Arc::new(StringArray::from(vec![plan_text])),
        ],
    ) {
        Ok(b) => b,
        Err(e) => {
            tracing::debug!("Failed to build plan-capture RecordBatch: {e}");
            return plan_text.to_string();
        }
    };
    match pretty_format_batches(&[batch]) {
        Ok(formatted) => formatted.to_string(),
        Err(e) => {
            tracing::debug!("Failed to pretty-format plan-capture batch: {e}");
            plan_text.to_string()
        }
    }
}

/// Emit a `task_history` `plan` child span while the parent `sql_query` span
/// is current. Natural `OTel` nesting supplies `parent_span_id` (same mechanism
/// as [`super::stage_history::emit_stage_span`]).
pub(crate) fn emit_plan_span(input: &str, captured_output: &str) {
    let explain_query = format!("EXPLAIN ANALYZE {input}");
    let plan_span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "plan",
        input = %explain_query,
        runtime_query = true,
        plan_capture = true
    );
    let _guard = plan_span.enter();
    tracing::info!(target: "task_history", captured_output = %captured_output);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema as ArrowSchema;
    use datafusion::common::Statistics;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    };
    use std::fmt::{Debug, Formatter};

    fn cfg(
        mode: TaskHistoryCapturedPlan,
        min_plan: Option<f64>,
        min_sql: Option<f64>,
    ) -> PlanCaptureConfig {
        PlanCaptureConfig {
            captured_plan: mode,
            min_plan_duration_ms: min_plan,
            min_sql_duration_ms: min_sql,
        }
    }

    #[test]
    fn eligible_requires_analyze_mode() {
        assert!(!plan_capture_eligible(
            100.0,
            &cfg(TaskHistoryCapturedPlan::None, None, None)
        ));
        assert!(!plan_capture_eligible(
            100.0,
            &cfg(TaskHistoryCapturedPlan::Explain, None, None)
        ));
        assert!(plan_capture_eligible(
            100.0,
            &cfg(TaskHistoryCapturedPlan::ExplainAnalyze, None, None)
        ));
    }

    #[test]
    fn sql_is_explain_uses_parser() {
        assert!(sql_is_explain("EXPLAIN SELECT 1"));
        assert!(sql_is_explain("  explain analyze SELECT 1"));
        assert!(sql_is_explain("EXPLAIN VERBOSE SELECT 1"));
        assert!(!sql_is_explain("SELECT 1"));
        assert!(!sql_is_explain(""));
        assert!(!sql_is_explain("SELECT 'explain' AS x"));
    }

    #[test]
    fn eligible_enforces_duration_thresholds() {
        assert!(!plan_capture_eligible(
            5.0,
            &cfg(TaskHistoryCapturedPlan::ExplainAnalyze, Some(10.0), None)
        ));
        assert!(plan_capture_eligible(
            10.0,
            &cfg(TaskHistoryCapturedPlan::ExplainAnalyze, Some(10.0), None)
        ));
        assert!(!plan_capture_eligible(
            5.0,
            &cfg(TaskHistoryCapturedPlan::ExplainAnalyze, None, Some(10.0))
        ));
        assert!(plan_capture_eligible(
            10.0,
            &cfg(TaskHistoryCapturedPlan::ExplainAnalyze, None, Some(10.0))
        ));
    }

    #[test]
    fn should_capture_explain_plan_matches_exporter_predicate() {
        assert!(should_capture_explain_plan(
            "sql_query",
            "SELECT 1",
            50.0,
            None
        ));
        assert!(!should_capture_explain_plan(
            "sql_query",
            "EXPLAIN SELECT 1",
            50.0,
            None
        ));
        assert!(!should_capture_explain_plan(
            "ai_chat", "SELECT 1", 50.0, None
        ));
        assert!(!should_capture_explain_plan("sql_query", "", 50.0, None));
        assert!(!should_capture_explain_plan(
            "sql_query",
            "SELECT 1",
            5.0,
            Some(10.0)
        ));
        assert!(should_capture_explain_plan(
            "sql_query",
            "SELECT 1",
            10.0,
            Some(10.0)
        ));
    }

    struct MetricTestPlan {
        metrics: Option<MetricsSet>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        properties: Arc<PlanProperties>,
    }

    impl MetricTestPlan {
        fn leaf(metrics: MetricsSet) -> Self {
            Self {
                metrics: Some(metrics),
                children: vec![],
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(ArrowSchema::empty())),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Final,
                    Boundedness::Bounded,
                )),
            }
        }
    }

    impl Debug for MetricTestPlan {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MetricTestPlan")
        }
    }

    impl DisplayAs for MetricTestPlan {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "MetricTestPlan")
        }
    }

    impl ExecutionPlan for MetricTestPlan {
        fn name(&self) -> &'static str {
            "MetricTestPlan"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.children.iter().collect()
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn metrics(&self) -> Option<MetricsSet> {
            self.metrics.clone()
        }

        fn partition_statistics(
            &self,
            _partition: Option<usize>,
        ) -> datafusion::common::Result<Arc<Statistics>> {
            Ok(Arc::new(Statistics::new_unknown(self.schema().as_ref())))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
            unimplemented!("not used in plan_capture tests")
        }
    }

    #[test]
    fn render_local_includes_metrics_and_table_wrapper() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics_set).output_rows(0).add(42);
        let plan = MetricTestPlan::leaf(metrics_set.clone_inner());
        let rendered = render_local_plan_with_metrics(&plan);
        assert!(
            rendered.contains("Plan with Metrics"),
            "missing table wrapper: {rendered}"
        );
        assert!(
            rendered.contains("MetricTestPlan"),
            "missing operator name: {rendered}"
        );
        assert!(
            rendered.contains("output_rows=42"),
            "missing output_rows: {rendered}"
        );
    }
}
