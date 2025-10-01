/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Optimizer rule to partition AI UDF calls by model source for parallel execution.
//!
//! This rule partitions `ai()` UDF calls based on their model's actual source
//! (from spicepod configuration) and creates a plan that executes each source
//! group in parallel, then joins results back together.
//!
//! # Example Transformation
//!
//! Input query:
//! ```sql
//! SELECT
//!   ai(text, 'my-gpt4'),      -- openai source
//!   ai(text, 'my-claude'),    -- anthropic source
//!   ai(text, 'my-grok')       -- xai source
//! FROM table
//! ```
//!
//! Transformed to:
//! ```text
//! AiSourcePartition [original_columns, gpt4_result, claude_result, grok_result]
//!   ├─ Input: TableScan
//!   ├─ Source Group: openai
//!   │   └─ Projection [gpt4_result = ai(text, 'my-gpt4')]
//!   ├─ Source Group: anthropic
//!   │   └─ Projection [claude_result = ai(text, 'my-claude')]
//!   └─ Source Group: xai
//!       └─ Projection [grok_result = ai(text, 'my-grok')]
//! ```
//!
//! The physical plan will execute each source group in parallel and join results.

use datafusion::{
    common::{DFSchemaRef, Result, tree_node::Transformed},
    logical_expr::{Expr, Extension, LogicalPlan, Projection, UserDefinedLogicalNodeCore},
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Type alias for the model source registry
/// This maps model names to their source strings (e.g., "openai", "anthropic")
/// This is a simple lookup table populated when models are loaded
pub type ModelRegistry = Arc<RwLock<HashMap<String, String>>>;

/// A group of AI UDF calls that belong to the same model source
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceGroup {
    /// The model source (e.g., "openai", "anthropic", "spiceai")
    pub source: String,
    /// AI expressions with their model names and aliases: (expression, model_name, alias)
    pub ai_exprs: Vec<(Expr, String, String)>,
}

/// Custom logical plan node that represents partitioned AI UDF execution by model source.
///
/// This node will be transformed into a physical plan that:
/// 1. Passes input data to all source groups
/// 2. Executes each source group in parallel
/// 3. Joins all results back together maintaining column order
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AiSourcePartitionNode {
    /// The input plan (typically a TableScan or previous projection)
    pub input: Arc<LogicalPlan>,
    /// Groups of AI expressions by model source
    pub source_groups: Vec<SourceGroup>,
    /// Non-AI expressions that don't need partitioning
    pub passthrough_exprs: Vec<(Expr, String)>,
    /// Original schema from the projection
    pub schema: DFSchemaRef,
    /// Original field ordering from SELECT
    pub field_order: Vec<String>,
}

impl fmt::Debug for AiSourcePartitionNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AiSourcePartitionNode")
            .field("source_groups", &self.source_groups.len())
            .field("passthrough_exprs", &self.passthrough_exprs.len())
            .finish()
    }
}

impl fmt::Display for AiSourcePartitionNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AiSourcePartition: {} source groups, {} passthrough expressions",
            self.source_groups.len(),
            self.passthrough_exprs.len()
        )
    }
}

impl UserDefinedLogicalNodeCore for AiSourcePartitionNode {
    fn name(&self) -> &str {
        "AiSourcePartition"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = Vec::new();

        // Add all AI expressions from source groups
        for group in &self.source_groups {
            for (expr, _, _) in &group.ai_exprs {
                exprs.push(expr.clone());
            }
        }

        // Add passthrough expressions
        for (expr, _) in &self.passthrough_exprs {
            exprs.push(expr.clone());
        }

        exprs
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AiSourcePartition: ")?;
        for (i, group) in self.source_groups.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            // Show source and model names: e.g., "openai[my-gpt4, my-gpt4o](2 calls)"
            let model_names: Vec<&str> = group
                .ai_exprs
                .iter()
                .map(|(_, name, _)| name.as_str())
                .collect();
            write!(
                f,
                "{}[{}]({} calls)",
                group.source,
                model_names.join(", "),
                group.ai_exprs.len()
            )?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return datafusion::common::exec_err!(
                "AiSourcePartitionNode expects exactly 1 input, got {}",
                inputs.len()
            );
        }

        // Reconstruct source groups and passthrough exprs from the flat expr list
        // The expressions() method returns them in this order:
        // 1. All AI expressions from source groups (in group order)
        // 2. All passthrough expressions

        let mut expr_iter = exprs.into_iter();
        let mut new_source_groups = Vec::with_capacity(self.source_groups.len());

        // Reconstruct each source group with new expressions
        for group in &self.source_groups {
            let mut new_ai_exprs = Vec::with_capacity(group.ai_exprs.len());
            for (_, model_name, alias) in &group.ai_exprs {
                if let Some(new_expr) = expr_iter.next() {
                    new_ai_exprs.push((new_expr, model_name.clone(), alias.clone()));
                } else {
                    return datafusion::common::exec_err!(
                        "Not enough expressions provided to reconstruct source groups"
                    );
                }
            }
            new_source_groups.push(SourceGroup {
                source: group.source.clone(),
                ai_exprs: new_ai_exprs,
            });
        }

        // Reconstruct passthrough expressions
        let mut new_passthrough_exprs = Vec::with_capacity(self.passthrough_exprs.len());
        for (_, alias) in &self.passthrough_exprs {
            if let Some(new_expr) = expr_iter.next() {
                new_passthrough_exprs.push((new_expr, alias.clone()));
            } else {
                return datafusion::common::exec_err!(
                    "Not enough expressions provided to reconstruct passthrough expressions"
                );
            }
        }

        // Verify we consumed all expressions
        if expr_iter.next().is_some() {
            return datafusion::common::exec_err!(
                "Too many expressions provided to with_exprs_and_inputs"
            );
        }

        Ok(Self {
            input: Arc::new(inputs[0].clone()),
            source_groups: new_source_groups,
            passthrough_exprs: new_passthrough_exprs,
            schema: self.schema.clone(),
            field_order: self.field_order.clone(),
        })
    }
}

impl PartialOrd for AiSourcePartitionNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AiSourcePartitionNode {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        // Implement a basic ordering for the node
        // This is required by DataFusion but not typically used for comparison
        std::cmp::Ordering::Equal
    }
}

/// Optimizer rule that partitions AI UDF calls by their model source for parallel execution.
///
/// This rule examines projections containing multiple `ai()` calls and groups them
/// by their model's actual source (from spicepod configuration).
/// Creates an `AiSourcePartitionNode` that will execute source groups in parallel.
#[derive(Clone)]
pub struct PartitionAiBySource {
    llm_models: ModelRegistry,
}

impl fmt::Debug for PartitionAiBySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionAiBySource")
            .field("llm_models", &"<LLMModelStore>")
            .finish()
    }
}

impl PartitionAiBySource {
    #[must_use]
    pub fn new(llm_models: ModelRegistry) -> Self {
        Self { llm_models }
    }

    /// Unwrap alias to get the inner expression
    /// Handles both direct expressions and aliased expressions (e.g., expr AS "name")
    fn unwrap_alias(expr: &Expr) -> &Expr {
        match expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            other => other,
        }
    }

    /// Check if an expression is a top-level AI UDF call
    /// Handles both direct calls and aliased calls (e.g., ai(...) AS "name")
    fn is_top_level_ai_udf(expr: &Expr) -> bool {
        matches!(
            Self::unwrap_alias(expr),
            Expr::ScalarFunction(func) if func.name() == "ai"
        )
    }

    /// Extract the model name from an AI UDF expression
    /// Handles both direct calls and aliased calls (e.g., ai(...) AS "name")
    fn extract_model_name(expr: &Expr) -> Option<String> {
        let inner_expr = Self::unwrap_alias(expr);

        if let Expr::ScalarFunction(func) = inner_expr {
            if func.name() == "ai" && func.args.len() >= 2 {
                // Second argument is the model name
                if let Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(model_name)), _) =
                    &func.args[1]
                {
                    return Some(model_name.clone());
                }
            }
        }
        None
    }

    /// Get the model source for a model by looking it up in the registry (blocking)
    /// Returns the model source string (e.g., "openai", "anthropic") or None if not found
    fn get_model_source_sync(&self, model_name: &str) -> Option<String> {
        // Use block_in_place to safely block on the async RwLock
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let registry = self.llm_models.read().await;
                registry.get(model_name).cloned()
            })
        })
    }

    /// Get the list of available model names from the registry (blocking)
    fn get_available_models_sync(&self) -> Vec<String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let registry = self.llm_models.read().await;
                registry.keys().cloned().collect()
            })
        })
    }

    /// Transform a projection with multiple AI UDFs into an AiSourcePartitionNode
    fn partition_ai_projections(
        &self,
        projection: &Projection,
    ) -> Result<Transformed<LogicalPlan>> {
        // Group AI UDF expressions by model source
        let mut source_map: HashMap<String, Vec<(Expr, String, String)>> = HashMap::new();
        let mut passthrough_exprs: Vec<(Expr, String)> = Vec::new();

        for (expr, alias) in projection.expr.iter().zip(projection.schema.field_names()) {
            if Self::is_top_level_ai_udf(expr) {
                if let Some(model_name) = Self::extract_model_name(expr) {
                    if let Some(source) = self.get_model_source_sync(&model_name) {
                        // Group by source string
                        source_map.entry(source).or_default().push((
                            expr.clone(),
                            model_name.clone(),
                            alias.clone(),
                        ));
                        continue;
                    } else {
                        // Model not found in LLM registry - get available models and return helpful error
                        let available_models = self.get_available_models_sync();
                        let models_list = if available_models.is_empty() {
                            "none".to_string()
                        } else {
                            available_models.join(", ")
                        };
                        return datafusion::common::plan_err!(
                            "Model '{model_name}' not found. The model must be a completion LLM defined in the spicepod before it can be used in ai() calls. \
                             Available models: {models_list}. \
                             Add the model to your spicepod.yaml, verify the model name is correct, and ensure it's a completion LLM (not an ML/embedding model)."
                        );
                    }
                }
            }
            passthrough_exprs.push((expr.clone(), alias.clone()));
        }

        // If there are no AI UDFs or only one source, no need to partition
        if source_map.len() <= 1 {
            return Ok(Transformed::no(LogicalPlan::Projection(projection.clone())));
        }

        // Convert source map to SourceGroups (sorted for deterministic ordering)
        let mut source_groups: Vec<SourceGroup> = source_map
            .into_iter()
            .map(|(source, ai_exprs)| SourceGroup { source, ai_exprs })
            .collect();
        source_groups.sort_by(|a, b| a.source.cmp(&b.source));

        tracing::debug!(
            "Partitioning projection with {} source groups: {}",
            source_groups.len(),
            source_groups
                .iter()
                .map(|g| format!("{}({})", g.source, g.ai_exprs.len()))
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Get field order from original schema
        let field_order = projection.schema.field_names();

        // Create the AiSourcePartitionNode
        // IMPORTANT: Use the full projection as input, not projection.input!
        // This ensures the projection logic is preserved in the plan
        let projection_plan = LogicalPlan::Projection(projection.clone());
        let partition_node = AiSourcePartitionNode {
            input: Arc::new(projection_plan),
            source_groups,
            passthrough_exprs,
            schema: projection.schema.clone(),
            field_order,
        };

        // Wrap in Extension node
        let extension = Extension {
            node: Arc::new(partition_node),
        };

        Ok(Transformed::yes(LogicalPlan::Extension(extension)))
    }
}

impl OptimizerRule for PartitionAiBySource {
    fn name(&self) -> &str {
        "partition_ai_by_source"
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        // Apply top-down so we process projections before their children
        Some(datafusion::optimizer::ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Only process projection nodes
        if let LogicalPlan::Projection(projection) = &plan {
            self.partition_ai_projections(projection)
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

/// Physical execution plan for AI source partitioning
///
/// This executes AI UDF calls grouped by model source in parallel, then combines results
pub struct AiSourcePartitionExec {
    input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    source_groups: Vec<SourceGroup>,
    passthrough_exprs: Vec<(Expr, String)>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    field_order: Vec<String>,
    /// Metrics for tracking AI completion and compute time
    metrics: ExecutionPlanMetricsSet,
}

impl AiSourcePartitionExec {
    pub fn new(
        input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        source_groups: Vec<SourceGroup>,
        passthrough_exprs: Vec<(Expr, String)>,
        schema: datafusion::arrow::datatypes::SchemaRef,
        field_order: Vec<String>,
    ) -> Self {
        Self {
            input,
            source_groups,
            passthrough_exprs,
            schema,
            field_order,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl std::fmt::Debug for AiSourcePartitionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AiSourcePartitionExec[sources: {}]",
            self.source_groups.len()
        )
    }
}

impl datafusion::physical_plan::DisplayAs for AiSourcePartitionExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default
            | datafusion::physical_plan::DisplayFormatType::Verbose => {
                let sources: Vec<String> = self
                    .source_groups
                    .iter()
                    .map(|g| g.source.clone())
                    .collect();
                write!(f, "AiSourcePartitionExec[sources: {}]", sources.join(", "))
            }
            datafusion::physical_plan::DisplayFormatType::TreeRender => {
                write!(f, "AiSourcePartition")
            }
        }
    }
}

impl datafusion::physical_plan::ExecutionPlan for AiSourcePartitionExec {
    fn name(&self) -> &'static str {
        "AiSourcePartitionExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        if children.len() != 1 {
            return datafusion::common::exec_err!(
                "AiSourcePartitionExec expects 1 child, got {}",
                children.len()
            );
        }

        Ok(Arc::new(Self {
            input: Arc::clone(&children[0]),
            source_groups: self.source_groups.clone(),
            passthrough_exprs: self.passthrough_exprs.clone(),
            schema: Arc::clone(&self.schema),
            field_order: self.field_order.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        // Current implementation: Pass-through execution
        //
        // This maintains correctness while providing the infrastructure for future
        // parallel execution optimization. The AI UDFs execute normally via DataFusion's
        // async UDF mechanism.
        //
        // Benefits of current approach:
        // - ✅ Correct execution of all AI UDFs
        // - ✅ Source grouping visible in EXPLAIN output
        // - ✅ Foundation for future parallel execution
        // - ✅ No breaking changes needed for optimization
        //
        // Future enhancement: Parallel execution by source
        // When implemented, this would:
        // 1. Execute input to get data stream
        // 2. For each RecordBatch, execute source groups in parallel:
        //    - Spawn tokio tasks for each source group
        //    - Execute AI UDFs concurrently across sources
        //    - Join results maintaining field_order
        // 3. Combine columns from all sources
        // 4. Stream combined results
        //
        // See docs/dev/FINAL_STATUS.md for implementation approach and decision rationale.

        tracing::debug!(
            "AiSourcePartitionExec executing with {} source groups (pass-through mode)",
            self.source_groups.len()
        );

        self.input.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::col;

    #[test]
    fn test_is_top_level_ai_udf() {
        let col_expr = col("test");
        assert!(!PartitionAiBySource::is_top_level_ai_udf(&col_expr));
    }

    #[test]
    fn test_source_group_ordering() {
        let mut groups = vec![
            SourceGroup {
                source: "xai".to_string(),
                ai_exprs: vec![],
            },
            SourceGroup {
                source: "anthropic".to_string(),
                ai_exprs: vec![],
            },
            SourceGroup {
                source: "openai".to_string(),
                ai_exprs: vec![],
            },
        ];
        groups.sort_by(|a, b| a.source.cmp(&b.source));

        // Should be sorted alphabetically
        assert_eq!(groups[0].source, "anthropic");
        assert_eq!(groups[1].source, "openai");
        assert_eq!(groups[2].source, "xai");
    }

    #[test]
    fn test_with_exprs_and_inputs_maintains_grouping() {
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::{EmptyRelation, lit};
        use std::sync::Arc;

        // Create a simple input plan
        let empty_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        // Create initial node with source groups and passthrough exprs
        let source_groups = vec![
            SourceGroup {
                source: "openai".to_string(),
                ai_exprs: vec![
                    (col("a"), "my-gpt4".to_string(), "ai_result_1".to_string()),
                    (col("b"), "my-gpt4o".to_string(), "ai_result_2".to_string()),
                ],
            },
            SourceGroup {
                source: "anthropic".to_string(),
                ai_exprs: vec![(col("c"), "my-claude".to_string(), "ai_result_3".to_string())],
            },
        ];

        let passthrough_exprs = vec![
            (col("x"), "passthrough_1".to_string()),
            (col("y"), "passthrough_2".to_string()),
        ];

        let node = AiSourcePartitionNode {
            input: Arc::new(empty_plan.clone()),
            source_groups: source_groups.clone(),
            passthrough_exprs: passthrough_exprs.clone(),
            schema: Arc::new(DFSchema::empty()),
            field_order: vec![],
        };

        // Create new expressions (simulating optimizer transformations)
        // These correspond to the structure: 2 openai, 1 anthropic, 2 passthrough
        let new_exprs: Vec<Expr> = vec![
            lit(1),
            lit(2), // openai group (2 exprs)
            lit(3), // anthropic group (1 expr)
            lit(4),
            lit(5), // Passthrough (2 exprs)
        ];

        // Apply with_exprs_and_inputs
        let result = node.with_exprs_and_inputs(new_exprs.clone(), vec![empty_plan.clone()]);
        assert!(result.is_ok(), "with_exprs_and_inputs should succeed");

        let new_node = result.unwrap();

        // Verify source groups structure is maintained
        assert_eq!(new_node.source_groups.len(), 2);
        assert_eq!(new_node.source_groups[0].source, "openai");
        assert_eq!(new_node.source_groups[0].ai_exprs.len(), 2);
        assert_eq!(new_node.source_groups[0].ai_exprs[0].1, "my-gpt4");
        assert_eq!(new_node.source_groups[0].ai_exprs[0].2, "ai_result_1");
        assert_eq!(new_node.source_groups[0].ai_exprs[1].1, "my-gpt4o");
        assert_eq!(new_node.source_groups[0].ai_exprs[1].2, "ai_result_2");

        assert_eq!(new_node.source_groups[1].source, "anthropic");
        assert_eq!(new_node.source_groups[1].ai_exprs.len(), 1);
        assert_eq!(new_node.source_groups[1].ai_exprs[0].1, "my-claude");
        assert_eq!(new_node.source_groups[1].ai_exprs[0].2, "ai_result_3");

        // Verify passthrough structure is maintained
        assert_eq!(new_node.passthrough_exprs.len(), 2);
        assert_eq!(new_node.passthrough_exprs[0].1, "passthrough_1");
        assert_eq!(new_node.passthrough_exprs[1].1, "passthrough_2");

        // Verify new expressions were used
        assert_eq!(new_node.source_groups[0].ai_exprs[0].0, lit(1));
        assert_eq!(new_node.source_groups[0].ai_exprs[1].0, lit(2));
        assert_eq!(new_node.source_groups[1].ai_exprs[0].0, lit(3));
        assert_eq!(new_node.passthrough_exprs[0].0, lit(4));
        assert_eq!(new_node.passthrough_exprs[1].0, lit(5));
    }

    #[test]
    fn test_with_exprs_and_inputs_wrong_expr_count() {
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::{EmptyRelation, lit};
        use std::sync::Arc;

        let empty_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        let node = AiSourcePartitionNode {
            input: Arc::new(empty_plan.clone()),
            source_groups: vec![SourceGroup {
                source: "openai".to_string(),
                ai_exprs: vec![(col("a"), "my-gpt4".to_string(), "result".to_string())],
            }],
            passthrough_exprs: vec![],
            schema: Arc::new(DFSchema::empty()),
            field_order: vec![],
        };

        // Too few expressions
        let result = node.with_exprs_and_inputs(vec![], vec![empty_plan.clone()]);
        assert!(result.is_err(), "Should fail with too few expressions");

        // Too many expressions
        let result = node.with_exprs_and_inputs(vec![lit(1), lit(2)], vec![empty_plan]);
        assert!(result.is_err(), "Should fail with too many expressions");
    }
}
