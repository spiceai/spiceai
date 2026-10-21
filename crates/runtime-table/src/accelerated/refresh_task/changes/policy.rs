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

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_tools::schema_evolution::{self, EvolutionContext, SchemaEvolution, WideningPlan};
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use runtime_acceleration::change_sink::source_policy::{CdcPolicy, SchemaDecision};
use runtime_acceleration::change_sink::{ChangeCapabilities, SchemaEvolutionSupport};
use runtime_component::dataset::OnSchemaChange;
use runtime_component::schema_evolution::{
    SCHEMA_EVOLUTION_APPLIED, SCHEMA_EVOLUTION_DETECTED, SCHEMA_EVOLUTION_FAILED,
    emit_schema_evolution_event, evolution_allowed, schema_evolution_labels, widening_plan_kind,
};

use super::{
    CdcSchemaEvolution, align_nullability_for_classify, cdc_data_schema_matches,
    schema_evolution_first_warn,
};

/// The warning for a widening the dataset's `on_schema_change` policy does not
/// apply: values are cast to the current schema, and a change it cannot hold
/// exactly is refused.
fn blocked_by_policy_warning(dataset: &str, change: &str, policy: OnSchemaChange) -> String {
    format!(
        "Dataset '{dataset}' received a widening schema change on its CDC stream ({change}), but `on_schema_change: {policy}` only evolves added columns, so values are cast to the current schema and a change with a value that schema cannot hold exactly is refused. Set `on_schema_change: sync_all_columns` to evolve types. See: https://spiceai.org/docs/features/data-acceleration"
    )
}

/// The warning for a widening the acceleration engine applies only at restart.
fn restart_required_warning(dataset: &str, change: &str) -> String {
    format!(
        "Dataset '{dataset}' received a widening schema change on its CDC stream ({change}), but its acceleration engine cannot evolve mid-stream, so until Spice restarts values are cast to the current schema (new columns are dropped) and a change with a value that schema cannot hold exactly is refused. Restart Spice to apply the evolution. See: https://spiceai.org/docs/features/data-acceleration"
    )
}

/// Immutable source policy. No refresh task, sink, or source committer is captured.
pub(super) struct RefreshCdcPolicy {
    pub dataset: TableReference,
    pub settings: Option<Arc<CdcSchemaEvolution>>,
    pub type_rewrites: arrow_tools::type_rewrite::TypeRewriteRules,
}

impl CdcPolicy for RefreshCdcPolicy {
    fn classify(
        &self,
        incoming: &SchemaRef,
        target: &SchemaRef,
        capabilities: ChangeCapabilities,
    ) -> Result<SchemaDecision> {
        let Some(settings) = &self.settings else {
            return Ok(SchemaDecision::Proceed);
        };
        if settings.policy == OnSchemaChange::Block
            || cdc_data_schema_matches(target, incoming, self.type_rewrites)
        {
            return Ok(SchemaDecision::Proceed);
        }
        let normalized = if self.type_rewrites.is_empty() {
            Arc::clone(incoming)
        } else {
            Arc::new(arrow_tools::type_rewrite::apply_rules(
                incoming,
                self.type_rewrites,
            ))
        };
        let aligned = align_nullability_for_classify(target, &normalized);
        let context = EvolutionContext {
            constraint_columns: &settings.constraint_columns,
        };
        let dataset = self.dataset.to_string();
        match schema_evolution::classify(target, &aligned, &context) {
            SchemaEvolution::Identical => Ok(SchemaDecision::Proceed),
            SchemaEvolution::Widening(plan) => {
                let kind = widening_plan_kind(&plan);
                let change = plan.describe();
                SCHEMA_EVOLUTION_DETECTED
                    .add(1, &schema_evolution_labels(&dataset, kind, "cdc_stream"));
                if settings.policy == OnSchemaChange::Fail {
                    SCHEMA_EVOLUTION_FAILED
                        .add(1, &schema_evolution_labels(&dataset, kind, "fail_policy"));
                    emit_schema_evolution_event(&dataset, "fail_policy", &change, true);
                    return Err(DataFusionError::Execution(format!(
                        "schema change detected on the CDC stream for {dataset} ({change}) and `on_schema_change: fail` is set. Revert the source schema change, or set `on_schema_change: append_new_columns`/`sync_all_columns` to evolve"
                    )));
                }
                if !evolution_allowed(settings.policy, &plan) {
                    SCHEMA_EVOLUTION_FAILED.add(
                        1,
                        &schema_evolution_labels(&dataset, kind, "blocked_by_policy"),
                    );
                    if schema_evolution_first_warn(format!("{dataset}|policy|{change}")) {
                        tracing::warn!(dataset = %dataset, "{}", blocked_by_policy_warning(&dataset, &change, settings.policy));
                        emit_schema_evolution_event(&dataset, "blocked_by_policy", &change, true);
                    }
                    return Ok(SchemaDecision::Proceed);
                }
                match capabilities.schema_evolution {
                    SchemaEvolutionSupport::Live => Ok(SchemaDecision::Evolve(plan)),
                    SchemaEvolutionSupport::Recreate => {
                        SCHEMA_EVOLUTION_FAILED.add(
                            1,
                            &schema_evolution_labels(&dataset, kind, "partitioned_unsupported"),
                        );
                        emit_schema_evolution_event(
                            &dataset,
                            "partitioned_unsupported",
                            &change,
                            true,
                        );
                        // The backend supplies the engine-specific refusal before mutation.
                        Ok(SchemaDecision::Evolve(plan))
                    }
                    SchemaEvolutionSupport::Restart => {
                        SCHEMA_EVOLUTION_FAILED.add(
                            1,
                            &schema_evolution_labels(&dataset, kind, "restart_required"),
                        );
                        if schema_evolution_first_warn(format!("{dataset}|restart|{change}")) {
                            tracing::warn!(dataset = %dataset, "{}", restart_required_warning(&dataset, &change));
                            emit_schema_evolution_event(
                                &dataset,
                                "restart_required",
                                &change,
                                true,
                            );
                        }
                        Ok(SchemaDecision::Proceed)
                    }
                }
            }
            SchemaEvolution::Incompatible { reason } => {
                SCHEMA_EVOLUTION_DETECTED.add(
                    1,
                    &schema_evolution_labels(&dataset, "incompatible", "cdc_stream"),
                );
                if settings.policy == OnSchemaChange::Fail {
                    SCHEMA_EVOLUTION_FAILED.add(
                        1,
                        &schema_evolution_labels(&dataset, "incompatible", "fail_policy"),
                    );
                    emit_schema_evolution_event(&dataset, "fail_policy", &reason, true);
                    return Err(DataFusionError::Execution(format!(
                        "incompatible schema change detected on the CDC stream for {dataset}: {reason}. `on_schema_change: fail` is set"
                    )));
                }
                SCHEMA_EVOLUTION_FAILED.add(
                    1,
                    &schema_evolution_labels(&dataset, "incompatible", "incompatible"),
                );
                if schema_evolution_first_warn(format!("{dataset}|incompatible|{reason}")) {
                    tracing::warn!(dataset = %dataset, "incompatible schema change detected on the CDC stream: {reason}. Values continue to be cast to the current schema");
                    emit_schema_evolution_event(&dataset, "incompatible", &reason, true);
                }
                Ok(SchemaDecision::Proceed)
            }
        }
    }

    fn applied(&self, plan: &WideningPlan) {
        let dataset = self.dataset.to_string();
        let kind = widening_plan_kind(plan);
        let change = plan.describe();
        SCHEMA_EVOLUTION_APPLIED.add(1, &schema_evolution_labels(&dataset, kind, "cdc_live"));
        tracing::info!(dataset = %dataset, "applied live schema evolution from the CDC stream: {change}");
        emit_schema_evolution_event(&dataset, "cdc_live", &change, false);
    }

    fn split_on_schema_change(&self) -> bool {
        self.settings
            .as_ref()
            .is_some_and(|settings| settings.policy != OnSchemaChange::Block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_change_warnings_name_the_dataset_the_consequence_and_the_fix() {
        assert_eq!(
            blocked_by_policy_warning(
                "r1",
                "1 widened (k: Int32 -> Float64)",
                OnSchemaChange::AppendNewColumns
            ),
            "Dataset 'r1' received a widening schema change on its CDC stream (1 widened (k: Int32 -> Float64)), but `on_schema_change: append_new_columns` only evolves added columns, so values are cast to the current schema and a change with a value that schema cannot hold exactly is refused. Set `on_schema_change: sync_all_columns` to evolve types. See: https://spiceai.org/docs/features/data-acceleration"
        );
        assert_eq!(
            restart_required_warning("r1", "1 widened (k: Int32 -> Float64)"),
            "Dataset 'r1' received a widening schema change on its CDC stream (1 widened (k: Int32 -> Float64)), but its acceleration engine cannot evolve mid-stream, so until Spice restarts values are cast to the current schema (new columns are dropped) and a change with a value that schema cannot hold exactly is refused. Restart Spice to apply the evolution. See: https://spiceai.org/docs/features/data-acceleration"
        );
    }
}
