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

//! Logical extension node for a distributed full-text search.
//!
//! [`DistributedSearchNode`] is planned into a
//! [`DistributedSearchExec`](crate::exec::DistributedSearchExec) by
//! [`DistributedSearchExtensionPlanner`](crate::exec::DistributedSearchExtensionPlanner).

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
};

use datafusion::{
    common::{DFSchemaRef, Result},
    error::DataFusionError,
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    prelude::Expr,
};

use crate::exec::{DistributedExecutor, DistributedSearchParams};

/// Logical extension node for a distributed full-text search.
#[derive(Debug)]
pub struct DistributedSearchNode {
    stats_input: LogicalPlan,
    schema: DFSchemaRef,
    params: DistributedSearchParams,
    executors: Vec<DistributedExecutor>,
}

impl DistributedSearchNode {
    #[must_use]
    pub fn new(
        stats_input: LogicalPlan,
        schema: DFSchemaRef,
        params: DistributedSearchParams,
        executors: Vec<DistributedExecutor>,
    ) -> Self {
        Self {
            stats_input,
            schema,
            params,
            executors,
        }
    }

    #[must_use]
    pub fn params(&self) -> &DistributedSearchParams {
        &self.params
    }

    #[must_use]
    pub fn executors(&self) -> &[DistributedExecutor] {
        &self.executors
    }

    #[must_use]
    pub fn output_schema(&self) -> &DFSchemaRef {
        &self.schema
    }
}

impl Hash for DistributedSearchNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stats_input.hash(state);
        self.params.hash(state);
        for executor in &self.executors {
            executor.id.hash(state);
        }
    }
}

impl PartialEq for DistributedSearchNode {
    fn eq(&self, other: &Self) -> bool {
        self.stats_input == other.stats_input
            && self.params == other.params
            && self
                .executors
                .iter()
                .map(|e| &e.id)
                .eq(other.executors.iter().map(|e| &e.id))
    }
}

impl Eq for DistributedSearchNode {}

impl PartialOrd for DistributedSearchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.stats_input.partial_cmp(&other.stats_input)
    }
}

impl UserDefinedLogicalNodeCore for DistributedSearchNode {
    fn name(&self) -> &'static str {
        "DistributedSearchNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.stats_input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedSearchNode query={} executors={}",
            self.params.query,
            self.executors.len()
        )
    }

    /// The output schema differs from the stats input schema, so there is no
    /// column passthrough mapping to expose for projection push-down.
    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        None
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "DistributedSearchNode expects no expressions, got {}",
                exprs.len()
            )));
        }
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "DistributedSearchNode expects exactly one input, got {}",
                inputs.len()
            )));
        }
        let stats_input = inputs.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("DistributedSearchNode requires one input".to_string())
        })?;
        Ok(Self {
            stats_input,
            schema: std::sync::Arc::clone(&self.schema),
            params: self.params.clone(),
            executors: self.executors.clone(),
        })
    }
}
