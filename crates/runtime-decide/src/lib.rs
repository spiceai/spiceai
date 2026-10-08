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

//! SQL decision functions. `ai_if`, `ai_probability`, `ai_classify`, `ai_score` and
//! `ai_decide` ask typed questions about each row of a decision model (`TypeSafe` Jev,
//! an `OpenAI` decision model) or of any chat model, through the System One evaluation
//! contract ([`evaluate_api::Evaluate`]).
//!
//! Arguments are read by parameter: `DataFusion` drops omitted optional arguments, so
//! `ai_if(x, 'c', on_error => 'null')` arrives with as many arguments as
//! `ai_if(x, 'c', 'jev')`, and the parameter name a named literal carries tells them
//! apart. Constants are checked while the query is planned, before any model is called.
//!
//! [`DecisionPlacement`] (an optimizer rule) lowers every call to `ai_decide` computed
//! in a projection below the node that uses it. Typed calls on the same input and
//! model in one node share one request per row (each distinct `ai_decide` call is its
//! own, and identical ones share it), a `WHERE` clause's other predicates run first,
//! and the functions work in `ORDER BY`, `GROUP BY`, window functions and inner-join
//! conditions, where `DataFusion` cannot evaluate an async function.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::async_udf::AsyncScalarUDF;
use datafusion::optimizer::OptimizerRule;
use evaluate_api::EvaluateModelStore;
use runtime_status::RuntimeStatus;
use tokio::sync::RwLock;

mod args;
mod exec;
mod functions;
mod guard;
mod output;
mod planner;

pub use guard::guard_async_calls;
pub use planner::DecisionPlacement;

use exec::Decider;
use functions::{DecisionUdf, Kind};

/// `ai_if(input, condition)`: whether `condition` holds for `input`.
pub const AI_IF_NAME: &str = "ai_if";
/// `ai_probability(input, condition)`: the probability that `condition` holds.
pub const AI_PROBABILITY_NAME: &str = "ai_probability";
/// `ai_classify(input, labels)`: the label that best fits `input`.
pub const AI_CLASSIFY_NAME: &str = "ai_classify";
/// `ai_score(input, instructions, levels)`: where `input` sits on ordered levels.
pub const AI_SCORE_NAME: &str = "ai_score";
/// `ai_decide(input, questions)`: every answer to a set of typed questions.
pub const AI_DECIDE_NAME: &str = "ai_decide";

runtime_udfs_api::register_spice_function!(AI_IF_SPICE_FUNCTION, AI_IF_NAME);
runtime_udfs_api::register_spice_function!(AI_PROBABILITY_SPICE_FUNCTION, AI_PROBABILITY_NAME);
runtime_udfs_api::register_spice_function!(AI_CLASSIFY_SPICE_FUNCTION, AI_CLASSIFY_NAME);
runtime_udfs_api::register_spice_function!(AI_SCORE_SPICE_FUNCTION, AI_SCORE_NAME);
runtime_udfs_api::register_spice_function!(AI_DECIDE_SPICE_FUNCTION, AI_DECIDE_NAME);

/// Every decision function name.
pub const DECISION_FUNCTION_NAMES: [&str; 5] = [
    AI_IF_NAME,
    AI_PROBABILITY_NAME,
    AI_CLASSIFY_NAME,
    AI_SCORE_NAME,
    AI_DECIDE_NAME,
];

/// The five decision functions over one model store, and the planner rule that goes
/// with them.
#[derive(Debug, Clone)]
pub struct DecisionFunctions {
    set: Arc<FunctionSet>,
}

/// The registered function instances. The planner rule recognizes a call by equality
/// with one of these, not by its name, so a function registered under the same name by
/// someone else is never rewritten.
#[derive(Debug)]
pub(crate) struct FunctionSet {
    pub(crate) condition: Arc<ScalarUDF>,
    pub(crate) probability: Arc<ScalarUDF>,
    pub(crate) classify: Arc<ScalarUDF>,
    pub(crate) score: Arc<ScalarUDF>,
    pub(crate) decide: Arc<ScalarUDF>,
}

impl FunctionSet {
    pub(crate) fn kind_of(&self, func: &ScalarUDF) -> Option<Kind> {
        [
            (&self.condition, Kind::If),
            (&self.probability, Kind::Probability),
            (&self.classify, Kind::Classify),
            (&self.score, Kind::Score),
            (&self.decide, Kind::Decide),
        ]
        .into_iter()
        .find_map(|(udf, kind)| (udf.as_ref() == func).then_some(kind))
    }
}

impl DecisionFunctions {
    /// Decision functions answered by the models in `models`. `status` explains a named
    /// model that is configured but not loaded.
    #[must_use]
    pub fn new(models: Arc<RwLock<EvaluateModelStore>>, status: Arc<RuntimeStatus>) -> Self {
        let decider = Arc::new(Decider::new(models, status));
        let udf = |kind| {
            Arc::new(
                AsyncScalarUDF::new(Arc::new(DecisionUdf::new(kind, Arc::clone(&decider))))
                    .into_scalar_udf(),
            )
        };
        Self {
            set: Arc::new(FunctionSet {
                condition: udf(Kind::If),
                probability: udf(Kind::Probability),
                classify: udf(Kind::Classify),
                score: udf(Kind::Score),
                decide: udf(Kind::Decide),
            }),
        }
    }

    /// The five functions, to register in a session.
    #[must_use]
    pub fn udfs(&self) -> Vec<Arc<ScalarUDF>> {
        vec![
            Arc::clone(&self.set.condition),
            Arc::clone(&self.set.probability),
            Arc::clone(&self.set.classify),
            Arc::clone(&self.set.score),
            Arc::clone(&self.set.decide),
        ]
    }

    /// The optimizer rule that places and merges model calls. It must run after the
    /// other optimizer rules, which can move or merge the plan nodes it builds.
    #[must_use]
    pub fn optimizer_rule(&self) -> Arc<dyn OptimizerRule + Send + Sync> {
        Arc::new(DecisionPlacement::new(Arc::clone(&self.set)))
    }

    /// Registers the functions in `ctx` and appends the planner rule after its existing
    /// optimizer rules.
    pub fn register(&self, ctx: &SessionContext) {
        for udf in self.udfs() {
            ctx.register_udf(udf.as_ref().clone());
        }
        ctx.add_optimizer_rule(self.optimizer_rule());
    }
}
