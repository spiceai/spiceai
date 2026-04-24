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

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use crate::datafusion::udtf::flatten_json::{
    FLATTEN_JSON_UDTF_NAME, FlattenJsonScalar, FlattenJsonTableFunc,
};
use crate::datafusion::udtf::json_properties::{
    FLATTEN_JSON_PROPERTIES_UDTF_NAME, FlattenJsonPropertiesScalar, FlattenJsonPropertiesTableFunc,
};
use crate::datafusion::udtf::json_tree::{JSON_TREE_UDTF_NAME, JsonTreeScalar, JsonTreeTableFunc};
use crate::embeddings::udtf::{VECTOR_SEARCH_UDTF_NAME, VectorSearchTableFunc};
use crate::search::full_text::udtf::{TEXT_SEARCH_UDTF_NAME, TextSearchTableFunc};
use crate::search::rerank::{RERANK_UDTF_NAME, RerankTableFunc};
use crate::search::rrf;
use crate::search::rrf::RRF_UDF_NAME;
use crate::search::util::parse_explicit_primary_keys;
use datafusion::functions::math::random::RandomFunc;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::supported_functions::{FunctionRestriction, FunctionSupport};
#[cfg(feature = "models")]
use runtime_datafusion_udfs::{
    ai::{AI_UDF_NAME, Ai},
    embed::{self, EMBED_UDF_NAME},
};
#[cfg(not(feature = "models"))]
const EMBED_UDF_NAME: &str = "embed";
use runtime_datafusion_udfs::{
    alias::ScalarUDFAlias,
    bucket::{BUCKET_SCALAR_UDF_NAME, Bucket},
    cosine_distance::{COSINE_DISTANCE_UDF_NAME, CosineDistance},
    digest_many::{DIGEST_UDF_NAME, INSTANCE},
    truncate::{TRUNCATE_SCALAR_UDF_NAME, Truncate},
};

/// Register core scalar UDFs that have no runtime dependencies.
///
/// These UDFs only need a [`SessionContext`] and can be registered on any
/// context, including isolated ones like the refresh-task context.
pub fn register_core_scalar_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDFAlias::new(Arc::new(RandomFunc::default()), "rand").into());
    ctx.register_udf(Bucket::new().into());
    ctx.register_udf(CosineDistance::new().into());
    ctx.register_udf(Truncate::new().into());
    ctx.register_udf(INSTANCE.clone());
}

pub async fn register_udfs(runtime: &crate::Runtime) {
    let ctx = &runtime.df.ctx;
    register_core_scalar_udfs(ctx);

    ctx.register_udf(TextSearchTableFunc::new(Arc::downgrade(&runtime.df)).into());
    ctx.register_udtf(
        TEXT_SEARCH_UDTF_NAME,
        Arc::new(TextSearchTableFunc::new(Arc::downgrade(&runtime.df))),
    );

    let explicit_pks = parse_explicit_primary_keys(runtime.app()).await;
    ctx.register_udf(
        VectorSearchTableFunc::new(Arc::downgrade(&runtime.df), explicit_pks.clone()).into(),
    );
    ctx.register_udtf(
        VECTOR_SEARCH_UDTF_NAME,
        Arc::new(VectorSearchTableFunc::new(
            Arc::downgrade(&runtime.df),
            explicit_pks,
        )),
    );

    ctx.register_udf(rrf::ReciprocalRankFusion::from_ctx(ctx).into());
    ctx.register_udtf(
        RRF_UDF_NAME,
        Arc::new(rrf::ReciprocalRankFusion::from_ctx(ctx)),
    );

    // `rerank(input, model => ..., document => ..., ...)` — reorders a
    // scored result set using a reranker model. Registered as both a scalar
    // UDF stub (so `rerank(...)` can appear nested inside another UDTF's arg
    // list, same trick vector_search/text_search/rrf use) and a UDTF (the
    // actual `FROM rerank(...)` implementation).
    let session_ctx: Arc<SessionContext> = Arc::clone(ctx);
    ctx.register_udf(
        RerankTableFunc::new(
            Arc::downgrade(&runtime.df),
            Arc::clone(&session_ctx),
            runtime.rerankers(),
            runtime.completion_llms(),
        )
        .into(),
    );
    ctx.register_udtf(
        RERANK_UDTF_NAME,
        Arc::new(RerankTableFunc::new(
            Arc::downgrade(&runtime.df),
            session_ctx,
            runtime.rerankers(),
            runtime.completion_llms(),
        )),
    );

    // `flatten_json_properties` / `flatten_json` / `json_tree` — JSON-Schema
    // and generic JSON shredders. Registered as both UDTF (FROM-clause,
    // literal input) and ScalarUDF returning `List<Struct<...>>` (per-row /
    // LATERAL via UNNEST).
    ctx.register_udtf(
        FLATTEN_JSON_PROPERTIES_UDTF_NAME,
        Arc::new(FlattenJsonPropertiesTableFunc::new()),
    );
    ctx.register_udf(FlattenJsonPropertiesScalar::new().into());
    ctx.register_udtf(
        FLATTEN_JSON_UDTF_NAME,
        Arc::new(FlattenJsonTableFunc::new()),
    );
    ctx.register_udf(FlattenJsonScalar::new().into());
    ctx.register_udtf(JSON_TREE_UDTF_NAME, Arc::new(JsonTreeTableFunc::new()));
    ctx.register_udf(JsonTreeScalar::new().into());

    #[cfg(feature = "models")]
    {
        ctx.register_udf(embed::Embed::new(runtime.embeds()).into());
        ctx.register_udf(
            Ai::new(runtime.completion_llms(), runtime.model_rate_controllers())
                .into_async_udf()
                .into_scalar_udf(),
        );
    }
}

static DENY_SPICE_SPECIFIC_FUNCTIONS: LazyLock<FunctionSupport> = LazyLock::new(|| {
    let builtin = [
        "rand",
        BUCKET_SCALAR_UDF_NAME,
        COSINE_DISTANCE_UDF_NAME,
        TRUNCATE_SCALAR_UDF_NAME,
        EMBED_UDF_NAME,
        #[cfg(feature = "models")]
        AI_UDF_NAME,
        DIGEST_UDF_NAME,
        FLATTEN_JSON_PROPERTIES_UDTF_NAME,
        FLATTEN_JSON_UDTF_NAME,
        JSON_TREE_UDTF_NAME,
        RERANK_UDTF_NAME,
    ];

    FunctionSupport::new(
        Some(FunctionRestriction::Deny(
            builtin
                .iter()
                .map(ToString::to_string)
                .chain(json_functions())
                .collect::<Vec<_>>(),
        )),
        None,
        None,
    )
});

/// Return the cached [`FunctionSupport`] that denies Spice-specific functions for federation.
#[must_use]
pub fn deny_spice_specific_functions() -> &'static FunctionSupport {
    &DENY_SPICE_SPECIFIC_FUNCTIONS
}

fn json_functions() -> Vec<String> {
    let mut ctx = SessionContext::new();
    let existing: HashSet<_> = ctx.state().scalar_functions().keys().cloned().collect();
    let _ = datafusion_functions_json::register_all(&mut ctx);
    ctx.state()
        .scalar_functions()
        .keys()
        .filter(|&k| !existing.contains(k))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::prelude::{Expr, lit};
    use datafusion_functions_json::udfs::{
        json_as_text_udf, json_contains_udf, json_get_bool_udf, json_get_float_udf,
        json_get_int_udf, json_get_json_udf, json_get_str_udf, json_get_udf, json_length_udf,
    };

    use super::*;

    /// Helper to create a scalar function expression for testing function support.
    fn make_json_expr(udf: Arc<datafusion::logical_expr::ScalarUDF>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![lit("{}"), lit("key")]))
    }

    fn spice_udf(
        impl_: impl Into<datafusion::logical_expr::ScalarUDF>,
    ) -> Arc<datafusion::logical_expr::ScalarUDF> {
        Arc::new(impl_.into())
    }

    #[test]
    fn deny_list_blocks_json_functions() {
        let support = deny_spice_specific_functions();

        let json_udfs = vec![
            json_get_udf(),
            json_get_str_udf(),
            json_get_int_udf(),
            json_get_float_udf(),
            json_get_bool_udf(),
            json_get_json_udf(),
            json_as_text_udf(),
            json_contains_udf(),
            json_length_udf(),
        ];

        for udf in json_udfs {
            let name = udf.name().to_string();
            let expr = make_json_expr(udf);
            assert!(
                !support.supports(&expr),
                "{name} should be denied by deny_spice_specific_functions"
            );
        }
    }

    #[test]
    fn deny_list_blocks_spice_builtins() {
        let support = deny_spice_specific_functions();

        let spice_udfs: Vec<Arc<datafusion::logical_expr::ScalarUDF>> = vec![
            spice_udf(CosineDistance::new()),
            spice_udf(Bucket::new()),
            spice_udf(Truncate::new()),
            Arc::new(INSTANCE.clone()),
            spice_udf(FlattenJsonPropertiesScalar::new()),
            spice_udf(FlattenJsonScalar::new()),
            spice_udf(JsonTreeScalar::new()),
        ];

        for udf in spice_udfs {
            let name = udf.name().to_string();
            let expr = make_json_expr(udf);
            assert!(
                !support.supports(&expr),
                "{name} should be denied by deny_spice_specific_functions"
            );
        }
    }
}
