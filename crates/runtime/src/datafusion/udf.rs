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
use std::sync::Arc;

use crate::embeddings::udtf::{VECTOR_SEARCH_UDTF_NAME, VectorSearchTableFunc};
use crate::search::full_text::udtf::{TEXT_SEARCH_UDTF_NAME, TextSearchTableFunc};
use crate::search::rrf;
use crate::search::rrf::RRF_UDF_NAME;
use crate::search::util::parse_explicit_primary_keys;
use datafusion::functions::math::random::RandomFunc;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::supported_functions::{FunctionRestriction, FunctionSupport};
#[cfg(feature = "models")]
use runtime_datafusion_udfs::{
    ai::{AI_UDF_NAME, Ai},
    embed,
};
use runtime_datafusion_udfs::{
    alias::ScalarUDFAlias,
    bucket::{BUCKET_SCALAR_UDF_NAME, Bucket},
    cosine_distance::{COSINE_DISTANCE_UDF_NAME, CosineDistance},
    digest_many::{DIGEST_UDF_NAME, INSTANCE},
    embed::EMBED_UDF_NAME,
    truncate::{TRUNCATE_SCALAR_UDF_NAME, Truncate},
};

pub async fn register_udfs(runtime: &crate::Runtime) {
    let ctx = &runtime.df.ctx;
    ctx.register_udf(ScalarUDFAlias::new(Arc::new(RandomFunc::default()), "rand").into());
    ctx.register_udf(Bucket::new().into());
    ctx.register_udf(CosineDistance::new().into());
    ctx.register_udf(Truncate::new().into());

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

    #[cfg(feature = "models")]
    {
        ctx.register_udf(embed::Embed::new(runtime.embeds()).into());
        ctx.register_udf(
            Ai::new(runtime.completion_llms())
                .into_async_udf()
                .into_scalar_udf(),
        );
    }

    ctx.register_udf(INSTANCE.clone());
}

/// Create a [`FunctionSupport`] with all spice specific functions as unsupported for federation.
pub fn deny_spice_specific_functions() -> FunctionSupport {
    let builtin = [
        "rand",
        BUCKET_SCALAR_UDF_NAME,
        COSINE_DISTANCE_UDF_NAME,
        TRUNCATE_SCALAR_UDF_NAME,
        EMBED_UDF_NAME,
        #[cfg(feature = "models")]
        AI_UDF_NAME,
        DIGEST_UDF_NAME,
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
