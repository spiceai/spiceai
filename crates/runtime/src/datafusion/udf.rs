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

use std::sync::Arc;

use crate::embeddings::udtf::{VECTOR_SEARCH_UDTF_NAME, VectorSearchTableFunc};
use crate::search::full_text::udtf::{TEXT_SEARCH_UDTF_NAME, TextSearchTableFunc};
use crate::search::rrf;
use crate::search::rrf::RRF_UDF_NAME;
use crate::search::util::parse_explicit_primary_keys;
use datafusion::functions::math::random::RandomFunc;
#[cfg(feature = "models")]
use runtime_datafusion_udfs::ai;
use runtime_datafusion_udfs::{alias, bucket, cosine_distance, digest_many, embed, truncate};

pub async fn register_udfs(runtime: &crate::Runtime) {
    let ctx = &runtime.df.ctx;
    ctx.register_udf(alias::ScalarUDFAlias::new(Arc::new(RandomFunc::default()), "rand").into());
    ctx.register_udf(bucket::Bucket::new().into());
    ctx.register_udf(cosine_distance::CosineDistance::new().into());
    ctx.register_udf(truncate::Truncate::new().into());

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

    ctx.register_udf(embed::Embed::new(runtime.embeds()).into());

    #[cfg(feature = "models")]
    {
        ctx.register_udf(
            ai::Ai::new(runtime.completion_llms())
                .into_async_udf()
                .into_scalar_udf(),
        );
    }

    ctx.register_udf(digest_many::INSTANCE.clone());
}
