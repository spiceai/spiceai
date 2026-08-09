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

//! Registration of the scalar UDFs that need nothing but a `SessionContext`.
//!
//! Kept apart from the runtime's own UDF registration, which needs an
//! `Arc<Runtime>` to reach models, tools and secrets. These can go on any
//! context — including the isolated one a refresh task builds for itself.

use std::sync::Arc;

use datafusion::functions::math::random::RandomFunc;
use datafusion::prelude::SessionContext;
use runtime_datafusion_udfs::{
    alias::ScalarUDFAlias,
    assert::Assert,
    bucket::Bucket,
    cosine_distance::CosineDistance,
    digest_many::INSTANCE,
    inner_product::InnerProduct,
    l2_distance::{L2Distance, L2SquaredDistance},
    l2_norm::L2Norm,
    truncate::Truncate,
};

use crate::pg_catalog::register_postgres_comment_udfs;

/// Register core scalar UDFs that have no runtime dependencies.
///
/// These UDFs only need a [`SessionContext`] and can be registered on any
/// context, including isolated ones like the refresh-task context.
pub fn register_core_scalar_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDFAlias::new(Arc::new(RandomFunc::default()), "rand").into());
    ctx.register_udf(Bucket::new().into());
    ctx.register_udf(CosineDistance::new().into());
    ctx.register_udf(InnerProduct::new().into());
    ctx.register_udf(L2Distance::new().into());
    ctx.register_udf(L2SquaredDistance::new().into());
    ctx.register_udf(L2Norm::new().into());
    ctx.register_udf(Truncate::new().into());
    ctx.register_udf(Assert::new().into());
    ctx.register_udf(INSTANCE.clone());
    register_postgres_comment_udfs(ctx);
}
