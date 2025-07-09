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

use datafusion::{functions::math::random::RandomFunc, prelude::SessionContext};

pub mod alias;
pub mod bucket;
pub mod cosine_distance;
pub mod truncate;

// UDFs that need a reference to [`crate::datafusion::DataFusion`] must be defined in [`crate::builder::RuntimeBuilder::build`].
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(cosine_distance::CosineDistance::new().into());
    ctx.register_udf(alias::ScalarUDFAlias::new(Arc::new(RandomFunc::default()), "rand").into());
    ctx.register_udf(bucket::Bucket::new().into());
    ctx.register_udf(truncate::Truncate::new().into());
}
