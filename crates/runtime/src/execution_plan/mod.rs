/*
Copyright 2024 The Spice.ai OSS Authors

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

use datafusion::{execution::context::SessionState, logical_expr::Expr};

pub mod fallback_on_zero_results;
pub mod slice;
pub mod tee;

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct TableScanParams {
    state: SessionState,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl TableScanParams {
    #[must_use]
    pub fn new(
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        Self {
            state: state.clone(),
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
        }
    }
}
