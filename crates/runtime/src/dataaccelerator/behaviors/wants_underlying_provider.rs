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

use std::{
    fmt::{self, Debug},
    sync::Arc,
};

use datafusion::catalog::TableProvider;
use datafusion::error::Result as DataFusionResult;

pub struct WantsUnderlyingTableProvider {
    callback_underlying_provider: Box<dyn FnOnce(Arc<dyn TableProvider>) -> DataFusionResult<()>>,
}

impl Debug for WantsUnderlyingTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WantsUnderlyingTableProvider")
            .finish_non_exhaustive()
    }
}

impl WantsUnderlyingTableProvider {
    #[must_use]
    pub fn new(
        callback_underlying_provider: Box<
            dyn FnOnce(Arc<dyn TableProvider>) -> DataFusionResult<()>,
        >,
    ) -> Self {
        Self {
            callback_underlying_provider,
        }
    }

    pub fn set(self, underlying_provider: Arc<dyn TableProvider>) -> DataFusionResult<()> {
        (self.callback_underlying_provider)(underlying_provider)
    }
}
