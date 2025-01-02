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

use crate::{
    spice_metrics::{self, get_metrics_table_reference},
    Result, Runtime, UnableToStartLocalMetricsSnafu,
};
use snafu::prelude::*;

impl Runtime {
    pub(crate) async fn register_metrics_table(&self, metrics_enabled: bool) -> Result<()> {
        if metrics_enabled {
            let table_reference = get_metrics_table_reference();
            let metrics_table = self.df.get_table(&table_reference).await;

            if metrics_table.is_none() {
                tracing::debug!("Registering local metrics table");
                spice_metrics::register_metrics_table(&self.df)
                    .await
                    .context(UnableToStartLocalMetricsSnafu)?;
            }
        }

        Ok(())
    }
}
