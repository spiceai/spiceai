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

use scheduler::Result;
use scheduler::task::ScheduledTask;
use tonic::async_trait;

use crate::component::dataset::Dataset;

#[async_trait]
impl ScheduledTask for Dataset {
    async fn execute(&self) -> Result<()> {
        match self
            .runtime()
            .datafusion()
            .refresh_table(&self.name, None)
            .await
        {
            Ok(()) => {
                // Successfully refreshed the dataset
            }
            Err(e) => {
                // Handle the error
                todo!("Handle when refresh fails: {e}");
            }
        }

        Ok(())
    }
}
