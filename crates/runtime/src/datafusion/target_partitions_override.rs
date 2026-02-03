/*
Copyright 2026 The Spice.ai OSS Authors

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

#[derive(Debug, Clone, Copy)]
pub struct TargetPartitionsOverride {
    target_partitions: usize,
}

impl TargetPartitionsOverride {
    #[must_use]
    pub fn new(target_partitions: usize) -> Self {
        Self { target_partitions }
    }

    #[must_use]
    pub fn target_partitions(&self) -> usize {
        self.target_partitions
    }
}
