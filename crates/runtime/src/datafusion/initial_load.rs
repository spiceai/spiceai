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

use std::sync::MutexGuard;

use crate::DataFusion;

impl DataFusion {
    pub fn mark_initial_load_complete(&self) {
        let mut guard = self.get_initial_load_guard();
        *guard = true;
    }

    pub fn is_initial_load_complete(&self) -> bool {
        let guard = self.get_initial_load_guard();
        *guard
    }

    fn get_initial_load_guard(&self) -> MutexGuard<bool> {
        match self.initial_load_complete.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
