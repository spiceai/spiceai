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

use std::collections::{HashSet, VecDeque};

/// A bounded FIFO set with insertion-order eviction.
///
/// This structure stores unique keys up to a fixed capacity. When full,
/// the oldest inserted key is evicted.
#[derive(Debug, Default)]
pub struct BoundedFifoSet {
    seen: HashSet<String>,
    insertion_order: VecDeque<String>,
    limit: usize,
}

impl BoundedFifoSet {
    pub fn with_capacity(limit: usize) -> Self {
        Self {
            seen: HashSet::with_capacity(limit),
            insertion_order: VecDeque::with_capacity(limit),
            limit,
        }
    }

    /// Inserts a key if it is not already present.
    ///
    /// Returns `true` if inserted, `false` if it was already present.
    pub fn insert_new(&mut self, key: String) -> bool {
        if self.seen.contains(&key) {
            return false;
        }

        if self.seen.len() >= self.limit
            && let Some(oldest_key) = self.insertion_order.pop_front()
        {
            self.seen.remove(&oldest_key);
        }

        self.insertion_order.push_back(key.clone());
        self.seen.insert(key)
    }
}
