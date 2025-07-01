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

mod wants_underlying_provider;
pub use wants_underlying_provider::WantsUnderlyingTableProvider;

#[derive(Debug, Default)]
pub struct Behaviors(Vec<Behavior>);

impl Behaviors {
    #[must_use]
    pub fn new() -> Self {
        Self(vec![])
    }

    #[must_use]
    pub fn add_behavior(mut self, behavior: Behavior) -> Self {
        self.0.push(behavior);
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = &Behavior> {
        self.0.iter()
    }

    #[must_use]
    pub fn is_default(&self) -> bool {
        self.0.is_empty()
    }
}

impl IntoIterator for Behaviors {
    type Item = Behavior;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Behaviors {
    type Item = &'a Behavior;
    type IntoIter = std::slice::Iter<'a, Behavior>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug)]
pub enum Behavior {
    WantsUnderlyingTableProvider(WantsUnderlyingTableProvider),
}
