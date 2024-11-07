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

use spicepod::component::tool::Tool;

pub mod catalog;
pub mod load;
pub mod store;

// Model tools must also be added to [`super::memory::factory::MemoryToolFactory`]
#[must_use]
pub fn get_memory_tool_spec() -> Vec<Tool> {
    vec![]
}
