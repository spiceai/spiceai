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

use rand::Rng;

pub mod component;
pub mod consistency;

fn get_random_element<T>(vec: &[T]) -> Option<&T> {
    if vec.is_empty() {
        None
    } else {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..vec.len());
        Some(&vec[index])
    }
}
