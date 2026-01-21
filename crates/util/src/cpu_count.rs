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

//! CPU count utilities.
//!
//! Provides functions to get the number of available CPUs.

/// Returns the number of available CPUs.
///
/// This is a wrapper around [`std::thread::available_parallelism`] that returns
/// a default value of 1 if the count cannot be determined.
///
/// # Examples
///
/// ```
/// use util::cpu_count::get;
///
/// let cpus = get();
/// assert!(cpus >= 1);
/// ```
#[must_use]
pub fn get() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_returns_positive_value() {
        let count = get();
        assert!(count >= 1, "CPU count should be at least 1");
    }

    #[test]
    fn test_get_returns_reasonable_value() {
        let count = get();
        // Most systems have between 1 and 1024 CPUs
        assert!(count <= 1024, "CPU count should be reasonable");
    }

    #[test]
    fn test_get_is_consistent() {
        let count1 = get();
        let count2 = get();
        assert_eq!(count1, count2, "CPU count should be consistent");
    }
}
