/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Identity hasher for pre-hashed `u64` keys.
//!
//! Cache keys are already hashed before they reach this crate. Re-hashing them
//! inside each shard would add work on every get without improving distribution,
//! because sharding already uses the low bits (`key % NUM_SHARDS`).

use std::hash::{BuildHasher, Hasher};

/// Hasher that returns a `u64` key unchanged.
///
/// `HashMap` hashes a `u64` via [`Hasher::write_u64`], which this stores and
/// returns from [`Hasher::finish`]. Byte-wise [`Hasher::write`] is implemented
/// for the trait but is not used for `u64` keys.
#[derive(Clone, Default)]
pub struct IdentityHasher {
    hash: u64,
}

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash = self
                .hash
                .wrapping_mul(0x0100_0000_01b3)
                .wrapping_add(u64::from(*byte));
        }
    }

    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }
}

/// [`BuildHasher`] that produces [`IdentityHasher`].
#[derive(Clone, Debug, Default)]
pub struct IdentityBuildHasher;

impl BuildHasher for IdentityBuildHasher {
    type Hasher = IdentityHasher;

    fn build_hasher(&self) -> Self::Hasher {
        IdentityHasher::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    #[test]
    fn u64_keys_pass_through() {
        assert_eq!(IdentityBuildHasher.hash_one(42u64), 42);
    }
}
