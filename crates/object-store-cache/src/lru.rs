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

//! A map of at most a fixed number of entries that forgets the least
//! recently used entry first.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

#[derive(Debug)]
pub(crate) struct Lru<K, V> {
    map: HashMap<K, (V, u64)>,
    /// Last use → key, least recent first.
    order: BTreeMap<u64, K>,
    tick: u64,
    cap: usize,
}

impl<K: Hash + Eq + Clone, V> Lru<K, V> {
    /// An empty map of at most `cap` entries.
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: BTreeMap::new(),
            tick: 0,
            cap,
        }
    }

    /// The value of `key`, marked as used now.
    pub(crate) fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (value, used) = self.map.get_mut(key)?;
        self.tick += 1;
        if let Some(k) = self.order.remove(used) {
            self.order.insert(self.tick, k);
        }
        *used = self.tick;
        Some(value)
    }

    /// The value of `key`, without marking it used.
    pub(crate) fn peek<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.get(key).map(|(value, _)| value)
    }

    pub(crate) fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.contains_key(key)
    }

    /// Inserts or replaces the value of `key`, marked as used now, then
    /// forgets the least recently used entries past the bound.
    pub(crate) fn insert(&mut self, key: K, value: V) {
        self.tick += 1;
        if let Some((_, used)) = self.map.insert(key.clone(), (value, self.tick)) {
            self.order.remove(&used);
        }
        self.order.insert(self.tick, key);
        while self.map.len() > self.cap {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            self.map.remove(&oldest);
        }
    }

    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (value, used) = self.map.remove(key)?;
        self.order.remove(&used);
        Some(value)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bound holds, the least recently used entry goes first (a `get`
    /// counts as a use, a `peek` does not), and the use order stays in step
    /// with the entries through replacements and removals.
    #[test]
    fn forgets_the_least_recently_used_past_the_bound() {
        let mut lru: Lru<String, u32> = Lru::new(3);
        for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
            lru.insert(k.to_string(), v);
        }
        assert_eq!(lru.get("a"), Some(&1));
        assert_eq!(lru.peek("b"), Some(&2));
        lru.insert("d".to_string(), 4);
        assert!(!lru.contains("b"), "b was the least recently used");
        assert!(lru.contains("a") && lru.contains("c") && lru.contains("d"));
        lru.insert("c".to_string(), 30);
        assert_eq!(lru.peek("c"), Some(&30));
        lru.insert("e".to_string(), 5);
        assert!(!lru.contains("a"), "a was used before c was replaced");
        assert_eq!(lru.remove("d"), Some(4));
        assert_eq!(lru.remove("d"), None);
        assert_eq!(lru.len(), 2);
        assert_eq!(lru.order.len(), lru.map.len());
        assert_eq!(
            lru.order.values().cloned().collect::<Vec<_>>(),
            vec!["c".to_string(), "e".to_string()]
        );
    }
}
