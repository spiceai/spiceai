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

//! Naming for the per-partition child tables of a partitioned Cayenne table.
//!
//! Every partition of a partitioned table is itself a catalog table, named from
//! the parent's name and the partition's key. Two callers need the same
//! derivation: the partition creator mints the names (behind the
//! `partition-table-provider` feature), and the catalog derives them to drop a
//! partitioned table's children along with it. The convention therefore lives
//! here, in a module both can reach, rather than in either caller.

/// Child table name for the partition identified by `partition_key` under
/// `parent`.
///
/// `partition_key` is the composite key recorded in
/// `cayenne_partition.partition_key`; it is hex-encoded so the name is a single
/// identifier regardless of what the partition values contain.
#[must_use]
pub fn partition_child_table_name(parent: &str, partition_key: &str) -> String {
    format!("{parent}_p{}", encode_identifier_hex(partition_key))
}

/// Pre-composite-key child table name, derived from the partition's value
/// strings.
///
/// Retained so a partition created by an older runtime is still recognized —
/// the creator falls back to opening this name, so the catalog must recognize it
/// too or a legacy child would outlive its parent.
#[must_use]
pub fn legacy_partition_child_table_name(parent: &str, partition_values: &[String]) -> String {
    format!("{}_{}", parent, partition_values.join("_"))
}

fn encode_identifier_hex(value: &str) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        let _ = write!(encoded, "{byte:02X}");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_name_is_a_single_identifier_for_a_hostile_key() {
        let name = partition_child_table_name("events", "v1:11:v1.utf8.v61/../x");
        assert!(
            name.strip_prefix("events_p")
                .expect("the parent prefix is kept")
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
            "'{name}' must encode the key as hex so the name stays one identifier"
        );
    }

    #[test]
    fn legacy_child_name_joins_the_partition_values() {
        assert_eq!(
            legacy_partition_child_table_name("events", &["a".to_string(), "b".to_string()]),
            "events_a_b"
        );
    }
}
