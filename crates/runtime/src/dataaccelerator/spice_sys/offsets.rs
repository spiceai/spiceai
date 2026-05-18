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

use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
};

use data_components::kafka::KafkaOffset;

#[derive(Default)]
pub(crate) struct OffsetSchemaState {
    // This is intentionally scoped to a single sidecar instance. The DDL is
    // idempotent, so separate KafkaSys/DebeziumKafkaSys instances may each
    // ensure their table once without sharing process-wide state.
    ensured: AtomicBool,
}

impl OffsetSchemaState {
    pub(crate) fn needs_ensure(&self) -> bool {
        !self.ensured.load(Ordering::Acquire)
    }

    pub(crate) fn mark_ensured(&self) {
        self.ensured.store(true, Ordering::Release);
    }
}

/// Sort offsets by (topic, partition) for deterministic comparison and
/// storage order.
pub(crate) fn sort_offsets(offsets: &mut [KafkaOffset]) {
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
}

/// Diagnostic helper: walk the incoming offsets against the prior set, log a
/// warning whenever an offset goes backward, and return the merged result.
///
/// The per-partition storage tables use `MAX(...)`/`GREATEST(...)` ON CONFLICT
/// for the authoritative resolution; this function exists purely to surface
/// regressions to operators. A backward offset usually points at a buggy
/// upstream producer or unexpected out-of-order redelivery.
pub(crate) fn merge_offsets(
    dataset_name: &str,
    existing: Vec<KafkaOffset>,
    incoming: &[KafkaOffset],
) -> Vec<KafkaOffset> {
    let mut merged: HashMap<(String, i32), KafkaOffset> = existing
        .into_iter()
        .map(|offset| ((offset.topic.clone(), offset.partition), offset))
        .collect();

    for offset in incoming {
        merged
            .entry((offset.topic.clone(), offset.partition))
            .and_modify(|existing_offset| {
                if offset.offset < existing_offset.offset {
                    tracing::warn!(
                        dataset = %dataset_name,
                        topic = %offset.topic,
                        partition = offset.partition,
                        existing_offset = existing_offset.offset,
                        incoming_offset = offset.offset,
                        "Kafka offset went backward for partition; keeping the higher value. \
                         This usually indicates a buggy upstream producer or out-of-order \
                         redelivery."
                    );
                }
                existing_offset.offset = existing_offset.offset.max(offset.offset);
            })
            .or_insert_with(|| offset.clone());
    }

    let mut out: Vec<KafkaOffset> = merged.into_values().collect();
    sort_offsets(&mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn off(topic: &str, partition: i32, offset: i64) -> KafkaOffset {
        KafkaOffset {
            topic: topic.to_string(),
            partition,
            offset,
        }
    }

    #[test]
    fn merge_takes_max_per_partition() {
        let existing = vec![off("t", 0, 10), off("t", 1, 20)];
        let incoming = vec![off("t", 0, 15), off("t", 1, 5), off("t", 2, 1)];
        let merged = merge_offsets("ds", existing, &incoming);
        assert_eq!(
            merged,
            vec![off("t", 0, 15), off("t", 1, 20), off("t", 2, 1)]
        );
    }

    #[test]
    fn merge_preserves_other_topics_partitions() {
        let existing = vec![off("a", 0, 10), off("b", 0, 20)];
        let incoming = vec![off("a", 0, 11)];
        let merged = merge_offsets("ds", existing, &incoming);
        assert_eq!(merged, vec![off("a", 0, 11), off("b", 0, 20)]);
    }
}
