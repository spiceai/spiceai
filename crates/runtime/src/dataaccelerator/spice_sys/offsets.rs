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

use super::{Error, Result};

#[derive(Default)]
pub(crate) struct OffsetSchemaState {
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

pub(crate) fn serialize_offsets(offsets: &[KafkaOffset]) -> Result<String> {
    serde_json::to_string(offsets).map_err(Error::external)
}

pub(crate) fn deserialize_offsets(offsets_json: Option<&str>) -> Result<Vec<KafkaOffset>> {
    offsets_json.map_or_else(
        || Ok(Vec::new()),
        |offsets_json| serde_json::from_str(offsets_json).map_err(Error::external),
    )
}

pub(crate) fn serialize_merged_offsets(
    offsets_json: Option<&str>,
    offsets: &[KafkaOffset],
) -> Result<String> {
    let existing_offsets = deserialize_offsets(offsets_json)?;
    let merged_offsets = merge_offsets(existing_offsets, offsets);
    serialize_offsets(&merged_offsets)
}

fn merge_offsets(existing_offsets: Vec<KafkaOffset>, offsets: &[KafkaOffset]) -> Vec<KafkaOffset> {
    let mut merged_offsets: HashMap<(String, i32), KafkaOffset> = existing_offsets
        .into_iter()
        .map(|offset| ((offset.topic.clone(), offset.partition), offset))
        .collect();

    for offset in offsets {
        merged_offsets
            .entry((offset.topic.clone(), offset.partition))
            .and_modify(|existing_offset| {
                existing_offset.offset = existing_offset.offset.max(offset.offset);
            })
            .or_insert_with(|| offset.clone());
    }

    let mut offsets = merged_offsets.into_values().collect::<Vec<_>>();
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    offsets
}
