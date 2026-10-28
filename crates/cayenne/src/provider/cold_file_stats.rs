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

//! Decoded statistics of the cold manifest's files, so a scan decodes each
//! file's persisted statistics blob once rather than on every query.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_common::Statistics;
use parking_lot::Mutex;

use crate::metadata::ColdTierFile;

struct CachedStatistics {
    blob: Arc<[u8]>,
    row_count: i64,
    schema: SchemaRef,
    statistics: Option<Arc<Statistics>>,
}

/// Per-file scan statistics, keyed by cold file URL.
///
/// An entry is reused only for the identical blob, row count and schema: a
/// file's manifest row can be rewritten in place (its blob refreshed or
/// cleared), and a schema change re-types every bound.
#[derive(Default)]
pub(crate) struct ColdFileStatisticsCache {
    entries: Mutex<HashMap<String, CachedStatistics>>,
}

impl ColdFileStatisticsCache {
    /// `file`'s statistics decoded against `schema`; `None` when its blob is
    /// missing or undecodable.
    pub(crate) fn statistics_for(
        &self,
        file: &ColdTierFile,
        schema: &SchemaRef,
    ) -> Option<Arc<Statistics>> {
        if let Some(cached) = self.entries.lock().get(&file.file_url)
            && cached.row_count == file.row_count
            && *cached.blob == *file.statistics_blob
            && (Arc::ptr_eq(&cached.schema, schema) || cached.schema == *schema)
        {
            return cached.statistics.clone();
        }
        let statistics = crate::stats::statistics_from_persisted_blob(
            &file.statistics_blob,
            schema,
            file.row_count,
        );
        self.entries.lock().insert(
            file.file_url.clone(),
            CachedStatistics {
                blob: Arc::from(file.statistics_blob.as_slice()),
                row_count: file.row_count,
                schema: Arc::clone(schema),
                statistics: statistics.clone(),
            },
        );
        statistics
    }

    /// Drops entries for files no longer in `files` once the cache has grown well
    /// past the live set, so memory stays bounded by the live manifest.
    pub(crate) fn trim(&self, files: &[&ColdTierFile]) {
        let mut entries = self.entries.lock();
        if entries.len() > files.len().saturating_mul(2).saturating_add(8) {
            let live: HashSet<&str> = files.iter().map(|file| file.file_url.as_str()).collect();
            entries.retain(|url, _| live.contains(url.as_str()));
        }
    }
}
