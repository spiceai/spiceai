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
use std::collections::HashSet;

use datafusion::sql::TableReference;
use spicepod::semantic::{Column, FullTextSearchConfig, IndexStore};

use crate::component::dataset::FullTextSearchDatasetConfig;

#[expect(clippy::type_complexity)] // From a two-part `.unzip()`.
#[must_use]
pub fn full_text_search_config(
    columns: &[Column],
    name: &TableReference,
) -> Option<FullTextSearchDatasetConfig> {
    let (search_fields_and_primary_key_overrides, indexes): (
        Vec<(String, Option<Vec<String>>)>,
        Vec<(IndexStore, Option<String>)>,
    ) = columns
        .iter()
        .filter_map(|c| {
            let Some(FullTextSearchConfig {
                enabled: true,
                row_ids,
                index_store,
                index_directory,
            }) = &c.full_text_search
            else {
                return None;
            };

            if index_store.is_some_and(|is| is == IndexStore::Memory) && index_directory.is_some() {
                tracing::warn!("Table '{name}' column '{}' has `index_store: memory` but also sets `index_directory`. These options are mutually exclusive. Defaulting to `index_store: memory`.", c.name);
            }
            Some(((c.name.clone(), row_ids.clone()), (index_store.unwrap_or_default(), index_directory.clone())))
        })
        .unzip();
    let (search_fields, primary_key_overrides): (Vec<String>, Vec<Option<Vec<String>>>) =
        search_fields_and_primary_key_overrides.into_iter().unzip();

    // No columns have full text search fields defined.
    if search_fields.is_empty() {
        return None;
    }

    // For all full text search columns, find the first with a non-null primary key override and
    // if there are multiple, warn if they are different.
    let mut first_pks: Option<Vec<String>> = None;
    let mut first_search_field: Option<String> = None;
    for (search_field, pk_overrides) in search_fields.iter().zip(primary_key_overrides.iter()) {
        let Some(mut pks) = pk_overrides.clone() else {
            continue;
        };
        pks.sort();

        // If this is not the first FTS column that defined row ids, check if they match the previous.
        // Otherwise set to be used for next comparison.
        if let (Some(f), Some(s)) = (&first_pks, &first_search_field) {
            if *pks != *f {
                tracing::warn!(
                    "Table '{name}' has different primary keys for different full-text search columns. Using first.\n  Column '{}'. Key: {}.\n  Column '{}'. Key: {}.",
                    s,
                    f.join(", "),
                    search_field,
                    pks.join(", "),
                );
            }
        } else {
            first_pks = Some(pks.clone());
            first_search_field = Some(search_field.clone());
        }
    }

    let index_paths: HashSet<String> = indexes
        .iter()
        .filter_map(|(_, directory)| directory.clone())
        .collect();
    let index_path_len = index_paths.len();
    let index_path: Option<String> = index_paths.into_iter().next();

    if let Some(ref path) = index_path
        && index_path_len > 1
    {
        tracing::warn!(
            "Table '{name}' has several full text search index directories provided. Using '{path}'.",
        );
    }

    let index_store = if indexes.iter().any(|(store, _)| *store == IndexStore::File) {
        IndexStore::File
    } else {
        IndexStore::Memory
    };

    Some(FullTextSearchDatasetConfig {
        index_store,
        index_path,
        search_fields,
        primary_key: first_pks.unwrap_or_default(),
    })
}
