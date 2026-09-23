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

//! Guard for the DataFusion fork's metadata-column pruning patch
//! (spiceai/datafusion#229): a `_last_modified`/`_size`/`_location` predicate must
//! prune the object-store listing before any file is opened, rather than falling
//! back to a row-level `FilterExec` that opens every file (see
//! <https://github.com/spiceai/spiceai/issues/14264>).
//!
//! If a fork re-cut drops the patch, this test fails one of two ways:
//! - `pruned_partition_list_with_metadata` no longer exists — a build failure; or
//! - the non-matching file is no longer pruned — an assertion failure.

use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::listing::helpers::pruned_partition_list_with_metadata;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{col, lit};
use datafusion_datasource::metadata::MetadataColumn;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStoreExt;

#[tokio::test]
async fn metadata_predicate_prunes_the_listing_before_opening_files() {
    let store = Arc::new(InMemory::new());
    for (name, size) in [
        ("tablepath/small.jsonl", 10usize),
        ("tablepath/big.jsonl", 500usize),
    ] {
        store
            .put(&Path::from(name), vec![0u8; size].into())
            .await
            .expect("put object");
    }

    let state = SessionStateBuilder::new().build();

    // `_size > 100` matches only `big.jsonl`; `small.jsonl` must be pruned from the
    // listing by its ObjectMeta before it is ever opened.
    let filter = col("_size").gt(lit(100u64));
    let pruned = pruned_partition_list_with_metadata(
        &state,
        store.as_ref(),
        &ListingTableUrl::parse("file:///tablepath/").expect("parse url"),
        &[], // no partition filters
        ".jsonl",
        &[], // unpartitioned
        &[filter],
        &[MetadataColumn::Size],
    )
    .await
    .expect("metadata pruning failed")
    .try_collect::<Vec<_>>()
    .await
    .expect("collect pruned listing");

    assert_eq!(
        pruned.len(),
        1,
        "only the file matching `_size > 100` should survive the prune"
    );
    assert_eq!(
        pruned[0].object_meta.location.as_ref(),
        "tablepath/big.jsonl"
    );
}
