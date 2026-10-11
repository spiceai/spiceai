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

//! `deletion_mode: auto` on a primary-key table: position deletes while the table
//! is within its bound, reclaimed by a whole-table rewrite that runs alongside
//! writers; key deletes once it outgrows the bound.

use super::*;
use crate::metadata::DeletionMode;

/// A primary-key upsert table with inlining and background compaction off, so
/// every write lands in a Vortex file and the maintenance here is the only
/// maintenance that runs. The reclaim trigger is two tombstones.
async fn table(
    name: &str,
    deletion_mode: DeletionMode,
    target_vortex_file_size_mb: usize,
) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    let ctx = SessionContext::new();
    create_cdc_upsert_table_with_vortex_config(
        name,
        ctx.runtime_env(),
        VortexConfig {
            deletion_mode,
            target_vortex_file_size_mb,
            bake_deletion_index_trigger: 2,
            inline_max_rows: 0,
            inline_max_bytes: 0,
            inline_max_buffer_bytes: 0,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    )
    .await
}

async fn write(provider: &CayenneTableProvider, ids: &[i64], value: i64) {
    let schema = Arc::clone(&provider.table_metadata.schema);
    insert_batch(
        provider,
        id_value_batch(schema, ids, &vec![value; ids.len()]),
    )
    .await;
    provider
        .flush_pending_maintenance()
        .await
        .expect("post-write maintenance");
}

/// Rebuild the keyset cold and capture positions, as the seeded position-mode
/// tests do, so the next upsert of these keys records position deletes.
async fn capture_positions(provider: &CayenneTableProvider) {
    let pk_indices = provider
        .primary_key_indices()
        .expect("resolve pk indices")
        .expect("table has a primary key");
    let pk_converter = provider
        .build_pk_converter(&pk_indices)
        .expect("build pk converter");
    let (_, checkout) = provider.take_cached_pk_index();
    let index = provider
        .load_existing_pk_index_serial(&pk_indices, &pk_converter, true, None)
        .await
        .expect("cold keyset rebuild");
    provider.store_cached_pk_index(index, checkout);
    provider
        .run_position_capture()
        .await
        .expect("position capture succeeds");
}

/// Write `ids` with values that do not compress, so the table's size on disk
/// tracks its row count.
async fn write_incompressible(provider: &CayenneTableProvider, ids: &[i64]) {
    let schema = Arc::clone(&provider.table_metadata.schema);
    let values: Vec<i64> = ids
        .iter()
        .map(|id| {
            // SplitMix64 of the key.
            let mut z = (*id as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            i64::from_ne_bytes((z ^ (z >> 31)).to_ne_bytes())
        })
        .collect();
    insert_batch(provider, id_value_batch(schema, ids, &values)).await;
    provider
        .flush_pending_maintenance()
        .await
        .expect("post-write maintenance");
}

fn position_deletes(provider: &CayenneTableProvider) -> u64 {
    provider
        .pk_deletion_strategy
        .position_cache()
        .load()
        .values()
        .map(|dv| dv.len())
        .sum()
}

#[tokio::test]
async fn a_small_auto_table_uses_position_deletes_and_a_large_one_key_deletes() {
    let (small, _catalog, _tmp) = table("auto_small", DeletionMode::Auto, 256).await;
    assert!(
        small.should_capture_positions(),
        "an empty auto table is within its bound and starts on position deletes"
    );
    assert!(
        !small.position_rewrites_exclude_writers(),
        "an auto table's position-delete rewrite runs alongside writers"
    );
    assert!(
        !small.hides_repeated_keys_with_position_deletes(),
        "an auto table folds a key repeated within one write, as key mode does"
    );

    let (explicit, _catalog, _tmp) = table("explicit_position", DeletionMode::Position, 256).await;
    assert!(explicit.should_capture_positions());
    assert!(explicit.position_rewrites_exclude_writers());
    assert!(explicit.hides_repeated_keys_with_position_deletes());

    let (key, _catalog, _tmp) = table("explicit_key", DeletionMode::Key, 256).await;
    assert!(!key.should_capture_positions());
    assert!(!key.position_rewrites_exclude_writers());

    // A table that is already past its bound when it opens starts on key deletes.
    // The 1 MiB target file size makes the bound 256 KiB.
    let (seeded, catalog, tmp) = table("auto_reopened_large", DeletionMode::Auto, 1).await;
    let ids: Vec<i64> = (0..60_000).collect();
    write_incompressible(&seeded, &ids).await;
    let stats = catalog
        .table_storage_stats(&seeded.table_metadata.table_id)
        .await
        .expect("storage stats");
    let bytes = CayenneTableProvider::warm_table_bytes(&stats);
    assert!(
        bytes > 256 * 1024,
        "premise: the seeded table ({bytes} B) must be past the 256 KiB bound"
    );
    seeded
        .drain_in_flight_maintenance()
        .await
        .expect("quiesce before reopening");
    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), SessionContext::new().runtime_env())
        .open("auto_reopened_large")
        .await
        .expect("reopen");
    assert!(
        !reopened.should_capture_positions(),
        "an auto table past its bound at open starts on key deletes"
    );
    drop(tmp);
}

/// The reclaim of an auto table on position deletes is a whole-table rewrite that
/// leaves `write_lock` free for writers while it re-encodes; an explicit
/// `deletion_mode: position` table holds writers off for the whole rewrite.
#[tokio::test]
async fn an_auto_tables_reclaim_rewrite_leaves_writers_free() {
    for (mode, writers_free) in [(DeletionMode::Auto, true), (DeletionMode::Position, false)] {
        let name = format!("reclaim_writers_{}", mode.as_str());
        let (provider, _catalog, _tmp) = table(&name, mode, 256).await;
        write(&provider, &[0, 1, 2, 3], 1).await;
        write(&provider, &[0, 1, 2, 3], 2).await;
        assert!(
            provider.deletion_index_len() >= 2,
            "premise: the upsert left tombstones past the reclaim trigger"
        );

        let observed = Arc::new(std::sync::Mutex::new(None));
        let observed_in_hook = Arc::clone(&observed);
        let write_lock = provider.write_lock_arc();
        *provider.test_pre_rewrite_commit_hook.lock() = Some(Box::new(move || {
            Box::pin(async move {
                let free = write_lock.try_lock().is_ok();
                *observed_in_hook.lock().expect("observation lock") = Some(free);
            })
        }));

        assert!(
            provider
                .compact_current_snapshot_small_files()
                .await
                .expect("reclaim pass"),
            "{mode:?}: the deletion-index trigger runs the rewrite and it commits"
        );
        assert_eq!(
            provider.last_small_file_compact_path(),
            LastSmallFileCompactPath::Full
        );
        assert_eq!(
            *observed.lock().expect("observation lock"),
            Some(writers_free),
            "{mode:?}: write_lock free during the rewrite"
        );
        let ctx = SessionContext::new();
        assert_eq!(
            collect_id_value_pairs(&ctx, &provider, &name).await,
            vec![(0, 2), (1, 2), (2, 2), (3, 2)]
        );
    }
}

/// An upsert of a row whose position the read-back captured, landing while an
/// auto table's rewrite re-encodes, records a position delete against a file the
/// rewrite then replaces. The key tombstone recorded with it is newer than the
/// rewrite's cutoff, so the rewrite keeps it and it hides the superseded copy in
/// the rewritten file: every key still has exactly its latest row.
#[tokio::test]
async fn an_upsert_racing_an_auto_tables_rewrite_stays_superseded() {
    let name = "auto_racing_upsert";
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
    let ids: Vec<i64> = (0..8).collect();
    write(&provider, &ids, 100).await;
    // Fold the seed into the current snapshot, where the read-back locates rows.
    provider
        .rewrite_current_snapshot_for_compaction_tracked(None)
        .await
        .expect("fold the seed into the current snapshot");
    capture_positions(&provider).await;
    assert_eq!(position_deletes(&provider), 0);

    // Upsert every key while a rewrite of the current snapshot sits between its
    // re-encode, which read the seed rows, and its commit. The keys are located
    // in the files the rewrite replaces, so the upsert records position deletes
    // against them.
    let racing = provider.clone_for_write();
    let race_ids = ids.clone();
    *provider.test_pre_rewrite_commit_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            write(&racing, &race_ids, 300).await;
        })
    }));
    {
        let _pass = provider.compaction_lock.write().await;
        assert!(
            provider
                .rewrite_current_snapshot_for_compaction()
                .await
                .expect("rewrite"),
            "the rewrite commits alongside the racing upsert"
        );
    }
    assert!(
        position_deletes(&provider) > 0,
        "premise: the racing upsert recorded position deletes against the replaced files"
    );

    let ctx = SessionContext::new();
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, name).await,
        ids.iter().map(|id| (*id, 300)).collect::<Vec<_>>(),
        "the racing upsert's key tombstones hide the copies the rewrite kept"
    );
}

/// Past its bound, an auto table's reclaim moves it to key deletes instead of
/// rewriting it whole; the seq-prefix bake reclaims from then on, and positions
/// the read-back captured are forgotten, so no later upsert records a position
/// delete.
#[tokio::test]
async fn an_auto_table_past_its_bound_switches_to_key_deletes() {
    let name = "auto_outgrown";
    // 1 MiB target file size: a 256 KiB bound.
    let (provider, catalog, _tmp) = table(name, DeletionMode::Auto, 1).await;
    let ids: Vec<i64> = (0..8).collect();
    write(&provider, &ids, 100).await;
    provider
        .rewrite_current_snapshot_for_compaction_tracked(None)
        .await
        .expect("fold the seed into the current snapshot");
    capture_positions(&provider).await;
    write(&provider, &ids, 200).await;
    let positions_before = position_deletes(&provider);
    assert!(
        positions_before > 0,
        "premise: the table recorded position deletes while within its bound"
    );
    assert!(provider.should_capture_positions());

    // Grow the table past its bound.
    let grow: Vec<i64> = (1_000..61_000).collect();
    write_incompressible(&provider, &grow).await;
    let stats = catalog
        .table_storage_stats(&provider.table_metadata.table_id)
        .await
        .expect("storage stats");
    let bytes = CayenneTableProvider::warm_table_bytes(&stats);
    assert!(
        bytes > 256 * 1024,
        "premise: the grown table ({bytes} B) must be past the 256 KiB bound"
    );

    assert!(
        !provider
            .compact_current_snapshot_small_files()
            .await
            .expect("reclaim pass"),
        "past its bound the pass switches the table instead of rewriting it"
    );
    assert_eq!(
        provider.last_small_file_compact_path(),
        LastSmallFileCompactPath::None,
        "the pass switched the table instead of rewriting it"
    );
    assert!(
        !provider.should_capture_positions(),
        "the table is on key deletes now"
    );
    {
        let guard = provider.pk_keyset_cache.lock();
        if let Some(CachedPkIndex::Exact(keyset)) = guard.as_ref() {
            assert!(
                keyset
                    .locations()
                    .all(|location| !matches!(location, RowLocation::FilePositioned { .. })),
                "the switch forgets every captured position"
            );
            assert!(keyset.captured_files.is_empty());
        }
    }

    // A later upsert records key deletes only; the position deletes from before
    // the switch still apply.
    write(&provider, &ids, 300).await;
    assert_eq!(
        position_deletes(&provider),
        positions_before,
        "no position delete is recorded after the switch"
    );
    let ctx = SessionContext::new();
    let rows = collect_id_value_pairs(&ctx, &provider, name).await;
    assert_eq!(rows.len(), ids.len() + grow.len(), "one row per key");
    let latest: Vec<(i64, i64)> = rows.iter().copied().filter(|(id, _)| *id < 8).collect();
    assert_eq!(
        latest,
        ids.iter().map(|id| (*id, 300)).collect::<Vec<_>>(),
        "every seed key shows its latest row"
    );
}

/// A key repeated within one write is folded out of an auto table's files, as key
/// mode does, so statistics stay exact; an explicit `deletion_mode: position`
/// table hides the superseded copy with a position delete instead.
#[tokio::test]
async fn an_auto_table_folds_a_key_repeated_within_one_write() {
    for (mode, expect_position_deletes) in
        [(DeletionMode::Auto, false), (DeletionMode::Position, true)]
    {
        let name = format!("repeated_{}", mode.as_str());
        let (provider, _catalog, _tmp) = table(&name, mode, 256).await;
        let schema = Arc::clone(&provider.table_metadata.schema);
        let ctx = SessionContext::new();
        let first = id_value_batch(Arc::clone(&schema), &[0, 1, 2], &[1, 1, 1]);
        let second = id_value_batch(Arc::clone(&schema), &[1, 2, 3], &[2, 2, 2]);
        let plan = provider
            .insert_into(
                &ctx.state(),
                MemorySourceConfig::try_new_exec(&[vec![first, second]], schema, None)
                    .expect("source"),
                InsertOp::Overwrite,
            )
            .await
            .expect("plan");
        collect(plan, ctx.task_ctx()).await.expect("overwrite");
        provider
            .flush_pending_maintenance()
            .await
            .expect("maintenance");

        assert_eq!(
            position_deletes(&provider) > 0,
            expect_position_deletes,
            "{mode:?}: whether repeated keys left position deletes"
        );
        assert_eq!(
            collect_id_value_pairs(&ctx, &provider, &name).await,
            vec![(0, 1), (1, 2), (2, 2), (3, 2)],
            "{mode:?}: the last copy of each key wins"
        );
    }
}
