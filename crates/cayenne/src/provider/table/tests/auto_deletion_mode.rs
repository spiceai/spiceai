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
            let mut z = id.cast_unsigned().wrapping_add(0x9E37_79B9_7F4A_7C15);
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
    let reopened =
        CayenneTableProviderBuilder::new(Arc::clone(&catalog), SessionContext::new().runtime_env())
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
    let recorded = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let recorded_in_hook = Arc::clone(&recorded);
    *provider.test_pre_rewrite_commit_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            write(&racing, &race_ids, 300).await;
            recorded_in_hook.store(
                position_deletes(&racing),
                std::sync::atomic::Ordering::SeqCst,
            );
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
    assert_eq!(
        recorded.load(std::sync::atomic::Ordering::SeqCst),
        u64::try_from(ids.len()).expect("small"),
        "premise: the racing upsert recorded a position delete per key against the replaced files"
    );
    assert_eq!(
        position_deletes(&provider),
        0,
        "the commit retires the position deletes of the files it replaced"
    );

    let ctx = SessionContext::new();
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, name).await,
        ids.iter().map(|id| (*id, 300)).collect::<Vec<_>>(),
        "the racing upsert's key tombstones hide the copies the rewrite kept"
    );
}

/// Past its bound, an auto table's reclaim moves it to key deletes instead of
/// rewriting it whole. The switch forgets the positions the read-back captured,
/// and the upsert path stops recording position deletes, so a later upsert of a
/// key that was located records only a key tombstone; position deletes recorded
/// before the switch keep applying.
#[tokio::test]
async fn an_auto_table_past_its_bound_switches_to_key_deletes() {
    let name = "auto_outgrown";
    // 1 MiB target file size: a 256 KiB bound.
    let (provider, catalog, _tmp) = table(name, DeletionMode::Auto, 1).await;
    // Keys 0..8 are upserted while the table is on position deletes; keys 8..16
    // stay located, unupserted, until after the switch.
    let upserted: Vec<i64> = (0..8).collect();
    let located: Vec<i64> = (8..16).collect();
    let seed: Vec<i64> = (0..16).collect();
    write(&provider, &seed, 100).await;
    provider
        .rewrite_current_snapshot_for_compaction_tracked(None)
        .await
        .expect("fold the seed into the current snapshot");
    capture_positions(&provider).await;
    write(&provider, &upserted, 200).await;
    let positions_before = position_deletes(&provider);
    assert!(
        positions_before > 0,
        "premise: the table recorded position deletes while within its bound"
    );
    let located_positions = |provider: &CayenneTableProvider| -> usize {
        let pk_indices = provider
            .primary_key_indices()
            .expect("pk indices")
            .expect("primary key");
        let converter = provider.build_pk_converter(&pk_indices).expect("converter");
        let schema = Arc::clone(&provider.table_metadata.schema);
        let batch = id_value_batch(schema, &located, &vec![0; located.len()]);
        let rows = converter
            .convert_columns(&[Arc::clone(batch.column(0))])
            .expect("convert keys");
        let guard = provider.pk_keyset_cache.lock();
        let Some(CachedPkIndex::Exact(keyset)) = guard.as_ref() else {
            panic!("premise: an exact keyset tracks the located keys");
        };
        (0..rows.num_rows())
            .filter(|row| {
                let digest = pk_digest(&rows.row(*row).owned());
                matches!(
                    keyset.location_by_digest(digest),
                    Some(RowLocation::FilePositioned { .. })
                )
            })
            .count()
    };
    assert_eq!(
        located_positions(&provider),
        located.len(),
        "premise: the unupserted keys are located"
    );

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
    assert_eq!(
        located_positions(&provider),
        0,
        "the switch forgets every captured position"
    );

    // An upsert of the formerly located keys records key deletes only; the
    // position deletes from before the switch still apply.
    write(&provider, &located, 300).await;
    assert_eq!(
        position_deletes(&provider),
        positions_before,
        "no position delete is recorded after the switch"
    );
    let ctx = SessionContext::new();
    let rows = collect_id_value_pairs(&ctx, &provider, name).await;
    assert_eq!(rows.len(), seed.len() + grow.len(), "one row per key");
    let expected: Vec<(i64, i64)> = upserted
        .iter()
        .map(|id| (*id, 200))
        .chain(located.iter().map(|id| (*id, 300)))
        .collect();
    let latest: Vec<(i64, i64)> = rows.iter().copied().filter(|(id, _)| *id < 16).collect();
    assert_eq!(latest, expected, "every seed key shows its latest row");
}

/// An auto table within its size bound moves to key deletes once its whole-table
/// reclaims take more than a fiftieth of the time. Reclaims are judged only over a
/// full window, so back-to-back reclaims inside one leave the table on position
/// deletes; judged, they move it, and later upserts record key deletes only.
#[tokio::test]
async fn an_auto_table_whose_reclaims_take_too_much_of_the_time_switches_to_key_deletes() {
    let name = "auto_busy_reclaims";
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
    let auto = Arc::clone(provider.pk_deletion_mode.auto().expect("an auto table"));
    let keys: Vec<i64> = (0..16).collect();
    write(&provider, &keys, 100).await;

    // Inside a window that has not closed, nothing is judged, however busy.
    auto.set_reclaim_window(std::time::Duration::from_secs(3_600));
    for value in [200, 300] {
        write(&provider, &keys, value).await;
        assert!(
            provider
                .rewrite_current_snapshot_for_reclaim_tracked(None)
                .await
                .expect("reclaim pass"),
            "the reclaim of a table within its bound commits"
        );
    }
    assert!(
        provider.should_capture_positions(),
        "reclaims inside an open window do not move the table"
    );

    // Close the window at the next reclaim: back-to-back reclaims took far more
    // than a fiftieth of the time since the table opened.
    auto.set_reclaim_window(std::time::Duration::ZERO);
    write(&provider, &keys, 400).await;
    assert!(
        provider
            .rewrite_current_snapshot_for_reclaim_tracked(None)
            .await
            .expect("reclaim pass"),
        "the judged reclaim itself commits"
    );
    assert!(
        !provider.should_capture_positions(),
        "the table is on key deletes once its reclaims took too much of the time"
    );

    write(&provider, &keys, 500).await;
    let ctx = SessionContext::new();
    let rows = collect_id_value_pairs(&ctx, &provider, name).await;
    let expected: Vec<(i64, i64)> = keys.iter().map(|id| (*id, 500)).collect();
    assert_eq!(rows, expected, "every key shows its latest row, once");
}

/// A writer that checked the keyset out while the table was on position deletes
/// and returns it after the switch to key deletes records no position delete with
/// it: the upsert path reads the table's mode, not the positions the keyset still
/// carries.
#[tokio::test]
async fn a_keyset_checked_out_before_the_switch_records_no_position_delete_after_it() {
    let name = "auto_checkout_across_switch";
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
    let keys: Vec<i64> = (0..16).collect();
    write(&provider, &keys, 100).await;
    provider
        .rewrite_current_snapshot_for_compaction_tracked(None)
        .await
        .expect("fold the seed into the current snapshot");
    capture_positions(&provider).await;

    // A writer holds the keyset, located, across the switch.
    let (index, checkout) = provider.take_cached_pk_index();
    let located = match index.as_ref() {
        Some(CachedPkIndex::Exact(keyset)) => keys
            .iter()
            .filter(|id| {
                let schema = Arc::clone(&provider.table_metadata.schema);
                let batch = id_value_batch(schema, &[**id], &[0]);
                let pk_indices = provider
                    .primary_key_indices()
                    .expect("pk indices")
                    .expect("primary key");
                let rows = provider
                    .build_pk_converter(&pk_indices)
                    .expect("converter")
                    .convert_columns(&[Arc::clone(batch.column(0))])
                    .expect("convert key");
                matches!(
                    keyset.location_by_digest(pk_digest(&rows.row(0).owned())),
                    Some(RowLocation::FilePositioned { .. })
                )
            })
            .count(),
        _ => 0,
    };
    assert_eq!(
        located,
        keys.len(),
        "premise: the checked-out keyset locates every key"
    );
    assert!(
        provider.switch_auto_to_key_deletes("test"),
        "the table switches to key deletes"
    );
    provider.store_cached_pk_index(index.expect("an exact keyset"), checkout);

    write(&provider, &keys, 200).await;
    assert_eq!(
        position_deletes(&provider),
        0,
        "an upsert after the switch records key deletes only"
    );
    let ctx = SessionContext::new();
    let rows = collect_id_value_pairs(&ctx, &provider, name).await;
    let expected: Vec<(i64, i64)> = keys.iter().map(|id| (*id, 200)).collect();
    assert_eq!(rows, expected, "every key shows its latest row, once");
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

/// A pipelined staged upsert reserves its sequences under `write_lock` (stage A)
/// and publishes its replacement rows and tombstones later, without it (stage B).
/// A writer-concurrent full rewrite that captured its cutoff while such a write
/// was staged would see neither the write's rows nor its tombstones, keep the
/// superseded copies in its output, and then — the write's tombstones being at or
/// below that cutoff once published — prune exactly the tombstones that hide
/// them. Both key mode and `auto`'s position regime take that rewrite.
#[tokio::test]
async fn a_staged_upsert_straddling_a_fenced_rewrite_stays_superseded() {
    for mode in [DeletionMode::Key, DeletionMode::Auto] {
        let name = format!("straddled_rewrite_{}", mode.as_str());
        let ctx = SessionContext::new();
        let (provider, _catalog, _tmp) = create_cdc_upsert_table_with_vortex_config(
            &name,
            ctx.runtime_env(),
            VortexConfig {
                deletion_mode: mode,
                cdc_durability: crate::metadata::CdcDurability::File,
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        )
        .await;
        let ids: Vec<i64> = (0..8).collect();
        write(&provider, &ids, 100).await;
        provider
            .rewrite_current_snapshot_for_compaction_tracked(None)
            .await
            .expect("fold the seed into the current snapshot");

        // Stage A of an upsert of every key: sequences reserved, rows staged, the
        // publish still to come.
        let schema = Arc::clone(&provider.table_metadata.schema);
        let staged = provider
            .write_cdc_append_stream(
                single_batch_stream(id_value_batch(schema, &ids, &vec![300; ids.len()])),
                &ctx.task_ctx(),
            )
            .await
            .expect("stage the upsert");

        // Stage B lands shortly after; the rewrite cannot commit before it has.
        let (published_tx, published_rx) = tokio::sync::oneshot::channel::<()>();
        let finisher = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            staged.finish().await.expect("publish the staged upsert");
            published_tx.send(()).expect("report the publish");
        });
        *provider.test_pre_rewrite_commit_hook.lock() = Some(Box::new(move || {
            Box::pin(async move {
                published_rx.await.expect("the staged upsert publishes");
            })
        }));
        {
            let _pass = provider.compaction_lock.write().await;
            provider
                .rewrite_current_snapshot_for_compaction()
                .await
                .expect("rewrite");
        }
        finisher.await.expect("join the publish");

        assert_eq!(
            collect_id_value_pairs(&ctx, &provider, &name).await,
            ids.iter().map(|id| (*id, 300)).collect::<Vec<_>>(),
            "{mode:?}: every key has exactly its staged replacement after the rewrite"
        );
    }
}

/// Every entry in the deletion index, including insert-only ones.
fn deletion_index_entries(provider: &CayenneTableProvider) -> usize {
    match provider.pk_deletion_snapshot() {
        PkDeletionSnapshot::Int64Pk { tombstones } => tombstones.len(),
        PkDeletionSnapshot::RowConverterBased { tombstones } => tombstones.len(),
        PkDeletionSnapshot::PositionBased => 0,
    }
}

/// A writer-concurrent full rewrite reclaims an upsert's tombstone together with
/// the re-insert marker recorded beside it: the rows both describe are in the
/// rewritten file, and every later delete of the key is newer than the
/// re-insertion, so the marker decides nothing. Left behind, such markers would
/// accumulate one per key ever upserted, walked by every scan's tombstone
/// pushdown and held in the index's memory reservation. Key mode's seq-prefix
/// bake reclaims through the same prune.
#[tokio::test]
async fn a_reclaim_leaves_no_reinsert_marker_at_or_below_its_cutoff() {
    for mode in [DeletionMode::Key, DeletionMode::Auto] {
        let name = format!("reinsert_markers_{}", mode.as_str());
        let (provider, _catalog, _tmp) = table(&name, mode, 256).await;
        let ids: Vec<i64> = (0..100).collect();
        write(&provider, &ids, 1).await;
        write(&provider, &ids, 2).await;
        assert_eq!(
            deletion_index_entries(&provider),
            ids.len(),
            "{mode:?} premise: the upsert recorded a tombstone and re-insert marker per key"
        );
        {
            let _pass = provider.compaction_lock.write().await;
            assert!(
                provider
                    .rewrite_current_snapshot_for_compaction()
                    .await
                    .expect("rewrite"),
                "{mode:?}: the rewrite commits"
            );
        }
        assert_eq!(
            deletion_index_entries(&provider),
            0,
            "{mode:?}: the rewrite reclaims the re-insert markers with the tombstones"
        );
        let ctx = SessionContext::new();
        assert_eq!(
            collect_id_value_pairs(&ctx, &provider, &name).await,
            ids.iter().map(|id| (*id, 2)).collect::<Vec<_>>()
        );
    }
}
