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
    load_keyset(provider).await;
    provider
        .run_position_capture()
        .await
        .expect("position capture succeeds");
}

/// Rebuild the table's exact keyset from its files, every key unlocated.
async fn load_keyset(provider: &CayenneTableProvider) {
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
}

/// How many of `ids` `keyset` locates by file position.
fn located_keys(provider: &CayenneTableProvider, keyset: &CachedPkKeyset, ids: &[i64]) -> usize {
    let pk_indices = provider
        .primary_key_indices()
        .expect("resolve pk indices")
        .expect("table has a primary key");
    let converter = provider
        .build_pk_converter(&pk_indices)
        .expect("build pk converter");
    ids.iter()
        .filter(|id| {
            let batch = id_value_batch(Arc::clone(&provider.table_metadata.schema), &[**id], &[0]);
            let rows = converter
                .convert_columns(&[Arc::clone(batch.column(0))])
                .expect("convert key");
            matches!(
                keyset.location_by_digest(pk_digest(&rows.row(0).owned())),
                Some(RowLocation::FilePositioned { .. })
            )
        })
        .count()
}

/// The cached keyset's captured files and how many of `ids` it locates by file
/// position.
fn cached_captured_positions(provider: &CayenneTableProvider, ids: &[i64]) -> (usize, usize) {
    let guard = provider.pk_keyset_cache.lock();
    let Some(CachedPkIndex::Exact(keyset)) = guard.as_ref() else {
        panic!("the table caches an exact keyset");
    };
    (
        keyset.captured_files.len(),
        located_keys(provider, keyset, ids),
    )
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
    // The 1 MiB target file size makes the bound 32 KiB.
    let (seeded, catalog, tmp) = table("auto_reopened_large", DeletionMode::Auto, 1).await;
    let ids: Vec<i64> = (0..60_000).collect();
    write_incompressible(&seeded, &ids).await;
    let bytes = seeded.warm_listed_bytes().await.expect("size the table");
    assert!(
        bytes > 32 * 1024,
        "premise: the seeded table ({bytes} B) must be past the 32 KiB bound"
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

/// The switch to key deletes forgets the positions the read-back captured, and the
/// upsert path stops recording position deletes, so a later upsert of a key that
/// was located records only a key tombstone; position deletes recorded before the
/// switch keep applying.
#[tokio::test]
async fn the_switch_to_key_deletes_forgets_captured_positions() {
    let name = "auto_switch_forgets_positions";
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
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

    assert!(provider.switch_auto_to_key_deletes(), "the table switches");
    assert!(
        !provider.should_capture_positions(),
        "the table is on key deletes now"
    );
    assert_eq!(
        located_positions(&provider),
        0,
        "the switch forgets every captured position"
    );

    write(&provider, &located, 300).await;
    assert_eq!(
        position_deletes(&provider),
        positions_before,
        "no position delete is recorded after the switch"
    );
    let ctx = SessionContext::new();
    let rows = collect_id_value_pairs(&ctx, &provider, name).await;
    let expected: Vec<(i64, i64)> = upserted
        .iter()
        .map(|id| (*id, 200))
        .chain(located.iter().map(|id| (*id, 300)))
        .collect();
    assert_eq!(rows, expected, "every key shows its latest row, once");
}

/// Past its bound, an auto table's reclaim pass moves it to key deletes. Its rows
/// are in the current snapshot — the seed was folded there — where the bake cannot
/// reach them, so the same pass reclaims the index with a whole-table rewrite, as
/// a key-delete table, alongside writers.
#[tokio::test]
async fn an_auto_table_past_its_bound_switches_to_key_deletes() {
    let name = "auto_outgrown";
    // 1 MiB target file size: a 32 KiB bound.
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 1).await;
    let upserted: Vec<i64> = (0..8).collect();
    let seed: Vec<i64> = (0..16).collect();
    write(&provider, &seed, 100).await;
    provider
        .rewrite_current_snapshot_for_compaction_tracked(None)
        .await
        .expect("fold the seed into the current snapshot");
    write(&provider, &upserted, 200).await;

    // Grow the table past its bound.
    let grow: Vec<i64> = (1_000..61_000).collect();
    write_incompressible(&provider, &grow).await;
    let bytes = provider.warm_listed_bytes().await.expect("size the table");
    assert!(
        bytes > 32 * 1024,
        "premise: the grown table ({bytes} B) must be past the 32 KiB bound"
    );
    assert!(
        deletion_index_entries(&provider) >= 2,
        "premise: the index is past its trigger"
    );

    assert!(
        provider
            .compact_current_snapshot_small_files()
            .await
            .expect("reclaim pass"),
        "the pass reclaims the rows in the current snapshot"
    );
    assert!(
        !provider.should_capture_positions(),
        "past its bound the table is on key deletes"
    );
    assert_eq!(
        provider.last_small_file_compact_path(),
        LastSmallFileCompactPath::Full,
        "the reclaim rewrote the table whole"
    );
    assert_eq!(
        deletion_index_entries(&provider),
        0,
        "the reclaim cleared the index"
    );

    let located: Vec<i64> = (8..16).collect();
    write(&provider, &located, 300).await;
    assert_eq!(
        position_deletes(&provider),
        0,
        "no position delete is recorded on key deletes"
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

/// An auto table paces its whole-table reclaims on position and on key deletes:
/// one that is not yet due leaves the index past its trigger, and runs once it
/// is.
#[tokio::test]
async fn an_auto_table_paces_its_whole_table_reclaims_on_either_kind_of_delete() {
    for on_key_deletes in [false, true] {
        let name = format!("auto_paced_reclaims_{on_key_deletes}");
        let (provider, _catalog, _tmp) = table(&name, DeletionMode::Auto, 256).await;
        let auto = Arc::clone(provider.pk_deletion_mode.auto().expect("an auto table"));
        let keys: Vec<i64> = (0..16).collect();
        write(&provider, &keys, 100).await;
        provider
            .rewrite_current_snapshot_for_compaction_tracked(None)
            .await
            .expect("fold the seed into the current snapshot");
        if on_key_deletes {
            assert!(
                provider.switch_auto_to_key_deletes(),
                "premise: on key deletes"
            );
        }

        // As if the last whole-table reclaim had taken a minute: a paced one is
        // due nine minutes after it.
        auto.record_reclaim(
            std::time::Duration::from_secs(60),
            std::time::Instant::now(),
        );
        write(&provider, &keys, 200).await;
        let pending = deletion_index_entries(&provider);
        assert!(
            pending >= 2,
            "premise: the index is past its trigger ({pending})"
        );
        let decision = provider.deletion_index_reclaim_decision();
        assert_eq!(
            (decision, decision.declined_outcome()),
            (ReclaimDecision::Paced, CompactionOutcome::DeclinedPaced),
            "a held-back reclaim is counted as paced, not as below its trigger (key deletes: {on_key_deletes})"
        );
        assert!(
            !provider
                .compact_current_snapshot_small_files()
                .await
                .expect("reclaim pass"),
            "a reclaim that is not due waits (key deletes: {on_key_deletes})"
        );
        assert_eq!(
            deletion_index_entries(&provider),
            pending,
            "the deferred reclaim left the index as it was (key deletes: {on_key_deletes})"
        );
        auto.forget_reclaim_pace();
        assert!(
            provider
                .compact_current_snapshot_small_files()
                .await
                .expect("due reclaim pass"),
            "a due reclaim runs (key deletes: {on_key_deletes})"
        );
        assert_eq!(
            deletion_index_entries(&provider),
            0,
            "the reclaim cleared the index"
        );
        let ctx = SessionContext::new();
        let rows = collect_id_value_pairs(&ctx, &provider, &name).await;
        let expected: Vec<(i64, i64)> = keys.iter().map(|id| (*id, 200)).collect();
        assert_eq!(rows, expected, "every key shows its latest row, once");
    }
}

/// An auto table on key deletes whose rows sit in its current snapshot — where
/// every whole-table rewrite leaves them — reclaims by rewriting itself whole: the
/// bake folds only protected snapshots, so it declines, and without this the index
/// would keep every delete of those rows for the life of the table.
#[tokio::test]
async fn an_auto_table_on_key_deletes_reclaims_rows_the_bake_cannot_reach() {
    let name = "auto_key_current_snapshot";
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
    let keys: Vec<i64> = (0..16).collect();
    write(&provider, &keys, 100).await;
    provider
        .rewrite_current_snapshot_for_compaction_tracked(None)
        .await
        .expect("fold the seed into the current snapshot");
    assert!(
        provider.switch_auto_to_key_deletes(),
        "premise: on key deletes"
    );

    // Enough protected snapshots for the bake to pick a prefix.
    for value in 200..206 {
        write(&provider, &keys, value).await;
    }
    let pending = deletion_index_entries(&provider);
    assert!(
        pending >= 2,
        "premise: the index is past its trigger ({pending})"
    );
    assert!(
        !provider
            .bake_seq_prefix_protected_snapshots()
            .await
            .expect("bake pass"),
        "premise: the bake cannot reach rows in the current snapshot"
    );
    assert_eq!(
        deletion_index_entries(&provider),
        pending,
        "premise: the bake left the index as it was"
    );

    assert!(
        provider
            .compact_current_snapshot_small_files()
            .await
            .expect("reclaim pass"),
        "the table rewrites itself whole"
    );
    assert_eq!(
        deletion_index_entries(&provider),
        0,
        "the reclaim cleared the index"
    );
    assert!(
        !provider.should_capture_positions(),
        "the reclaim did not move the table back to position deletes"
    );
    let ctx = SessionContext::new();
    let rows = collect_id_value_pairs(&ctx, &provider, name).await;
    let expected: Vec<(i64, i64)> = keys.iter().map(|id| (*id, 205)).collect();
    assert_eq!(rows, expected, "every key shows its latest row, once");
}

/// An auto table on key deletes whose rows are all in protected snapshots leaves
/// its index to the bake: the trigger fires, but the pass finds the current
/// snapshot empty and does not rewrite the table.
#[tokio::test]
async fn an_auto_table_on_key_deletes_leaves_reachable_rows_to_the_bake() {
    let name = "auto_key_protected_rows";
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
    assert!(
        provider.switch_auto_to_key_deletes(),
        "premise: on key deletes"
    );
    let keys: Vec<i64> = (0..16).collect();
    for value in 100..103 {
        write(&provider, &keys, value).await;
    }
    provider
        .drain_in_flight_maintenance()
        .await
        .expect("settle maintenance");
    assert!(
        provider
            .list_snapshot_files_with_sizes(&provider.get_current_snapshot_id())
            .await
            .expect("list the current snapshot")
            .is_empty(),
        "premise: every row is in a protected snapshot"
    );
    assert!(
        provider.deletion_index_reclaim_trigger().is_some(),
        "premise: the trigger fires"
    );
    assert!(
        !provider
            .compact_current_snapshot_small_files()
            .await
            .expect("reclaim pass"),
        "a table whose rows the bake can reach is not rewritten whole"
    );
    assert_eq!(
        provider.last_small_file_compact_path(),
        LastSmallFileCompactPath::None,
        "the pass left the table to the bake"
    );
    let ctx = SessionContext::new();
    let rows = collect_id_value_pairs(&ctx, &provider, name).await;
    let expected: Vec<(i64, i64)> = keys.iter().map(|id| (*id, 102)).collect();
    assert_eq!(rows, expected, "every key shows its latest row, once");
}

/// The whole-table reclaim trigger fires for an auto table on key deletes at the
/// bake's trigger, and never for an explicit `deletion_mode: key` table, which
/// keeps the bake alone.
#[tokio::test]
async fn the_whole_table_reclaim_trigger_covers_auto_on_key_deletes_not_explicit_key() {
    let (auto_key, _catalog, _tmp) = table("auto_key_trigger", DeletionMode::Auto, 256).await;
    assert!(
        auto_key.switch_auto_to_key_deletes(),
        "premise: on key deletes"
    );
    install_int64_deletes(&auto_key, &[(0, 10)]);
    assert_eq!(
        auto_key.deletion_index_reclaim_trigger(),
        None,
        "below the trigger nothing fires"
    );
    install_int64_deletes(&auto_key, &[(0, 10), (1, 11)]);
    assert_eq!(
        auto_key.deletion_index_reclaim_trigger(),
        Some(SnapshotMaintenanceTrigger::DeletionIndexSize {
            deletion_index_len: 2,
            trigger_len: 2,
            over_memory_ceiling: false,
        }),
        "at the trigger an auto table on key deletes may rewrite itself whole"
    );

    let (key, _key_catalog, _key_tmp) = table("explicit_key_trigger", DeletionMode::Key, 256).await;
    install_int64_deletes(&key, &[(0, 10), (1, 11), (2, 12)]);
    assert_eq!(
        key.deletion_index_reclaim_trigger(),
        None,
        "an explicit key table keeps the bake alone"
    );
}

/// An auto table on key deletes defers its whole-table reclaim while the CDC apply
/// is at or over capacity, as its bake does. The pass runs the bake's back-pressure
/// gate first; the reclaim reads the same sample through its own cursor, so it
/// yields too instead of running the costlier rewrite while the bake yields.
#[tokio::test]
async fn an_auto_table_on_key_deletes_defers_its_whole_table_reclaim_with_the_bake() {
    let (provider, _catalog, _tmp) = table("auto_key_backpressure", DeletionMode::Auto, 256).await;
    assert!(
        provider.switch_auto_to_key_deletes(),
        "premise: on key deletes"
    );
    install_int64_deletes(&provider, &[(0, 10), (1, 11)]);
    assert!(
        provider.deletion_index_reclaim_trigger().is_some(),
        "premise: over the trigger with a healthy apply, the reclaim fires"
    );
    // 200 ms of apply per batch against the microsecond arrival gaps of a tight
    // loop, past warmup: the apply is far behind its offered load.
    for _ in 0..(crate::provider::tuning::WARMUP_BATCHES * 3) {
        provider
            .context
            .record_ingest(100, 1, 10_000, Duration::from_millis(200), None);
    }
    assert!(
        provider.context.bake_should_defer_for_apply(),
        "premise: the pass's bake reads the sample first and defers"
    );
    assert_eq!(
        provider.deletion_index_reclaim_trigger(),
        None,
        "the whole-table reclaim defers with the bake"
    );
}

/// A writer that checked the keyset out while the table was on position deletes
/// and returns it after the switch to key deletes records no position delete with
/// it: the upsert path reads the table's mode. The keyset it returns forgets its
/// captured positions, as the switch did for the cached one.
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
        Some(CachedPkIndex::Exact(keyset)) => located_keys(&provider, keyset, &keys),
        _ => 0,
    };
    assert_eq!(
        located,
        keys.len(),
        "premise: the checked-out keyset locates every key"
    );
    assert!(
        provider.switch_auto_to_key_deletes(),
        "the table switches to key deletes"
    );
    // Cache cell was `None` while the keyset was checked out, so the switch
    // scrubbed nothing; restoring must forget positions itself.
    provider.store_cached_pk_index(index.expect("an exact keyset"), checkout);
    assert_eq!(
        cached_captured_positions(&provider, &keys),
        (0, 0),
        "the returned keyset keeps no captured file or position: (captured files, located keys)"
    );

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

/// The whole-table reclaim of an auto table on position deletes takes no permit
/// from the process-global encode budget, so a write never waits out its encode.
/// Here a writer holds the budget's only permit while the reclaim runs; holding a
/// permit itself, the reclaim would wait for that writer, and a writer asking for
/// the budget's whole width would wait for the reclaim.
#[tokio::test]
async fn an_auto_tables_position_reclaim_takes_no_encode_permit() {
    struct InstalledBudget;
    impl Drop for InstalledBudget {
        fn drop(&mut self) {
            crate::provider::write_budget::clear_global_encode_budget();
        }
    }
    // Dropped after `_installed`, which removes the budget first.
    let _serialized = crate::provider::write_budget::GLOBAL_ENCODE_BUDGET_TEST_LOCK
        .lock()
        .await;
    crate::provider::write_budget::set_global_encode_concurrency(1);
    let _installed = InstalledBudget;

    let name = "auto_reclaim_unbudgeted";
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
    let keys: Vec<i64> = (0..16).collect();
    write(&provider, &keys, 100).await;
    // The upsert's deletes put the index past its trigger. Its maintenance, and
    // the reclaim that schedules, run below.
    insert_batch(
        &provider,
        id_value_batch(
            Arc::clone(&provider.table_metadata.schema),
            &keys,
            &[200; 16],
        ),
    )
    .await;
    assert!(
        provider.should_capture_positions(),
        "premise: the table is on position deletes"
    );

    let held = crate::provider::write_budget::acquire_encode_permits(
        1,
        crate::provider::delta_encoding::WriteClass::Delta,
        false,
    )
    .await
    .expect("the budget is installed");
    tokio::time::timeout(Duration::from_secs(30), async {
        provider
            .flush_pending_maintenance()
            .await
            .expect("post-write maintenance");
        provider
            .compact_current_snapshot_small_files()
            .await
            .expect("reclaim pass");
        provider
            .drain_in_flight_maintenance()
            .await
            .expect("drain maintenance");
    })
    .await
    .expect("the reclaim waited for the encode permit a writer holds");
    drop(held);

    assert_eq!(
        provider.last_small_file_compact_path(),
        LastSmallFileCompactPath::Full,
        "the whole-table reclaim committed"
    );
    assert_eq!(
        provider.deletion_index_len(),
        0,
        "the reclaim cleared the index"
    );
    let ctx = SessionContext::new();
    let rows = collect_id_value_pairs(&ctx, &provider, name).await;
    let expected: Vec<(i64, i64)> = keys.iter().map(|id| (*id, 200)).collect();
    assert_eq!(rows, expected, "every key shows its latest row, once");
}

/// A position-capture pass that began while an auto table was on position deletes
/// and publishes after the switch to key deletes captures nothing: the switch
/// forgot the table's positions, and the pass reads the mode again before it
/// records any.
#[tokio::test]
async fn a_capture_pass_that_publishes_after_the_switch_captures_nothing() {
    let name = "auto_capture_across_switch";
    let (provider, _catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
    let keys: Vec<i64> = (0..16).collect();
    write(&provider, &keys, 100).await;
    provider
        .rewrite_current_snapshot_for_compaction_tracked(None)
        .await
        .expect("fold the seed into the current snapshot");
    load_keyset(&provider).await;
    assert_eq!(
        cached_captured_positions(&provider, &keys),
        (0, 0),
        "premise: nothing captured yet"
    );
    assert!(
        provider.switch_auto_to_key_deletes(),
        "the table switches to key deletes"
    );

    // The pass passed its mode check before the switch; it lists and scans the
    // current snapshot's files now.
    provider
        .capture_new_file_positions()
        .await
        .expect("capture pass");

    assert_eq!(
        cached_captured_positions(&provider, &keys),
        (0, 0),
        "a pass that publishes after the switch captures no file or position: (captured files, located keys)"
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

/// A full rewrite reads a fresh capture of the table whatever view reuse the
/// table allows its queries. A read-only `refresh_mode: changes`
/// dataset lets queries serve a view up to a second old. A rewrite that took such a
/// view would fold, and drop, a protected snapshot published after it without
/// that snapshot's rows in its output.
#[tokio::test]
async fn a_rewrite_reads_a_fresh_view_where_queries_may_reuse_an_older_one() {
    for mode in [
        DeletionMode::Key,
        DeletionMode::Auto,
        DeletionMode::Position,
    ] {
        let name = format!("stale_view_rewrite_{}", mode.as_str());
        let ctx = SessionContext::new();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = temp_dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}/cayenne.db", metadata_dir.display()))
                .expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog initialized");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let options = CreateTableOptions {
            table_name: name.clone(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(
                datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                    "id".to_string(),
                ]),
            )),
            base_path: temp_dir.path().join("data").display().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode: mode,
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        };
        let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
            .with_scan_view_reuse(ScanViewReuse::WithinLag(Duration::from_hours(1)))
            .create(options)
            .await
            .expect("table created");
        let ids: Vec<i64> = (0..8).collect();
        write(&provider, &ids, 100).await;
        provider
            .rewrite_current_snapshot_for_compaction_tracked(None)
            .await
            .expect("fold the seed into the current snapshot");
        // A query captures the table's view, then every key is upserted.
        assert_eq!(
            collect_id_value_pairs(&ctx, &provider, &name).await,
            ids.iter().map(|id| (*id, 100)).collect::<Vec<_>>(),
            "{mode:?}: premise: the seed"
        );
        write(&provider, &ids, 200).await;
        {
            let _pass = provider.compaction_lock.write().await;
            assert!(
                provider
                    .rewrite_current_snapshot_for_compaction()
                    .await
                    .expect("rewrite"),
                "{mode:?}: premise: the rewrite commits"
            );
        }
        // Read what the rewrite committed through a fresh provider, which
        // reuses no view.
        let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
            .open(&name)
            .await
            .expect("reopen the table");
        let reopened_ctx = SessionContext::new();
        assert_eq!(
            collect_id_value_pairs(&reopened_ctx, &reopened, &name).await,
            ids.iter().map(|id| (*id, 200)).collect::<Vec<_>>(),
            "{mode:?}: every key keeps the value upserted after the query's view"
        );
    }
}

/// A writer-concurrent full rewrite holds `write_lock` only to capture what it
/// reads. A write can land after that capture, before the rewrite builds its plan,
/// and the rewrite then reads none of that write's rows and keeps the deletes that
/// hide the rows it replaced.
#[tokio::test]
async fn a_write_landing_after_a_rewrites_capture_stays_right() {
    for mode in [DeletionMode::Key, DeletionMode::Auto] {
        let name = format!("post_capture_write_{}", mode.as_str());
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
        // Deletes for the rewrite to fold: every key upserted once more.
        write(&provider, &ids, 200).await;

        // After the rewrite releases `write_lock` and before it builds its plan,
        // upsert half the keys again and add new ones.
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel::<()>();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel::<()>();
        *provider.test_rewrite_post_capture_hook.lock() = Some(Box::new(move || {
            Box::pin(async move {
                let _ = reached_tx.send(());
                let _ = resume_rx.await;
            })
        }));
        let late: Vec<i64> = (0..4).collect();
        let added: Vec<i64> = (100..104).collect();
        let rewrite = async {
            let _pass = provider.compaction_lock.write().await;
            provider
                .rewrite_current_snapshot_for_compaction()
                .await
                .expect("rewrite")
        };
        let late_writes = async {
            reached_rx
                .await
                .expect("the rewrite reaches the point after its capture");
            let schema = Arc::clone(&provider.table_metadata.schema);
            let wrote = tokio::time::timeout(Duration::from_secs(10), async {
                insert_batch(
                    &provider,
                    id_value_batch(Arc::clone(&schema), &late, &vec![300; late.len()]),
                )
                .await;
                insert_batch(
                    &provider,
                    id_value_batch(schema, &added, &vec![400; added.len()]),
                )
                .await;
            })
            .await
            .is_ok();
            resume_tx.send(()).expect("resume the rewrite");
            wrote
        };
        let (committed, wrote) = tokio::join!(rewrite, late_writes);
        assert!(
            wrote,
            "{mode:?}: the writes waited for the rewrite, which still held write_lock after its capture"
        );
        assert!(
            committed,
            "{mode:?}: premise: the rewrite commits with the writes landed after its capture"
        );
        let mut expected: Vec<(i64, i64)> = late.iter().map(|id| (*id, 300)).collect();
        expected.extend((4..8).map(|id| (id, 200)));
        expected.extend(added.iter().map(|id| (*id, 400)));
        assert_eq!(
            collect_id_value_pairs(&ctx, &provider, &name).await,
            expected,
            "{mode:?}: every key has its latest value after the rewrite"
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

        // Stage B lands once the rewrite holds `write_lock` to capture, after giving
        // the rewrite a second to take its cutoff. A rewrite that waits for staged
        // writes cannot take it before the publish; one that does not, takes it
        // within that second. The rewrite cannot commit before the publish either.
        let (lock_held_tx, lock_held_rx) = tokio::sync::oneshot::channel::<()>();
        let (captured_tx, mut captured_rx) = tokio::sync::oneshot::channel::<()>();
        *provider.test_rewrite_capture_probe.lock() = Some((lock_held_tx, captured_tx));
        let (published_tx, published_rx) = tokio::sync::oneshot::channel::<()>();
        let finisher = tokio::spawn(async move {
            lock_held_rx
                .await
                .expect("the rewrite holds write_lock to capture");
            let captured_first = matches!(
                tokio::time::timeout(Duration::from_secs(1), &mut captured_rx).await,
                Ok(Ok(()))
            );
            staged.finish().await.expect("publish the staged upsert");
            published_tx.send(()).expect("report the publish");
            captured_first
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
        let captured_first = finisher.await.expect("join the publish");
        assert!(
            !captured_first,
            "{mode:?}: the rewrite took its cutoff while a staged write was in flight"
        );

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

/// The line an auto table logs when it outgrows position deletes names the
/// dataset in quotes, gives the bound in MiB (in KiB below a whole MiB), and
/// links the docs.
#[test]
fn the_key_deletes_switch_line_names_the_dataset_the_bound_and_the_docs() {
    assert_eq!(
        auto_switched_to_key_deletes_message("orders", 8 * 1024 * 1024),
        "Dataset 'orders' grew past 8 MiB on disk, the largest size \
         `cayenne_deletion_mode: auto` keeps on position deletes, so it now uses key \
         deletes: queries still see every write, applying recent updates and deletes \
         as they read until compaction merges them in. See: \
         https://spiceai.org/docs/components/data-accelerators/cayenne#deletion-strategies"
    );
    assert_eq!(
        auto_switched_to_key_deletes_message("sales.orders", 32 * 1024),
        "Dataset 'sales.orders' grew past 32 KiB on disk, the largest size \
         `cayenne_deletion_mode: auto` keeps on position deletes, so it now uses key \
         deletes: queries still see every write, applying recent updates and deletes \
         as they read until compaction merges them in. See: \
         https://spiceai.org/docs/components/data-accelerators/cayenne#deletion-strategies"
    );
}

/// An auto table is sized by its live snapshot directories, not the metastore's
/// storage figures. Those still count a protected snapshot that a merge replaced
/// until its registration is swept: in the timing harness they read a 4.8 MB
/// table as 9.4 MB during its load and moved it past an 8 MiB bound. A snapshot
/// the metastore still registers but the table no longer reads stands in for one
/// here.
#[tokio::test]
async fn an_auto_table_is_sized_by_its_live_snapshots_not_the_storage_figures() {
    let name = "auto_sized_live";
    // 1 MiB target file size: a 32 KiB bound.
    let (provider, catalog, _tmp) = table(name, DeletionMode::Auto, 1).await;
    let auto = Arc::clone(provider.pk_deletion_mode.auto().expect("an auto table"));
    let keys: Vec<i64> = (0..16).collect();
    write(&provider, &keys, 100).await;
    let table_id = provider.table_metadata.table_id.clone();
    catalog
        .set_snapshot_sequence(&table_id, "replaced-protected-snapshot", 1)
        .await
        .expect("register the replaced snapshot");
    catalog
        .upsert_snapshot_file(&crate::metadata::SnapshotFile {
            table_id: table_id.clone(),
            snapshot_id: "replaced-protected-snapshot".to_string(),
            file_path: "replaced.vortex".to_string(),
            row_count: 1_000,
            file_size_bytes: 1024 * 1024,
            min_sequence: 1,
            max_sequence: 1,
            digest: None,
        })
        .await
        .expect("register its file");
    let listed = provider.warm_listed_bytes().await.expect("size the table");
    let stats = catalog
        .table_storage_stats(&table_id)
        .await
        .expect("storage stats");
    let counted = u64::try_from(stats.current_bytes + stats.protected_bytes).expect("bytes");
    let bound = auto.max_position_bytes();
    assert!(
        listed <= bound && counted > bound,
        "premise: the live snapshots ({listed} B) fit the {bound} B bound while the storage figures ({counted} B) do not"
    );

    assert!(provider.sample_storage_metrics().await, "the sample ran");
    assert!(
        !auto.switch_requested() && provider.should_capture_positions(),
        "a table within its bound stays on position deletes"
    );
}

/// Only a table that was kept fully compacted reports outgrowing it at `info`: one
/// that a whole-table reclaim rewrote while it was on position deletes. A table
/// that passes the bound before any such reclaim — a large table's first load —
/// was never rewritten whole, so its reads do not change and it says so only at
/// `debug`.
#[tokio::test]
async fn only_a_table_kept_compacted_reports_outgrowing_it() {
    for reclaim_first in [false, true] {
        let name = format!("auto_kept_compacted_{reclaim_first}");
        // 1 MiB target file size: a 32 KiB bound.
        let (provider, _catalog, _tmp) = table(&name, DeletionMode::Auto, 1).await;
        let auto = Arc::clone(provider.pk_deletion_mode.auto().expect("an auto table"));
        let keys: Vec<i64> = (0..16).collect();
        write(&provider, &keys, 100).await;
        if reclaim_first {
            write(&provider, &keys, 200).await;
            assert!(
                provider
                    .compact_current_snapshot_small_files()
                    .await
                    .expect("reclaim pass"),
                "premise: a whole-table reclaim committed on position deletes"
            );
        }
        let grow: Vec<i64> = (1_000..61_000).collect();
        write_incompressible(&provider, &grow).await;
        assert!(
            provider.warm_listed_bytes().await.expect("size the table") > 32 * 1024,
            "premise: the table is past its bound"
        );
        assert!(provider.switch_auto_to_key_deletes(), "the table switches");
        assert_eq!(
            auto.was_kept_compacted(),
            reclaim_first,
            "kept compacted exactly when a reclaim rewrote it whole (reclaim first: {reclaim_first})"
        );
    }
}

/// A whole-table rewrite persists the primary-key keyset checkpoint for its new
/// snapshot before its commit makes that snapshot current. The commit drops the
/// in-memory keyset, so a write right after it loads this checkpoint instead of
/// scanning every key. Persisted after the commit, a write in between paid that
/// scan: 36–52 ms of write latency per rewrite on a 500K-row table.
#[tokio::test]
async fn a_whole_table_rewrite_persists_the_keyset_checkpoint_before_it_commits() {
    let name = "auto_checkpoint_before_commit";
    let (provider, catalog, _tmp) = table(name, DeletionMode::Auto, 256).await;
    let keys: Vec<i64> = (0..16).collect();
    write(&provider, &keys, 100).await;
    write(&provider, &keys, 200).await;
    let table_id = provider.table_metadata.table_id.clone();
    let before = provider.get_current_snapshot_id();
    let at_commit: Arc<std::sync::Mutex<Option<Option<String>>>> =
        Arc::new(std::sync::Mutex::new(None));
    {
        let at_commit = Arc::clone(&at_commit);
        let catalog = Arc::clone(&catalog);
        let table_id = table_id.clone();
        *provider.test_pre_rewrite_commit_hook.lock() = Some(Box::new(move || {
            let at_commit = Arc::clone(&at_commit);
            let catalog = Arc::clone(&catalog);
            let table_id = table_id.clone();
            Box::pin(async move {
                let tag = catalog
                    .get_pk_index(&table_id)
                    .await
                    .expect("read the checkpoint")
                    .map(|(snapshot_id, _)| snapshot_id);
                *at_commit.lock().expect("observation lock") = Some(tag);
            })
        }));
    }
    assert!(
        provider
            .compact_current_snapshot_small_files()
            .await
            .expect("reclaim pass"),
        "premise: the whole-table reclaim commits"
    );
    let after = provider.get_current_snapshot_id();
    assert_ne!(
        before, after,
        "premise: the rewrite made a new current snapshot"
    );
    assert_eq!(
        at_commit.lock().expect("observation lock").clone(),
        Some(Some(after)),
        "just before the commit, the checkpoint already named the new snapshot"
    );
}
