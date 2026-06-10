use crate::CayenneCatalog;
use crate::metadata::VortexConfig;
use crate::provider::compaction::{CompactionRunner, MemTierCheckpointRunner};

use super::*;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_federation::schema_cast::record_convert::try_cast_to;
use rstest::rstest;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

use test_framework::arrow_record_batch_gen::*;

fn protected_snapshot_id_at_unix_time(seconds: u64) -> String {
    uuid::Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, seconds, 0)).to_string()
}

/// cycle-5 TASK 2a: packed-i64 tombstone encoding round-trips and is compact
/// (1-byte tag + 8 bytes/key, no Arrow framing).
#[test]
fn test_tombstone_packed_i64_roundtrip_and_compact() {
    let keys: Vec<Box<[u8]>> = [1_i64, -7, i64::MAX, 0, 42]
        .iter()
        .map(|pk| pk.to_be_bytes().to_vec().into_boxed_slice())
        .collect();
    let blob =
        serialize_delete_keys_to_ipc(&keys, /* is_int64_pk */ true).expect("serialize packed i64");
    assert_eq!(blob.first().copied(), Some(tombstone_format::PACKED_I64));
    assert_eq!(blob.len(), 1 + keys.len() * 8, "packed = tag + 8 bytes/key");
    let decoded = deserialize_delete_keys_from_ipc(&blob).expect("decode packed i64");
    assert_eq!(decoded, keys);
}

/// cycle-5 TASK 2a: composite-key tombstones use LZ4-compressed Arrow IPC and
/// round-trip exactly.
#[test]
fn test_tombstone_compressed_ipc_roundtrip() {
    // Encoded composite row-keys with shared prefixes (compress well).
    let keys: Vec<Box<[u8]>> = (0..200_u32)
        .map(|i| {
            let mut k = b"warehouse-0007-district-".to_vec();
            k.extend_from_slice(&i.to_be_bytes());
            k.into_boxed_slice()
        })
        .collect();
    let blob = serialize_delete_keys_to_ipc(&keys, /* is_int64_pk */ false)
        .expect("serialize compressed ipc");
    assert_eq!(
        blob.first().copied(),
        Some(tombstone_format::COMPRESSED_IPC)
    );
    let decoded = deserialize_delete_keys_from_ipc(&blob).expect("decode compressed ipc");
    assert_eq!(decoded, keys);
}

/// cycle-5 TASK 2a: a LEGACY (pre-cycle-5) blob — a bare uncompressed Arrow
/// IPC stream of the `row_key` `BinaryArray`, with NO format-tag prefix — must
/// still decode after an in-place upgrade. Its first byte is the IPC
/// continuation marker `0xFF`, which the deserializer routes to the legacy
/// reader (never colliding with the `0x00`/`0x01` tags).
#[test]
fn test_tombstone_legacy_uncompressed_ipc_still_decodes() {
    let keys: Vec<Box<[u8]>> = [10_i64, 20, 30]
        .iter()
        .map(|pk| pk.to_be_bytes().to_vec().into_boxed_slice())
        .collect();
    // Reproduce the exact pre-cycle-5 encoding: uncompressed Arrow IPC of a
    // single `row_key` BinaryArray, no prefix tag.
    let array = BinaryArray::from_iter_values(keys.iter().map(std::convert::AsRef::as_ref));
    let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
        "row_key",
        DataType::Binary,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("build legacy batch");
    let legacy_blob = serialize_batches_to_ipc(&[batch]).expect("legacy ipc");
    assert_eq!(
        legacy_blob.first().copied(),
        Some(0xFF),
        "a legacy Arrow IPC stream begins with the 0xFF continuation marker"
    );
    let decoded = deserialize_delete_keys_from_ipc(&legacy_blob).expect("decode legacy");
    assert_eq!(decoded, keys);
}

/// cycle-5 TASK 1: the pending tombstone-delta queue applies removals above a
/// base seq, walks only the new suffix, and bounds via its cap.
#[test]
fn test_pending_tombstone_deltas_removal_above_and_cap() {
    let mut q = PendingTombstoneDeltas::default();
    assert_eq!(q.push(100, vec![1, 2], vec![]), 1);
    assert_eq!(q.push(200, vec![3], vec![]), 2);
    assert_eq!(q.push(300, vec![4], vec![]), 3);

    // Removal above base seq 1 => deltas 2 and 3 (pks 3,4), not 1.
    let (map, seq) = q.removal_above(1);
    assert_eq!(seq, 3, "reports the queue's current seq");
    assert_eq!(map.int64_pk.get(&3), Some(&200));
    assert_eq!(map.int64_pk.get(&4), Some(&300));
    assert!(
        !map.int64_pk.contains_key(&1),
        "delta at/below base excluded"
    );
    assert!(!map.int64_pk.contains_key(&2));

    // Above the current seq => empty (no work).
    let (empty, _) = q.removal_above(3);
    assert!(empty.int64_pk.is_empty());

    // drain_through removes the front baked-in deltas, keeps `seq` monotonic.
    q.drain_through(2);
    let (after, seq_after) = q.removal_above(0);
    assert_eq!(seq_after, 3, "seq stays monotonic across a drain");
    assert_eq!(after.int64_pk.get(&4), Some(&300));
    assert!(!after.int64_pk.contains_key(&1), "drained deltas are gone");
    assert!(!after.int64_pk.contains_key(&3));
}

#[test]
fn subset_merge_write_shape_serializes_position_tables_only() {
    // The gate the parallel-compaction change rides on (review-caught:
    // an earlier revision keyed on `is_position_based()` alone, which
    // covers only PK-less tables and would have widened PK tables whose
    // resolved deletion_mode is `position`).
    const CORES: usize = 16;
    const BYTES: u64 = 64 * 1024 * 1024;

    // Position-scoped deletes (either family) => serial single writer.
    assert_eq!(
        subset_merge_write_shape(true, CORES, BYTES),
        (1, None),
        "position tables must keep the serial (1, None) merge shape"
    );
    // Key/no-delete tables => widened, size-estimated parallel shape.
    assert_eq!(
        subset_merge_write_shape(false, CORES, BYTES),
        (CORES, Some(BYTES)),
        "non-position tables must widen to the session partitions with \
             the tier's byte estimate"
    );
    // A zeroed session config must not propagate a 0 cap (defensive
    // parity with `target_partitions().max(1)` call sites elsewhere).
    assert_eq!(
        subset_merge_write_shape(false, 0, BYTES),
        (1, Some(BYTES)),
        "target_partitions == 0 must clamp to a single writer, not zero"
    );
}

#[test]
fn protected_snapshot_size_tier_classifies_by_geometric_ceilings() {
    let base = 8 * 1024 * 1024; // 8 MiB
    let growth = 8;

    // Tier 0: everything at or below the base ceiling.
    assert_eq!(protected_snapshot_size_tier(0, base, growth), 0);
    assert_eq!(protected_snapshot_size_tier(1, base, growth), 0);
    assert_eq!(protected_snapshot_size_tier(base, base, growth), 0);

    // Just over the base ceiling rolls into tier 1.
    assert_eq!(protected_snapshot_size_tier(base + 1, base, growth), 1);
    assert_eq!(protected_snapshot_size_tier(base * growth, base, growth), 1);

    // Just over tier 1's ceiling rolls into tier 2.
    assert_eq!(
        protected_snapshot_size_tier(base * growth + 1, base, growth),
        2
    );
    assert_eq!(
        protected_snapshot_size_tier(base * growth * growth, base, growth),
        2
    );
}

#[test]
fn protected_snapshot_size_tier_handles_degenerate_growth() {
    let base = 8 * 1024 * 1024;
    // growth <= 1 cannot form ceilings: everything collapses to tier 0.
    assert_eq!(protected_snapshot_size_tier(base * 100, base, 1), 0);
    assert_eq!(protected_snapshot_size_tier(base * 100, base, 0), 0);
}

#[test]
fn protected_snapshot_size_tier_saturates_on_overflow() {
    // A near-u64::MAX input must not panic; it maps to the top tier.
    let tier = protected_snapshot_size_tier(u64::MAX, 1, 2);
    assert!(tier >= 63, "expected a high tier, got {tier}");
}

fn sized(id: &str, bytes: u64) -> (String, i64, u64) {
    (id.to_string(), 0, bytes)
}

#[test]
fn select_merge_tier_picks_lowest_tier_with_enough_runs() {
    let base = 8 * 1024 * 1024;
    let growth = 8;
    // Two tier-0 runs (small) and two tier-1 runs (large). With min_runs=2
    // the LOWEST qualifying tier (tier 0) is selected.
    let inputs = vec![
        sized("a", 1024),     // tier 0
        sized("b", 2048),     // tier 0
        sized("c", base * 4), // tier 1
        sized("d", base * 5), // tier 1
    ];
    let selected = select_protected_snapshot_merge_tier(&inputs, 2, 32, base, growth);
    let ids: Vec<&str> = selected.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b"]);
}

#[test]
fn select_merge_tier_returns_empty_when_no_tier_qualifies() {
    let base = 8 * 1024 * 1024;
    let growth = 8;
    // One run per tier — no tier reaches min_runs = 2.
    let inputs = vec![sized("a", 1024), sized("b", base * 4)];
    let selected = select_protected_snapshot_merge_tier(&inputs, 2, 32, base, growth);
    assert!(selected.is_empty());
}

#[test]
fn select_merge_tier_respects_max_width_and_keeps_oldest_first() {
    let base = 8 * 1024 * 1024;
    let growth = 8;
    // Four tier-0 runs; max_width = 2 caps the merge to the two oldest.
    let inputs = vec![
        sized("a", 100),
        sized("b", 200),
        sized("c", 300),
        sized("d", 400),
    ];
    let selected = select_protected_snapshot_merge_tier(&inputs, 2, 2, base, growth);
    let ids: Vec<&str> = selected.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b"]);
}

#[test]
fn select_merge_tier_rejects_degenerate_inputs() {
    let base = 8 * 1024 * 1024;
    let growth = 8;
    // Fewer than two inputs, or a sub-2 floor, can never merge.
    assert!(select_protected_snapshot_merge_tier(&[sized("a", 1)], 2, 32, base, growth).is_empty());
    assert!(
        select_protected_snapshot_merge_tier(&[sized("a", 1), sized("b", 2)], 1, 32, base, growth)
            .is_empty()
    );
}

#[test]
fn protected_snapshot_maintenance_trigger_uses_compaction_count_threshold() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let warning_keys = ParkingMutex::new(BoundedWarningKeys::default());
    let protected_snapshots =
        HashMap::from([("snapshot-1".to_string(), 1), ("snapshot-2".to_string(), 2)]);

    assert_eq!(
        protected_snapshot_maintenance_trigger(
            &warning_keys,
            &protected_snapshots,
            2,
            Some(Duration::from_mins(5)),
            now,
        ),
        Some(SnapshotMaintenanceTrigger::ProtectedSnapshotCount {
            protected_snapshot_count: 2,
            trigger_count: 2,
        })
    );
}

#[test]
fn protected_snapshot_maintenance_trigger_uses_oldest_snapshot_age() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let warning_keys = ParkingMutex::new(BoundedWarningKeys::default());
    let protected_snapshots = HashMap::from([
        (protected_snapshot_id_at_unix_time(900), 1),
        (protected_snapshot_id_at_unix_time(990), 2),
    ]);

    assert_eq!(
        protected_snapshot_maintenance_trigger(
            &warning_keys,
            &protected_snapshots,
            8,
            Some(Duration::from_mins(1)),
            now,
        ),
        Some(SnapshotMaintenanceTrigger::ProtectedSnapshotAge {
            protected_snapshot_count: 2,
            oldest_snapshot_age: Duration::from_secs(100),
            trigger_age: Duration::from_mins(1),
        })
    );
}

#[test]
fn protected_snapshot_maintenance_trigger_ignores_invalid_uuid_for_age() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let warning_keys = ParkingMutex::new(BoundedWarningKeys::default());
    let protected_snapshots = HashMap::from([("not-a-uuid".to_string(), 1)]);

    assert_eq!(
        protected_snapshot_maintenance_trigger(
            &warning_keys,
            &protected_snapshots,
            8,
            Some(Duration::from_mins(1)),
            now,
        ),
        None
    );
}

#[test]
fn protected_snapshot_maintenance_trigger_ignores_future_uuid_for_age() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let warning_keys = ParkingMutex::new(BoundedWarningKeys::default());
    let protected_snapshots = HashMap::from([(protected_snapshot_id_at_unix_time(1_100), 1)]);

    assert_eq!(
        protected_snapshot_maintenance_trigger(
            &warning_keys,
            &protected_snapshots,
            8,
            Some(Duration::from_mins(1)),
            now,
        ),
        None
    );
}

/// End-to-end coverage for the fast protected-snapshot compaction
/// (`compact_protected_snapshots_subset`). The other tests cover only the
/// pure tier-selection math; this exercises the real rewrite + CAS swap +
/// in-memory reconciliation. It builds more than the trigger floor of small
/// (tier-0) protected snapshots via upsert-table inserts, runs the subset
/// compaction, and asserts it (a) merges — reducing the protected-snapshot
/// count, (b) preserves every visible row, and (c) leaves the merged
/// snapshot's in-memory threshold equal to its persisted sequence number
/// (reload-stable: the partial-deletion filter must behave identically
/// before and after a restart).
#[tokio::test]
async fn protected_snapshot_subset_compaction_merges_and_preserves_rows() {
    use arrow::datatypes::{DataType, Field, Schema};

    const TRIGGER: usize = 4;
    let ctx = SessionContext::new();

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");
    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let options = CreateTableOptions {
        table_name: "compact_subset".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        )),
        base_path: data_dir,
        partition_column: None,
        vortex_config: VortexConfig {
            // Disable the inline memtable so each upsert-table write lands in
            // a file-backed protected snapshot (the compaction's domain)
            // instead of being absorbed inline.
            inline_max_rows: 0,
            // Deterministic, low trigger floor so a handful of snapshots merge.
            compaction_trigger_protected_snapshots: TRIGGER,
            // Pin the background compactor far out so only our explicit call
            // runs — the test must not race the 30s background tick.
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    };
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .create(options)
        .await
        .expect("table created");

    let compaction_setup_guard = provider.compaction_lock.lock().await;

    // Each insert into an upsert table publishes a new protected snapshot.
    // Create more than the trigger floor of small (tier-0) snapshots.
    let n = i64::try_from(TRIGGER).expect("TRIGGER fits in i64") + 2;
    for i in 0..n {
        insert_batch(
            &provider,
            id_value_batch(Arc::clone(&schema), &[i], &[i * 10]),
        )
        .await;
    }

    let before = provider.protected_snapshots.load_full().len();
    assert!(
        before >= TRIGGER,
        "expected >= {TRIGGER} protected snapshots before compaction, got {before}"
    );

    let expected: Vec<(i64, i64)> = (0..n).map(|i| (i, i * 10)).collect();
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "compact_subset").await,
        expected,
        "sanity: all inserted rows visible before compaction"
    );

    drop(compaction_setup_guard);

    let merged = provider
        .compact_protected_snapshots_subset(usize::MAX)
        .await
        .expect("compaction should not error");
    assert!(merged, "a tier with >= {TRIGGER} runs should have merged");

    // (a) Inputs replaced by a single merged snapshot → count drops.
    let after = provider.protected_snapshots.load_full().len();
    assert!(
        after < before,
        "compaction must reduce the protected-snapshot count: {before} -> {after}"
    );

    // (b) Every visible row preserved through the merge.
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "compact_subset").await,
        expected,
        "compaction must preserve all visible rows"
    );

    // (c) Reload-stable thresholds: every in-memory protected-snapshot
    // threshold must equal its persisted sequence number, or a scan would
    // return different rows before vs after a restart.
    let in_mem = provider.protected_snapshots.load_full();
    let persisted = catalog
        .get_all_snapshot_sequences(&provider.table_metadata.table_id)
        .await
        .expect("persisted snapshot sequences");
    for (id, threshold) in in_mem.iter() {
        assert_eq!(
            persisted.get(id),
            Some(threshold),
            "protected snapshot {id}: in-memory threshold {threshold} must equal persisted {:?}",
            persisted.get(id),
        );
    }
}

/// Engagement test for the size-aware PARALLEL merge encode: a subset
/// merge whose selected inputs exceed one target file must shard its
/// output across multiple concurrently-encoded files (bounded by the
/// write concurrency), while preserving every visible row. The sibling
/// test above covers the floor: a merge smaller than one target file
/// stays a single output file (read fan-out unchanged).
#[tokio::test]
async fn protected_snapshot_subset_compaction_parallelizes_large_merges() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    const TRIGGER: usize = 4;
    /// Rows per snapshot × ~4 KiB payload ≈ 800 KiB raw per snapshot; six
    /// snapshots ≈ 4.8 MiB raw. The shard gate sizes the fan-out from
    /// ON-DISK bytes (`list_snapshot_files_with_sizes`), so the payload is
    /// pseudo-random (near-incompressible — see [`entropy_payload`]) to
    /// keep on-disk ≈ raw, and the test still does not *assume* a
    /// compression ratio: it measures the merged inputs' on-disk total
    /// and asserts it clears two 1 MiB target files before relying on the
    /// widened path. (A repetitive payload compresses ~6:1 here, leaving
    /// the gate at one shard — the multi-file output would then come from
    /// serial file rolling and the test would pass without exercising the
    /// parallel path at all.)
    const ROWS_PER_SNAPSHOT: i64 = 200;
    const PAYLOAD_BYTES: usize = 4096;
    // Mirrors `target_vortex_file_size_mb: 1` in the fixture config.
    const TARGET_FILE_SIZE_BYTES: u64 = 1024 * 1024;

    let ctx = SessionContext::new();
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    tokio::fs::create_dir_all(&metadata_dir)
        .await
        .expect("metadata dir created");
    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let options = CreateTableOptions {
        table_name: "compact_subset_parallel".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        )),
        base_path: data_dir.clone(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            // 1 MiB target files so the merged tier spans several of them.
            target_vortex_file_size_mb: 1,
            compaction_trigger_protected_snapshots: TRIGGER,
            compaction_background_interval_ms: 3_600_000,
            // The parallel-merge path requires a NON-position deletion
            // mode: the default (`auto`) resolves to `position` for PK
            // tables, which the gate deliberately keeps single-writer
            // (file-path-scoped tombstones). Pin `key` so this test
            // exercises the widened path; the sibling test below pins
            // that position-mode tables stay serial.
            deletion_mode: crate::metadata::DeletionMode::Key,
            ..VortexConfig::default()
        },
    };
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .create(options)
        .await
        .expect("table created");

    let compaction_setup_guard = provider.compaction_lock.lock().await;

    let snapshots = i64::try_from(TRIGGER).expect("TRIGGER fits in i64") + 2;
    let mut expected_rows: usize = 0;
    for snapshot in 0..snapshots {
        let start = snapshot * ROWS_PER_SNAPSHOT;
        let ids: Vec<i64> = (start..start + ROWS_PER_SNAPSHOT).collect();
        let payloads: Vec<String> = ids
            .iter()
            .map(|id| format!("{id:08}_{}", entropy_payload(*id, PAYLOAD_BYTES)))
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
            ],
        )
        .expect("payload batch");
        expected_rows += batch.num_rows();
        insert_batch(&provider, batch).await;
    }

    let before: std::collections::HashSet<String> = provider
        .protected_snapshots
        .load_full()
        .keys()
        .cloned()
        .collect();
    assert!(
        before.len() >= TRIGGER,
        "expected >= {TRIGGER} protected snapshots before compaction"
    );
    // Record each input snapshot's ON-DISK size before compaction (the
    // inputs may be cleaned up after the merge). The shard gate sizes the
    // fan-out from these on-disk bytes, so the engagement precondition
    // below must be pinned against them — not the raw payload bytes.
    let mut on_disk_bytes: std::collections::HashMap<String, u64> =
        std::collections::HashMap::with_capacity(before.len());
    for id in &before {
        let dir = std::path::Path::new(&data_dir)
            .join(&provider.table_metadata.table_id)
            .join(id);
        on_disk_bytes.insert(id.clone(), sum_vortex_file_bytes(&dir).await);
    }
    drop(compaction_setup_guard);

    let merged = provider
        .compact_protected_snapshots_subset(usize::MAX)
        .await
        .expect("compaction should not error");
    assert!(merged, "a tier with >= {TRIGGER} runs should have merged");

    // Find the NEW merged snapshot and count its vortex shard files.
    let after = provider.protected_snapshots.load_full();
    let new_snapshot = after
        .keys()
        .find(|id| !before.contains(*id))
        .expect("the merge must publish a new protected snapshot")
        .clone();

    // Engagement precondition: the snapshots this merge consumed must
    // exceed two target files ON DISK, or `snapshot_shard_count`
    // (`floor(bytes / target_file_size)`, min 1) never earns a second
    // shard and the `shard_files > 1` assertion below tests nothing. If
    // a future encoding change compresses this fixture below the bar,
    // fail HERE with the cause instead of as a mysterious single-file
    // merge.
    let merged_input_bytes: u64 = before
        .iter()
        .filter(|id| !after.contains_key(id.as_str()))
        .map(|id| on_disk_bytes[id])
        .sum();
    assert!(
        merged_input_bytes > 2 * TARGET_FILE_SIZE_BYTES,
        "fixture precondition: merged inputs must exceed two 1 MiB target \
             files on disk to earn >1 encoder shard (got {merged_input_bytes} \
             bytes) — grow ROWS_PER_SNAPSHOT/PAYLOAD_BYTES or make the \
             payload less compressible"
    );
    let snapshot_dir = std::path::Path::new(&data_dir)
        .join(&provider.table_metadata.table_id)
        .join(&new_snapshot);
    // NOTE: >1 files alone does not prove PARALLEL encode (a serial
    // writer also rolls files past the target size) — the widened-shape
    // decision is pinned by the pure `subset_merge_write_shape` unit
    // test. This bounds the fan-out and smoke-tests the multi-file merge
    // output end-to-end.
    let shard_files = count_vortex_files(&snapshot_dir).await;
    assert!(
        shard_files > 1,
        "a merge spanning multiple target files must produce multiple \
             output files (got {shard_files})"
    );
    // Upper bound: at most DEFAULT_WRITE_CONCURRENCY shard writers, each
    // of which may additionally roll its stream at the target file size —
    // so the ceiling is shards + (input bytes ÷ target size) roll-overs.
    // Generous on purpose: it catches pathological per-batch/per-row file
    // explosion without re-deriving the writer's exact roll math.
    let max_expected_files = DEFAULT_WRITE_CONCURRENCY
        + usize::try_from(merged_input_bytes / TARGET_FILE_SIZE_BYTES)
            .expect("file-count bound fits usize");
    assert!(
        shard_files <= max_expected_files,
        "output file count must stay bounded by write concurrency plus \
             target-size roll-overs (got {shard_files} > {max_expected_files})"
    );

    // Every row survives the parallel merge — content, not just count:
    // collect the id column, sort, and compare against the exact expected
    // id set (a pathological regression could drop some ids and duplicate
    // others while keeping the total stable).
    let scan_ctx = SessionContext::new();
    let plan = provider
        .scan(&scan_ctx.state(), Some(&vec![0]), &[], None)
        .await
        .expect("scan plan");
    let batches = datafusion::physical_plan::collect(plan, scan_ctx.task_ctx())
        .await
        .expect("collect rows");
    let mut scanned_ids: Vec<i64> = Vec::with_capacity(expected_rows);
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        scanned_ids.extend(ids.values().iter().copied());
    }
    scanned_ids.sort_unstable();
    let expected_ids: Vec<i64> = (0..snapshots * ROWS_PER_SNAPSHOT).collect();
    assert_eq!(
        scanned_ids, expected_ids,
        "parallel merge must preserve exactly the inserted id set"
    );
}

/// Count `.vortex` files in a snapshot directory (async fs — keeps the
/// tokio worker unblocked in async tests).
async fn count_vortex_files(snapshot_dir: &std::path::Path) -> usize {
    let mut shard_files = 0_usize;
    let mut entries = tokio::fs::read_dir(snapshot_dir)
        .await
        .expect("read merged snapshot dir");
    while let Some(entry) = entries.next_entry().await.expect("dir entry") {
        if entry.path().extension().is_some_and(|ext| ext == "vortex") {
            shard_files += 1;
        }
    }
    shard_files
}

/// Deterministic pseudo-random payload (xorshift64 over a 64-symbol
/// alphabet), near-incompressible so on-disk size tracks raw size — the
/// subset-merge shard gate reads ON-DISK bytes, and a repetitive payload
/// compresses far below the target-file threshold the tests must cross.
/// Seeded per row id so payloads are unique and reproducible.
fn entropy_payload(seed: i64, len: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    // Bit-preserving i64 -> u64; the splitmix-style multiply spreads
    // small sequential ids across the state space, `| 1` avoids the
    // xorshift zero fixed point.
    let mut state = u64::from_le_bytes(seed.to_le_bytes()).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = String::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for byte in state.to_le_bytes() {
            if out.len() >= len {
                break;
            }
            out.push(char::from(ALPHABET[usize::from(byte & 63)]));
        }
    }
    out
}

/// Sum the on-disk bytes of `.vortex` files in a snapshot directory —
/// the same quantity `list_snapshot_files_with_sizes` feeds the shard
/// gate as `total_input_bytes`.
async fn sum_vortex_file_bytes(snapshot_dir: &std::path::Path) -> u64 {
    let mut total = 0_u64;
    let mut entries = tokio::fs::read_dir(snapshot_dir)
        .await
        .expect("read snapshot dir");
    while let Some(entry) = entries.next_entry().await.expect("dir entry") {
        if entry.path().extension().is_some_and(|ext| ext == "vortex") {
            total += entry.metadata().await.expect("file metadata").len();
        }
    }
    total
}

/// Sibling of the parallel-merge engagement test: a PK table left on the
/// DEFAULT deletion mode (`auto` resolves to `position`) must keep the
/// serial single-file merge shape even when the tier spans multiple
/// target files — position tombstones are file-path scoped and the
/// rewrite's bake-in assumes one output sequence. Pins the
/// `serialize_position_deletes || is_position_based()` gate.
#[tokio::test]
async fn protected_snapshot_subset_compaction_keeps_position_mode_serial() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    const TRIGGER: usize = 4;
    const ROWS_PER_SNAPSHOT: i64 = 200;
    const PAYLOAD_BYTES: usize = 2048;

    let ctx = SessionContext::new();
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    tokio::fs::create_dir_all(&metadata_dir)
        .await
        .expect("metadata dir created");
    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let options = CreateTableOptions {
        table_name: "compact_subset_position_serial".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        )),
        base_path: data_dir.clone(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            target_vortex_file_size_mb: 1,
            compaction_trigger_protected_snapshots: TRIGGER,
            compaction_background_interval_ms: 3_600_000,
            // Deliberately NOT overridden: default `auto` resolves to
            // `position` for this PK table — the case that must stay
            // serial.
            ..VortexConfig::default()
        },
    };
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .create(options)
        .await
        .expect("table created");
    assert!(
        provider.should_capture_positions(),
        "fixture must resolve to position mode or this test pins nothing"
    );

    let compaction_setup_guard = provider.compaction_lock.lock().await;
    let snapshots = i64::try_from(TRIGGER).expect("TRIGGER fits in i64") + 2;
    let mut expected_rows: usize = 0;
    for snapshot in 0..snapshots {
        let start = snapshot * ROWS_PER_SNAPSHOT;
        let ids: Vec<i64> = (start..start + ROWS_PER_SNAPSHOT).collect();
        let payloads: Vec<String> = ids
            .iter()
            .map(|id| format!("{id:08}_{}", "p".repeat(PAYLOAD_BYTES)))
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
            ],
        )
        .expect("payload batch");
        expected_rows += batch.num_rows();
        insert_batch(&provider, batch).await;
    }
    let before: std::collections::HashSet<String> = provider
        .protected_snapshots
        .load_full()
        .keys()
        .cloned()
        .collect();
    drop(compaction_setup_guard);

    let merged = provider
        .compact_protected_snapshots_subset(usize::MAX)
        .await
        .expect("compaction should not error");
    assert!(merged, "a tier with >= {TRIGGER} runs should have merged");

    let after = provider.protected_snapshots.load_full();
    let new_snapshot = after
        .keys()
        .find(|id| !before.contains(*id))
        .expect("the merge must publish a new protected snapshot")
        .clone();
    let snapshot_dir = std::path::Path::new(&data_dir)
        .join(&provider.table_metadata.table_id)
        .join(&new_snapshot);
    // File count is NOT asserted == 1: even the serial writer rolls
    // multiple files when the merged output exceeds the target file size
    // (this payload does). The serial-shape decision itself is pinned by
    // the pure `subset_merge_write_shape` unit test; this test pins the
    // end-to-end correctness of a position-mode merge under the corrected
    // gate, with the count only bounded.
    let shard_files = count_vortex_files(&snapshot_dir).await;
    assert!(
        (1..=DEFAULT_WRITE_CONCURRENCY).contains(&shard_files),
        "position-mode merge output file count out of bounds: {shard_files}"
    );

    let scan_ctx = SessionContext::new();
    let plan = provider
        .scan(&scan_ctx.state(), Some(&vec![0]), &[], None)
        .await
        .expect("scan plan");
    let batches = datafusion::physical_plan::collect(plan, scan_ctx.task_ctx())
        .await
        .expect("collect rows");
    let scanned_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        scanned_rows, expected_rows,
        "position-mode merge must preserve every visible row"
    );
}

#[test]
fn pk_deletion_snapshot_is_stable_after_cache_publish() {
    let deletion_snapshot = Arc::new(ArcSwap::from_pointee(RowConverterDeletionSnapshot::empty()));
    let strategy = PkDeletionStrategyWithCache::RowConverterBased {
        deletion_snapshot: Arc::clone(&deletion_snapshot),
        position_deletions: Arc::new(ArcSwap::from_pointee(PositionBitmap::new())),
    };

    deletion_snapshot.store(Arc::new(RowConverterDeletionSnapshot::from_index(
        KeyDeletionIndex::from_map(HashMap::from([(
            Box::<[u8]>::from([42_u8].as_slice()),
            1_i64,
        )])),
    )));

    let scan_snapshot = pk_deletion_snapshot_for_strategy(&strategy);
    assert!(scan_snapshot.has_deletions());

    deletion_snapshot.store(Arc::new(RowConverterDeletionSnapshot::from_index(
        KeyDeletionIndex::from_map(HashMap::from([(
            Box::<[u8]>::from([99_u8].as_slice()),
            2_i64,
        )])),
    )));

    let PkDeletionSnapshot::RowConverterBased { tombstones } = scan_snapshot else {
        panic!("expected row-converter deletion snapshot");
    };
    assert_eq!(
        tombstones.get(&[42_u8]).map(|t| t.delete_sequence),
        Some(1_i64)
    );
    assert_eq!(tombstones.get(&[99_u8]), None);
    assert_eq!(deletion_snapshot.load().tombstones.get(&[42_u8]), None);
    assert_eq!(
        deletion_snapshot
            .load()
            .tombstones
            .get(&[99_u8])
            .map(|t| t.delete_sequence),
        Some(2_i64)
    );
}

#[test]
fn table_statistics_to_df_uses_persisted_vortex_stats() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        true,
    )]));
    let column_stats = ColumnStatistics {
        null_count: datafusion_common::stats::Precision::Exact(1),
        min_value: datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(10))),
        max_value: datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(20))),
        sum_value: datafusion_common::stats::Precision::Absent,
        distinct_count: datafusion_common::stats::Precision::Absent,
        byte_size: datafusion_common::stats::Precision::Absent,
    };
    let stats_set = crate::stats::column_stats_to_stats_set(&column_stats);
    let file_stats = crate::stats::build_file_statistics(vec![stats_set], &schema);
    let statistics_blob =
        crate::stats::serialize_file_statistics(&file_stats).expect("stats should serialize");
    let table_stats = TableStatistics {
        table_id: "table_id".to_string(),
        statistics_blob,
        num_rows: 3,
        ndv_sketches: None,
    };

    let stats = CayenneTableProvider::table_statistics_to_df(&schema, &table_stats)
        .expect("table stats should deserialize");

    assert_eq!(
        stats.num_rows,
        datafusion_common::stats::Precision::Exact(3)
    );
    assert_eq!(stats.column_statistics[0].min_value, column_stats.min_value);
    assert_eq!(stats.column_statistics[0].max_value, column_stats.max_value);
    assert_eq!(
        stats.column_statistics[0].null_count,
        column_stats.null_count
    );
}

#[test]
fn compute_column_stats_uses_typed_min_max_for_int64() {
    let array = Int64Array::from(vec![Some(10), None, Some(-4), Some(7)]);

    let stats = ColumnStatsAccumulator::compute_column_stats(&array);

    assert_eq!(
        stats.null_count,
        datafusion_common::stats::Precision::Exact(1)
    );
    assert_eq!(
        stats.min_value,
        datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(-4)))
    );
    assert_eq!(
        stats.max_value,
        datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(10)))
    );
}

#[test]
fn compute_column_stats_skips_float_nan_values() {
    let array = Float64Array::from(vec![Some(f64::NAN), Some(5.0), None, Some(-2.0)]);

    let stats = ColumnStatsAccumulator::compute_column_stats(&array);

    assert_eq!(
        stats.null_count,
        datafusion_common::stats::Precision::Exact(1)
    );
    assert_eq!(
        stats.min_value,
        datafusion_common::stats::Precision::Exact(ScalarValue::Float64(Some(-2.0)))
    );
    assert_eq!(
        stats.max_value,
        datafusion_common::stats::Precision::Exact(ScalarValue::Float64(Some(5.0)))
    );
}

#[test]
fn compute_column_stats_uses_typed_min_max_for_utf8_view() {
    let array = StringViewArray::from(vec![Some("beta"), Some("alpha"), None]);

    let stats = ColumnStatsAccumulator::compute_column_stats(&array);

    assert_eq!(
        stats.null_count,
        datafusion_common::stats::Precision::Exact(1)
    );
    assert_eq!(
        stats.min_value,
        datafusion_common::stats::Precision::Exact(ScalarValue::Utf8View(Some(
            "alpha".to_string()
        )))
    );
    assert_eq!(
        stats.max_value,
        datafusion_common::stats::Precision::Exact(ScalarValue::Utf8View(Some("beta".to_string())))
    );
}

#[test]
fn statistics_to_inexact_downgrades_exact_values_for_mutable_overlays() {
    let stats = Statistics {
        num_rows: datafusion_common::stats::Precision::Exact(3),
        total_byte_size: datafusion_common::stats::Precision::Exact(24),
        column_statistics: vec![ColumnStatistics {
            null_count: datafusion_common::stats::Precision::Exact(0),
            min_value: datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(1))),
            max_value: datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(3))),
            sum_value: datafusion_common::stats::Precision::Absent,
            distinct_count: datafusion_common::stats::Precision::Exact(3),
            byte_size: datafusion_common::stats::Precision::Exact(24),
        }],
    };

    let stats = CayenneTableProvider::statistics_to_inexact(stats);

    assert_eq!(
        stats.num_rows,
        datafusion_common::stats::Precision::Inexact(3)
    );
    assert_eq!(
        stats.column_statistics[0].min_value,
        datafusion_common::stats::Precision::Inexact(ScalarValue::Int64(Some(1)))
    );
    assert_eq!(
        stats.column_statistics[0].distinct_count,
        datafusion_common::stats::Precision::Inexact(3)
    );
}

#[test]
fn inline_memtable_pressure_is_absent_below_thresholds() {
    let stats = InlinedDataStats {
        record_count: INLINE_FLUSH_MAX_ROWS - 1,
        entry_count: INLINE_FLUSH_MAX_SEGMENTS,
        ipc_bytes: INLINE_FLUSH_MAX_BYTES - 1,
    };

    assert_eq!(inline_memtable_pressure(stats), None);
}

#[test]
fn inline_memtable_pressure_detects_thresholds() {
    assert_eq!(
        inline_memtable_pressure(InlinedDataStats {
            record_count: INLINE_FLUSH_MAX_ROWS,
            ..InlinedDataStats::default()
        }),
        Some(InlineMemtablePressure::Rows)
    );
    assert_eq!(
        inline_memtable_pressure(InlinedDataStats {
            entry_count: INLINE_FLUSH_MAX_SEGMENTS + 1,
            ..InlinedDataStats::default()
        }),
        Some(InlineMemtablePressure::Segments)
    );
    assert_eq!(
        inline_memtable_pressure(InlinedDataStats {
            ipc_bytes: INLINE_FLUSH_MAX_BYTES,
            ..InlinedDataStats::default()
        }),
        Some(InlineMemtablePressure::IpcBytes)
    );
}

/// A `TableProviderFactory` implementation to create new instances of `CayenneTableProvider`.
// Not used outside of tests until https://github.com/spiceai/spiceai/issues/8534 is resolved
#[derive(Debug)]
pub struct CayenneTableProviderFactory {}

#[async_trait]
impl TableProviderFactory for CayenneTableProviderFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> std::result::Result<Arc<dyn TableProvider>, DataFusionError> {
        let metastore_type = cmd
            .options
            .get("cayenne_metastore")
            .map_or("sqlite", String::as_str);

        let metadata_dir =
            cmd.options
                .get("cayenne_metadata_dir")
                .cloned()
                .ok_or(DataFusionError::Execution(
                    "cayenne_metadata_dir option is required".to_string(),
                ))?;

        // Ensure metadata directory exists
        std::fs::create_dir_all(&metadata_dir).map_err(DataFusionError::IoError)?;

        let connection_string = match metastore_type {
            "turso" => format!("libsql://{metadata_dir}/cayenne.db"),
            "sqlite" => format!("sqlite://{metadata_dir}/cayenne.db"),
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported cayenne_metastore type: {metastore_type}"
                )));
            }
        };

        let catalog = async move {
            let catalog = Arc::new(
                CayenneCatalog::new(connection_string)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ) as Arc<dyn MetadataCatalog>;

            catalog
                .init()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok::<Arc<dyn MetadataCatalog>, DataFusionError>(catalog)
        }
        .await?;

        // Support vortex configuration via options: https://github.com/spiceai/spiceai/issues/8533
        let vortex_config = VortexConfig::default();

        // Use file_path if provided as base, otherwise use default: spice_data_base_path() + dataset_name
        let dir_path =
            cmd.options
                .get("cayenne_data_dir")
                .cloned()
                .ok_or(DataFusionError::Execution(
                    "cayenne_metadata_dir option is required".to_string(),
                ))?;

        let table_options = CreateTableOptions {
            table_name: cmd.name.to_string(),
            schema: Arc::clone(cmd.schema.inner()),
            primary_key: vec![], // No PK by default, can be set by caller
            on_conflict: None,   // No on-conflict behavior by default
            base_path: dir_path,
            partition_column: None, // Non-partitioned table
            vortex_config,
        };

        let retention_filters = Vec::new();

        // Create CayenneTableProvider
        let cayenne_table = CayenneTableProvider::create_table_with_retention(
            catalog,
            table_options,
            retention_filters,
            Arc::clone(state.runtime_env()),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(cayenne_table) as Arc<dyn TableProvider>)
    }
}

async fn arrow_cayenne_round_trip(
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    let factory = CayenneTableProviderFactory {};

    let temp_dir = tempfile::tempdir().expect("temp dir created");

    let cmd_options = HashMap::from([
        (
            "cayenne_metadata_dir".to_string(),
            format!(
                "{}/metadata",
                temp_dir.path().to_str().expect("should be str")
            ),
        ),
        (
            "cayenne_data_dir".to_string(),
            format!("{}/data", temp_dir.path().to_str().expect("should be str")),
        ),
    ]);

    let ctx = SessionContext::new();
    let cmd = CreateExternalTable {
        schema: Arc::new(arrow_record.schema().to_dfschema().expect("to df schema")),
        name: table_name.into(),
        location: String::new(),
        file_type: String::new(),
        table_partition_cols: vec![],
        if_not_exists: false,
        or_replace: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: cmd_options,
        constraints: Constraints::default(),
        column_defaults: HashMap::new(),
        temporary: false,
    };
    let table_provider = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let ctx = SessionContext::new();

    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![arrow_record.clone()]],
        arrow_record.schema(),
        None,
    )
    .expect("memory exec created");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), mem_exec, InsertOp::Append)
        .await
        .expect("insert plan created");

    let _ = collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert done");

    ctx.register_table(table_name, table_provider)
        .expect("Table should be registered");
    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");
    let casted_record =
        try_cast_to(record_batch[0].clone(), source_schema).expect("should cast record batch");

    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Cayenne returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
    assert_eq!(casted_record, arrow_record);
}

#[rstest]
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::large_binary(get_arrow_large_binary_record_batch(), "large_binary")]
#[ignore = "Vortex does not support FixedSizeBinary yet. Planned: https://github.com/vortex-data/vortex/issues/2116"]
#[case::fixed_size_binary(get_arrow_fixed_sized_binary_record_batch(), "fixed_size_binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::float16(get_arrow_float16_record_batch(), "float16")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[case::utf8_view(get_arrow_utf8_view_record_batch(), "utf8_view")]
#[case::binary_view(get_arrow_binary_view_record_batch(), "binary_view")]
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[case::date(get_arrow_date_record_batch(), "date")]
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[ignore = "Vortex does not support Interval yet. See: https://github.com/vortex-data/vortex/issues/2116"]
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[ignore = "Vortex does not support Duration yet. Not on roadmap: https://github.com/vortex-data/vortex/issues/2116"]
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[case::list_of_structs(get_arrow_list_of_structs_record_batch(), "list_of_structs")]
#[case::list_of_fixed_size_lists(
    get_arrow_list_of_fixed_size_lists_record_batch(),
    "list_of_fixed_size_lists"
)]
#[case::list_of_lists(get_arrow_list_of_lists_record_batch(), "list_of_lists")]
#[ignore = "Vortex does not support Map yet. Not on roadmap: https://github.com/vortex-data/vortex/issues/2116"]
#[case::map(get_arrow_map_record_batch(), "map")]
#[case::dictionary(get_arrow_dictionary_array_record_batch(), "dictionary")]
#[test_log::test(tokio::test)]
async fn test_arrow_cayenne_roundtrip(
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    arrow_cayenne_round_trip(
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}

/// Helper: build a single-column Int64 `RecordBatch` and the matching `RowConverter`.
fn make_int64_pk_batch(values: &[i64]) -> (RecordBatch, RowConverter) {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, false)]));
    let col = Arc::new(Int64Array::from(values.to_vec()));
    let batch = RecordBatch::try_new(schema, vec![col]).expect("valid batch");
    let converter =
        RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("valid converter");
    (batch, converter)
}

fn single_batch_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

#[tokio::test]
async fn test_process_stream_into_keyset_int64pk_filters_deleted() {
    let (batch, converter) = make_int64_pk_batch(&[1, 2, 3]);

    // Delete pk=2 with del_seq=1
    let deleted_index = DeletionIndex::from_map(HashMap::from([(2_i64, 1_i64)]));
    let strategy = PkDeletionStrategyWithCache::Int64Pk {
        deletion_snapshot: Arc::new(ArcSwap::from_pointee(Int64PkDeletionSnapshot::from_index(
            deleted_index.clone(),
        ))),
        position_deletions: Arc::new(ArcSwap::from_pointee(PositionBitmap::new())),
    };

    let mut keyset = CachedPkKeyset::with_capacity(0);
    let mut row_id_base: i64 = 0;

    CayenneTableProvider::process_stream_into_keyset(
        single_batch_stream(batch),
        &strategy,
        &[0],
        &converter,
        &[0],
        Some(&deleted_index),
        None,
        None, // all deletions apply
        "test_table",
        &mut keyset,
        &mut row_id_base,
    )
    .await
    .expect("process_stream_into_keyset should succeed");

    assert_eq!(keyset.len(), 2, "pk=2 should be filtered out");
    assert_eq!(row_id_base, 3);
}

#[tokio::test]
async fn test_process_stream_into_keyset_threshold_filters_partial() {
    let (batch, converter) = make_int64_pk_batch(&[1, 2, 3]);

    // pk=1 deleted at seq 5, pk=2 deleted at seq 15
    let deleted_index = DeletionIndex::from_map(HashMap::from([(1_i64, 5_i64), (2_i64, 15_i64)]));
    let strategy = PkDeletionStrategyWithCache::Int64Pk {
        deletion_snapshot: Arc::new(ArcSwap::from_pointee(Int64PkDeletionSnapshot::from_index(
            deleted_index.clone(),
        ))),
        position_deletions: Arc::new(ArcSwap::from_pointee(PositionBitmap::new())),
    };

    let mut keyset = CachedPkKeyset::with_capacity(0);
    let mut row_id_base: i64 = 0;

    // threshold=10: only deletions with del_seq > 10 apply
    CayenneTableProvider::process_stream_into_keyset(
        single_batch_stream(batch),
        &strategy,
        &[0],
        &converter,
        &[0],
        Some(&deleted_index),
        None,
        Some(10),
        "test_table",
        &mut keyset,
        &mut row_id_base,
    )
    .await
    .expect("process_stream_into_keyset should succeed");

    // pk=1 (del_seq=5 <= 10) => visible, pk=2 (del_seq=15 > 10) => filtered, pk=3 => visible
    assert_eq!(
        keyset.len(),
        2,
        "only pk=2 should be filtered (del_seq 15 > threshold 10)"
    );
    assert_eq!(row_id_base, 3);
}

#[tokio::test]
async fn test_process_stream_into_keyset_no_deletions() {
    let (batch, converter) = make_int64_pk_batch(&[10, 20, 30]);

    let strategy = PkDeletionStrategyWithCache::empty_int64_pk();

    let mut keyset = CachedPkKeyset::with_capacity(0);
    let mut row_id_base: i64 = 0;

    CayenneTableProvider::process_stream_into_keyset(
        single_batch_stream(batch),
        &strategy,
        &[0],
        &converter,
        &[0],
        None,
        None,
        None,
        "test_table",
        &mut keyset,
        &mut row_id_base,
    )
    .await
    .expect("process_stream_into_keyset should succeed");

    assert_eq!(keyset.len(), 3, "all rows should be in keyset");
    assert_eq!(row_id_base, 3, "row_id_base should advance by batch size");
}

#[test]
fn test_row_key_to_i64_rejects_invalid_length() {
    let err = CayenneTableProvider::row_key_to_i64(&[1, 2, 3], "test_table")
        .expect_err("invalid inlined Int64 key should fail");

    assert!(
        err.to_string().contains("expected 8 bytes"),
        "unexpected error: {err}"
    );
}

fn int64_id_batch(values: &[i64]) -> RecordBatch {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
        .expect("valid batch")
}

/// Compose-trap regression: the in-memory CDC tier's eager merge-on-read
/// filter (`filter_inlined_batch_for_deletions`) must inherit b3's
/// disjoint-skip. A batch whose Int64 PK window is disjoint from the
/// tombstone range passes through UNFILTERED (no per-row probe, no rows
/// removed), even when tombstones exist — while a batch that overlaps the
/// range still has its deleted PK removed (the gate never over-skips).
#[tokio::test]
async fn mem_tier_disjoint_batch_skips_per_row_probe() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let (provider, _temp_dir) = create_cayenne_table_for_sharding(
        "compose_trap",
        Arc::clone(&schema),
        vec![],
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;
    assert!(
        provider.pk_deletion_strategy.is_int64_pk(),
        "single Int64 PK must select the Int64Pk strategy"
    );

    // Tombstones at PKs {100, 200}, delete_sequence = 5 (the RAM-tier map the
    // mem-tier scan path projects into `InlinedDeletionMaps`).
    let mut inlined_deletions = InlinedDeletionMaps::default();
    inlined_deletions.int64_pk.insert(100, 5);
    inlined_deletions.int64_pk.insert(200, 5);

    // DISJOINT batch: PKs {1,2,3} (max 3 < tombstone min 100). The disjoint
    // gate fires and returns the batch unfiltered, all rows kept — at a
    // data_sequence BELOW the tombstones (3 < 5), which the per-row path
    // would NOT have mattered for since none of these PKs are deleted, but
    // the point is the gate skips the probe entirely.
    let disjoint = int64_id_batch(&[1, 2, 3]);
    let out = provider
        .filter_inlined_batch_for_deletions(disjoint, 3, &inlined_deletions)
        .expect("filter ok")
        .expect("disjoint batch is fully kept");
    assert_eq!(
        out.num_rows(),
        3,
        "disjoint batch passes through unfiltered"
    );
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 id col");
    assert_eq!(col.values(), &[1, 2, 3], "disjoint rows are identical");

    // OVERLAPPING batch: PKs {100, 150} at data_sequence = 3. The gate does
    // NOT fire (range overlaps), the per-row probe runs: PK 100 has
    // delete_seq 5 >= 3 ⇒ removed; PK 150 has no tombstone ⇒ kept.
    let overlapping = int64_id_batch(&[100, 150]);
    let out = provider
        .filter_inlined_batch_for_deletions(overlapping, 3, &inlined_deletions)
        .expect("filter ok")
        .expect("one row survives");
    assert_eq!(
        out.num_rows(),
        1,
        "the deleted PK is removed; the live PK is kept"
    );
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 id col");
    assert_eq!(col.values(), &[150], "only the non-deleted PK survives");

    // FRESH overlapping batch: PK 100 re-inserted at data_sequence = 10 (above
    // the tombstone's 5) ⇒ visible (the upsert supersedes the delete).
    let reinsert = int64_id_batch(&[100]);
    let out = provider
        .filter_inlined_batch_for_deletions(reinsert, 10, &inlined_deletions)
        .expect("filter ok")
        .expect("re-inserted row is visible");
    assert_eq!(out.num_rows(), 1, "data_sequence above the delete is kept");
}

/// Collect the sorted `id` column of a full scan, for exactly-once
/// convergence assertions.
async fn scan_sorted_ids(provider: &CayenneTableProvider) -> Vec<i64> {
    use arrow::array::Int64Array;
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), Some(&vec![0]), &[], None)
        .await
        .expect("scan plan");
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("collect rows");
    let mut ids = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        ids.extend(col.values().iter().copied());
    }
    ids.sort_unstable();
    ids
}

/// Crash-recovery exactly-once (correctness item #5, gates promotion past
/// P1): rows appended to the in-memory CDC tier are DISCARDED on a crash
/// before any checkpoint (the slot is the source of truth), and re-applying
/// the same batches converges to the same set with no loss and no duplicate.
///
/// Models the crash by dropping the provider (RAM gone) WITHOUT checkpointing
/// and reopening the table from the same durable metastore — exactly what the
/// runtime does on restart, where the source then re-streams from the slot.
#[tokio::test]
#[expect(
    clippy::items_after_statements,
    reason = "the recording SlotAdvancer is defined inline next to its single use to keep the test self-contained"
)]
async fn mem_tier_crash_before_checkpoint_loses_ram_then_reapply_converges() {
    use arrow::datatypes::{DataType, Field, Schema};

    let runtime_env = SessionContext::new().runtime_env();
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");

    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init catalog");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let vortex_config = VortexConfig {
        // Memory mode — the path under test.
        cdc_durability: crate::metadata::CdcDurability::Memory,
        ..VortexConfig::default()
    };
    let options = CreateTableOptions {
        table_name: "crash_recovery".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir,
        partition_column: None,
        vortex_config,
    };

    // --- First "process lifetime": append to RAM, no checkpoint, then crash.
    {
        let provider =
            CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
                .create(options.clone())
                .await
                .expect("table created");
        assert!(provider.is_cdc_memory_mode(), "memory mode must be active");

        let no_deletions = OnConflictDeletions::default();
        let bytes = int64_id_batch(&[1, 2, 3]).get_array_memory_size() as u64;
        provider
            .append_to_mem_tier(vec![int64_id_batch(&[1, 2, 3])], &no_deletions, bytes, 0)
            .await
            .expect("append epoch 1");
        let bytes2 = int64_id_batch(&[4, 5]).get_array_memory_size() as u64;
        provider
            .append_to_mem_tier(vec![int64_id_batch(&[4, 5])], &no_deletions, bytes2, 0)
            .await
            .expect("append epoch 2");

        // The RAM rows are visible to a scan immediately (read-union).
        assert_eq!(
            scan_sorted_ids(&provider).await,
            vec![1, 2, 3, 4, 5],
            "RAM-tier rows are visible before any checkpoint"
        );
        // CRASH: drop without checkpointing. RAM is lost; nothing durable.
    }

    // --- Restart: reopen the SAME table from the durable metastore.
    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
        .open("crash_recovery")
        .await
        .expect("reopen table");
    // The un-checkpointed RAM tier is GONE — the durable table is empty,
    // proving the slot (not RAM) is the source of truth and a crash loses no
    // MORE than the un-acked tail (which the source re-streams).
    assert_eq!(
        scan_sorted_ids(&reopened).await,
        Vec::<i64>::new(),
        "a crash before checkpoint discards the RAM tier entirely"
    );

    // --- Source re-streams the same batches (exactly-once via re-apply).
    let no_deletions = OnConflictDeletions::default();
    let bytes = int64_id_batch(&[1, 2, 3]).get_array_memory_size() as u64;
    reopened
        .append_to_mem_tier(vec![int64_id_batch(&[1, 2, 3])], &no_deletions, bytes, 0)
        .await
        .expect("re-append epoch 1");
    let bytes2 = int64_id_batch(&[4, 5]).get_array_memory_size() as u64;
    reopened
        .append_to_mem_tier(vec![int64_id_batch(&[4, 5])], &no_deletions, bytes2, 0)
        .await
        .expect("re-append epoch 2");
    // This time the epoch is checkpointed durable via the PERIODIC tick path
    // (`run_mem_tier_checkpoint_tick`), not a manual `checkpoint_mem_tier` —
    // proving the background task that A1 spawns advances durability
    // identically to the manual call. The tick gates on `has_slot_advancer()`,
    // so arm a recording advancer (as the runtime does) and assert it fired
    // with the flushed epoch.
    struct TickRecorder(std::sync::Arc<std::sync::atomic::AtomicU64>);
    #[async_trait::async_trait]
    impl crate::provider::mem_tier::SlotAdvancer for TickRecorder {
        async fn on_checkpoint_durable(&self, durable_epoch: u64) {
            self.0
                .store(durable_epoch, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let durable = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    reopened.install_slot_advancer(std::sync::Arc::new(TickRecorder(std::sync::Arc::clone(
        &durable,
    ))));
    reopened.run_mem_tier_checkpoint_tick().await;
    assert_eq!(
        durable.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the periodic checkpoint tick fired the slot advancer with the flushed epoch"
    );

    // Converges to the exact set — no loss, no duplicate.
    assert_eq!(
        scan_sorted_ids(&reopened).await,
        vec![1, 2, 3, 4, 5],
        "re-applying the same batches converges exactly-once after a durable checkpoint"
    );

    // And it survives a SECOND reopen now that it is durable.
    let reopened2 = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .open("crash_recovery")
        .await
        .expect("reopen after checkpoint");
    assert_eq!(
        scan_sorted_ids(&reopened2).await,
        vec![1, 2, 3, 4, 5],
        "checkpointed rows are durable across restart"
    );
}

/// Build a fresh memory-mode provider (armed-ready) for the periodic-tick /
/// cap tests. Returns the provider and the temp dir (keep alive).
async fn create_memory_mode_table_with_caps(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
    cdc_mem_tier_max_bytes: i64,
    cdc_mem_tier_max_age_ms: u64,
) -> (CayenneTableProvider, tempfile::TempDir) {
    use arrow::datatypes::{DataType, Field, Schema};
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init catalog");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let vortex_config = VortexConfig {
        cdc_durability: crate::metadata::CdcDurability::Memory,
        cdc_mem_tier_max_bytes,
        cdc_mem_tier_max_age_ms,
        ..VortexConfig::default()
    };
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir,
        partition_column: None,
        vortex_config,
    };
    let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
        .create(options)
        .await
        .expect("table created");
    (provider, temp_dir)
}

/// A1-T2 — idle/pure-upsert source: the PERIODIC checkpoint tick advances the
/// deferred slot ack even though no delete/truncate event trigger ever fires.
/// This is the regression guard for the root cause's "0 checkpoints fired on
/// pure upserts" finding: appends accumulate in RAM, then a single
/// `run_mem_tier_checkpoint_tick()` flushes them and fires the advancer.
#[tokio::test]
#[expect(
    clippy::items_after_statements,
    reason = "the recording SlotAdvancer is defined inline next to its single use"
)]
async fn mem_tier_periodic_tick_advances_idle_source_slot() {
    let runtime_env = SessionContext::new().runtime_env();
    // Large caps so the WRITE PATH never spills — only the periodic tick can
    // make this durable, exactly the idle/pure-upsert case under test.
    let (provider, _tmp) =
        create_memory_mode_table_with_caps("idle_periodic", Arc::clone(&runtime_env), i64::MAX, 0)
            .await;
    assert!(provider.is_cdc_memory_mode(), "memory mode active");

    struct Recorder(std::sync::Arc<std::sync::atomic::AtomicU64>);
    #[async_trait::async_trait]
    impl crate::provider::mem_tier::SlotAdvancer for Recorder {
        async fn on_checkpoint_durable(&self, durable_epoch: u64) {
            self.0
                .store(durable_epoch, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let durable = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    provider.install_slot_advancer(std::sync::Arc::new(Recorder(std::sync::Arc::clone(
        &durable,
    ))));

    // A tick on an EMPTY tier is a no-op (no advance, no panic).
    provider.run_mem_tier_checkpoint_tick().await;
    assert_eq!(
        durable.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "an empty tier tick advances nothing"
    );

    // Two pure-upsert bursts (no deletions → no event trigger fires).
    let no_deletions = OnConflictDeletions::default();
    let b1 = int64_id_batch(&[1, 2, 3]);
    let bytes1 = b1.get_array_memory_size() as u64;
    provider
        .append_to_mem_tier(vec![b1], &no_deletions, bytes1, 0)
        .await
        .expect("append epoch 1");
    let b2 = int64_id_batch(&[4, 5]);
    let bytes2 = b2.get_array_memory_size() as u64;
    provider
        .append_to_mem_tier(vec![b2], &no_deletions, bytes2, 0)
        .await
        .expect("append epoch 2");

    // The periodic tick flushes the accumulated tier and advances the slot to
    // the highest flushed epoch (2) — the bug was that nothing ever fired this.
    provider.run_mem_tier_checkpoint_tick().await;
    assert_eq!(
        durable.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the periodic tick advanced the deferred slot ack to the flushed epoch"
    );
    // Rows are durable: a fresh tier (empty) still scans them back.
    assert!(provider.mem_tier.load().is_empty(), "tier flushed empty");
    assert_eq!(
        scan_sorted_ids(&provider).await,
        vec![1, 2, 3, 4, 5],
        "flushed rows are durable and visible after the periodic checkpoint"
    );
}

/// A1 guard — the periodic tick must NOT fire when the table is memory-mode
/// but UNARMED (no slot advancer). An unarmed provider takes the durable
/// write path, so it must never have a RAM tier to flush; a tick on it is a
/// pure no-op (defensive: the tick re-checks `has_slot_advancer()` so a
/// checkpointer spawned at table-open — before the runtime arms — is inert).
#[tokio::test]
async fn mem_tier_periodic_tick_is_noop_when_unarmed() {
    let runtime_env = SessionContext::new().runtime_env();
    let (provider, _tmp) =
        create_memory_mode_table_with_caps("unarmed_periodic", runtime_env, i64::MAX, 0).await;
    assert!(provider.is_cdc_memory_mode(), "memory mode active");
    assert!(!provider.has_slot_advancer(), "not armed");
    // Must not panic and must leave the (empty) tier untouched.
    provider.run_mem_tier_checkpoint_tick().await;
    assert!(provider.mem_tier.load().is_empty());
}

/// A2-T1 — write-path BYTE cap self-fires. With a small `cdc_mem_tier_max_bytes`
/// the cap predicate trips once accumulated bytes cross it, which on the real
/// write path triggers a spill-then-append. Here we drive the predicate +
/// append + checkpoint directly to prove the non-zero default actually bounds
/// the tier (and the checkpoint advances the slot).
#[tokio::test]
#[expect(
    clippy::items_after_statements,
    reason = "the recording SlotAdvancer is defined inline next to its single use"
)]
async fn mem_tier_write_path_byte_cap_self_fires() {
    let runtime_env = SessionContext::new().runtime_env();
    let one_batch_bytes = int64_id_batch(&[1, 2, 3]).get_array_memory_size() as u64;
    // Cap at ~1.5 batches so the SECOND append's would-be size breaches it.
    let cap = one_batch_bytes + one_batch_bytes / 2;
    let (provider, _tmp) = create_memory_mode_table_with_caps(
        "byte_cap",
        runtime_env,
        i64::try_from(cap).expect("cap fits i64"),
        0,
    )
    .await;

    struct Recorder(std::sync::Arc<std::sync::atomic::AtomicU64>);
    #[async_trait::async_trait]
    impl crate::provider::mem_tier::SlotAdvancer for Recorder {
        async fn on_checkpoint_durable(&self, durable_epoch: u64) {
            self.0
                .store(durable_epoch, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let durable = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    provider.install_slot_advancer(std::sync::Arc::new(Recorder(std::sync::Arc::clone(
        &durable,
    ))));

    let no_deletions = OnConflictDeletions::default();
    // First append fits under the cap.
    assert!(
        !provider.mem_tier_per_table_cap_breached(one_batch_bytes),
        "first batch is under the byte cap"
    );
    provider
        .append_to_mem_tier(
            vec![int64_id_batch(&[1, 2, 3])],
            &no_deletions,
            one_batch_bytes,
            0,
        )
        .await
        .expect("append epoch 1");
    // A second batch's would-be cumulative size crosses the cap.
    assert!(
        provider.mem_tier_per_table_cap_breached(one_batch_bytes),
        "second batch breaches the byte cap (the write path would spill first)"
    );
    // Model the write-path spill: checkpoint, then the tier is durable + slot advanced.
    provider.run_mem_tier_checkpoint_tick().await;
    assert_eq!(
        durable.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the spill advanced the deferred slot ack"
    );
    assert!(provider.mem_tier.load().is_empty(), "tier spilled empty");
}

/// A2-T2 — write-path AGE cap fires. With a small `cdc_mem_tier_max_age_ms`,
/// after one append the tier's age crosses the cap and the predicate trips on
/// the next write (even though the byte cap is generous), forcing a spill on a
/// slow-trickle table.
#[tokio::test]
#[expect(
    clippy::items_after_statements,
    reason = "the no-op SlotAdvancer is defined inline next to its single use"
)]
async fn mem_tier_write_path_age_cap_fires() {
    let runtime_env = SessionContext::new().runtime_env();
    // Generous byte cap, tiny age cap.
    let (provider, _tmp) = create_memory_mode_table_with_caps(
        "age_cap",
        runtime_env,
        i64::MAX,
        10, // 10 ms
    )
    .await;
    struct NoopAdvancer;
    #[async_trait::async_trait]
    impl crate::provider::mem_tier::SlotAdvancer for NoopAdvancer {
        async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
    }
    provider.install_slot_advancer(Arc::new(NoopAdvancer));

    let no_deletions = OnConflictDeletions::default();
    let b = int64_id_batch(&[1]);
    let bytes = b.get_array_memory_size() as u64;
    provider
        .append_to_mem_tier(vec![b], &no_deletions, bytes, 0)
        .await
        .expect("append epoch 1");
    // Before the age elapses, a small incoming size is NOT cap-breached.
    assert!(
        !provider.mem_tier_per_table_cap_breached(bytes),
        "fresh tier under both byte and age caps"
    );
    // Wait past the age cap; now the predicate trips purely on age.
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(
        provider.mem_tier_per_table_cap_breached(bytes),
        "the age cap fires once the oldest segment is older than the cap"
    );
}

/// Replayability safety gate (`cdc_durability: memory` is the default): the RAM
/// path must engage ONLY after the runtime arms it via `install_slot_advancer`
/// — which the runtime does only on the first batch whose committer reports
/// `supports_deferral()` (a replayable source). Until armed, a memory-mode
/// provider takes the DURABLE write path, so a non-replayable changes source
/// never buffers un-acked rows in RAM (no crash-loss window). This is the
/// provider-side half of the gate (`is_cdc_memory_mode() && has_slot_advancer()`
/// in the CDC write path).
#[tokio::test]
#[expect(clippy::items_after_statements)]
async fn memory_mode_engages_ram_only_after_slot_advancer_armed() {
    use arrow::datatypes::{DataType, Field, Schema};

    let runtime_env = SessionContext::new().runtime_env();
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init catalog");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let vortex_config = VortexConfig {
        cdc_durability: crate::metadata::CdcDurability::Memory,
        ..VortexConfig::default()
    };
    let options = CreateTableOptions {
        table_name: "gate".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir,
        partition_column: None,
        vortex_config,
    };
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .create(options)
        .await
        .expect("table created");
    let ctx = provider.create_session_context();
    let task_ctx = ctx.task_ctx();

    // Config is memory mode, but the runtime has NOT armed it (no replayable
    // committer seen): the write MUST take the durable path.
    assert!(provider.is_cdc_memory_mode(), "config is memory mode");
    assert!(!provider.has_slot_advancer(), "not armed yet");
    let b1 = int64_id_batch(&[1, 2, 3]);
    let write1 = provider
        .write_cdc_append_stream(
            Box::pin(RecordBatchStreamAdapter::new(
                b1.schema(),
                futures::stream::iter([Ok::<_, datafusion_common::DataFusionError>(b1)]),
            )),
            &task_ctx,
        )
        .await
        .expect("durable write");
    assert_eq!(
        write1.in_memory_epoch(),
        None,
        "an unarmed memory-mode provider must write DURABLE, not to RAM"
    );

    // Arm it (as the runtime does on the first replayable committer); the next
    // write engages the RAM tier.
    struct TestAdvancer;
    #[async_trait::async_trait]
    impl crate::provider::mem_tier::SlotAdvancer for TestAdvancer {
        async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
    }
    provider.install_slot_advancer(Arc::new(TestAdvancer));
    assert!(provider.has_slot_advancer(), "armed");

    let b2 = int64_id_batch(&[4, 5]);
    let write2 = provider
        .write_cdc_append_stream(
            Box::pin(RecordBatchStreamAdapter::new(
                b2.schema(),
                futures::stream::iter([Ok::<_, datafusion_common::DataFusionError>(b2)]),
            )),
            &task_ctx,
        )
        .await
        .expect("ram write");
    assert!(
        write2.in_memory_epoch().is_some(),
        "an armed memory-mode provider engages the RAM tier"
    );
}

/// Helper to create a `CayenneTableProvider` with sort columns configured.
///
/// Returns the provider and the temp dir (must be kept alive for the test duration).
async fn create_sorted_cayenne_table(
    table_name: &str,
    schema: SchemaRef,
    sort_columns: Vec<String>,
    runtime_env: Arc<RuntimeEnv>,
) -> (CayenneTableProvider, tempfile::TempDir) {
    create_cayenne_table_for_sharding(table_name, schema, sort_columns, vec![], runtime_env).await
}

async fn create_cayenne_table_for_sharding(
    table_name: &str,
    schema: SchemaRef,
    sort_columns: Vec<String>,
    primary_key: Vec<String>,
    runtime_env: Arc<RuntimeEnv>,
) -> (CayenneTableProvider, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let metadata_dir = format!(
        "{}/metadata",
        temp_dir.path().to_str().expect("should be str")
    );
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("should be str"));

    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");

    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let vortex_config = VortexConfig {
        sort_columns,
        ..VortexConfig::default()
    };

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key,
        on_conflict: None,
        base_path: data_dir,
        partition_column: None,
        vortex_config,
    };

    let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
        .create(options)
        .await
        .expect("table created");

    (provider, temp_dir)
}

#[tokio::test]
async fn test_write_shard_format_unsorted_round_robin() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "parallel_unsorted_write",
        Arc::clone(&schema),
        vec![],
        ctx.runtime_env(),
    )
    .await;

    // Unsorted, no primary key: the sink shards round-robin across the
    // requested writer count for parallel encode (no key clustering).
    // `estimated_bytes = None` ⇒ unknown size ⇒ full fan-out (prior behavior).
    let tsb = provider.context.target_file_size_bytes();
    assert_eq!(provider.snapshot_shard_count(4, tsb, None), 4);
    let format = provider.write_shard_format(4, tsb, None);
    let write_shard = format
        .write_shard()
        .expect("unsorted multi-writer config should enable write sharding");
    assert_eq!(write_shard.write_concurrency, 4);
    assert!(
        write_shard.shard_key_columns.is_empty(),
        "PK-less tables shard round-robin, with no key columns"
    );
}

#[tokio::test]
async fn test_snapshot_shard_count_sized_writes_use_encode_shard_unit() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_cayenne_table_for_sharding(
        "encode_shard_unit",
        Arc::clone(&schema),
        vec![],
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // 256 MB target file size ⇒ encode-shard unit = target/16 = 16 MiB.
    let tsb = 256 * 1024 * 1024usize;
    let mib = 1024 * 1024u64;
    // The table sets no explicit cayenne_write_concurrency, so the shard
    // count is capped at `snapshot_write_concurrency` = DEFAULT_WRITE_CONCURRENCY
    // (4) clamped to session_target_partitions (8) ⇒ 4.
    // A small exact delta (< one unit) stays a single file.
    assert_eq!(provider.snapshot_shard_count(8, tsb, Some(2 * mib)), 1);
    // A checkpoint-sized flush earns real fan-out: 256 MiB / 16 MiB = 16
    // unit-shards, capped to the write-concurrency ceiling (4).
    assert_eq!(provider.snapshot_shard_count(8, tsb, Some(256 * mib)), 4);
    // Mid-size flush: 48 MiB / 16 MiB = 3 shards (under the cap ⇒ unit-driven).
    assert_eq!(provider.snapshot_shard_count(8, tsb, Some(48 * mib)), 3);
    // A tiny configured target (≤ 16 MiB) keeps the old whole-file unit.
    let small_tsb = 8 * 1024 * 1024usize;
    assert_eq!(
        provider.snapshot_shard_count(8, small_tsb, Some(7 * mib)),
        1
    );
    assert_eq!(
        provider.snapshot_shard_count(8, small_tsb, Some(17 * mib)),
        2
    );
}

#[tokio::test]
async fn test_write_shard_format_keyed_hashes_by_primary_key() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_cayenne_table_for_sharding(
        "parallel_keyed_write",
        Arc::clone(&schema),
        vec![],
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Keyed/upsert table: the sink hashes rows by the primary key so each
    // output file is PK-clustered (tight per-file zone maps).
    // `estimated_bytes = None` ⇒ unknown size ⇒ full fan-out (prior behavior).
    let tsb = provider.context.target_file_size_bytes();
    assert_eq!(provider.snapshot_shard_count(4, tsb, None), 4);
    let format = provider.write_shard_format(4, tsb, None);
    let write_shard = format
        .write_shard()
        .expect("keyed multi-writer config should enable write sharding");
    assert_eq!(write_shard.write_concurrency, 4);
    assert_eq!(write_shard.shard_key_columns, vec!["id".to_string()]);
}

#[tokio::test]
async fn test_write_shard_format_uses_configured_write_concurrency_override() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (mut provider, _temp_dir) = create_sorted_cayenne_table(
        "parallel_write_override",
        Arc::clone(&schema),
        vec![],
        ctx.runtime_env(),
    )
    .await;

    provider.context = CayenneContext::new(
        &VortexConfig {
            write_concurrency: Some(2),
            ..VortexConfig::default()
        },
        ctx.runtime_env(),
        "test",
    );

    // The explicit `write_concurrency` override wins over the session's
    // target-partition count. `estimated_bytes = None` ⇒ full fan-out, so
    // the override (2) is honored unclamped by size.
    let tsb = provider.context.target_file_size_bytes();
    assert_eq!(provider.snapshot_shard_count(4, tsb, None), 2);
    assert_eq!(
        provider
            .write_shard_format(4, tsb, None)
            .write_shard()
            .expect("override should enable write sharding")
            .write_concurrency,
        2
    );
}

#[tokio::test]
async fn test_write_shard_format_sorted_single_writer() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "parallel_sorted_write",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Sorted rewrites must stay on a single writer: sharding a globally
    // sorted stream would scatter its order across files. This holds
    // regardless of the size estimate, so pass a large `estimated_bytes`.
    let tsb = provider.context.target_file_size_bytes();
    let huge = Some(tsb as u64 * 64);
    assert_eq!(provider.snapshot_shard_count(4, tsb, huge), 1);
    assert!(
        provider
            .write_shard_format(4, tsb, huge)
            .write_shard()
            .is_none(),
        "sorted writes fall back to the unsharded base format"
    );
}

#[tokio::test]
async fn test_shard_count_small_write_uses_single_shard() {
    // The core of the size-aware change: a delta smaller than one target
    // file must produce a single output file even though the configured
    // write concurrency is 4. Sharding a tiny CDC delta into N files buys no
    // encode parallelism and only multiplies per-scan read amplification.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "shard_size_small",
        Arc::clone(&schema),
        vec![],
        ctx.runtime_env(),
    )
    .await;

    let tsb = provider.context.target_file_size_bytes();
    // A few KiB — far below one 256 MiB target file.
    let small = Some(4 * 1024);
    assert_eq!(
        provider.snapshot_shard_count(4, tsb, small),
        1,
        "a sub-target-file write must stay a single shard"
    );
    assert!(
        provider
            .write_shard_format(4, tsb, small)
            .write_shard()
            .is_none(),
        "single-shard writes use the unsharded base format (no WriteShardConfig)"
    );
}

#[tokio::test]
async fn test_shard_count_large_write_uses_full_concurrency() {
    // A write far larger than write_concurrency × target_file_size must fan
    // out to the full configured concurrency (capped there, not above).
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "shard_size_large",
        Arc::clone(&schema),
        vec![],
        ctx.runtime_env(),
    )
    .await;

    let tsb = provider.context.target_file_size_bytes();
    // 100 target files' worth of data, with a write concurrency of 4 ⇒
    // clamp to 4.
    let large = Some(tsb as u64 * 100);
    assert_eq!(
        provider.snapshot_shard_count(4, tsb, large),
        4,
        "a write much larger than write_concurrency target files clamps to write_concurrency"
    );
    let format = provider.write_shard_format(4, tsb, large);
    assert_eq!(
        format
            .write_shard()
            .expect("large write should enable sharding")
            .write_concurrency,
        4
    );
}

#[tokio::test]
async fn test_shard_count_boundary_scales_with_target_files() {
    // The shard count tracks the number of *encode-shard units* the write
    // fills, capped at write_concurrency. cycle-2: the unit is target/16
    // (floored at 16 MiB), not the whole target-file size — small CDC deltas
    // stay single-file while checkpoint-scale flushes fan out.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "shard_size_boundary",
        Arc::clone(&schema),
        vec![],
        ctx.runtime_env(),
    )
    .await;

    let tsb = provider.context.target_file_size_bytes();
    // Mirror the production unit formula so this is robust to the default
    // target_file_size: clamp(target/16, min(16 MiB, target), target).
    let target = tsb as u64;
    let unit = (target / 16).clamp((16 * 1024 * 1024u64).min(target), target);

    // < 1 unit ⇒ 1 shard (need to *fill* a unit to earn a second).
    assert_eq!(provider.snapshot_shard_count(8, tsb, Some(unit - 1)), 1);
    // Exactly 1 unit ⇒ 1 shard.
    assert_eq!(provider.snapshot_shard_count(8, tsb, Some(unit)), 1);
    // 3 units' worth ⇒ 3 shards (below the concurrency cap of 4).
    assert_eq!(provider.snapshot_shard_count(8, tsb, Some(unit * 3)), 3);
    // 3.9 units' worth still floors to 3 shards.
    assert_eq!(
        provider.snapshot_shard_count(8, tsb, Some(unit * 3 + unit * 9 / 10)),
        3
    );
    // 12 units' worth, but with no per-table override the default
    // write_concurrency is DEFAULT_WRITE_CONCURRENCY (4), so it clamps to 4.
    assert_eq!(provider.snapshot_shard_count(8, tsb, Some(unit * 12)), 4);
}

#[tokio::test]
async fn test_shard_count_unknown_size_preserves_full_concurrency() {
    // estimated_bytes == None is the explicit "unknown size" fallback: it
    // takes the full write concurrency (not a size-derived count), so opaque
    // streams (compaction-less staged appends, overwrites) shard at the
    // configured write_concurrency. With no per-table override that is the
    // default, DEFAULT_WRITE_CONCURRENCY (4), capped at the session's
    // target_partitions.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "shard_size_unknown",
        Arc::clone(&schema),
        vec![],
        ctx.runtime_env(),
    )
    .await;

    let tsb = provider.context.target_file_size_bytes();
    assert_eq!(provider.snapshot_shard_count(6, tsb, None), 4);
    assert_eq!(
        provider
            .write_shard_format(6, tsb, None)
            .write_shard()
            .expect("unknown-size write keeps full fan-out")
            .write_concurrency,
        4
    );
}

#[tokio::test]
async fn clear_cached_table_statistics_drops_optimizer_cache() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "clear_cached_stats_optimizer_test",
        schema,
        vec![],
        ctx.runtime_env(),
    )
    .await;

    {
        let mut cache = provider.table_statistics.write();
        cache.optimizer = Some(datafusion_common::Statistics::new_unknown(
            &provider.table_metadata.schema,
        ));
    }

    assert!(
        provider.table_statistics.read().optimizer.is_some(),
        "precondition: derived Statistics cache must be seeded"
    );

    provider.clear_cached_table_statistics_unlocked();

    assert!(
        provider.table_statistics.read().optimizer.is_none(),
        "clear must drop the derived Statistics cache"
    );
}

/// Helper to insert a `RecordBatch` into a `CayenneTableProvider`.
async fn insert_batch_with_context(
    ctx: &SessionContext,
    provider: &CayenneTableProvider,
    batch: RecordBatch,
) {
    let schema = batch.schema();

    let mem_exec = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
        .expect("memory exec created");

    let insert_plan = provider
        .insert_into(&ctx.state(), mem_exec, InsertOp::Append)
        .await
        .expect("insert plan created");

    let _ = collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert done");
}

/// Helper to insert a `RecordBatch` into a `CayenneTableProvider`.
async fn insert_batch(provider: &CayenneTableProvider, batch: RecordBatch) {
    let ctx = SessionContext::new();
    insert_batch_with_context(&ctx, provider, batch).await;
}

fn make_listing_parity_batch(schema: SchemaRef, start: i64, row_count: usize) -> RecordBatch {
    let row_count = i64::try_from(row_count).expect("test row count fits in i64");
    let ids = (start..start + row_count).collect::<Vec<_>>();
    let categories = ids
        .iter()
        .map(|id| format!("category_{}", id.rem_euclid(3)))
        .collect::<Vec<_>>();
    let values = ids
        .iter()
        .map(|id| id.saturating_mul(10))
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(categories)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("listing parity test batch is valid")
}

fn file_group_paths(file_groups: &[FileGroup]) -> Vec<Vec<String>> {
    file_groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|file| file.path().to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn file_group_row_counts(file_groups: &[FileGroup]) -> Vec<Vec<DFPrecision<usize>>> {
    file_groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|file| {
                    file.statistics
                        .as_ref()
                        .map_or(DFPrecision::Absent, |statistics| statistics.num_rows)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn collect_value_id_rows(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Vec<(i64, i64)> {
    let batches = collect(plan, ctx.task_ctx())
        .await
        .expect("scan plan should collect");
    let mut rows = Vec::new();

    for batch in batches {
        let value_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("projected value column should be Int64");
        let id_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("projected id column should be Int64");

        rows.extend((0..batch.num_rows()).map(|row| (value_col.value(row), id_col.value(row))));
    }

    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn direct_snapshot_scan_matches_listing_table_scan_behavior() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let config = SessionConfig::new()
        .with_target_partitions(2)
        .set_usize("datafusion.execution.meta_fetch_concurrency", 1);
    let ctx = SessionContext::new_with_config(config);
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "listing_table_parity",
        Arc::clone(&schema),
        vec![],
        ctx.runtime_env(),
    )
    .await;

    let rows_per_file = INLINE_MAX_ROWS + 16;
    for batch_idx in 0..3_usize {
        let start = i64::try_from(batch_idx * rows_per_file).expect("test batch start fits in i64");
        insert_batch_with_context(
            &ctx,
            &provider,
            make_listing_parity_batch(Arc::clone(&schema), start, rows_per_file),
        )
        .await;
    }

    let snapshot_id = provider.get_current_snapshot_id();
    let snapshot_dir_url = CayenneTableProvider::snapshot_dir_url(
        &provider.table_metadata.path,
        &provider.table_metadata.table_id,
        &snapshot_id,
    );
    let listing_table = CayenneTableProvider::create_listing_table_with_config(
        &snapshot_dir_url,
        Arc::clone(&provider.table_metadata.schema),
        provider.context.file_format(),
        &provider.pk_deletion_strategy,
        ctx.state().config(),
    )
    .expect("legacy listing table should be created");

    let table_url = ListingTableUrl::parse(&snapshot_dir_url).expect("snapshot URL parses");
    let options = CayenneTableProvider::create_listing_options(
        provider.context.file_format(),
        &provider.pk_deletion_strategy,
        ctx.state().config(),
    );
    let scan_schema =
        CayenneTableProvider::snapshot_scan_schema(&provider.table_metadata.schema, &options);
    let file_limit = Some(rows_per_file + 1);

    let direct_files = provider
        .list_files_for_snapshot_scan(&SnapshotScanListingRequest {
            state: &ctx.state(),
            table_url: &table_url,
            options: &options,
            partition_filters: &[],
            data_filters: &[],
            snapshot_id: &snapshot_id,
            limit: file_limit,
            scan_schema: Arc::clone(&scan_schema),
        })
        .await
        .expect("direct scan file listing should succeed");
    let listing_files = listing_table
        .list_files_for_scan(&ctx.state(), &[], file_limit)
        .await
        .expect("ListingTable file listing should succeed");

    assert_eq!(
        direct_files.grouped_by_partition,
        listing_files.grouped_by_partition
    );
    assert_eq!(direct_files.statistics, listing_files.statistics);
    assert_eq!(
        file_group_paths(&direct_files.file_groups),
        file_group_paths(&listing_files.file_groups),
        "direct scan planning must preserve ListingTable file grouping"
    );
    assert_eq!(
        file_group_row_counts(&direct_files.file_groups),
        file_group_row_counts(&listing_files.file_groups),
        "direct scan planning must preserve per-file row-count statistics"
    );

    let projection = vec![2, 0];
    let direct_plan = provider
        .create_snapshot_scan_plan(&ctx.state(), &snapshot_id, Some(&projection), &[], None)
        .await
        .expect("direct scan plan should be created");
    let listing_plan = listing_table
        .scan(&ctx.state(), Some(&projection), &[], None)
        .await
        .expect("ListingTable scan plan should be created");

    assert_eq!(direct_plan.schema(), listing_plan.schema());
    assert_eq!(
        direct_plan
            .partition_statistics(None)
            .expect("direct scan plan statistics should be available"),
        listing_plan
            .partition_statistics(None)
            .expect("ListingTable scan plan statistics should be available")
    );
    assert_eq!(
        direct_plan
            .properties()
            .output_partitioning()
            .partition_count(),
        listing_plan
            .properties()
            .output_partitioning()
            .partition_count()
    );
    assert_eq!(
        direct_plan.properties().output_ordering().is_some(),
        listing_plan.properties().output_ordering().is_some()
    );
    assert_eq!(
        collect_value_id_rows(&ctx, direct_plan).await,
        collect_value_id_rows(&ctx, listing_plan).await
    );
}

/// Helper to read all data from a `CayenneTableProvider` as `RecordBatch`es.
async fn read_all(
    ctx: &SessionContext,
    provider: &CayenneTableProvider,
    table_name: &str,
) -> Vec<RecordBatch> {
    ctx.deregister_table(table_name).ok();
    ctx.register_table(table_name, Arc::new(provider.clone_for_write()))
        .expect("table registered");
    let df = ctx
        .sql(&format!("SELECT * FROM {table_name}"))
        .await
        .expect("query created");
    df.collect().await.expect("collect succeeded")
}

/// Phase 2 (bloom fallback) helper: create an int64-PK upsert table with an
/// explicit keyset-cache budget (MB). `pk_keyset_cache_mb = 0` forces the
/// bounded-bloom existence path once any key is recorded, exercising the
/// over-budget fallback deterministically without needing millions of rows.
async fn create_budgeted_upsert_table(
    table_name: &str,
    pk_keyset_cache_mb: usize,
    runtime_env: Arc<RuntimeEnv>,
) -> (CayenneTableProvider, TempDir) {
    use arrow::datatypes::{DataType, Field, Schema};

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");

    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        )),
        base_path: data_dir,
        partition_column: None,
        vortex_config: VortexConfig {
            pk_keyset_cache_mb: Some(pk_keyset_cache_mb),
            ..VortexConfig::default()
        },
    };

    let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
        .create(options)
        .await
        .expect("table created");
    (provider, temp_dir)
}

async fn create_cdc_upsert_table(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    create_cdc_upsert_table_with_vortex_config(
        table_name,
        runtime_env,
        VortexConfig {
            inline_max_rows: 0,
            deletion_mode: crate::metadata::DeletionMode::Key,
            ..VortexConfig::default()
        },
    )
    .await
}

async fn create_cdc_upsert_table_with_vortex_config(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
    vortex_config: VortexConfig,
) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    use arrow::datatypes::{DataType, Field, Schema};

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");

    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        )),
        base_path: data_dir,
        partition_column: None,
        vortex_config,
    };

    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .create(options)
        .await
        .expect("table created");
    (provider, catalog, temp_dir)
}

fn id_value_batch(schema: SchemaRef, ids: &[i64], values: &[i64]) -> RecordBatch {
    use arrow::array::Int64Array;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .expect("id/value batch is valid")
}

fn id_value_batch_for_range(schema: SchemaRef, start_id: i64, rows: i64) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + rows).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
    id_value_batch(schema, &ids, &values)
}

/// Read back all `(id, value)` pairs, sorted by id, for assertion.
async fn collect_id_value_pairs(
    ctx: &SessionContext,
    provider: &CayenneTableProvider,
    table_name: &str,
) -> Vec<(i64, i64)> {
    use arrow::array::Int64Array;
    let batches = read_all(ctx, provider, table_name).await;
    let mut pairs = Vec::new();
    for batch in &batches {
        let id_idx = batch.schema().index_of("id").expect("id column");
        let value_idx = batch.schema().index_of("value").expect("value column");
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let values = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value is Int64");
        for row in 0..batch.num_rows() {
            pairs.push((ids.value(row), values.value(row)));
        }
    }
    pairs.sort_unstable();
    pairs
}

// ----------------------------------------------------------------------
// Lever B2 — in-memory sequence allocator + reserve-ahead persistence.
// ----------------------------------------------------------------------

/// (B2.1) Monotonic across a simulated reopen. Reserve a few interleaved
/// blocks, drop the provider WITHOUT flushing anything extra (a crash with an
/// unused block tail), reopen from the same DB, reserve more. Every new value
/// must be strictly greater than every old one, no value repeats, and the DB
/// high-water after reopen is >= the max old value (the reserve-ahead floor).
#[tokio::test]
async fn test_b2_monotonic_across_simulated_reopen() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_cdc_upsert_table("b2_monotonic_reopen", ctx.runtime_env()).await;
    let table_id = provider.table_metadata.table_id.clone();

    let mut handed_out = Vec::new();
    for count in [1_u32, 2, 3, 1, 2] {
        let first = provider
            .reserve_sequences_local(count)
            .await
            .expect("reserve");
        for offset in 0..i64::from(count) {
            handed_out.push(first + offset);
        }
    }
    let max_pre = *handed_out.iter().max().expect("some handed out");

    // Simulate a crash: drop the provider (in-memory tail is lost) and reopen
    // from the same metastore DB. The new allocator reseeds from the DB row.
    drop(provider);
    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .open("b2_monotonic_reopen")
        .await
        .expect("reopen");

    // The DB high-water must already be >= every value handed out pre-crash
    // (reserve-ahead persists the block before any handout).
    let db_hi = catalog
        .get_sequence_number(&table_id)
        .await
        .expect("db high-water");
    assert!(
        db_hi >= max_pre,
        "DB high-water {db_hi} must be >= max handed-out {max_pre} (reserve-ahead floor)"
    );

    let mut after = Vec::new();
    for count in [1_u32, 3, 2] {
        let first = reopened
            .reserve_sequences_local(count)
            .await
            .expect("reserve after reopen");
        for offset in 0..i64::from(count) {
            after.push(first + offset);
        }
    }

    // No reissue: every post-reopen value strictly above every pre-crash one.
    for v in &after {
        assert!(
            *v > max_pre,
            "post-reopen value {v} must exceed every pre-crash value (max {max_pre})"
        );
    }
    // Global uniqueness across the crash boundary.
    let mut all: Vec<i64> = handed_out.iter().chain(after.iter()).copied().collect();
    let total = all.len();
    all.sort_unstable();
    all.dedup();
    assert_eq!(
        all.len(),
        total,
        "no sequence value may be handed out twice"
    );
}

/// (B2.2) Reserve-ahead exhaustion fires exactly `ceil(total / BLOCK)` DB
/// writes. Each refill bumps the durable `current_sequence_number` by exactly
/// `SEQ_RESERVE_BLOCK`, so after `total` single-unit reservations the DB
/// high-water == `ceil(total / BLOCK) * BLOCK` — i.e. precisely that many
/// refills happened, not one per reservation. Contiguity + strict
/// monotonicity hold across the refill boundary.
#[tokio::test]
async fn test_b2_reserve_ahead_block_exhaustion() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_cdc_upsert_table("b2_block_exhaustion", ctx.runtime_env()).await;
    let table_id = provider.table_metadata.table_id.clone();

    // Cross at least two block boundaries with single-unit reservations.
    let total: i64 = SEQ_RESERVE_BLOCK * 2 + 5;
    let mut handed_out = Vec::with_capacity(usize::try_from(total).expect("fits"));
    for _ in 0..total {
        handed_out.push(provider.reserve_sequences_local(1).await.expect("reserve"));
    }

    // Strictly increasing and contiguous (1..=total) — a brand-new table
    // starts at DB high-water 0, so the first handed-out value is 1.
    for (expected, actual) in (1_i64..).zip(&handed_out) {
        assert_eq!(*actual, expected, "single-unit reservations are dense");
    }

    // Exactly ceil(total / BLOCK) refills, each += BLOCK. (`i64::div_ceil`
    // is still unstable on this toolchain, so compute it by hand; `total`
    // and `SEQ_RESERVE_BLOCK` are both positive here.)
    let expected_refills = (total + SEQ_RESERVE_BLOCK - 1) / SEQ_RESERVE_BLOCK;
    let db_hi = catalog
        .get_sequence_number(&table_id)
        .await
        .expect("db high-water");
    assert_eq!(
        db_hi,
        expected_refills * SEQ_RESERVE_BLOCK,
        "DB high-water proves exactly {expected_refills} refills (not one per reservation)"
    );
}

/// (B2.3) Crash mid-block wastes the unused tail but never reissues. Hand out
/// `k < BLOCK` values, capture the durable DB high-water `H`, reopen, reserve
/// again. The first new value == `H + 1` (the tail `(handed_out_max, H]` is
/// skipped = wasted) and is strictly above every pre-crash value.
#[tokio::test]
async fn test_b2_crash_mid_block_wastes_but_never_reissues() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_cdc_upsert_table("b2_crash_mid_block", ctx.runtime_env()).await;
    let table_id = provider.table_metadata.table_id.clone();

    // One reservation triggers a full-BLOCK refill; hand out only a few.
    let mut handed_out = Vec::new();
    for _ in 0..3 {
        handed_out.push(provider.reserve_sequences_local(1).await.expect("reserve"));
    }
    let max_pre = *handed_out.iter().max().expect("some");
    let durable_hi = catalog
        .get_sequence_number(&table_id)
        .await
        .expect("db high-water");
    assert!(
        durable_hi > max_pre,
        "the block reserved past the handed-out values (tail {durable_hi} > {max_pre})"
    );

    drop(provider);
    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .open("b2_crash_mid_block")
        .await
        .expect("reopen");

    let first_after = reopened
        .reserve_sequences_local(1)
        .await
        .expect("reserve after reopen");
    assert_eq!(
        first_after,
        durable_hi + 1,
        "the unused block tail is wasted; the next value resumes at DB high-water + 1"
    );
    assert!(
        first_after > max_pre,
        "no pre-crash value is reissued ({first_after} > {max_pre})"
    );
}

/// (B2.4) Concurrent bursts get disjoint, strictly-monotone blocks. N tasks
/// each reserve M times on one shared provider; the union has no duplicates,
/// size N*M*count, and is a gap-allowed strictly-increasing set when sorted.
#[tokio::test]
async fn test_b2_concurrent_bursts_disjoint() {
    const TASKS: usize = 8;
    const PER_TASK: usize = 64;

    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_cdc_upsert_table("b2_concurrent_bursts", ctx.runtime_env()).await;
    let provider = Arc::new(provider);

    let mut handles = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let provider = Arc::clone(&provider);
        handles.push(tokio::spawn(async move {
            let mut local = Vec::with_capacity(PER_TASK * 2);
            for i in 0..PER_TASK {
                // Alternate count 1 and 2 to exercise multi-unit blocks too.
                let count = if i % 2 == 0 { 1_u32 } else { 2 };
                let first = provider
                    .reserve_sequences_local(count)
                    .await
                    .expect("reserve in task");
                for offset in 0..i64::from(count) {
                    local.push(first + offset);
                }
            }
            local
        }));
    }

    let mut all = Vec::new();
    for handle in handles {
        all.extend(handle.await.expect("task join"));
    }

    // Per task: floor(PER_TASK/2) reservations of count 1 + ceil(PER_TASK/2)
    // of count 2 (even index -> 1 unit, odd index -> 2 units).
    let units_per_task = PER_TASK / 2 + PER_TASK.div_ceil(2) * 2;
    let expected_len = TASKS * units_per_task;
    assert_eq!(all.len(), expected_len, "every reserved unit accounted for");
    let total = all.len();
    all.sort_unstable();
    all.dedup();
    assert_eq!(
        all.len(),
        total,
        "concurrent bursts must hand out fully disjoint sequence values"
    );
    // Strictly increasing after sort+dedup (already guaranteed by dedup ==
    // total), and each adjacent pair differs by >= 1 (gaps are legal).
    for window in all.windows(2) {
        assert!(window[1] > window[0], "sorted set is strictly increasing");
    }
}

/// (B2.5) I5 regression: the inline-INSERT-with-file-deletion path stamps the
/// inlined row's sequence strictly ABOVE the file `delete_seq`. Both are
/// allocated from the one in-memory allocator as a contiguous block
/// (`delete_seq = base`, `inline_seq = base + 1`). Previously the inline-row
/// seq was derived by a second DB-counter bump inside `commit_inlined_mutation`;
/// this guards the unification onto the allocator.
#[tokio::test]
async fn test_b2_inline_seq_above_delete_seq() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_inline_enabled_upsert_table("b2_inline_above_delete", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    // Seed PK=7 into a FILE (large batch, no conflict yet).
    let seed = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                7,
                70,
                900_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("seed PK=7 into a file");
    if seed.has_pending_finalize() {
        seed.finish().await.expect("finalize seed");
    }

    // SMALL upsert of PK=7: fits the inline window, so it takes the
    // inline-insert path with a file deletion for the prior file-backed PK=7.
    let small = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[7], &[777])),
            &ctx.task_ctx(),
        )
        .await
        .expect("inline upsert of PK=7");
    if small.has_pending_finalize() {
        small.finish().await.expect("finalize inline upsert");
    }

    // The new copy of PK=7 must now live inline (the inline-insert path ran).
    let inlined = catalog
        .get_inlined_data(&table_id)
        .await
        .expect("read inlined data");
    let inline_seq = inlined
        .iter()
        .map(|row| row.sequence_number)
        .max()
        .expect("the inline upsert appended an inlined row");

    // The file `DeleteFile` hiding the prior file-backed PK=7 carries delete_seq.
    let delete_files = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("read delete files");
    let delete_seq = delete_files
        .iter()
        .map(|df| df.sequence_number)
        .max()
        .expect("a DeleteFile was written for the superseded file row");

    assert!(
        inline_seq > delete_seq,
        "I5: inlined-row seq {inline_seq} must be strictly above the file delete_seq {delete_seq}"
    );
    // And both are above every prior sequence (the durable high-water covers them).
    let db_hi = catalog
        .get_sequence_number(&table_id)
        .await
        .expect("db high-water");
    assert!(
        db_hi >= inline_seq,
        "reserve-ahead keeps the DB high-water {db_hi} >= the stamped inline seq {inline_seq}"
    );

    // Observable correctness: only the latest value for PK=7, no resurrected copy.
    let pairs = collect_id_value_pairs(&ctx, &provider, "b2_inline_above_delete").await;
    assert!(
        pairs.contains(&(7, 777)),
        "latest PK=7 visible, got {pairs:?}"
    );
    assert!(
        !pairs.contains(&(7, 70)),
        "old file-backed PK=7 hidden, got {pairs:?}"
    );
}

/// (B2.6) R4 regression: a metastore export captures a
/// `current_sequence_number` that is >= every value handed out — i.e. the
/// reserve-ahead floor keeps a restored snapshot from reissuing sequences.
/// (This would FAIL under a fold-on-commit design that leaves the in-memory
/// counter ahead of the DB row between commits.)
#[tokio::test]
async fn test_b2_export_high_water_not_stale() {
    use crate::metastore::snapshot::SliceValue;

    let ctx = SessionContext::new();
    let (provider, catalog, tmp) =
        create_cdc_upsert_table("b2_export_high_water", ctx.runtime_env()).await;

    // Hand out several values across more than one block boundary, but stop
    // mid-block so the in-memory `next` is strictly below `persisted_hi`.
    let mut max_handed_out = 0_i64;
    for _ in 0..(SEQ_RESERVE_BLOCK + 7) {
        max_handed_out = provider.reserve_sequences_local(1).await.expect("reserve");
    }

    // Export via the same path a cold-start snapshot uses (R4).
    let slice = catalog
        .export_dataset_slice("b2_export_high_water", tmp.path())
        .await
        .expect("export dataset slice");
    let table_rows = slice
        .tables
        .get("cayenne_table")
        .expect("cayenne_table in slice");
    assert_eq!(table_rows.len(), 1, "exactly one table row");
    // `current_sequence_number` is the 11th column (index 10) of
    // `cayenne_table` in `EXPECTED_TABLES` column order.
    let exported_hi = match &table_rows[0][10] {
        SliceValue::Integer(v) => *v,
        other => panic!("current_sequence_number must export as Integer, got {other:?}"),
    };

    assert!(
        exported_hi >= max_handed_out,
        "exported high-water {exported_hi} must be >= max handed-out {max_handed_out} \
             (reserve-ahead keeps the snapshot floor safe)"
    );
}

#[tokio::test]
async fn test_cdc_upsert_returns_pending_finalize_and_defers_visibility() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_cdc_upsert_table("cdc_upsert_pending", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;

    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[100])),
            &ctx.task_ctx(),
        )
        .await
        .expect("cdc upsert write should prepare");

    assert!(
        write.has_pending_finalize(),
        "CDC upsert should return after durable prepare with finalize pending"
    );
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "cdc_upsert_pending").await,
        vec![(1, 10)],
        "replacement rows should not be visible until finalize moves staged files"
    );

    write.finish().await.expect("finalize staged upsert");
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "cdc_upsert_pending").await,
        vec![(1, 100)]
    );
}

#[tokio::test]
#[expect(clippy::items_after_statements)]
async fn test_mem_tier_upsert_tombstones_cover_inline_and_disk_across_checkpoint() {
    let ctx = SessionContext::new();
    let runtime_env = ctx.runtime_env();
    let (provider, catalog, _tmp) = create_cdc_upsert_table_with_vortex_config(
        "mem_tier_cross_tier",
        Arc::clone(&runtime_env),
        VortexConfig {
            cdc_durability: crate::metadata::CdcDurability::Memory,
            deletion_mode: crate::metadata::DeletionMode::Key,
            inline_max_rows: 1024,
            ..VortexConfig::default()
        },
    )
    .await;
    assert!(provider.is_cdc_memory_mode(), "memory mode must be active");
    let schema = Arc::clone(&provider.table_metadata.schema);

    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    provider
        .checkpoint_inlined_data()
        .await
        .expect("move PK=1 from inline to disk tier");
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[2], &[20])).await;
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "mem_tier_cross_tier").await,
        vec![(1, 10), (2, 20)],
        "precondition: one old row is disk-backed and one is metastore-inline"
    );

    // Arm the RAM tier: the runtime installs the slot advancer on the first
    // replayable committer; a direct provider test must arm it explicitly, or
    // the engagement gate (`is_cdc_memory_mode() && has_slot_advancer()`) keeps
    // the durable path and the mem-tier tombstone logic under test never runs.
    struct TestAdvancer;
    #[async_trait::async_trait]
    impl crate::provider::mem_tier::SlotAdvancer for TestAdvancer {
        async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
    }
    provider.install_slot_advancer(Arc::new(TestAdvancer));

    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(
                Arc::clone(&schema),
                &[1, 2, 3],
                &[100, 200, 300],
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("memory-mode CDC upsert should append to RAM");
    assert_eq!(write.in_memory_epoch(), Some(1));
    assert!(
        !write.has_pending_finalize(),
        "memory-mode CDC writes publish through the RAM tier, not staged finalize"
    );

    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "mem_tier_cross_tier").await,
        vec![(1, 100), (2, 200), (3, 300)],
        "RAM tombstones must hide older disk and inline copies before checkpoint"
    );
    assert_eq!(
        query_count_star(&ctx, &provider, "mem_tier_cross_tier").await,
        3,
        "COUNT(*) must see the same exact visible row set before checkpoint"
    );

    assert_eq!(
        provider
            .checkpoint_mem_tier()
            .await
            .expect("checkpoint RAM tier"),
        3,
        "the checkpoint should flush the three visible RAM rows"
    );
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "mem_tier_cross_tier").await,
        vec![(1, 100), (2, 200), (3, 300)],
        "durable checkpoint must not resurrect old disk or inline copies"
    );
    assert_eq!(
        query_count_star(&ctx, &provider, "mem_tier_cross_tier").await,
        3,
        "COUNT(*) must remain exact after checkpoint"
    );

    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .open("mem_tier_cross_tier")
        .await
        .expect("reopen checkpointed memory-mode table");
    assert_eq!(
        collect_id_value_pairs(&ctx, &reopened, "mem_tier_cross_tier").await,
        vec![(1, 100), (2, 200), (3, 300)],
        "checkpointed RAM tombstones and replacement rows must be reload-stable"
    );
    assert_eq!(
        query_count_star(&ctx, &reopened, "mem_tier_cross_tier").await,
        3,
        "reopened disk-tier state must preserve exact COUNT(*)"
    );
}

/// Moonshot lever 1+2 correctness guard: the two-phase `checkpoint_mem_tier`
/// runs the encode + `BEGIN IMMEDIATE` commit OUTSIDE the listing fence and
/// takes the fence only for the in-memory swap. A CDC append that interleaves
/// with that off-fence window must be NEITHER lost (vanish) NOR re-materialized
/// (double-count): `clear_mem_tier_up_to_epoch` preserves any segment whose
/// epoch is above the flushed one, and the fresh `new_snapshot_id` is invisible
/// to readers until the under-fence listing swap. Runs the checkpoint and a
/// fresh-key append concurrently and asserts the exact final row set across
/// the RAM→file boundary and after reopen (durable).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_tier_checkpoint_off_fence_preserves_concurrent_append() {
    let ctx = SessionContext::new();
    let runtime_env = ctx.runtime_env();
    let (provider, catalog, _tmp) = create_cdc_upsert_table_with_vortex_config(
        "mem_ckpt_concurrent",
        Arc::clone(&runtime_env),
        VortexConfig {
            cdc_durability: crate::metadata::CdcDurability::Memory,
            deletion_mode: crate::metadata::DeletionMode::Key,
            inline_max_rows: 1024,
            ..VortexConfig::default()
        },
    )
    .await;
    assert!(provider.is_cdc_memory_mode(), "memory mode must be active");
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Arm the RAM tier (the runtime does this on the first replayable committer).
    struct TestAdvancer;
    #[async_trait::async_trait]
    impl crate::provider::mem_tier::SlotAdvancer for TestAdvancer {
        async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
    }
    provider.install_slot_advancer(Arc::new(TestAdvancer));

    // Append batch A → RAM (epoch 1).
    let write_a = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(
                Arc::clone(&schema),
                &[1, 2, 3],
                &[10, 20, 30],
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("append A to RAM tier");
    assert_eq!(
        write_a.in_memory_epoch(),
        Some(1),
        "batch A lands in RAM epoch 1"
    );

    // Concurrently: checkpoint (flushes epoch 1 with encode+commit OFF the
    // fence) AND append batch B with fresh keys (a higher epoch that must
    // survive `clear_mem_tier_up_to_epoch`).
    let provider = Arc::new(provider);
    let checkpoint_provider = Arc::clone(&provider);
    let append_provider = Arc::clone(&provider);
    let append_schema = Arc::clone(&schema);
    let append_task_ctx = ctx.task_ctx();

    let checkpoint = tokio::spawn(async move { checkpoint_provider.checkpoint_mem_tier().await });
    let append = tokio::spawn(async move {
        append_provider
            .write_cdc_append_stream(
                single_batch_stream(id_value_batch(append_schema, &[4, 5, 6], &[40, 50, 60])),
                &append_task_ctx,
            )
            .await
    });

    checkpoint
        .await
        .expect("join checkpoint task")
        .expect("off-fence checkpoint of epoch 1 succeeds");
    let write_b = append
        .await
        .expect("join append task")
        .expect("concurrent append B succeeds");
    assert!(
        write_b.in_memory_epoch().is_some(),
        "batch B engages the RAM tier (a higher epoch that must survive the clear)"
    );

    // Flush whatever epoch survived the first checkpoint's clear.
    provider
        .checkpoint_mem_tier()
        .await
        .expect("second checkpoint flushes the survivor epoch");

    // No vanish (all six present) AND no double-count (each exactly once).
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "mem_ckpt_concurrent").await,
        vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)],
        "a concurrent append during the off-fence checkpoint must neither vanish nor double-count"
    );
    assert_eq!(
        query_count_star(&ctx, &provider, "mem_ckpt_concurrent").await,
        6,
        "COUNT(*) is exact after the concurrent checkpoint + append"
    );

    // The flushed rows are durable: a reopened table sees the same set.
    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .open("mem_ckpt_concurrent")
        .await
        .expect("reopen checkpointed memory-mode table");
    assert_eq!(
        query_count_star(&ctx, &reopened, "mem_ckpt_concurrent").await,
        6,
        "the off-fence durable checkpoint is reload-stable"
    );
}

/// Stress regression for the heavy-upsert OVER-COUNT the off-fence checkpoint
/// surfaced at SF-100 (customer/stock ended with MORE rows than the source).
/// Repeatedly upserts a FIXED keyspace (so every write after the first round
/// supersedes a prior copy) while interleaving checkpoints, then quiesces and
/// asserts the final durable state has EXACTLY one row per key — no surplus
/// (over-count) and no deficit (vanish). This reproduces the production race
/// (an upsert's tombstone vs. the checkpoint that flushes the row it must hide)
/// deterministically and in-process, without a 40-minute SF-100 run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_tier_checkpoint_no_overcount_under_interleaved_upserts() {
    const KEYS: i64 = 64;
    const ROUNDS: i64 = 40; // 40 upserts per key — heavy supersession

    let ctx = SessionContext::new();
    let runtime_env = ctx.runtime_env();
    let (provider, catalog, _tmp) = create_cdc_upsert_table_with_vortex_config(
        "mem_overcount",
        Arc::clone(&runtime_env),
        VortexConfig {
            cdc_durability: crate::metadata::CdcDurability::Memory,
            deletion_mode: crate::metadata::DeletionMode::Key,
            inline_max_rows: 1024,
            ..VortexConfig::default()
        },
    )
    .await;
    assert!(provider.is_cdc_memory_mode(), "memory mode must be active");
    let schema = Arc::clone(&provider.table_metadata.schema);

    struct TestAdvancer;
    #[async_trait::async_trait]
    impl crate::provider::mem_tier::SlotAdvancer for TestAdvancer {
        async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
    }
    provider.install_slot_advancer(Arc::new(TestAdvancer));
    let provider = Arc::new(provider);

    // Writer: upsert all KEYS each round (value = round, so the last round's
    // value must win). Serialized writes (one provider write lock) but each
    // round's batch interleaves with the concurrent checkpoint loop's off-fence
    // encode window — the exact production interleaving.
    let writer_provider = Arc::clone(&provider);
    let writer_schema = Arc::clone(&schema);
    let writer_ctx = ctx.task_ctx();
    let done = Arc::new(AtomicBool::new(false));
    let writer_done = Arc::clone(&done);
    let writer = tokio::spawn(async move {
        let ids: Vec<i64> = (0..KEYS).collect();
        for round in 0..ROUNDS {
            let values: Vec<i64> = (0..KEYS).map(|_| round).collect();
            let write = writer_provider
                .write_cdc_append_stream(
                    single_batch_stream(id_value_batch(Arc::clone(&writer_schema), &ids, &values)),
                    &writer_ctx,
                )
                .await
                .expect("interleaved upsert round succeeds");
            drop(write); // memory-mode append publishes via the RAM tier; nothing to finalize
            tokio::task::yield_now().await;
        }
        writer_done.store(true, Ordering::Relaxed);
    });

    // Checkpoint loop: flush concurrently with the writer until it finishes.
    let ckpt_provider = Arc::clone(&provider);
    let checkpointer = tokio::spawn(async move {
        let mut ticks = 0u32;
        while !done.load(Ordering::Relaxed) && ticks < 10_000 {
            ckpt_provider
                .checkpoint_mem_tier()
                .await
                .expect("interleaved checkpoint succeeds");
            ticks += 1;
            tokio::task::yield_now().await;
        }
    });

    writer.await.expect("join writer");
    checkpointer.await.expect("join checkpointer");

    // Quiesce: a final checkpoint flushes any survivor segments.
    provider
        .checkpoint_mem_tier()
        .await
        .expect("final quiescent checkpoint");

    // EXACTLY one row per key — no over-count (the SF-100 surplus), no vanish.
    assert_eq!(
        query_count_star(&ctx, &provider, "mem_overcount").await,
        KEYS,
        "interleaved upsert+checkpoint must leave exactly one row per key (no over-count)"
    );
    // And the last round's value won for every key (no stale-copy resurrection).
    let pairs = collect_id_value_pairs(&ctx, &provider, "mem_overcount").await;
    let expected: Vec<(i64, i64)> = (0..KEYS).map(|id| (id, ROUNDS - 1)).collect();
    assert_eq!(
        pairs, expected,
        "every key must show the LAST upserted value, exactly once"
    );

    // Durable + reload-stable.
    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .open("mem_overcount")
        .await
        .expect("reopen");
    assert_eq!(
        query_count_star(&ctx, &reopened, "mem_overcount").await,
        KEYS,
        "reopened durable state has exactly one row per key"
    );
}

#[tokio::test]
async fn test_overlapping_cdc_upserts_see_staged_keys_before_finalize() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_cdc_upsert_table("cdc_upsert_overlap", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    let first = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[100])),
            &ctx.task_ctx(),
        )
        .await
        .expect("first cdc upsert should prepare");
    assert!(first.has_pending_finalize());

    let second = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[200])),
            &ctx.task_ctx(),
        )
        .await
        .expect("second cdc upsert should prepare while first finalize is pending");
    assert!(second.has_pending_finalize());

    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "cdc_upsert_overlap").await,
        Vec::<(i64, i64)>::new(),
        "neither staged protected snapshot should be visible before finalize"
    );

    first.finish().await.expect("finalize first staged upsert");
    second
        .finish()
        .await
        .expect("finalize second staged upsert");

    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "cdc_upsert_overlap").await,
        vec![(1, 200)],
        "the second staged upsert must tombstone the first staged value"
    );
}

#[tokio::test]
async fn test_cdc_upsert_reopen_recovers_prepared_protected_snapshot() {
    let ctx = SessionContext::new();
    let runtime_env = ctx.runtime_env();
    let (provider, catalog, _tmp) =
        create_cdc_upsert_table("cdc_upsert_recovery", Arc::clone(&runtime_env)).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[100])),
            &ctx.task_ctx(),
        )
        .await
        .expect("cdc upsert write should prepare");
    assert!(write.has_pending_finalize());
    drop(write);

    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .open("cdc_upsert_recovery")
        .await
        .expect("reopen should recover staged protected snapshot WAL");

    assert_eq!(
        collect_id_value_pairs(&ctx, &reopened, "cdc_upsert_recovery").await,
        vec![(1, 100)],
        "reopen recovery must make the prepared CDC upsert visible exactly once"
    );
}

/// Regression: an upsert that replaces existing rows must NOT inflate the
/// tracked live `num_rows`. The CDC finalize path nets inserts against the
/// rows the on-conflict resolution supersedes
/// (`live_rows_delta = inserted - superseded`). A regression that forgets
/// the subtraction drifts the cached COUNT(*) up by the conflict count on
/// every upsert, so a long-running upsert workload would report a row count
/// far above the true live set.
#[tokio::test]
async fn test_cdc_upsert_live_row_count_does_not_drift_on_replace() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_cdc_upsert_table("cdc_upsert_rowcount", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    // Initial load: three distinct keys → three live rows.
    provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(
                Arc::clone(&schema),
                &[1, 2, 3],
                &[10, 20, 30],
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("initial cdc load should prepare")
        .finish()
        .await
        .expect("finalize initial load");
    provider
        .flush_pending_maintenance()
        .await
        .expect("flush stats after initial load");

    let initial_num_rows = catalog
        .get_table_statistics(&table_id)
        .await
        .expect("stats query after load")
        .expect("stats present after load")
        .num_rows;
    assert_eq!(initial_num_rows, 3, "three inserts → three live rows");

    // Upsert two of the three keys. Each conflicts with an existing row:
    // two inserts, two supersedes → a net live delta of zero.
    provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1, 2], &[100, 200])),
            &ctx.task_ctx(),
        )
        .await
        .expect("upsert should prepare")
        .finish()
        .await
        .expect("finalize upsert");
    provider
        .flush_pending_maintenance()
        .await
        .expect("flush stats after upsert");

    // Data is correct: replaced values for 1 and 2, original for 3.
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "cdc_upsert_rowcount").await,
        vec![(1, 100), (2, 200), (3, 30)],
        "upsert must replace conflicting rows in place"
    );

    // The tracked live count must stay at 3 — NOT drift to 5. Before the
    // fix, `live_rows_delta` counted the two inserts without subtracting the
    // two superseded rows, leaving `num_rows` at 3 + 2 = 5.
    let after_num_rows = catalog
        .get_table_statistics(&table_id)
        .await
        .expect("stats query after upsert")
        .expect("stats present after upsert")
        .num_rows;
    assert_eq!(
        after_num_rows, 3,
        "upsert replacing two rows must leave the live count at 3, not drift to 5"
    );
}

/// Regression: every protected snapshot's in-memory deletion threshold must
/// equal its persisted `cayenne_snapshot_sequence` value. The partial
/// deletion filter applies deletions with `delete_seq > threshold`, and on
/// restart `load_protected_snapshots` rebuilds the thresholds from the
/// persisted per-snapshot sequence numbers. If the in-memory publish used
/// the live global max delete sequence instead of the snapshot's own
/// sequence, the two would diverge and a scan would return different rows
/// before vs after a reload.
#[tokio::test]
async fn test_cdc_upsert_protected_threshold_matches_persisted_sequence() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_cdc_upsert_table("cdc_upsert_threshold", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1, 2], &[10, 20])),
            &ctx.task_ctx(),
        )
        .await
        .expect("initial cdc load should prepare")
        .finish()
        .await
        .expect("finalize initial load");

    // Upsert both keys — this stages a protected snapshot whose deletion
    // threshold is published from the snapshot's own allocated sequence.
    provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1, 2], &[100, 200])),
            &ctx.task_ctx(),
        )
        .await
        .expect("upsert should prepare")
        .finish()
        .await
        .expect("finalize upsert");

    let in_mem = provider.protected_snapshots.load_full();
    let persisted = catalog
        .get_all_snapshot_sequences(&table_id)
        .await
        .expect("persisted snapshot sequences");

    assert!(
        !in_mem.is_empty(),
        "the staged CDC upsert must register at least one protected snapshot"
    );
    assert_eq!(
        *in_mem, persisted,
        "in-memory protected-snapshot thresholds must equal the persisted \
             per-snapshot sequence numbers (matching load_protected_snapshots), or \
             scans return different rows before vs after a reload"
    );
}

#[tokio::test]
async fn test_compaction_skips_pending_cdc_upsert_finalize() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) = create_cdc_upsert_table_with_vortex_config(
        "cdc_upsert_compaction_pending",
        ctx.runtime_env(),
        VortexConfig {
            target_vortex_file_size_mb: 1,
            compaction_trigger_files: 4,
            compaction_max_levels: 1,
            compaction_max_files_per_pick: 4,
            compaction_background_interval_ms: 0,
            inline_max_rows: 0,
            deletion_mode: crate::metadata::DeletionMode::Key,
            ..VortexConfig::default()
        },
    )
    .await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    let batch_rows = 10_000_i64;
    for batch_idx in 0..4_i64 {
        let start = 1 + batch_idx * batch_rows;
        provider
            .write_cdc_append_stream(
                single_batch_stream(id_value_batch_for_range(
                    Arc::clone(&schema),
                    start,
                    batch_rows,
                )),
                &ctx.task_ctx(),
            )
            .await
            .expect("initial CDC batch should prepare")
            .finish()
            .await
            .expect("finalize initial CDC batch");
    }

    let sequence_count_before_pending = catalog
        .get_all_snapshot_sequences(&table_id)
        .await
        .expect("initial snapshot sequences")
        .len();

    let mut pending = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[100])),
            &ctx.task_ctx(),
        )
        .await
        .expect("cdc upsert should prepare");
    assert!(pending.has_pending_finalize());

    assert_eq!(
        catalog
            .get_all_snapshot_sequences(&table_id)
            .await
            .expect("snapshot sequence persisted for pending CDC upsert")
            .len(),
        sequence_count_before_pending + 1,
        "Stage A must durably register the protected target before finalize"
    );

    let prepared_append = pending
        .prepared_append
        .take()
        .expect("staged CDC upsert should have a prepared append");
    let prepared_on_conflict = pending
        .prepared_on_conflict
        .take()
        .expect("staged CDC upsert should have prepared on-conflict metadata");

    prepared_append
        .apply_under_held_barrier()
        .await
        .expect("apply staged files before protected metadata publish");
    assert!(
        provider.has_inflight_staging_appends(),
        "prepared append must remain inflight until protected metadata is published and finish() runs"
    );

    assert!(
        !CompactionRunner::run_compaction_trigger(&provider)
            .await
            .expect("compaction trigger should skip while protected metadata publish is pending"),
        "compaction must not rewrite after staged files move but before protected metadata publish"
    );
    assert_eq!(
        catalog
            .get_all_snapshot_sequences(&table_id)
            .await
            .expect("pending sequence should survive skipped compaction")
            .len(),
        sequence_count_before_pending + 1,
        "skipped compaction must not bulk-delete a pending protected snapshot sequence"
    );

    provider
        .publish_prepared_on_conflict_deletions(prepared_on_conflict)
        .expect("publish protected metadata");
    assert_eq!(
        prepared_append
            .finish()
            .await
            .expect("finish pending cdc upsert"),
        1
    );
    assert!(
        !provider.has_inflight_staging_appends(),
        "finish should clear the prepared append inflight marker"
    );

    let pairs = collect_id_value_pairs(&ctx, &provider, "cdc_upsert_compaction_pending").await;
    assert_eq!(
        pairs.len(),
        usize::try_from(batch_rows * 4).expect("row count fits usize")
    );
    assert_eq!(pairs[0], (1, 100));
    assert_eq!(pairs[1], (2, 20));

    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .open("cdc_upsert_compaction_pending")
        .await
        .expect("reopen should preserve finalized protected CDC upsert");
    let reopened_pairs =
        collect_id_value_pairs(&ctx, &reopened, "cdc_upsert_compaction_pending").await;
    assert_eq!(
        reopened_pairs.len(),
        usize::try_from(batch_rows * 4).expect("row count fits usize"),
        "pending CDC upsert data must remain reload-stable after compaction skip"
    );
    assert_eq!(reopened_pairs[0], (1, 100));
    assert_eq!(reopened_pairs[1], (2, 20));
}

#[tokio::test]
async fn test_staged_position_deletions_fail_on_row_id_overflow() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_cdc_upsert_table("cdc_upsert_position_overflow", ctx.runtime_env()).await;

    let overflow_row_id = u64::from(u32::MAX) + 1;
    let err = provider
        .write_position_deletion_vectors_for_staged_on_conflict(
            HashMap::from([(
                Arc::<str>::from("snapshot/file.vortex"),
                vec![overflow_row_id],
            )]),
            1,
        )
        .await
        .expect_err("row ids above u32::MAX must fail the staged upsert");

    assert!(
        err.to_string().contains("exceeds u32::MAX"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_pk_bloom_has_no_false_negatives() {
    // A bloom must never report an inserted key as absent — a false negative
    // would drop a real upsert conflict. It should also keep the
    // false-positive rate low for absent keys at a realistic fill.
    let mut bloom = PkBloom::with_byte_budget(1024 * 1024);
    let present: Vec<[u8; 8]> = (0..50_000u64).map(u64::to_be_bytes).collect();
    for key in &present {
        bloom.insert(key);
    }
    for key in &present {
        assert!(
            bloom.maybe_contains(key),
            "bloom must never miss an inserted key"
        );
    }
    let mut false_positives = 0usize;
    for absent in 1_000_000u64..1_010_000 {
        if bloom.maybe_contains(&absent.to_be_bytes()) {
            false_positives += 1;
        }
    }
    assert!(
        false_positives < 1_000,
        "false-positive rate should stay well under 10% (saw {false_positives}/10000)"
    );
}

#[test]
fn test_pk_bloom_sidecar_roundtrip() {
    let mut bloom = PkBloom::with_expected_keys(10_000, 64 * 1024 * 1024);
    let keys: Vec<[u8; 8]> = (0..10_000u64).map(u64::to_be_bytes).collect();
    for key in &keys {
        bloom.insert(key);
    }

    let bytes = serialize_pk_bloom_sidecar(&bloom, "snap-abc-123");
    let (restored, snapshot_id) = deserialize_pk_bloom_sidecar(&bytes).expect("sidecar roundtrips");

    assert_eq!(snapshot_id, "snap-abc-123");
    assert_eq!(restored.bit_mask, bloom.bit_mask);
    assert_eq!(restored.inserted_keys, bloom.inserted_keys);
    for key in &keys {
        assert!(
            restored.maybe_contains(key),
            "a restored bloom must retain every inserted key (no false negatives across persistence)"
        );
    }

    // Truncated / garbage / wrong-magic inputs must fail closed (→ full-scan fallback).
    assert!(deserialize_pk_bloom_sidecar(&bytes[..6]).is_none());
    assert!(deserialize_pk_bloom_sidecar(b"GARBAGE!").is_none());
    assert!(deserialize_pk_bloom_sidecar(&[]).is_none());
}

#[tokio::test]
async fn test_over_budget_upsert_keyset_converts_to_bloom() {
    let ctx = SessionContext::new();
    // Budget 0 => any recorded key exceeds the budget.
    let (provider, _tmp) =
        create_budgeted_upsert_table("bloom_conversion", 0, ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    insert_batch(&provider, id_value_batch(schema, &[1, 2, 3], &[10, 20, 30])).await;

    let guard = provider.pk_keyset_cache.lock();
    assert!(
        matches!(guard.as_ref(), Some(CachedPkIndex::Bloom(_))),
        "an upsert table over its keyset byte budget must cache a bloom, not drop the cache"
    );
}

#[tokio::test]
async fn test_bloom_path_upsert_keeps_latest_per_key() {
    let ctx = SessionContext::new();
    // Budget 0 forces the bloom existence path from batch 2 onward.
    let (provider, _tmp) =
        create_budgeted_upsert_table("bloom_upsert_latest", 0, ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Batch 1 builds the index, then converts to a bloom (budget 0).
    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &[1, 2, 3], &[10, 20, 30]),
    )
    .await;
    // Batch 2 (bloom path): update 2 and 3, insert 4.
    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &[2, 3, 4], &[200, 300, 40]),
    )
    .await;
    // Batch 3 (bloom path): update 1, insert 5.
    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &[1, 5], &[111, 50]),
    )
    .await;

    let pairs = collect_id_value_pairs(&ctx, &provider, "bloom_upsert_latest").await;
    assert_eq!(
        pairs,
        vec![(1, 111), (2, 200), (3, 300), (4, 40), (5, 50)],
        "bloom-path upserts must keep exactly one latest row per key (no drops, no duplicates)"
    );
}

#[tokio::test]
async fn test_persisted_bloom_loaded_on_reopen_preserves_correctness() {
    use arrow::datatypes::{DataType, Field, Schema};

    let ctx = SessionContext::new();
    let runtime_env = ctx.runtime_env();
    // Shared catalog + data dir so we can reopen the same table (restart sim).
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");
    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let options = CreateTableOptions {
        table_name: "bloom_restart".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        )),
        base_path: data_dir,
        partition_column: None,
        vortex_config: VortexConfig {
            pk_keyset_cache_mb: Some(0),
            ..VortexConfig::default()
        },
    };
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
        .create(options)
        .await
        .expect("table created");

    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &[1, 2, 3], &[10, 20, 30]),
    )
    .await;
    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &[2, 4], &[200, 40]),
    )
    .await;

    // Force a compaction, which persists the PK-index bloom checkpoint.
    provider
        .rewrite_current_snapshot_for_compaction()
        .await
        .expect("compaction rewrite");

    assert!(
        catalog
            .get_pk_index(&provider.table_metadata.table_id)
            .await
            .expect("query pk index")
            .is_some(),
        "compaction must persist the PK-index bloom checkpoint to the metastore"
    );

    // Simulate a restart: open a fresh provider (empty cache) over the same
    // catalog + data directory.
    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
        .open("bloom_restart")
        .await
        .expect("reopen table");

    // The cold path must reconstruct the index from the sidecar (a Bloom),
    // not a full keyset scan.
    let pk_indices = reopened
        .primary_key_indices()
        .expect("pk indices")
        .expect("table has a primary key");
    let converter = reopened.build_pk_converter(&pk_indices).expect("converter");
    let loaded = reopened
        .try_load_persisted_pk_index(&pk_indices, &converter)
        .await
        .expect("load persisted index");
    assert!(
        matches!(loaded, Some(CachedPkIndex::Bloom(_))),
        "reopen must load the persisted bloom checkpoint instead of a full scan"
    );

    // An upsert after the checkpoint-accelerated reopen must remain correct.
    insert_batch(
        &reopened,
        id_value_batch(Arc::clone(&schema), &[1, 5], &[111, 50]),
    )
    .await;
    let pairs = collect_id_value_pairs(&ctx, &reopened, "bloom_restart").await;
    assert_eq!(
        pairs,
        vec![(1, 111), (2, 200), (3, 30), (4, 40), (5, 50)],
        "upserts after a checkpoint-accelerated reopen must stay correct (no drops/duplicates)"
    );
}

#[tokio::test]
async fn test_sort_and_rewrite_data_sorts_by_column() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "sort_rewrite_test",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Insert data in deliberately unsorted order across multiple batches
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![30, 10, 50])),
            Arc::new(Int64Array::from(vec![300, 100, 500])),
        ],
    )
    .expect("valid batch");
    insert_batch(&provider, batch1).await;

    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![20, 40])),
            Arc::new(Int64Array::from(vec![200, 400])),
        ],
    )
    .expect("valid batch");
    insert_batch(&provider, batch2).await;

    // Verify data is present but unsorted before rewrite
    let before = read_all(&ctx, &provider, "sort_rewrite_test").await;
    let total_rows_before: usize = before.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows_before, 5, "should have 5 rows before sort");

    // Sort and rewrite
    provider
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort_and_rewrite_data should succeed");

    // Read back and verify data is sorted by "id" ascending
    let after = read_all(&ctx, &provider, "sort_rewrite_test").await;
    let total_rows_after: usize = after.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows_after, 5, "should still have 5 rows after sort");

    // Collect all id values in order
    let mut all_ids: Vec<i64> = Vec::new();
    let mut all_values: Vec<i64> = Vec::new();
    for batch in &after {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        let val_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column should be Int64");
        for i in 0..batch.num_rows() {
            all_ids.push(id_col.value(i));
            all_values.push(val_col.value(i));
        }
    }

    assert_eq!(
        all_ids,
        vec![10, 20, 30, 40, 50],
        "ids should be sorted ascending"
    );
    assert_eq!(
        all_values,
        vec![100, 200, 300, 400, 500],
        "values should follow their corresponding ids"
    );
}

#[tokio::test]
async fn test_sort_and_rewrite_data_empty_table() {
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "sort_empty_test",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Sort and rewrite on empty table should succeed without error
    provider
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort_and_rewrite_data on empty table should succeed");

    let after = read_all(&ctx, &provider, "sort_empty_test").await;
    let total_rows: usize = after.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 0, "empty table should remain empty after sort");
}

#[tokio::test]
async fn test_sort_and_rewrite_data_preserves_all_rows() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let (provider, _temp_dir) = create_sorted_cayenne_table(
        "sort_preserve_test",
        Arc::clone(&schema),
        vec!["ts".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Insert multiple batches with overlapping timestamp ranges
    for i in (0..5).rev() {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![i * 10 + 5, i * 10])),
                Arc::new(StringArray::from(vec![
                    format!("row_{}", i * 10 + 5),
                    format!("row_{}", i * 10),
                ])),
            ],
        )
        .expect("valid batch");
        insert_batch(&provider, batch).await;
    }

    // Sort and rewrite
    provider
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort_and_rewrite_data should succeed");

    // Read back and verify all 10 rows are present and sorted
    let after = read_all(&ctx, &provider, "sort_preserve_test").await;
    let total_rows: usize = after.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 10, "all 10 rows should be preserved");

    let mut all_ts: Vec<i64> = Vec::new();
    let mut all_names: Vec<String> = Vec::new();
    for batch in &after {
        let ts_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ts column should be Int64");
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column should be Utf8");
        for i in 0..batch.num_rows() {
            all_ts.push(ts_col.value(i));
            all_names.push(name_col.value(i).to_string());
        }
    }

    // Verify sorted by ts ascending
    let expected_ts: Vec<i64> = (0..10).map(|i| i * 5).collect();
    assert_eq!(all_ts, expected_ts, "timestamps should be sorted ascending");

    // Verify each name corresponds to its timestamp
    for (ts, name) in all_ts.iter().zip(all_names.iter()) {
        assert_eq!(
            name,
            &format!("row_{ts}"),
            "name should match its timestamp"
        );
    }
}

// ========================================================================
// Issue #10125 §6.4 — listing_fence regression guards
// ========================================================================
//
// These tests pin the fence semantics that scan() relies on. They access
// the private `listing_fence` field directly, so they must live in this
// module rather than in an integration test crate.
//
// Property under test: `scan()` holds `listing_fence.read()` across the
// inner DataFusion listing call, and `refresh_listing_table` /
// `update_listing_table_for_snapshot` hold `listing_fence.write()` across
// the ArcSwap store. Any reader/writer overlap is therefore serialized by
// the fence.

/// A held `listing_fence` read guard blocks an attempted write fence
/// acquisition until the read guard is dropped.
///
/// This is the load-bearing guarantee for the append-side coordinator
/// (future work): with the read guard held by an in-flight scan, a
/// writer's `apply_under_barrier` (which is the future code path that
/// will replace `refresh_listing_table` for cross-partition commits) is
/// fenced out.
#[tokio::test]
async fn read_fence_blocks_write_fence_acquisition() {
    let temp_dir = tempfile::TempDir::new().expect("create tempdir");
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("create data dir");

    let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("create catalog"));
    catalog.init().await.expect("init catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let options = CreateTableOptions {
        table_name: "fence_test".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig::default(),
    };
    let runtime_env = SessionContext::new().runtime_env();
    let catalog_dyn: Arc<dyn MetadataCatalog> = Arc::clone(&catalog) as Arc<dyn MetadataCatalog>;
    let table = CayenneTableProvider::create_table(catalog_dyn, options, runtime_env)
        .await
        .expect("create table");

    // Take the read fence — this models an in-flight scan().
    let fence_arc = Arc::clone(&table.listing_fence);
    let read_guard = fence_arc.read().await;

    // Spawn a refresh: it must block on the write fence until we drop the
    // read guard. (Cloning via clone_for_write shares the same fence.)
    let table_for_writer = table.clone_for_write();
    let writer = tokio::spawn(async move { table_for_writer.refresh_listing_table().await });

    // Within a generous slice, the writer is still pending.
    match tokio::time::timeout(std::time::Duration::from_millis(50), writer).await {
        Err(_) => {
            // Timeout — expected. Drop the read guard and verify the
            // writer can now make progress.
            drop(read_guard);
            let table_for_writer = table.clone_for_write();
            let writer =
                tokio::spawn(async move { table_for_writer.refresh_listing_table().await });
            tokio::time::timeout(std::time::Duration::from_secs(5), writer)
                .await
                .expect("refresh completes once the read fence is released")
                .expect("spawned task did not panic")
                .expect("refresh_listing_table returned Ok");
        }
        Ok(completed) => {
            panic!("refresh_listing_table completed despite held read fence: {completed:?}");
        }
    }
}

/// A held `listing_fence` write guard blocks reader-side fence
/// acquisitions. Pairs with the previous test: under contention the fence
/// is bidirectional, so concurrent scans and the writer barrier always
/// observe consistent state.
#[tokio::test]
async fn write_fence_blocks_read_fence_acquisition() {
    // Pure fence-primitive test — no need to construct a full
    // CayenneTableProvider, since the field is just
    // `Arc<tokio::sync::RwLock<()>>`.
    let fence: Arc<tokio::sync::RwLock<()>> = Arc::new(tokio::sync::RwLock::new(()));

    let write_guard = fence.write().await;

    let fence_for_reader = Arc::clone(&fence);
    let reader = tokio::spawn(async move {
        let _read = fence_for_reader.read().await;
    });

    match tokio::time::timeout(std::time::Duration::from_millis(50), reader).await {
        Err(_) => {
            // Expected: reader blocked. Release the writer and ensure the
            // reader can now proceed.
            drop(write_guard);
            let fence_for_reader = Arc::clone(&fence);
            let reader = tokio::spawn(async move {
                let _read = fence_for_reader.read().await;
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), reader)
                .await
                .expect("read fence acquires once writer is released")
                .expect("spawned task did not panic");
        }
        Ok(completed) => panic!("read fence acquired despite held write fence: {completed:?}"),
    }
}

// =================================
// UUID7 snapshot timestamp parsing
// =================================

#[test]
fn uuid7_snapshot_timestamp_is_extractable_and_ordered() {
    // Simulate two snapshot IDs created at different times via Uuid::now_v7().
    let older = uuid::Uuid::now_v7();
    // Advance the embedded timestamp by creating a second UUID slightly later.
    std::thread::sleep(std::time::Duration::from_millis(10));
    let newer = uuid::Uuid::now_v7();

    let ts_older = older
        .get_timestamp()
        .expect("UUID v7 should have an extractable timestamp")
        .to_unix();
    let ts_newer = newer
        .get_timestamp()
        .expect("UUID v7 should have an extractable timestamp")
        .to_unix();

    assert!(
        ts_older <= ts_newer,
        "older UUID7 timestamp must be <= newer UUID7 timestamp"
    );

    // Verify round-trip through string representation (as used by cleanup).
    let older_str = older.to_string();
    let newer_str = newer.to_string();

    let parsed_older_ts = uuid::Uuid::parse_str(&older_str)
        .expect("valid UUID string")
        .get_timestamp()
        .expect("parsed UUID v7 should yield a timestamp")
        .to_unix();
    let parsed_newer_ts = uuid::Uuid::parse_str(&newer_str)
        .expect("valid UUID string")
        .get_timestamp()
        .expect("parsed UUID v7 should yield a timestamp")
        .to_unix();

    assert!(
        parsed_older_ts <= parsed_newer_ts,
        "timestamp ordering must survive string round-trip"
    );
}

#[test]
fn cleanup_skips_snapshots_newer_than_current() {
    let tmp = TempDir::new().expect("create temp dir");
    let table_path = tmp.path().to_str().expect("valid UTF-8 path");
    let table_id = uuid::Uuid::now_v7().to_string();

    // Create the table directory.
    let table_dir = tmp.path().join(&table_id);
    std::fs::create_dir_all(&table_dir).expect("create table dir");

    // Create 3 snapshot directories:
    // - old_snapshot (older than current) → should be deleted
    // - current_snapshot → should be kept
    // - newer_snapshot (newer than current, simulating in-flight write) → should be kept
    let old_snapshot = uuid::Uuid::now_v7().to_string();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let current_snapshot = uuid::Uuid::now_v7().to_string();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let newer_snapshot = uuid::Uuid::now_v7().to_string();

    std::fs::create_dir(table_dir.join(&old_snapshot)).expect("create old snapshot dir");
    std::fs::create_dir(table_dir.join(&current_snapshot)).expect("create current dir");
    std::fs::create_dir(table_dir.join(&newer_snapshot)).expect("create newer dir");

    let protected: HashSet<String> = HashSet::new();

    CayenneTableProvider::cleanup_old_snapshots_blocking(
        table_path,
        &table_id,
        &current_snapshot,
        &protected,
    )
    .expect("cleanup should succeed");

    // old_snapshot should be deleted
    assert!(
        !table_dir.join(&old_snapshot).exists(),
        "old snapshot should be deleted"
    );
    // current_snapshot should be kept
    assert!(
        table_dir.join(&current_snapshot).exists(),
        "current snapshot must be preserved"
    );
    // newer_snapshot should be kept (in-flight write protection)
    assert!(
        table_dir.join(&newer_snapshot).exists(),
        "snapshot newer than current must be preserved (in-flight write)"
    );
}

fn col(name: &str) -> Expr {
    Expr::Column(datafusion_common::Column::new_unqualified(name))
}

fn lit_i64(v: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(Some(v)), None)
}

#[test]
fn pk_eq_literal_simple() {
    let expr = col("id").eq(lit_i64(42));
    assert!(pk_column_equals_literal(&expr, "id"));
}

#[test]
fn pk_eq_literal_flipped() {
    let expr = lit_i64(42).eq(col("id"));
    assert!(pk_column_equals_literal(&expr, "id"));
}

#[test]
fn pk_eq_with_type_coerced_literal() {
    let casted = Expr::Cast(datafusion_expr::Cast::new(
        Box::new(lit_i64(42)),
        datafusion::arrow::datatypes::DataType::Int64,
    ));
    let expr = col("id").eq(casted);
    assert!(pk_column_equals_literal(&expr, "id"));
}

#[test]
fn pk_eq_with_casted_column() {
    let casted = Expr::Cast(datafusion_expr::Cast::new(
        Box::new(col("id")),
        datafusion::arrow::datatypes::DataType::Int64,
    ));
    let expr = casted.eq(lit_i64(42));
    assert!(pk_column_equals_literal(&expr, "id"));
}

#[test]
fn pk_eq_inside_conjunction() {
    let expr = col("id").eq(lit_i64(42)).and(col("name").eq(lit_i64(5)));
    assert!(pk_column_equals_literal(&expr, "id"));
}

#[test]
fn non_pk_eq_rejected() {
    let expr = col("name").eq(lit_i64(42));
    assert!(!pk_column_equals_literal(&expr, "id"));
}

#[test]
fn pk_range_rejected() {
    let expr = col("id").gt(lit_i64(42));
    assert!(!pk_column_equals_literal(&expr, "id"));
}

#[test]
fn pk_selective_small_inlist() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        vec![lit_i64(1), lit_i64(2), lit_i64(3)],
        false,
    ));
    assert!(pk_selective_in_or_range(&in_list, "id"));
}

#[test]
fn pk_selective_large_inlist_rejected() {
    let values: Vec<Expr> = (0..64).map(lit_i64).collect();
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        values,
        false,
    ));
    assert!(!pk_selective_in_or_range(&in_list, "id"));
}

#[test]
fn pk_selective_tight_between() {
    assert!(pk_selective_in_or_range(&between_int("id", 10, 20), "id"));
}

#[test]
fn pk_selective_wide_between_rejected() {
    assert!(!pk_selective_in_or_range(
        &between_int("id", 1, 10_000),
        "id"
    ));
}

#[test]
fn pk_eq_other_column_rejected() {
    let expr = col("id").eq(col("other_id"));
    assert!(!pk_column_equals_literal(&expr, "id"));
}

fn between_int(name: &str, lo: i64, hi: i64) -> Expr {
    Expr::Between(datafusion_expr::expr::Between::new(
        Box::new(col(name)),
        false,
        Box::new(lit_i64(lo)),
        Box::new(lit_i64(hi)),
    ))
}

#[test]
fn rewrites_consecutive_inlist_to_between() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        vec![lit_i64(5), lit_i64(6), lit_i64(7), lit_i64(8)],
        false,
    ));
    let rewritten = rewrite_consecutive_inlist_to_range(in_list);
    assert_eq!(rewritten, between_int("id", 5, 8));
}

#[test]
fn rewrites_consecutive_inlist_out_of_order() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        vec![lit_i64(8), lit_i64(5), lit_i64(7), lit_i64(6)],
        false,
    ));
    let rewritten = rewrite_consecutive_inlist_to_range(in_list);
    assert_eq!(rewritten, between_int("id", 5, 8));
}

#[test]
fn leaves_sparse_inlist_unchanged() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        vec![lit_i64(1), lit_i64(100), lit_i64(1000), lit_i64(1001)],
        false,
    ));
    let rewritten = rewrite_consecutive_inlist_to_range(in_list.clone());
    assert_eq!(rewritten, in_list);
}

#[test]
fn leaves_short_consecutive_inlist_unchanged() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        vec![lit_i64(5), lit_i64(6), lit_i64(7)],
        false,
    ));
    let rewritten = rewrite_consecutive_inlist_to_range(in_list.clone());
    assert_eq!(rewritten, in_list);
}

#[test]
fn leaves_negated_inlist_unchanged() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        vec![lit_i64(5), lit_i64(6), lit_i64(7)],
        true,
    ));
    let rewritten = rewrite_consecutive_inlist_to_range(in_list.clone());
    assert_eq!(rewritten, in_list);
}

#[test]
fn leaves_inlist_with_duplicates_unchanged() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        vec![lit_i64(5), lit_i64(6), lit_i64(6), lit_i64(7)],
        false,
    ));
    let rewritten = rewrite_consecutive_inlist_to_range(in_list.clone());
    assert_eq!(rewritten, in_list);
}

#[test]
fn leaves_inlist_with_string_literals_unchanged() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("name")),
        vec![
            Expr::Literal(ScalarValue::Utf8(Some("a".into())), None),
            Expr::Literal(ScalarValue::Utf8(Some("b".into())), None),
            Expr::Literal(ScalarValue::Utf8(Some("c".into())), None),
            Expr::Literal(ScalarValue::Utf8(Some("d".into())), None),
        ],
        false,
    ));
    let rewritten = rewrite_consecutive_inlist_to_range(in_list.clone());
    assert_eq!(rewritten, in_list);
}

#[test]
fn rewrites_inlist_with_mixed_int_widths() {
    let in_list = Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col("id")),
        vec![
            Expr::Literal(ScalarValue::Int32(Some(5)), None),
            Expr::Literal(ScalarValue::Int32(Some(6)), None),
            Expr::Literal(ScalarValue::Int32(Some(7)), None),
            Expr::Literal(ScalarValue::Int32(Some(8)), None),
        ],
        false,
    ));
    let rewritten = rewrite_consecutive_inlist_to_range(in_list);
    assert_eq!(rewritten, between_int("id", 5, 8));
}

// ========================================================================
// Inline-tombstone on-conflict path (Lever C) — `apply_on_conflict_deletions`
// now hides the prior inline copy of an upserted PK with a small inline
// tombstone (`add_inlined_delete`) instead of rewriting the whole inline
// corpus. These tests prove the scan-time result is correct AND that the
// tombstone's `delete_sequence` is ordered so it hides ONLY the old inline
// row, never the replacement.
// ========================================================================

/// Upsert table with inlining ENABLED (default `inline_max_rows`), so a small
/// batch lands in the inline memtable. The `create_cdc_upsert_table` helper
/// sets `inline_max_rows: 0` (inlining off); this one keeps the default so the
/// inline-conflict path is exercised.
async fn create_inline_enabled_upsert_table(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    create_cdc_upsert_table_with_vortex_config(
        table_name,
        runtime_env,
        VortexConfig {
            // Keep the default inline admission window so 1-2 row batches inline.
            deletion_mode: crate::metadata::DeletionMode::Key,
            ..VortexConfig::default()
        },
    )
    .await
}

/// Build a batch large enough to EXCEED the inline admission window
/// (`INLINE_MAX_ROWS`), so the write falls back to a staged Vortex file and
/// the on-conflict resolution runs through `apply_on_conflict_deletions`
/// (which writes inline TOMBSTONES for any prior-inline PKs it supersedes).
/// `conflict_pk`/`conflict_value` is included; the remaining rows are unique
/// filler PKs starting at `filler_start` (kept disjoint from the inline PKs).
fn large_upsert_batch_with_conflict(
    schema: SchemaRef,
    conflict_pk: i64,
    conflict_value: i64,
    filler_start: i64,
) -> RecordBatch {
    // One conflict row + enough filler rows to exceed INLINE_MAX_ROWS.
    let filler_rows = INLINE_MAX_ROWS + 8;
    let mut ids = Vec::with_capacity(filler_rows + 1);
    let mut values = Vec::with_capacity(filler_rows + 1);
    ids.push(conflict_pk);
    values.push(conflict_value);
    for offset in 0..filler_rows {
        let pk = filler_start + i64::try_from(offset).expect("filler offset fits in i64");
        ids.push(pk);
        values.push(pk * 10);
    }
    id_value_batch(schema, &ids, &values)
}

/// Cross-batch upsert against an inline row: the first (small) batch inlines
/// PK=1; a second, LARGE batch upserts PK=1 (large => it bypasses the inline
/// memtable and stages a file, so the on-conflict resolution writes an inline
/// tombstone for the prior inline copy). The old inline copy must be HIDDEN
/// and only the new value visible through a REAL `SELECT *` scan.
#[tokio::test]
async fn test_inline_tombstone_cross_batch_upsert_hides_old_inline_row() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_tombstone_xbatch", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Batch 1: small enough to inline.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    assert!(
        provider.cached_inlined_row_count() > 0,
        "precondition: first small batch must land in the inline memtable"
    );

    // Batch 2: large upsert containing PK=1 -> 999. The old inline copy is
    // hidden by an inline tombstone; the replacement is written to a file at a
    // higher sequence.
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                1,
                999,
                1_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("cdc upsert over inline row should succeed");
    if write.has_pending_finalize() {
        write.finish().await.expect("finalize staged upsert");
    }

    // An inline tombstone was written (this is the path under test).
    assert!(
        !catalog
            .get_inlined_deletes(&provider.table_metadata.table_id)
            .await
            .expect("read inline tombstones")
            .is_empty(),
        "the inline-conflicting upsert must write an inline tombstone"
    );

    let pairs = collect_id_value_pairs(&ctx, &provider, "inline_tombstone_xbatch").await;
    assert!(
        pairs.contains(&(1, 999)),
        "replacement value for PK=1 must be visible, got {pairs:?}"
    );
    assert!(
        !pairs.contains(&(1, 10)),
        "old inline value for PK=1 must be hidden by the tombstone, got {pairs:?}"
    );
    // No duplicate PK=1.
    assert_eq!(
        pairs.iter().filter(|(id, _)| *id == 1).count(),
        1,
        "exactly one visible row for PK=1, got {pairs:?}"
    );
}

/// Flatten `(id, value)` pairs from result batches, sorted. Shared by the
/// predicate-filtered scan tests.
fn collect_id_value_pairs_from_batches(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    use arrow::array::Int64Array;
    let mut pairs = Vec::new();
    for batch in batches {
        let id_idx = batch.schema().index_of("id").expect("id column");
        let value_idx = batch.schema().index_of("value").expect("value column");
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let values = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value is Int64");
        for row in 0..batch.num_rows() {
            pairs.push((ids.value(row), values.value(row)));
        }
    }
    pairs.sort_unstable();
    pairs
}

/// P1-1: a predicate-filtered scan over an actively-inlining CDC table must
/// return ONLY the rows matching the predicate. The inline branch is a
/// `MemoryExec` that does not support filter pushdown, so the scan now wraps
/// it with its own `FilterExec`; this drives the full `DataFusion` logical +
/// physical pipeline (where the post-scan `FilterExec` may be dropped) and
/// proves correctness is preserved while inline rows are live.
#[tokio::test]
async fn test_inline_cdc_filtered_scan_returns_only_matching_rows() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_filtered_scan", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Seed several rows into the inline memtable (each small batch inlines).
    for (id, value) in [(1_i64, 10_i64), (2, 20), (3, 30), (4, 40), (5, 50)] {
        insert_batch(
            &provider,
            id_value_batch(Arc::clone(&schema), &[id], &[value]),
        )
        .await;
    }
    assert!(
        provider.cached_inlined_row_count() >= 5,
        "precondition: all five rows must be live in the inline memtable, got {}",
        provider.cached_inlined_row_count()
    );

    ctx.deregister_table("inline_filtered_scan").ok();
    ctx.register_table("inline_filtered_scan", Arc::new(provider.clone_for_write()))
        .expect("table registered");

    // Equality predicate: exactly one inline row matches.
    let eq_rows = ctx
        .sql("SELECT id, value FROM inline_filtered_scan WHERE id = 3")
        .await
        .expect("eq query planned")
        .collect()
        .await
        .expect("eq query executed");
    let eq_pairs = collect_id_value_pairs_from_batches(&eq_rows);
    assert_eq!(
        eq_pairs,
        vec![(3, 30)],
        "WHERE id = 3 must return exactly the matching inline row, got {eq_pairs:?}"
    );

    // Range predicate: a strict subset of the inline rows match.
    let range_rows = ctx
        .sql("SELECT id, value FROM inline_filtered_scan WHERE id > 3")
        .await
        .expect("range query planned")
        .collect()
        .await
        .expect("range query executed");
    let range_pairs = collect_id_value_pairs_from_batches(&range_rows);
    assert_eq!(
        range_pairs,
        vec![(4, 40), (5, 50)],
        "WHERE id > 3 must return only the matching inline rows, got {range_pairs:?}"
    );

    // A predicate matching no row returns nothing (no inline row leaks).
    let none_rows = ctx
        .sql("SELECT id, value FROM inline_filtered_scan WHERE id = 999")
        .await
        .expect("empty query planned")
        .collect()
        .await
        .expect("empty query executed");
    assert!(
        collect_id_value_pairs_from_batches(&none_rows).is_empty(),
        "a non-matching predicate must return no inline rows"
    );
}

/// P1-2: `statistics()` must fold the live inline row count into `num_rows`
/// (as `Inexact`) so the join planner gets a real cardinality, instead of
/// returning `None`/an undercount, while inline CDC rows are live.
#[tokio::test]
async fn test_statistics_includes_inline_rows() {
    use datafusion_common::stats::Precision;

    let (provider, _catalog, _tmp) = create_inline_enabled_upsert_table(
        "inline_stats_rowcount",
        SessionContext::new().runtime_env(),
    )
    .await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    let seeded = 4_i64;
    for id in 1..=seeded {
        insert_batch(
            &provider,
            id_value_batch(Arc::clone(&schema), &[id], &[id * 10]),
        )
        .await;
    }
    assert_eq!(
        provider.cached_inlined_row_count(),
        seeded,
        "precondition: all seeded rows live inline"
    );

    // Before maintenance runs, no persisted stats exist yet — the
    // inline-only fallback returns the inlined_row_count as an Inexact
    // cardinality estimate so the join planner can still size the table.
    let pre_stats = provider
        .statistics()
        .expect("inline-only fallback must return Inexact count before maintenance");
    match pre_stats.num_rows {
        Precision::Inexact(n) => assert!(
            n >= usize::try_from(seeded).expect("fits"),
            "pre-maintenance Inexact num_rows must cover the {seeded} inline rows, got {n}"
        ),
        other => panic!("pre-maintenance num_rows must be Inexact, got {other:?}"),
    }

    // After maintenance, `live_rows_delta` propagates the inline row
    // count into persisted `num_rows`.
    provider.flush_pending_maintenance().await.expect("flush");
    let stats = provider
        .statistics()
        .expect("statistics must be present after maintenance flush");

    match stats.num_rows {
        Precision::Exact(n) | Precision::Inexact(n) => assert!(
            n >= usize::try_from(seeded).expect("fits"),
            "num_rows must include the {seeded} inline rows, got {n}"
        ),
        other @ Precision::Absent => panic!("num_rows must be present, got {other:?}"),
    }
}

/// Sequential delete+reinsert of the SAME PK across multiple large upserts
/// (the realistic burst the conflict-flush serializes into separate writes):
/// each upsert supersedes the prior version. Only the latest value may be
/// visible — the inline copy must never resurface and the row must never
/// vanish ("new wins").
#[tokio::test]
async fn test_inline_tombstone_sequential_reupsert_new_wins() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_tombstone_reupsert", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Seed PK=5 inline.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[5], &[50])).await;
    assert!(
        provider.cached_inlined_row_count() > 0,
        "precondition: inline seed"
    );

    // Re-upsert PK=5 several times via large (file-path) batches.
    for (value, filler_start) in [(5001_i64, 10_000_i64), (5002, 20_000), (5003, 30_000)] {
        let write = provider
            .write_cdc_append_stream(
                single_batch_stream(large_upsert_batch_with_conflict(
                    Arc::clone(&schema),
                    5,
                    value,
                    filler_start,
                )),
                &ctx.task_ctx(),
            )
            .await
            .expect("sequential re-upsert should succeed");
        if write.has_pending_finalize() {
            write.finish().await.expect("finalize staged upsert");
        }

        let pairs = collect_id_value_pairs(&ctx, &provider, "inline_tombstone_reupsert").await;
        assert_eq!(
            pairs.iter().filter(|(id, _)| *id == 5).count(),
            1,
            "exactly one visible row for PK=5 after value={value}, got {:?}",
            pairs.iter().filter(|(id, _)| *id == 5).collect::<Vec<_>>()
        );
        assert!(
            pairs.contains(&(5, value)),
            "PK=5 must show the latest value {value}, got {pairs:?}"
        );
    }
}

/// Inline-only conflict (no file-backed rows exist for the conflicting PK):
/// the only inline deletion is a tombstone. Exercises the code path that
/// reserves exactly ONE sequence for the tombstone (no file `DeleteFile`, no
/// insert record) and still keeps the replacement visible. A second inline PK
/// that is NOT upserted must remain untouched and inline.
#[tokio::test]
async fn test_inline_tombstone_inline_only_conflict() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_tombstone_only", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Two distinct PKs inline; neither has any file-backed copy.
    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &[7, 8], &[70, 80]),
    )
    .await;
    assert!(
        provider.cached_inlined_row_count() >= 2,
        "precondition: both PKs inline"
    );

    // Large upsert that supersedes ONLY PK=7 (inline). PK=8 is untouched.
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                7,
                777,
                100_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("inline-only conflict upsert should succeed");
    if write.has_pending_finalize() {
        write.finish().await.expect("finalize staged upsert");
    }

    let pairs = collect_id_value_pairs(&ctx, &provider, "inline_tombstone_only").await;
    assert!(
        pairs.contains(&(7, 777)),
        "PK=7 replacement visible, got {pairs:?}"
    );
    assert!(
        !pairs.contains(&(7, 70)),
        "old inline PK=7 hidden, got {pairs:?}"
    );
    assert!(
        pairs.contains(&(8, 80)),
        "untouched inline PK=8 still visible, got {pairs:?}"
    );
    let _ = catalog; // catalog kept alive for the table's metastore.
}

/// Ordering guard for the inline tombstone's `delete_sequence`.
///
/// `filter_inlined_batch_for_deletions` keeps an inline row iff
/// `data_sequence > delete_sequence`. The tombstone written by
/// `add_inlined_tombstone` MUST carry a `delete_sequence` that is
/// `>= the old inline row's sequence` (so it is hidden) AND
/// `< the replacement row's sequence` (so the replacement survives).
///
/// This test drives the real metastore: it reads back the durable inline
/// tombstone sequence and the table's high-water sequence and asserts the
/// strict ordering `old_inline_seq <= tombstone_seq < final_seq`. If a
/// regression assigned the tombstone a sequence at/above the replacement
/// (mis-ordering), the scan assertion below would resurrect the old row or
/// hide the new one, and the sequence assertion would fail outright.
#[tokio::test]
async fn test_inline_tombstone_delete_sequence_ordering_is_strict() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_tombstone_order", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    // Seed PK=3 inline and capture the sequence the inline row was assigned.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[3], &[30])).await;
    assert!(
        provider.cached_inlined_row_count() > 0,
        "precondition: inline seed"
    );
    let old_inline_seq = catalog
        .get_inlined_data(&table_id)
        .await
        .expect("read inline data")
        .iter()
        .map(|d| d.sequence_number)
        .max()
        .expect("at least one inline row");

    // Large upsert containing PK=3 -> 333 (forces the file path + tombstone).
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                3,
                333,
                200_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("cdc upsert should succeed");
    if write.has_pending_finalize() {
        write.finish().await.expect("finalize staged upsert");
    }

    // The inline tombstone must have been durably written.
    let tombstone_seq = catalog
        .get_inlined_deletes(&table_id)
        .await
        .expect("read inline tombstones")
        .iter()
        .map(|t| t.sequence_number)
        .max()
        .expect("an inline tombstone must exist after an inline-conflicting upsert");

    // The replacement landed in a snapshot whose sequence is the table's
    // current high-water mark (allocated strictly after the tombstone).
    let final_seq = catalog
        .get_sequence_number(&table_id)
        .await
        .expect("read current sequence");

    assert!(
        old_inline_seq <= tombstone_seq,
        "tombstone seq {tombstone_seq} must be >= old inline seq {old_inline_seq} to hide it"
    );
    assert!(
        tombstone_seq < final_seq,
        "tombstone seq {tombstone_seq} must be strictly below the replacement snapshot seq {final_seq}"
    );

    // And the observable result is correct.
    let pairs = collect_id_value_pairs(&ctx, &provider, "inline_tombstone_order").await;
    assert!(
        pairs.contains(&(3, 333)),
        "replacement visible, got {pairs:?}"
    );
    assert!(
        !pairs.contains(&(3, 30)),
        "old inline hidden, got {pairs:?}"
    );
}

// ------------------------------------------------------------------------
// Composite-PK (RowConverterBased) inline-tombstone coverage. The helpers
// above all build a single Int64 PK -> `Int64Pk` strategy. The hot CDC
// tables are composite-PK -> `RowConverterBased`, which is ALSO the branch
// the `build_pk_deletion_row_keys` Cow fix optimizes (it reuses the caller's
// already-encoded keys verbatim instead of cloning). These helpers build a
// 2-column PK `(region, id)` so the strategy resolves to `RowConverterBased`.
// ------------------------------------------------------------------------

/// Inline-enabled upsert table with a COMPOSITE primary key `(region, id)`.
/// Two PK columns (and a non-Int64 leading column) force the
/// `RowConverterBased` deletion strategy (see the `Int64Pk` gate in
/// `CayenneTableProvider::create`, which requires a single Int64 PK column).
async fn create_composite_pk_inline_table(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    use arrow::datatypes::{DataType, Field, Schema};

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");

    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["region".to_string(), "id".to_string()],
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "region".to_string(),
                "id".to_string(),
            ]),
        )),
        base_path: data_dir,
        partition_column: None,
        vortex_config: VortexConfig {
            // Keep the default inline admission window so small batches inline.
            deletion_mode: crate::metadata::DeletionMode::Key,
            ..VortexConfig::default()
        },
    };

    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .create(options)
        .await
        .expect("composite-PK table created");
    (provider, catalog, temp_dir)
}

fn region_id_value_batch(
    schema: SchemaRef,
    regions: &[&str],
    ids: &[i64],
    values: &[i64],
) -> RecordBatch {
    use arrow::array::Int64Array;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(regions.to_vec())),
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .expect("region/id/value batch is valid")
}

/// Large composite-PK batch (exceeds `INLINE_MAX_ROWS`) containing one
/// conflict row `(conflict_region, conflict_id) -> conflict_value` plus unique
/// filler rows, so the write takes the staged-file path and on-conflict
/// resolution writes an inline tombstone for the prior inline copy.
fn large_composite_upsert_batch_with_conflict(
    schema: SchemaRef,
    conflict_region: &str,
    conflict_id: i64,
    conflict_value: i64,
    filler_start: i64,
) -> RecordBatch {
    let filler_rows = INLINE_MAX_ROWS + 8;
    let mut regions: Vec<&str> = Vec::with_capacity(filler_rows + 1);
    let mut ids = Vec::with_capacity(filler_rows + 1);
    let mut values = Vec::with_capacity(filler_rows + 1);
    regions.push(conflict_region);
    ids.push(conflict_id);
    values.push(conflict_value);
    for offset in 0..filler_rows {
        let pk = filler_start + i64::try_from(offset).expect("filler offset fits in i64");
        // "filler" region keeps these PKs disjoint from the conflict key.
        regions.push("filler");
        ids.push(pk);
        values.push(pk * 10);
    }
    region_id_value_batch(schema, &regions, &ids, &values)
}

/// Read back all `(region, id, value)` triples, sorted, for assertion.
async fn collect_region_id_value_rows(
    ctx: &SessionContext,
    provider: &CayenneTableProvider,
    table_name: &str,
) -> Vec<(String, i64, i64)> {
    use arrow::array::Int64Array;
    let batches = read_all(ctx, provider, table_name).await;
    let mut rows = Vec::new();
    for batch in &batches {
        let region_idx = batch.schema().index_of("region").expect("region column");
        let id_idx = batch.schema().index_of("id").expect("id column");
        let value_idx = batch.schema().index_of("value").expect("value column");
        let regions = batch
            .column(region_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region is Utf8");
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let values = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value is Int64");
        for row in 0..batch.num_rows() {
            rows.push((
                regions.value(row).to_string(),
                ids.value(row),
                values.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

async fn create_order_line_cdc_table(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    create_order_line_cdc_table_with_inline_max_rows(table_name, runtime_env, 0).await
}

async fn create_order_line_cdc_table_with_inline_max_rows(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
    inline_max_rows: usize,
) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    use arrow::datatypes::{DataType, Field, Schema};

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");

    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("ol_w_id", DataType::Int64, false),
        Field::new("ol_d_id", DataType::Int64, false),
        Field::new("ol_o_id", DataType::Int64, false),
        Field::new("ol_number", DataType::Int64, false),
        Field::new("ol_delivery_d", DataType::Int64, false),
    ]));

    let pk = vec![
        "ol_w_id".to_string(),
        "ol_d_id".to_string(),
        "ol_o_id".to_string(),
        "ol_number".to_string(),
    ];
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: pk.clone(),
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(pk),
        )),
        base_path: data_dir,
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows,
            deletion_mode: crate::metadata::DeletionMode::Key,
            // Force the same over-budget existence-index mode that Ch-Bench
            // `order_line` reaches at SF-100 scale, without needing millions
            // of keys in the test.
            pk_keyset_cache_mb: Some(0),
            ..VortexConfig::default()
        },
    };

    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .create(options)
        .await
        .expect("order_line-style CDC table created");
    (provider, catalog, temp_dir)
}

fn order_line_batch(
    schema: SchemaRef,
    order_id: i64,
    line_count: i64,
    delivery_d: i64,
) -> RecordBatch {
    use arrow::array::Int64Array;

    let line_numbers: Vec<i64> = (1..=line_count).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1; line_numbers.len()])),
            Arc::new(Int64Array::from(vec![1; line_numbers.len()])),
            Arc::new(Int64Array::from(vec![order_id; line_numbers.len()])),
            Arc::new(Int64Array::from(line_numbers)),
            Arc::new(Int64Array::from(vec![
                delivery_d;
                usize::try_from(line_count)
                    .expect("line count fits usize")
            ])),
        ],
    )
    .expect("order_line batch is valid")
}

fn order_line_batch_with_extra_line(
    schema: SchemaRef,
    order_id: i64,
    line_count: i64,
    delivery_d: i64,
    extra_order_id: i64,
) -> RecordBatch {
    use arrow::array::Int64Array;

    let row_count = usize::try_from(line_count + 1).expect("line count fits usize");
    let mut warehouse_ids = Vec::with_capacity(row_count);
    let mut district_ids = Vec::with_capacity(row_count);
    let mut order_ids = Vec::with_capacity(row_count);
    let mut line_numbers = Vec::with_capacity(row_count);
    let mut delivery_dates = Vec::with_capacity(row_count);

    for line_number in 1..=line_count {
        warehouse_ids.push(1);
        district_ids.push(1);
        order_ids.push(order_id);
        line_numbers.push(line_number);
        delivery_dates.push(delivery_d);
    }

    warehouse_ids.push(1);
    district_ids.push(1);
    order_ids.push(extra_order_id);
    line_numbers.push(1);
    delivery_dates.push(delivery_d);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(warehouse_ids)),
            Arc::new(Int64Array::from(district_ids)),
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(Int64Array::from(line_numbers)),
            Arc::new(Int64Array::from(delivery_dates)),
        ],
    )
    .expect("order_line overflow batch is valid")
}

#[expect(clippy::similar_names)]
async fn collect_order_line_rows(
    ctx: &SessionContext,
    provider: &CayenneTableProvider,
    table_name: &str,
) -> Vec<(i64, i64, i64, i64, i64)> {
    use arrow::array::Int64Array;

    let batches = read_all(ctx, provider, table_name).await;
    let mut rows = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let w_idx = schema.index_of("ol_w_id").expect("ol_w_id column");
        let d_idx = schema.index_of("ol_d_id").expect("ol_d_id column");
        let o_idx = schema.index_of("ol_o_id").expect("ol_o_id column");
        let n_idx = schema.index_of("ol_number").expect("ol_number column");
        let delivery_idx = schema
            .index_of("ol_delivery_d")
            .expect("ol_delivery_d column");
        let w_ids = batch
            .column(w_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ol_w_id is Int64");
        let d_ids = batch
            .column(d_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ol_d_id is Int64");
        let o_ids = batch
            .column(o_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ol_o_id is Int64");
        let line_numbers = batch
            .column(n_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ol_number is Int64");
        let delivery_ds = batch
            .column(delivery_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ol_delivery_d is Int64");
        for row in 0..batch.num_rows() {
            rows.push((
                w_ids.value(row),
                d_ids.value(row),
                o_ids.value(row),
                line_numbers.value(row),
                delivery_ds.value(row),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

async fn query_count_star(
    ctx: &SessionContext,
    provider: &CayenneTableProvider,
    table_name: &str,
) -> i64 {
    ctx.deregister_table(table_name).ok();
    ctx.register_table(table_name, Arc::new(provider.clone_for_write()))
        .expect("table registered");
    let batches = ctx
        .sql(&format!("SELECT COUNT(*) AS count FROM {table_name}"))
        .await
        .expect("count query created")
        .collect()
        .await
        .expect("count query collected");
    let batch = batches.first().expect("count returns one batch");
    let value =
        ScalarValue::try_from_array(batch.column(0).as_ref(), 0).expect("count scalar extracted");
    match value {
        ScalarValue::Int64(Some(count)) => count,
        ScalarValue::UInt64(Some(count)) => i64::try_from(count).expect("count fits i64"),
        other => panic!("unexpected COUNT(*) scalar: {other:?}"),
    }
}

/// Composite-PK (`RowConverterBased`) inline-tombstone upsert: seed a small
/// inline row at composite PK `("us", 1)`, then upsert the SAME composite PK
/// via a large (file-path) batch. The prior inline copy must be HIDDEN by an
/// inline tombstone, only the new value visible, exactly one row for that PK —
/// asserted through a real `SELECT *` scan. This is the branch the Cow fix
/// optimizes (it forwards the caller's encoded keys without cloning), so the
/// test first asserts the strategy really is `RowConverterBased` (it must not
/// silently pass as `Int64Pk`).
#[tokio::test]
async fn test_inline_tombstone_composite_pk_hides_old_inline_row() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_composite_pk_inline_table("inline_tombstone_composite", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Guard: the table MUST use the composite/general strategy, not the
    // Int64 fast path — otherwise this would not exercise the Cow branch.
    assert!(
        matches!(
            provider.pk_deletion_strategy(),
            PkDeletionStrategyWithCache::RowConverterBased { .. }
        ),
        "composite PK must resolve to the RowConverterBased deletion strategy"
    );

    // Seed composite PK ("us", 1) -> 10 inline (small batch).
    insert_batch(
        &provider,
        region_id_value_batch(Arc::clone(&schema), &["us"], &[1], &[10]),
    )
    .await;
    assert!(
        provider.cached_inlined_row_count() > 0,
        "precondition: first small batch must land in the inline memtable"
    );

    // Large upsert containing the SAME composite PK ("us", 1) -> 999. The old
    // inline copy is hidden by an inline tombstone; the replacement is a file
    // row at a higher sequence.
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(large_composite_upsert_batch_with_conflict(
                Arc::clone(&schema),
                "us",
                1,
                999,
                1_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("composite-PK cdc upsert over inline row should succeed");
    if write.has_pending_finalize() {
        write.finish().await.expect("finalize staged upsert");
    }

    // An inline tombstone was written (this is the path under test).
    assert!(
        !catalog
            .get_inlined_deletes(&provider.table_metadata.table_id)
            .await
            .expect("read inline tombstones")
            .is_empty(),
        "the composite-PK inline-conflicting upsert must write an inline tombstone"
    );

    let rows = collect_region_id_value_rows(&ctx, &provider, "inline_tombstone_composite").await;
    assert!(
        rows.contains(&("us".to_string(), 1, 999)),
        "replacement value for composite PK (\"us\", 1) must be visible, got {rows:?}"
    );
    assert!(
        !rows.contains(&("us".to_string(), 1, 10)),
        "old inline value for composite PK (\"us\", 1) must be hidden by the tombstone, got {rows:?}"
    );
    assert_eq!(
        rows.iter()
            .filter(|(region, id, _)| region == "us" && *id == 1)
            .count(),
        1,
        "exactly one visible row for composite PK (\"us\", 1), got {rows:?}"
    );
}

/// Regression for the Ch-Bench `order_line` shape: Delivery updates replace
/// every line item for one order using the composite PK
/// `(ol_w_id, ol_d_id, ol_o_id, ol_number)`. The file-backed CDC upsert path
/// must emit one key tombstone per prior line item, keep only the updated
/// version visible, and keep SQL `COUNT(*)` in lockstep with the physical
/// scan count.
#[tokio::test]
#[expect(clippy::items_after_statements)]
async fn test_order_line_composite_pk_bloom_delivery_upsert_keeps_count_exact() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_order_line_cdc_table("order_line_delivery", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    assert!(
        matches!(
            provider.pk_deletion_strategy(),
            PkDeletionStrategyWithCache::RowConverterBased { .. }
        ),
        "order_line-style composite PK must use RowConverterBased deletion keys"
    );

    const LINE_COUNT: i64 = 128;
    provider
        .write_cdc_append_stream(
            single_batch_stream(order_line_batch(Arc::clone(&schema), 42, LINE_COUNT, 0)),
            &ctx.task_ctx(),
        )
        .await
        .expect("initial order_line insert should prepare")
        .finish()
        .await
        .expect("finalize initial order_line insert");
    provider
        .flush_pending_maintenance()
        .await
        .expect("flush stats after initial order_line insert");

    provider
        .write_cdc_append_stream(
            single_batch_stream(order_line_batch(Arc::clone(&schema), 42, LINE_COUNT, 1)),
            &ctx.task_ctx(),
        )
        .await
        .expect("delivery order_line upsert should prepare")
        .finish()
        .await
        .expect("finalize delivery order_line upsert");
    provider
        .flush_pending_maintenance()
        .await
        .expect("flush stats after delivery order_line upsert");

    let rows = collect_order_line_rows(&ctx, &provider, "order_line_delivery").await;
    assert_eq!(
        rows.len(),
        usize::try_from(LINE_COUNT).expect("line count fits usize"),
        "physical scan must expose exactly one row per order_line PK"
    );
    for line_number in 1..=LINE_COUNT {
        assert_eq!(
            rows.iter()
                .filter(|(w_id, d_id, o_id, ol_number, _)| {
                    *w_id == 1 && *d_id == 1 && *o_id == 42 && *ol_number == line_number
                })
                .count(),
            1,
            "line item {line_number} must appear exactly once"
        );
    }
    assert!(
        rows.iter().all(|(_, _, _, _, delivery_d)| *delivery_d == 1),
        "every visible order_line row must be the post-delivery replacement, got {rows:?}"
    );

    let count_star = query_count_star(&ctx, &provider, "order_line_delivery").await;
    assert_eq!(
        count_star, LINE_COUNT,
        "COUNT(*) must agree with the physical scan after composite-PK upserts"
    );
}

/// Ch-Bench durable-path regression: the original order lines are small
/// enough to live in the metastore inline tier, but the delivery update
/// overflows the inline gate and stages replacement rows on disk. Source
/// commit/caught-up state must not run ahead of that staged finalize, and
/// the inline tombstone must hide every old metastore row once the staged
/// files are visible.
#[tokio::test]
#[expect(clippy::items_after_statements)]
async fn test_order_line_metastore_inline_replaced_by_staged_disk_keeps_count_exact() {
    let ctx = SessionContext::new();
    const LINE_COUNT: i64 = 128;
    let (provider, _catalog, _tmp) = create_order_line_cdc_table_with_inline_max_rows(
        "order_line_inline_to_staged",
        ctx.runtime_env(),
        usize::try_from(LINE_COUNT).expect("line count fits usize"),
    )
    .await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    provider
        .write_cdc_append_stream(
            single_batch_stream(order_line_batch(Arc::clone(&schema), 42, LINE_COUNT, 0)),
            &ctx.task_ctx(),
        )
        .await
        .expect("initial order_line insert should prepare")
        .finish()
        .await
        .expect("finalize initial order_line insert");
    assert_eq!(
        provider.cached_inlined_row_count(),
        LINE_COUNT,
        "precondition: the initial order_line batch must live in metastore inline data"
    );

    assert_eq!(
        collect_order_line_rows(&ctx, &provider, "order_line_inline_to_staged")
            .await
            .len(),
        usize::try_from(LINE_COUNT).expect("line count fits usize"),
        "warming the inline cache should expose every original line"
    );

    let delivery = provider
        .write_cdc_append_stream(
            single_batch_stream(order_line_batch_with_extra_line(
                Arc::clone(&schema),
                42,
                LINE_COUNT,
                1,
                43,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("delivery order_line upsert should prepare");
    assert!(
        delivery.has_pending_finalize(),
        "the delivery batch must overflow the inline gate and stage replacement rows on disk"
    );

    let during = collect_order_line_rows(&ctx, &provider, "order_line_inline_to_staged").await;
    assert_eq!(
        during.len(),
        usize::try_from(LINE_COUNT).expect("line count fits usize"),
        "before staged finalize, the old inline rows remain visible and replacements are hidden"
    );
    assert!(
        during
            .iter()
            .all(|(_, _, _, _, delivery_d)| *delivery_d == 0),
        "before staged finalize, the inert tombstone must not hide old inline rows"
    );

    delivery
        .finish()
        .await
        .expect("finalize staged delivery upsert");

    let rows = collect_order_line_rows(&ctx, &provider, "order_line_inline_to_staged").await;
    assert_eq!(
        rows.len(),
        usize::try_from(LINE_COUNT + 1).expect("line count fits usize"),
        "finalized staged disk rows plus the extra new line must be visible exactly once"
    );
    for line_number in 1..=LINE_COUNT {
        assert_eq!(
            rows.iter()
                .filter(|(w_id, d_id, o_id, ol_number, delivery_d)| {
                    *w_id == 1
                        && *d_id == 1
                        && *o_id == 42
                        && *ol_number == line_number
                        && *delivery_d == 1
                })
                .count(),
            1,
            "updated line item {line_number} must appear exactly once"
        );
    }
    assert!(
        rows.contains(&(1, 1, 43, 1, 1)),
        "the overflow row that forced staging must also be visible, got {rows:?}"
    );
    assert_eq!(
        rows.iter()
            .filter(|(w_id, d_id, o_id, _, delivery_d)| {
                *w_id == 1 && *d_id == 1 && *o_id == 42 && *delivery_d == 0
            })
            .count(),
        0,
        "old metastore-inline order_line rows must be hidden after staged finalize, got {rows:?}"
    );

    let count_star = query_count_star(&ctx, &provider, "order_line_inline_to_staged").await;
    assert_eq!(
        count_star,
        LINE_COUNT + 1,
        "COUNT(*) must agree with the visible metastore-inline plus staged-disk row set"
    );
}

/// One `apply_on_conflict_deletions` batch where BOTH an inline conflict and a
/// file-backed key conflict are present. Seed PK=1 INLINE (small batch) and
/// PK=2 in a FILE (large batch), then upsert BOTH 1 and 2 in a single large
/// batch. That single on-conflict resolution has `has_inlined_deletions` (for
/// PK=1) AND file-backed key deletions (for PK=2), both sharing the reserved
/// `delete_sequence`. Assert the inline tombstone AND a file `DeleteFile` were
/// both written at that shared sequence, and the scan shows only the latest
/// value for each PK (no resurrect of either old copy).
#[tokio::test]
async fn test_on_conflict_mixed_inline_and_file_delete_in_one_batch() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_inline_enabled_upsert_table("mixed_inline_file_delete", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    // Seed PK=1 INLINE via a small batch.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    assert!(
        provider.cached_inlined_row_count() > 0,
        "precondition: PK=1 must be inline"
    );

    // Seed PK=2 into a FILE via a large batch (exceeds the inline window). It
    // carries no conflict (PK=2 is new), so no deletions yet.
    let seed_file = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                2,
                20,
                500_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("seeding PK=2 into a file should succeed");
    if seed_file.has_pending_finalize() {
        seed_file.finish().await.expect("finalize file seed");
    }
    assert!(
        catalog
            .get_inlined_deletes(&table_id)
            .await
            .expect("read inline tombstones")
            .is_empty(),
        "no inline tombstone should exist before the mixed-conflict upsert"
    );

    // Single large batch upserting BOTH PK=1 (inline conflict -> tombstone) and
    // PK=2 (file conflict -> file DeleteFile). The conflict row is PK=1; PK=2
    // is added as an extra explicit conflict row so the same batch supersedes
    // the file-backed PK=2 as well.
    let mixed = large_upsert_batch_with_conflict(Arc::clone(&schema), 1, 111, 600_000);
    let mixed = {
        // Append the PK=2 -> 222 conflict row to the large batch.
        use arrow::array::Int64Array;
        let ids = mixed
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let values = mixed
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value is Int64");
        let mut id_vec: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
        let mut value_vec: Vec<i64> = (0..values.len()).map(|i| values.value(i)).collect();
        id_vec.push(2);
        value_vec.push(222);
        id_value_batch(Arc::clone(&schema), &id_vec, &value_vec)
    };

    let write = provider
        .write_cdc_append_stream(single_batch_stream(mixed), &ctx.task_ctx())
        .await
        .expect("mixed inline+file conflict upsert should succeed");
    if write.has_pending_finalize() {
        write.finish().await.expect("finalize mixed upsert");
    }

    // The inline tombstone (for PK=1) must be durably written.
    let tombstones = catalog
        .get_inlined_deletes(&table_id)
        .await
        .expect("read inline tombstones");
    assert!(
        !tombstones.is_empty(),
        "an inline tombstone must be written for the inline-conflicting PK=1"
    );
    let tombstone_seq = tombstones
        .iter()
        .map(|t| t.sequence_number)
        .max()
        .expect("at least one tombstone");

    // The file `DeleteFile` (for PK=2) must be durably written.
    let delete_files = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("read delete files");
    assert!(
        !delete_files.is_empty(),
        "a file DeleteFile must be written for the file-conflicting PK=2"
    );

    // Both express the same "hide the prior version" intent at the SHARED
    // reserved `delete_sequence`: the tombstone sequence must equal the
    // DeleteFile sequence written by the same on-conflict batch.
    assert!(
        delete_files
            .iter()
            .any(|df| df.sequence_number == tombstone_seq),
        "the inline tombstone (seq {tombstone_seq}) and a file DeleteFile must share the on-conflict delete_sequence; delete files: {:?}",
        delete_files
            .iter()
            .map(|df| df.sequence_number)
            .collect::<Vec<_>>()
    );

    // Observable result: only the latest value for each PK, no resurrected copy.
    let pairs = collect_id_value_pairs(&ctx, &provider, "mixed_inline_file_delete").await;
    assert!(
        pairs.contains(&(1, 111)),
        "PK=1 replacement (inline-superseded) visible, got {pairs:?}"
    );
    assert!(
        !pairs.contains(&(1, 10)),
        "old inline PK=1 must be hidden, got {pairs:?}"
    );
    assert!(
        pairs.contains(&(2, 222)),
        "PK=2 replacement (file-superseded) visible, got {pairs:?}"
    );
    assert!(
        !pairs.contains(&(2, 20)),
        "old file PK=2 must be hidden, got {pairs:?}"
    );
    assert_eq!(
        pairs.iter().filter(|(id, _)| *id == 1).count(),
        1,
        "exactly one visible row for PK=1, got {pairs:?}"
    );
    assert_eq!(
        pairs.iter().filter(|(id, _)| *id == 2).count(),
        1,
        "exactly one visible row for PK=2, got {pairs:?}"
    );
}

/// Position-based (PK-less) upsert-less table. An empty `primary_key` resolves
/// to the `PositionBased` deletion strategy (see `CayenneTableProvider::create`).
async fn create_position_based_table(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    use arrow::datatypes::{DataType, Field, Schema};

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");

    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: data_dir,
        partition_column: None,
        vortex_config: VortexConfig::default(),
    };

    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env)
        .create(options)
        .await
        .expect("position-based table created");
    (provider, catalog, temp_dir)
}

/// Position-based tables have no PK and never apply inline deletion filtering,
/// so `add_inlined_tombstone` must early-return `Ok(false)` and write NOTHING
/// (the `is_position_based()` guard). Drives the real metastore: asserts the
/// return value is `false` and no inline tombstone row was persisted.
#[tokio::test]
async fn test_add_inlined_tombstone_position_based_is_noop() {
    let ctx = SessionContext::new();
    // A table with NO primary key resolves to the position-based strategy.
    let (provider, catalog, _tmp) =
        create_position_based_table("position_tombstone_noop", ctx.runtime_env()).await;
    let table_id = provider.table_metadata.table_id.clone();

    assert!(
        provider.pk_deletion_strategy().is_position_based(),
        "precondition: table must use the position-based deletion strategy"
    );

    // Call the tombstone writer directly with non-empty key inputs; the
    // position-based guard must short-circuit before any durable write.
    let row_key: Box<[u8]> = vec![0_u8, 0, 0, 1].into_boxed_slice();
    let written = provider
        .add_inlined_tombstone(&[1, 2, 3], std::slice::from_ref(&row_key), 7, true)
        .await
        .expect("add_inlined_tombstone must not error for a position-based table");

    assert!(
        written.is_none(),
        "add_inlined_tombstone must return Ok(None) for a position-based table"
    );
    assert!(
        catalog
            .get_inlined_deletes(&table_id)
            .await
            .expect("read inline tombstones")
            .is_empty(),
        "a position-based table must persist no inline tombstone"
    );
}

/// When position deletes are pending and the cached aggregate stats are not
/// available, `statistics()` must not fall back to raw `ListingTable` footer
/// stats. Those stats do not account for the Vortex access-plan deletion
/// bitmap and can overstate cardinality until maintenance repopulates the
/// cache.
#[tokio::test]
async fn test_statistics_cache_miss_with_position_deletes_returns_none() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_position_based_table("position_stats_cache_miss", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &[1, 2, 3], &[10, 20, 30]),
    )
    .await;
    provider
        .flush_pending_maintenance()
        .await
        .expect("flush stats");
    assert!(
        provider.statistics().is_some(),
        "precondition: position table should expose stats before deletes"
    );

    let delete_plan = provider
        .delete_from(
            &ctx.state(),
            vec![col("id").eq(datafusion_expr::lit(2_i64))],
        )
        .await
        .expect("delete plan");
    collect(delete_plan, ctx.task_ctx())
        .await
        .expect("delete executed");
    assert!(
        provider.has_pending_deletions(),
        "precondition: delete should leave a pending position bitmap"
    );

    provider.clear_cached_table_statistics_unlocked();
    assert!(
        provider.statistics().is_none(),
        "with pending position deletes and no cached aggregate, raw ListingTable stats must be suppressed"
    );
}

// ========================================================================
// Pipelined key-delete tables (Change B): a table that already holds pending
// PK deletions no longer forces the blocking synchronous path — it stages into
// a ProtectedSnapshot whose threshold is the stage-time-reserved sequence,
// above every existing tombstone. These tests prove delete-then-reinsert
// across multiple coalesced batches stays correct when pipelined.
// ========================================================================

/// A key-delete (`Int64Pk`) table under repeated cross-batch upserts that each
/// supersede the prior file-backed version. The second and later batches run
/// while the table holds pending PK deletions (so previously they took the
/// blocking path); now they pipeline. The scan after each finalize must show
/// exactly the latest value — no resurrected old version, no vanished row.
#[tokio::test]
async fn test_pipelined_key_delete_table_repeated_upserts_stay_correct() {
    let ctx = SessionContext::new();
    // `create_cdc_upsert_table` disables inlining, so every conflict is a
    // file-backed key deletion — exactly the pending-PK-deletion path.
    let (provider, _catalog, _tmp) =
        create_cdc_upsert_table("pipelined_key_delete", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Seed two file-backed rows.
    insert_batch(
        &provider,
        id_value_batch(Arc::clone(&schema), &[1, 2], &[10, 20]),
    )
    .await;

    // Upsert PK=1 several times in a row. From the 2nd onward the table has
    // pending PK deletions (a tombstone from the prior upsert), so the gate
    // that used to force the synchronous path is exercised.
    for value in [100, 1000, 10000] {
        let write = provider
            .write_cdc_append_stream(
                single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[value])),
                &ctx.task_ctx(),
            )
            .await
            .expect("pipelined upsert on pending-delete table should succeed");
        assert!(
            write.has_pending_finalize(),
            "a pending-PK-deletion upsert table must now PIPELINE (stage), not take the blocking path"
        );
        write.finish().await.expect("finalize staged upsert");

        assert_eq!(
            collect_id_value_pairs(&ctx, &provider, "pipelined_key_delete").await,
            vec![(1, value), (2, 20)],
            "after each finalize PK=1 shows the latest value and PK=2 is untouched"
        );
    }
}

/// Two coalesced upsert bursts targeting the SAME PK, both staged before
/// either finalizes (the burst-overlap that broke earlier naive pipelining).
/// The second burst's protected snapshot must win; neither old version may
/// resurface and the row must never vanish.
#[tokio::test]
async fn test_pipelined_key_delete_overlapping_bursts_same_pk() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_cdc_upsert_table("pipelined_key_delete_overlap", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Seed a file-backed row so the upserts conflict against a file row and
    // the table carries pending deletions after the first.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;

    let first = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[111])),
            &ctx.task_ctx(),
        )
        .await
        .expect("first staged upsert");
    assert!(first.has_pending_finalize());

    let second = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[222])),
            &ctx.task_ctx(),
        )
        .await
        .expect("second staged upsert while first finalize pending");
    assert!(second.has_pending_finalize());

    // Finalize in order; the later (higher-sequence) snapshot must win.
    first.finish().await.expect("finalize first");
    second.finish().await.expect("finalize second");

    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "pipelined_key_delete_overlap").await,
        vec![(1, 222)],
        "the later staged upsert wins; no resurface of 10 or 111, no vanish"
    );
}

// ========================================================================
// Staged inline-conflict tombstones (Option D — durable per-tombstone
// `published` flag). Inline-bearing upserts now PIPELINE (stage inert) like
// file-conflict upserts: the tombstone is written `published = false`, the
// read filter skips it, and `finish()` flips it durably before the
// replacement becomes discoverable. These three tests are the SAFETY GATE:
// they prove the staged inline tombstone is vanish-free under burst-overlap,
// across a reload (crash-before-finalize), and under the transient-vanish
// race a global watermark could not survive.
// ========================================================================

/// (a) Burst-overlap. Two upserts of the SAME PK are both STAGED before either
/// finalizes; the FIRST conflicts with the inline copy (writing an inert
/// inline tombstone), the SECOND conflicts with the first burst's staged file
/// copy. While both are staged inert, the OLD inline value must stay visible
/// (no vanish). After finalizing in order, the later (higher-sequence)
/// snapshot wins: exactly one visible row for the PK, neither the original
/// inline value nor the first burst's value resurfaced.
#[tokio::test]
async fn test_staged_inline_tombstone_overlapping_bursts_same_pk() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_inline_enabled_upsert_table("staged_inline_tombstone_overlap", ctx.runtime_env())
            .await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    // Seed PK=1 INLINE.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    assert!(
        provider.cached_inlined_row_count() > 0,
        "precondition: PK=1 must be inline"
    );

    // First large upsert of PK=1 conflicts with the inline copy -> writes an
    // inert (`published = false`) inline tombstone; stages.
    let first = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                1,
                111,
                1_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("first staged inline-conflict upsert");
    assert!(
        first.has_pending_finalize(),
        "an inline-conflict upsert must now PIPELINE (stage), not publish synchronously"
    );
    // The first burst's inline tombstone is durable and INERT.
    let staged = catalog
        .get_inlined_deletes(&table_id)
        .await
        .expect("read staged inline tombstones");
    assert!(
        !staged.is_empty() && staged.iter().all(|t| !t.published),
        "the first burst's inline tombstone must be durable and unpublished, got {staged:?}"
    );

    // Second large upsert of PK=1 while the first is still pending. (PK=1's
    // current durable copy is now the first burst's staged file, so this
    // conflict resolves via the file path — also staged.)
    let second = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                1,
                222,
                2_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("second staged upsert while first pending");
    assert!(second.has_pending_finalize());

    // While both bursts are staged inert, PK=1 still shows the OLD inline
    // value — it has not vanished and no replacement is visible yet.
    let during = collect_id_value_pairs(&ctx, &provider, "staged_inline_tombstone_overlap")
        .await
        .into_iter()
        .filter(|(id, _)| *id == 1)
        .collect::<Vec<_>>();
    assert_eq!(
        during,
        vec![(1, 10)],
        "while both bursts are staged, PK=1 still shows the OLD inline value (no vanish), got {during:?}"
    );

    // Finalize in order; the later snapshot wins.
    first.finish().await.expect("finalize first");
    second.finish().await.expect("finalize second");

    let pairs = collect_id_value_pairs(&ctx, &provider, "staged_inline_tombstone_overlap").await;
    assert_eq!(
        pairs.iter().filter(|(id, _)| *id == 1).count(),
        1,
        "exactly one visible row for PK=1 after both finalize, got {pairs:?}"
    );
    assert!(
        pairs.contains(&(1, 222)),
        "the later staged upsert wins (222); no resurface of 10 or 111, got {pairs:?}"
    );
}

/// (b) Reload (crash-before-finalize). An inline-conflict upsert is STAGED
/// (tombstone durable, `published = false`) but never finalized.
///
/// Two invariants are proven, in two phases:
///
/// 1. BEFORE reopen, the unpublished tombstone is INERT in the live process:
///    the OLD inline row is visible (never vanished), exactly as during any
///    staged window.
/// 2. AFTER reopen, `ensure_no_incomplete_write` recovers the interrupted
///    staged append (moving the replacement files into their snapshot — the
///    CDC source offset was already committed at Stage A, so the upsert must
///    NOT be lost), and the open-time orphan-tombstone activation flips the
///    tombstone. The upsert thus applies exactly ONCE across the crash:
///    replacement visible, old inline copy hidden, no duplicate, no vanish.
#[tokio::test]
async fn test_staged_inline_tombstone_reload_completes_upsert_exactly_once() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_inline_enabled_upsert_table("staged_inline_tombstone_reload", ctx.runtime_env())
            .await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    // Seed PK=5 INLINE.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[5], &[50])).await;

    // Stage an inline-conflict upsert of PK=5 but DROP the pending write
    // without finalizing — simulating a crash after Stage A (the tombstone,
    // the staged files, and the protected-snapshot sequence are durable) but
    // before finish().
    let pending = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                5,
                555,
                10_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("staged inline-conflict upsert");
    assert!(pending.has_pending_finalize());
    let staged = catalog
        .get_inlined_deletes(&table_id)
        .await
        .expect("read staged inline tombstone");
    assert!(
        staged.iter().any(|t| !t.published),
        "the staged inline tombstone must be durable and unpublished before finalize"
    );

    // Phase 1: in the live process, while staged-inert, PK=5 still shows the
    // OLD value — never vanished.
    let during: Vec<_> = collect_id_value_pairs(&ctx, &provider, "staged_inline_tombstone_reload")
        .await
        .into_iter()
        .filter(|(id, _)| *id == 5)
        .collect();
    assert_eq!(
        during,
        vec![(5, 50)],
        "while the inline tombstone is staged inert, PK=5 shows the OLD value (no vanish), got {during:?}"
    );

    // Crash: never call finish().
    drop(pending);

    // Reopen from the same catalog/metastore. `ensure_no_incomplete_write`
    // recovers the staged files; the open-time activation flips the orphan
    // tombstone.
    let reopened = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
        .open("staged_inline_tombstone_reload")
        .await
        .expect("reopen table after crash-before-finalize");

    // The orphan tombstone is now durably published.
    let reloaded = catalog
        .get_inlined_deletes(&table_id)
        .await
        .expect("read inline tombstones after reopen");
    assert!(
        !reloaded.is_empty() && reloaded.iter().all(|t| t.published),
        "open-time recovery must activate the orphan inline tombstone, got {reloaded:?}"
    );

    // Phase 2: the upsert applied exactly once across the crash.
    let pairs = collect_id_value_pairs(&ctx, &reopened, "staged_inline_tombstone_reload").await;
    let pk5: Vec<_> = pairs.iter().copied().filter(|(id, _)| *id == 5).collect();
    assert_eq!(
        pk5,
        vec![(5, 555)],
        "after reload the recovered upsert applies exactly once: PK=5 shows the NEW value, old copy hidden, no duplicate, no vanish, got {pairs:?}"
    );
}

/// (c) Transient-vanish — the race a global watermark CANNOT survive. Stage an
/// inline-conflict upsert of PK=7, then DURING the staged window drive a
/// concurrent same-table inline INSERT of a different PK. That insert advances
/// the global inline sequence AND bumps `inlined_generation`, forcing the next
/// scan to REBUILD the inline cache and re-read the staged tombstone from the
/// metastore. With a global watermark the rebuild would apply the tombstone
/// (its `delete_sequence` is now below the advanced floor) and hide PK=7 before
/// its replacement is visible — a vanish. With the per-tombstone `published`
/// flag the rebuild skips the inert tombstone, so PK=7 shows the OLD row
/// throughout the window, then the NEW row after finalize.
#[tokio::test]
async fn test_staged_inline_tombstone_no_vanish_under_concurrent_inline_insert() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_inline_enabled_upsert_table("staged_inline_tombstone_vanish", ctx.runtime_env())
            .await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    // Seed PK=7 INLINE.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[7], &[70])).await;
    assert!(
        provider.cached_inlined_row_count() > 0,
        "precondition: PK=7 must be inline"
    );

    // Stage (do not finalize) a large inline-conflict upsert of PK=7.
    let pending = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                7,
                777,
                100_000,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("staged inline-conflict upsert");
    assert!(pending.has_pending_finalize());
    let gen_before = provider.inlined_generation();

    // Concurrent same-table inline INSERT of a NEW PK during the staged
    // window. This is a real, separate write (it takes and releases the write
    // lock — the staged path already released it). It advances the global
    // inline sequence and bumps `inlined_generation`.
    provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[8], &[80])),
            &ctx.task_ctx(),
        )
        .await
        .expect("concurrent inline insert during staged window")
        .finish()
        .await
        .ok();
    assert!(
        provider.inlined_generation() > gen_before,
        "the concurrent inline insert must bump inlined_generation (forcing a cache rebuild)"
    );
    // The staged tombstone is still durably unpublished.
    assert!(
        catalog
            .get_inlined_deletes(&table_id)
            .await
            .expect("read staged inline tombstone")
            .iter()
            .any(|t| !t.published),
        "the staged tombstone must remain unpublished during the window"
    );

    // The scan now MUST rebuild the inline cache (generation changed) and
    // re-read the staged tombstone. PK=7 must still show the OLD value — never
    // empty — and the concurrently inserted PK=8 must be visible.
    let during = collect_id_value_pairs(&ctx, &provider, "staged_inline_tombstone_vanish").await;
    let pk7: Vec<_> = during.iter().copied().filter(|(id, _)| *id == 7).collect();
    assert_eq!(
        pk7,
        vec![(7, 70)],
        "VANISH GUARD: PK=7 shows the OLD inline value during the staged window, never empty, got {during:?}"
    );
    assert!(
        during.contains(&(8, 80)),
        "the concurrent inline insert (PK=8) is visible, got {during:?}"
    );

    // Finalize: the tombstone flips published and the replacement appears.
    pending.finish().await.expect("finalize staged upsert");
    let after = collect_id_value_pairs(&ctx, &provider, "staged_inline_tombstone_vanish").await;
    assert_eq!(
        after.iter().filter(|(id, _)| *id == 7).count(),
        1,
        "exactly one visible row for PK=7 after finalize, got {after:?}"
    );
    assert!(
        after.contains(&(7, 777)),
        "after finalize PK=7 shows the NEW value (777), got {after:?}"
    );
    assert!(
        after.contains(&(8, 80)),
        "PK=8 remains visible after finalize, got {after:?}"
    );
}

/// The protected-snapshot threshold for a pipelined upsert on a pending-delete
/// table must be the sequence RESERVED AT STAGE TIME, persisted to
/// `cayenne_snapshot_sequence`, NOT a live `get_max_delete_sequence()` read at
/// finalize. We assert the in-memory protected-snapshot threshold equals the
/// persisted snapshot sequence (so the partial-deletion filter is reload-stable).
#[tokio::test]
async fn test_pipelined_protected_threshold_is_reserved_stage_time_sequence() {
    let ctx = SessionContext::new();
    let (provider, catalog, _tmp) =
        create_cdc_upsert_table("pipelined_threshold", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);
    let table_id = provider.table_metadata.table_id.clone();

    // Seed and then upsert once so the table holds a pending deletion.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[100])),
            &ctx.task_ctx(),
        )
        .await
        .expect("first pipelined upsert");
    write.finish().await.expect("finalize first");

    // Now upsert again (table has pending deletions) and finalize.
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[1000])),
            &ctx.task_ctx(),
        )
        .await
        .expect("second pipelined upsert on pending-delete table");
    assert!(write.has_pending_finalize());
    write.finish().await.expect("finalize second");

    // Every live protected-snapshot threshold must equal the sequence
    // persisted for that snapshot in the catalog.
    let protected = provider.protected_snapshots.load_full();
    assert!(
        !protected.is_empty(),
        "a pipelined upsert must publish at least one protected snapshot"
    );
    for (snapshot_id, threshold) in protected.iter() {
        let persisted = catalog
            .get_snapshot_sequence(&table_id, snapshot_id)
            .await
            .expect("read persisted snapshot sequence")
            .expect("snapshot sequence must be persisted for a published protected snapshot");
        assert_eq!(
            *threshold, persisted,
            "protected-snapshot threshold must be the reserved stage-time sequence, not a live read"
        );
    }

    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "pipelined_threshold").await,
        vec![(1, 1000)],
        "final value visible after pipelined finalize"
    );
}

// ---- Incremental inline cache (FIX 1) -------------------------------------

/// A sequence of pure inline APPENDS must leave the structural epoch
/// unchanged (so each scan takes the append-only delta path) while every
/// appended row stays correctly visible and the materialized boundary
/// advances with the watermark.
#[tokio::test]
async fn test_inline_cache_append_only_keeps_structural_epoch_and_stays_correct() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_cache_append_delta", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // First small append + a scan to warm the cache from the sentinel (a full
    // rebuild). Capture the structural epoch AFTER the first scan so the
    // subsequent appends are measured against a real (non-sentinel) base.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "inline_cache_append_delta").await,
        vec![(1, 10)]
    );
    let epoch_after_first = provider.inlined_structural_epoch();

    // Several more pure appends, each followed by a scan that must rebuild the
    // cache (generation bumped) via the DELTA path — never structurally.
    for (id, value) in [(2, 20), (3, 30), (4, 40)] {
        let gen_before = provider.inlined_generation();
        insert_batch(
            &provider,
            id_value_batch(Arc::clone(&schema), &[id], &[value]),
        )
        .await;
        assert!(
            provider.inlined_generation() > gen_before,
            "a pure append must bump the generation (force a cache refresh)"
        );
        assert_eq!(
            provider.inlined_structural_epoch(),
            epoch_after_first,
            "a pure append must NOT bump the structural epoch (delta path stays eligible)"
        );
        // Scan repopulates the cache for this generation via the delta path.
        let _ = collect_id_value_pairs(&ctx, &provider, "inline_cache_append_delta").await;
    }

    // All appended rows visible, none duplicated, regardless of delta merges.
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "inline_cache_append_delta").await,
        vec![(1, 10), (2, 20), (3, 30), (4, 40)],
        "every appended row is visible exactly once after a chain of delta refreshes"
    );
    // The boundary advanced past the last appended row's sequence.
    assert!(
        provider.cached_inlined_materialized_through_sequence() > 0,
        "the materialized boundary advances with the watermark on the delta path"
    );
}

/// An inline-vs-inline UPSERT (which rewrites/removes an existing inline
/// entry, `removed_rows > 0`) MUST bump the structural epoch so the next scan
/// full-rebuilds, and the superseded value must be hidden (no stale base
/// reuse).
#[tokio::test]
async fn test_inline_cache_inline_upsert_bumps_structural_epoch_and_hides_old() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_cache_rewrite_struct", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Seed PK=1 inline and warm the cache.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "inline_cache_rewrite_struct").await,
        vec![(1, 10)]
    );
    let epoch_before = provider.inlined_structural_epoch();

    // A SMALL upsert of the same PK stays inline and rewrites the existing
    // inline entry (`removed_rows > 0`).
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(Arc::clone(&schema), &[1], &[999])),
            &ctx.task_ctx(),
        )
        .await
        .expect("inline upsert");
    write.finish().await.expect("finalize inline upsert");

    assert!(
        provider.inlined_structural_epoch() > epoch_before,
        "an inline rewrite/removal must bump the structural epoch (force a full rebuild)"
    );
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "inline_cache_rewrite_struct").await,
        vec![(1, 999)],
        "the rewritten value is visible and the old inline copy is hidden (no stale base reuse)"
    );
}

/// cycle-5 TASK 1: the cross-batch inline tombstone path (a LARGE upsert that
/// supersedes a prior-inline PK via an inline tombstone) is now DELTA-capable.
/// A published tombstone only REMOVES rows, so it bumps ONLY the generation
/// (NOT the structural epoch) and enqueues a removal in
/// `pending_tombstone_deltas`; the next inline-cache miss takes the delta path
/// and re-filters the reused base entries against just the tombstoned keys —
/// hiding the old inline copy WITHOUT the O(corpus) full rebuild that fired on
/// every upsert batch before. (Pre-cycle-5 this bumped the structural epoch.)
#[tokio::test]
async fn test_inline_cache_tombstone_publish_is_delta_not_structural() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_cache_tombstone_struct", ctx.runtime_env())
            .await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Seed PK=1 inline, warm the cache.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "inline_cache_tombstone_struct").await,
        vec![(1, 10)]
    );
    let epoch_before = provider.inlined_structural_epoch();
    let generation_before = provider.inlined_generation.load(Ordering::Acquire);

    // LARGE upsert containing PK=1 -> stages a file + writes an inline
    // tombstone for the prior inline copy. The replacement lands at a higher
    // sequence; the tombstone publish is now a REMOVAL delta.
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(large_upsert_batch_with_conflict(
                Arc::clone(&schema),
                1,
                999,
                100,
            )),
            &ctx.task_ctx(),
        )
        .await
        .expect("large inline-conflict upsert");
    write.finish().await.expect("finalize large upsert");

    // The publish bumped the generation (cache invalidated) but NOT the
    // structural epoch (the removal is delta-applied, not full-rebuilt).
    assert_eq!(
        provider.inlined_structural_epoch(),
        epoch_before,
        "publishing an inline tombstone must NOT bump the structural epoch (cycle-5: it is a \
             removal delta, not a full rebuild)"
    );
    assert!(
        provider.inlined_generation.load(Ordering::Acquire) > generation_before,
        "publishing an inline tombstone must bump the generation (invalidate the cache)"
    );

    // Correctness: the old inline copy is hidden by the delta, the replacement
    // is visible — exactly once, no transient duplicate.
    let pairs = collect_id_value_pairs(&ctx, &provider, "inline_cache_tombstone_struct").await;
    assert_eq!(
        pairs.iter().filter(|(id, _)| *id == 1).collect::<Vec<_>>(),
        vec![&(1, 999)],
        "exactly one visible row for PK=1 (the replacement); the old inline copy is hidden by \
             the tombstone-removal delta"
    );
}

/// Delta correctness across a held-back watermark: an inline entry committed
/// but not yet published (held by the watermark) must become visible on a
/// later delta refresh — the boundary is the build-time watermark, not the
/// corpus max, so the now-published entry (still above the OLD boundary) is
/// re-fetched. This is the gap a corpus-max boundary would miss.
#[tokio::test]
async fn test_inline_cache_delta_boundary_is_watermark_not_corpus_max() {
    let ctx = SessionContext::new();
    let (provider, _catalog, _tmp) =
        create_inline_enabled_upsert_table("inline_cache_watermark_delta", ctx.runtime_env()).await;
    let schema = Arc::clone(&provider.table_metadata.schema);

    // Two committed-and-published appends, warm via the delta chain.
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[1], &[10])).await;
    let _ = collect_id_value_pairs(&ctx, &provider, "inline_cache_watermark_delta").await;
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[2], &[20])).await;
    let during = collect_id_value_pairs(&ctx, &provider, "inline_cache_watermark_delta").await;
    assert_eq!(
        during,
        vec![(1, 10), (2, 20)],
        "both published appends are visible after delta refreshes"
    );

    // A further append + scan must continue to surface every row exactly once
    // (exercises repeated delta merges; no row dropped, none duplicated).
    insert_batch(&provider, id_value_batch(Arc::clone(&schema), &[3], &[30])).await;
    assert_eq!(
        collect_id_value_pairs(&ctx, &provider, "inline_cache_watermark_delta").await,
        vec![(1, 10), (2, 20), (3, 30)],
        "delta refreshes keep the full visible set gap-free and duplicate-free"
    );
}

// ---- List-files cache delta-apply (FIX 2) ---------------------------------

/// Build a `RuntimeEnv` with an explicit (empty) list-files cache so the
/// delta-apply helper has a cache to operate on regardless of the default
/// session configuration.
fn runtime_env_with_list_files_cache() -> Arc<RuntimeEnv> {
    use datafusion_execution::cache::DefaultListFilesCache;
    use datafusion_execution::cache::cache_manager::CacheManagerConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;

    let cache_config = CacheManagerConfig::default()
        .with_list_files_cache(Some(Arc::new(DefaultListFilesCache::default())));
    Arc::new(
        RuntimeEnvBuilder::new()
            .with_cache_manager(cache_config)
            .build()
            .expect("runtime env with list-files cache"),
    )
}

fn test_object_meta(location: &str, size: u64) -> ObjectMeta {
    ObjectMeta {
        location: ObjectStorePath::from(location),
        last_modified: chrono::Utc::now(),
        size,
        e_tag: None,
        version: None,
    }
}

/// Delta-apply onto an EXISTING cached listing must append the new files
/// (deduped by location) and keep the pre-existing ones — no eviction, no
/// duplicate, no dropped file.
#[test]
fn test_list_files_cache_delta_apply_merges_onto_existing() {
    let runtime_env = runtime_env_with_list_files_cache();
    let url = "file:///tmp/cayenne_list_delta/snap0/";
    let prefix = ListingTableUrl::parse(url)
        .expect("url parses")
        .prefix()
        .clone();
    let key = TableScopedPath {
        table: None,
        path: prefix.clone(),
    };
    let cache = runtime_env
        .cache_manager
        .get_list_files_cache()
        .expect("list-files cache present");

    // Seed an existing listing with one file.
    let existing_loc = prefix.clone().join("part-0.vortex");
    cache.put(
        &key,
        CachedFileList::new(vec![test_object_meta(existing_loc.as_ref(), 100)]),
    );

    // Delta-apply two new files, one of which duplicates the existing one.
    let new_a = prefix.join("part-1.vortex");
    let additions = vec![
        test_object_meta(new_a.as_ref(), 200),
        test_object_meta(existing_loc.as_ref(), 100), // duplicate location
    ];
    let applied =
        CayenneTableProvider::apply_list_files_cache_additions(&runtime_env, url, &additions);
    assert!(
        applied,
        "delta-apply must succeed when an entry already exists"
    );

    let merged = cache
        .get(&key)
        .expect("entry still present after delta-apply");
    let mut locations: Vec<String> = merged.iter().map(|m| m.location.to_string()).collect();
    locations.sort();
    assert_eq!(
        locations,
        vec![existing_loc.to_string(), new_a.to_string()],
        "merged listing has the existing + new file exactly once (duplicate deduped)"
    );
}

/// Delta-apply onto a COLD cache (no existing entry) must NOT seed a partial
/// listing — it returns false so the caller falls back to a full re-LIST.
/// Seeding here would hide every pre-existing on-disk file from the next scan.
#[test]
fn test_list_files_cache_delta_apply_cold_cache_falls_back() {
    let runtime_env = runtime_env_with_list_files_cache();
    let url = "file:///tmp/cayenne_list_cold/snap0/";
    let prefix = ListingTableUrl::parse(url)
        .expect("url parses")
        .prefix()
        .clone();
    let key = TableScopedPath {
        table: None,
        path: prefix.clone(),
    };

    let new_a = prefix.join("part-0.vortex");
    let additions = vec![test_object_meta(new_a.as_ref(), 200)];
    let applied =
        CayenneTableProvider::apply_list_files_cache_additions(&runtime_env, url, &additions);

    assert!(
        !applied,
        "delta-apply must DECLINE a cold cache so the caller evicts + re-LISTs"
    );
    assert!(
        runtime_env
            .cache_manager
            .get_list_files_cache()
            .expect("cache present")
            .get(&key)
            .is_none(),
        "no partial listing may be seeded on a cold-cache miss"
    );
}

// ========================================================================
// [b3 sub-lever 1] Plan-time branch-skip decision tests.
//
// These exercise `int64_branch_disjoint_from_deletions` /
// `branch_int64_pk_range` — the predicate the three `apply_deletion_filter*`
// sites use to decide whether to interpose the filter exec. The invariant:
// a branch may skip the filter ONLY when its EXACT Int64 PK scan window is
// provably disjoint from the deleted-key range. Any uncertainty
// (Inexact/Absent stats, composite PK, non-Int64 PK) must keep the filter.
// `MemorySourceConfig` does not synthesize Exact min/max, so a tiny
// stats-override wrapper supplies the column statistics under test.
// ========================================================================

/// Wraps an inner plan and reports caller-supplied `Statistics`, delegating
/// everything else. Lets a test feed Exact/Inexact/Absent PK bounds into
/// `branch_int64_pk_range` without a real file scan.
#[derive(Debug)]
struct StatsOverrideExec {
    inner: Arc<dyn ExecutionPlan>,
    stats: Statistics,
    properties: Arc<datafusion_physical_plan::PlanProperties>,
}

impl StatsOverrideExec {
    fn new(inner: Arc<dyn ExecutionPlan>, stats: Statistics) -> Self {
        let properties = Arc::clone(inner.properties());
        Self {
            inner,
            stats,
            properties,
        }
    }
}

impl datafusion_physical_plan::DisplayAs for StatsOverrideExec {
    fn fmt_as(
        &self,
        _t: datafusion_physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "StatsOverrideExec")
    }
}

impl ExecutionPlan for StatsOverrideExec {
    fn name(&self) -> &'static str {
        "StatsOverrideExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<datafusion_physical_plan::PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StatsOverrideExec::new(
            Arc::clone(&children[0]),
            self.stats.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion_common::Result<Statistics> {
        Ok(self.stats.clone())
    }
}

/// One-column Int64 child plan whose PK stats carry the given precision.
fn int64_child_with_pk_stats(
    min_value: DFPrecision<ScalarValue>,
    max_value: DFPrecision<ScalarValue>,
) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![
            0_i64,
        ]))],
    )
    .expect("batch");
    let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
        .expect("mem exec");
    let stats = Statistics {
        num_rows: DFPrecision::Absent,
        total_byte_size: DFPrecision::Absent,
        column_statistics: vec![ColumnStatistics {
            null_count: DFPrecision::Absent,
            min_value,
            max_value,
            sum_value: DFPrecision::Absent,
            distinct_count: DFPrecision::Absent,
            byte_size: DFPrecision::Absent,
        }],
    };
    Arc::new(StatsOverrideExec::new(mem, stats))
}

fn exact_i64(v: i64) -> DFPrecision<ScalarValue> {
    DFPrecision::Exact(ScalarValue::Int64(Some(v)))
}

#[test]
fn branch_int64_pk_range_reads_exact_bounds() {
    let plan = int64_child_with_pk_stats(exact_i64(1000), exact_i64(2000));
    assert_eq!(
        CayenneTableProvider::branch_int64_pk_range(&plan, &[0]),
        Some((1000, 2000)),
        "Exact Int64 PK bounds must be read off the plan stats"
    );
}

#[test]
fn disjoint_branch_skips_filter_int64() {
    // Child PK window [1000,2000]; deletes {1,2,3} (range [1,3]) → disjoint.
    let plan = int64_child_with_pk_stats(exact_i64(1000), exact_i64(2000));
    let index = DeletionIndex::from_map(HashMap::from([(1, 1), (2, 1), (3, 1)]));
    assert!(
        CayenneTableProvider::int64_branch_disjoint_from_deletions(&plan, &[0], &index),
        "a PK-disjoint branch must be eligible to skip the deletion filter"
    );
}

#[test]
fn overlapping_branch_keeps_filter_int64() {
    // Child PK window [1,5]; deletes {3} (range [3,3]) → overlapping.
    let plan = int64_child_with_pk_stats(exact_i64(1), exact_i64(5));
    let index = DeletionIndex::from_map(HashMap::from([(3, 1)]));
    assert!(
        !CayenneTableProvider::int64_branch_disjoint_from_deletions(&plan, &[0], &index),
        "an overlapping branch must keep the deletion filter"
    );
}

#[test]
fn inexact_stats_keep_filter() {
    // Inexact bounds → branch_int64_pk_range returns None → never skip, even
    // though [1000,2000] would be numerically disjoint from {1,2,3}. This is
    // the critical conservative-bias regression: uncertainty never skips.
    let plan = int64_child_with_pk_stats(
        DFPrecision::Inexact(ScalarValue::Int64(Some(1000))),
        DFPrecision::Inexact(ScalarValue::Int64(Some(2000))),
    );
    assert_eq!(
        CayenneTableProvider::branch_int64_pk_range(&plan, &[0]),
        None,
        "Inexact PK bounds must yield no range"
    );
    let index = DeletionIndex::from_map(HashMap::from([(1, 1), (2, 1), (3, 1)]));
    assert!(
        !CayenneTableProvider::int64_branch_disjoint_from_deletions(&plan, &[0], &index),
        "Inexact stats must keep the filter (no skip on uncertainty)"
    );
}

#[test]
fn absent_stats_keep_filter() {
    // Absent bounds (e.g. a plain MemorySourceConfig / mode=file with no
    // footer stats) → None → never skip → byte-identical to today.
    let plan = int64_child_with_pk_stats(DFPrecision::Absent, DFPrecision::Absent);
    assert_eq!(
        CayenneTableProvider::branch_int64_pk_range(&plan, &[0]),
        None
    );
    let index = DeletionIndex::from_map(HashMap::from([(1, 1)]));
    assert!(!CayenneTableProvider::int64_branch_disjoint_from_deletions(
        &plan,
        &[0],
        &index
    ));
}

#[test]
fn zero_delete_branch_skips_filter_via_no_range() {
    // An index with no deletions has no deleted_key_range → the disjoint
    // gate returns false (the existing has_deletions() guard already sheds
    // the filter; this locks that the range gate does not misfire).
    let plan = int64_child_with_pk_stats(exact_i64(1000), exact_i64(2000));
    let index = DeletionIndex::empty();
    assert_eq!(index.deleted_key_range(), None);
    assert!(
        !CayenneTableProvider::int64_branch_disjoint_from_deletions(&plan, &[0], &index),
        "no-deletion index yields no range → gate returns false (filter shed by has_deletions guard upstream)"
    );
}

#[test]
fn multi_column_pk_never_skips_at_plan_time() {
    // Composite PK (>1 index) → branch_int64_pk_range returns None → the
    // plan-time gate is INERT for composite keys (the documented no-op; the
    // composite win is the per-batch bloom sweep in sub-lever 2).
    let plan = int64_child_with_pk_stats(exact_i64(1000), exact_i64(2000));
    assert_eq!(
        CayenneTableProvider::branch_int64_pk_range(&plan, &[0, 1]),
        None,
        "multi-column PK must never be skipped at plan time"
    );
}
