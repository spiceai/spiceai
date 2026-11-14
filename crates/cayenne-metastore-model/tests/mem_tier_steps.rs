// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use cayenne_metastore_model::{MemTierStep, mem_tier_steps};

#[expect(
    clippy::expect_used,
    reason = "test traces must enable every listed action"
)]
fn trace(run: &str, actions: &[&str]) -> Vec<MemTierStep> {
    mem_tier_steps(
        run,
        &actions.iter().map(|s| (*s).into()).collect::<Vec<_>>(),
    )
    .expect("enabled model trace")
}

#[test]
fn exact_debug_actions_and_disabled_steps() {
    assert!(mem_tier_steps("mem-tier-seal-delete-only", &["Upsert(0,10)".into()]).is_some());
    assert_eq!(mem_tier_steps("unknown", &[]), None);
    assert_eq!(mem_tier_steps("seq-order-2-exhaust", &[]), None);
    for action in ["CkStage", "Upsert(0, 99)", "Upsert(2, 10)", "Garbage"] {
        assert_eq!(
            mem_tier_steps("mem-tier-seal-delete-only", &[action.into()]),
            None
        );
    }
}

#[test]
fn rewrite_then_seal_reproduces_current_defect_and_candidate_avoids_it() {
    let actions = [
        "FullRewriteCapture",
        "FullRewrite",
        "Upsert(0, 10)",
        "SealCapture",
        "SealStage",
        "SealEnd",
        "Crash",
        "Recover",
    ];
    let current = trace("mem-tier-current-3-exhaust", &actions);
    let candidate = trace("mem-tier-seal-delete-only", &actions);
    assert_eq!(current[5].live_rows, vec![(0, 10), (1, 2)]);
    assert_eq!(current[5].recovered_rows, vec![(0, 1), (0, 10), (1, 2)]);
    assert_eq!(current[5].durable_delete_vectors, vec![[(2, 4), (0, 0)]]);
    assert_eq!(candidate[5].durable_delete_vectors, vec![[(2, 0), (0, 0)]]);
    assert_eq!(current[7].live_rows, current[5].recovered_rows);
    assert!(!current[6].available);
    assert_eq!(candidate[5].recovered_rows, vec![(0, 10), (1, 2)]);
    assert_eq!(candidate[7].live_rows, candidate[5].reference_rows);
    println!("CURRENT_SEAL_STEP {:?}", current[5]);
    println!("CANDIDATE_SEAL_STEP {:?}", candidate[5]);
}

#[test]
fn checkpoint_flush_before_publish_and_replay_tail() {
    let steps = trace(
        "mem-tier-seal-delete-only",
        &[
            "Upsert(0, 10)",
            "CkCapture",
            "CkPrecompute",
            "Upsert(0, 11)",
            "CkStage",
            "Flush",
            "Crash",
            "Recover",
            "Upsert(0, 10)",
            "Upsert(0, 11)",
        ],
    );
    assert_eq!(steps[5].recovered_rows, vec![(0, 11), (1, 2)]);
    assert_eq!(steps[9].live_rows, vec![(0, 11), (1, 2)]);
    assert!(steps[9].available);
    // Inserts, snapshot and deletes land in the same record, including when
    // another operation's flush commits the checkpoint before its own end.
    let record = steps[5].durable_records.last().expect("checkpoint record");
    for name in ["Deletes(", "Snapshot(", "Inserts("] {
        assert!(record.iter().any(|a| a.starts_with(name)));
    }
}

#[test]
fn delete_only_checkpoint_and_ack_reopen() {
    let steps = trace(
        "mem-tier-seal-delete-only",
        &[
            "Delete(0)",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "CkPublish",
            "CkEnd",
            "Crash",
            "Recover",
        ],
    );
    assert_eq!(steps[7].live_rows, vec![(1, 2)]);
    assert_eq!(steps[7].reference_rows, vec![(1, 2)]);
    assert!(
        steps[5]
            .durable_records
            .iter()
            .flatten()
            .all(|a| !a.starts_with("Snapshot("))
    );
}

#[test]
fn deferred_capture_reservation_is_not_a_durable_record() {
    let steps = trace(
        "mem-tier-seal-delete-only",
        &["Delete(0)", "CkCapture", "Crash", "Recover", "Delete(0)"],
    );
    assert_eq!(steps[1].durable_records, steps[0].durable_records);
    assert_eq!(steps[4].live_rows, vec![(1, 2)]);
}

#[test]
fn split_staging_is_observable_and_bake_rewrites_protected_rows() {
    let steps = trace(
        "mem-tier-no-one-call",
        &[
            "Upsert(0, 10)",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "Flush",
            "CkStageRest",
            "CkPublish",
            "CkEnd",
            "Bake",
        ],
    );
    assert!(
        steps[4]
            .durable_records
            .iter()
            .flatten()
            .all(|a| !a.starts_with("Inserts("))
    );
    assert_eq!(steps[7].live_rows, steps[8].live_rows);
    assert_ne!(steps[7].durable_records, steps[8].durable_records);
}

#[test]
fn omitted_records_and_late_stamp_controls_actually_change_the_checkpoint() {
    let actions = [
        "Upsert(0, 10)",
        "CkCapture",
        "CkPrecompute",
        "CkStage",
        "Flush",
    ];
    let normal = trace("mem-tier-seal-delete-only", &actions);
    let omitted = trace("mem-tier-no-insert-records", &actions);
    assert!(
        normal[4]
            .durable_records
            .iter()
            .flatten()
            .any(|a| a == "Inserts([0], 4)")
    );
    assert!(
        omitted[4]
            .durable_records
            .iter()
            .flatten()
            .all(|a| !a.starts_with("Inserts("))
    );
    let late = trace(
        "mem-tier-late-stamp",
        &[
            "Upsert(0, 10)",
            "CkCapture",
            "CkPrecompute",
            "Delete(0)",
            "CkStage",
            "Flush",
        ],
    );
    assert!(
        late[5]
            .durable_records
            .iter()
            .flatten()
            .any(|a| a == "Inserts([0], 7)")
    );
    assert_eq!(late[5].recovered_rows, vec![(1, 2)]);
}

#[test]
fn redelivery_repairs_the_delete_after_a_late_seal_reservation() {
    let steps = trace(
        "mem-tier-no-capture-reservation",
        &[
            "Upsert(0, 10)",
            "SealCapture",
            "Delete(0)",
            "SealReserve",
            "SealStage",
            "SealEnd",
            "Crash",
            "Recover",
            "Delete(0)",
        ],
    );
    assert_eq!(steps[5].acknowledged_position, 1);
    assert_eq!(steps[5].recovered_rows, vec![(1, 2)]);
    assert_eq!(steps[8].live_rows, vec![(1, 2)]);
    assert!(
        steps[5]
            .durable_records
            .iter()
            .flatten()
            .any(|a| { a == "Inline([Row { key: 0, value: 10, seq: 6 }])" })
    );
}

#[test]
fn initial_protected_rows_and_fenced_rewrite_durable_cleanup() {
    let steps = trace(
        "mem-tier-seal-delete-only",
        &[
            "Upsert(0, 10)",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "CkPublish",
            "CkEnd",
            "FullRewriteCapture",
            "FullRewrite",
            "Crash",
            "Recover",
        ],
    );
    assert!(steps[0].durable_current_rows.is_empty());
    assert_eq!(
        steps[0].durable_protected_rows,
        vec![(1, vec![(0, 1), (1, 2)])]
    );
    assert!(!steps[5].durable_delete_vectors.is_empty());
    assert_eq!(steps[5].durable_insert_records, [4, 0]);
    assert!(steps[7].durable_protected_rows.is_empty());
    assert!(steps[7].durable_delete_vectors.is_empty());
    assert_eq!(steps[7].durable_insert_records, [0, 0]);
    assert_eq!(steps[9].live_rows, vec![(0, 10), (1, 2)]);
}

#[test]
fn rewrite_preserves_post_capture_checkpoint_and_deletes() {
    let steps = trace(
        "mem-tier-seal-delete-only",
        &[
            "FullRewriteCapture",
            "Upsert(0, 10)",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "CkPublish",
            "CkEnd",
            "FullRewrite",
            "Crash",
            "Recover",
        ],
    );
    assert_eq!(steps[7].durable_protected_rows.len(), 1);
    assert_eq!(steps[7].durable_protected_rows[0].0, 4);
    assert_eq!(steps[7].durable_delete_vectors, vec![[(2, 4), (0, 0)]]);
    assert_eq!(steps[7].durable_insert_records, [4, 0]);
    assert_eq!(steps[9].live_rows, vec![(0, 10), (1, 2)]);
}

#[test]
fn bake_prune_reinsert_keeps_source_rows_and_standalone_records() {
    let actions = [
        "Upsert(1, 10)",
        "Delete(0)",
        "CkCapture",
        "CkPrecompute",
        "CkStage",
        "CkPublish",
        "CkEnd",
        "Bake",
        "Upsert(0, 12)",
        "CkCapture",
        "CkPrecompute",
        "CkStage",
        "CkPublish",
        "CkEnd",
    ];
    let candidate = trace("mem-tier-seal-delete-only", &actions);
    let control = trace("mem-tier-no-insert-records", &actions);
    assert_eq!(candidate[13].recovered_rows, vec![(0, 12), (1, 10)]);
    assert_eq!(control[13].live_rows, vec![(0, 12), (1, 10)]);
    assert_eq!(control[13].recovered_rows, control[13].reference_rows);
    assert_ne!(control[13].durable, candidate[13].durable);
    assert_eq!(control[13].durable_insert_records, [0, 0]);
    println!(
        "BAKE_REINSERT candidate_live={:?} candidate_recovered={:?} control_live={:?} control_recovered={:?} reference={:?}",
        candidate[13].live_rows,
        candidate[13].recovered_rows,
        control[13].live_rows,
        control[13].recovered_rows,
        control[13].reference_rows
    );
}

#[test]
fn reinsert_vector_preserves_rows_without_a_standalone_record() {
    let actions = [
        "Delete(1)",
        "Delete(0)",
        "CkCapture",
        "CkPrecompute",
        "CkStage",
        "Upsert(0, 12)",
        "CkPublish",
        "CkEnd",
        "CkCapture",
        "CkPrecompute",
        "CkStage",
        "CkPublish",
        "CkEnd",
    ];
    let baseline = trace("mem-tier-seal-delete-only", &actions);
    let control = trace("mem-tier-no-insert-records", &actions);
    for step in &baseline {
        assert_eq!(step.live_rows, step.reference_rows);
        assert_eq!(step.recovered_rows, step.reference_rows);
    }
    assert_eq!(control[11].live_rows, control[11].reference_rows);
    assert_eq!(control[12].recovered_rows, control[12].reference_rows);
    assert_eq!(control[12].durable_insert_records, [0, 0]);
    assert_eq!(control[12].reference_rows, vec![(0, 12)]);
    println!(
        "MISSING_INSERT recovered={:?} reference={:?}",
        control[12].recovered_rows, control[12].reference_rows
    );
}

#[test]
fn split_control_metadata_is_partial_even_when_vectors_preserve_rows() {
    let actions = [
        "Delete(1)",
        "CkCapture",
        "Upsert(0, 11)",
        "Upsert(1, 12)",
        "CkPrecompute",
        "CkStage",
        "CkStageRest",
        "CkPublish",
        "CkEnd",
        "SealCapture",
        "SealStage",
        "SealStageRest",
        "SealEnd",
        "CkCapture",
        "CkPrecompute",
        "CkStage",
        "Flush",
    ];
    let baseline_actions: Vec<_> = actions
        .iter()
        .copied()
        .filter(|a| !matches!(*a, "CkStageRest" | "SealStageRest"))
        .collect();
    let baseline = trace("mem-tier-seal-delete-only", &baseline_actions);
    let control = trace("mem-tier-no-one-call", &actions);
    for step in &baseline {
        assert_eq!(step.live_rows, step.reference_rows);
        assert_eq!(step.recovered_rows, step.reference_rows);
    }
    assert_eq!(control[15].recovered_rows, control[15].reference_rows);
    assert_eq!(control[16].recovered_rows, control[16].reference_rows);
    assert_ne!(
        control[16].durable,
        baseline.last().expect("baseline end").durable
    );
    assert_eq!(control[16].durable_insert_records, [0, 0]);
    assert_eq!(control[16].reference_rows, vec![(0, 11), (1, 12)]);
    println!(
        "SPLIT_MINIMIZED recovered={:?} reference={:?}",
        control[16].recovered_rows, control[16].reference_rows
    );
}

#[test]
fn flush_between_split_stages_after_bake_exposes_partial_metadata() {
    let control = trace(
        "mem-tier-no-one-call",
        &[
            "Upsert(1, 10)",
            "Delete(0)",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "CkStageRest",
            "CkPublish",
            "CkEnd",
            "Bake",
            "Upsert(0, 12)",
            "SealCapture",
            "SealStage",
            "SealStageRest",
            "SealEnd",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "Flush",
        ],
    );
    let baseline = trace(
        "mem-tier-seal-delete-only",
        &[
            "Upsert(1, 10)",
            "Delete(0)",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "CkPublish",
            "CkEnd",
            "Bake",
            "Upsert(0, 12)",
            "SealCapture",
            "SealStage",
            "SealEnd",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "Flush",
        ],
    );
    assert_eq!(control[16].recovered_rows, vec![(0, 12), (1, 10)]);
    assert_eq!(control[17].recovered_rows, control[17].reference_rows);
    assert_ne!(control[17].durable, baseline[15].durable);
    assert_eq!(control[17].durable_insert_records, [0, 6]);
    assert_eq!(baseline[15].recovered_rows, vec![(0, 12), (1, 10)]);
    println!(
        "SPLIT_STAGE before_flush={:?} after_flush={:?} reference={:?}",
        control[16].recovered_rows, control[17].recovered_rows, control[17].reference_rows
    );
}

#[test]
fn sqlite_stage_is_commit_and_flush_is_idempotent() {
    let actions = ["Upsert(0, 10)", "CkCapture", "CkPrecompute", "CkStage"];
    let deferred = trace("mem-tier-seal-delete-only", &actions);
    let sqlite = trace("mem-tier-seal-delete-only-sqlite", &actions);
    assert!(!deferred[3].pending_changes.is_empty());
    assert!(sqlite[3].pending_changes.is_empty());
    assert_eq!(sqlite[3].durable, deferred[3].durable_plus_pending);
    assert_ne!(deferred[3].durable, deferred[3].durable_plus_pending);
    assert_eq!(deferred[3].durable_plus_pending.insert_records, [4, 0]);
    assert_eq!(deferred[3].durable_plus_pending.protected_rows.len(), 2);
    let sqlite = trace(
        "mem-tier-seal-delete-only-sqlite",
        &[
            "Upsert(0, 10)",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "Flush",
            "Flush",
        ],
    );
    assert_eq!(sqlite[3].durable, sqlite[5].durable);
    assert_eq!(sqlite[3].durable_records, sqlite[5].durable_records);
}

#[test]
fn bake_requires_two_inputs() {
    assert!(mem_tier_steps("mem-tier-seal-delete-only", &["Bake".into()]).is_none());
    let steps = trace(
        "mem-tier-seal-delete-only",
        &[
            "Delete(0)",
            "CkCapture",
            "CkPrecompute",
            "CkStage",
            "CkPublish",
            "CkEnd",
        ],
    );
    assert!(!steps[5].bake_enabled);
}

#[test]
fn generated_backend_traces_are_enabled() {
    for (name, text) in [
        (
            "mem-tier-seal-delete-only",
            include_str!("../../cayenne/tests/traces/mem-tier-seal-delete-only.jsonl"),
        ),
        (
            "mem-tier-seal-delete-only-sqlite",
            include_str!("../../cayenne/tests/traces/mem-tier-seal-delete-only-sqlite.jsonl"),
        ),
    ] {
        for (line, json) in text.lines().enumerate() {
            let body = json
                .split_once("\"actions\":[")
                .expect("actions")
                .1
                .split(']')
                .next()
                .expect("array");
            let actions = body
                .split('"')
                .skip(1)
                .step_by(2)
                .map(str::to_string)
                .collect::<Vec<_>>();
            assert!(
                mem_tier_steps(name, &actions).is_some(),
                "{name} line {}",
                line + 1
            );
        }
    }
}
