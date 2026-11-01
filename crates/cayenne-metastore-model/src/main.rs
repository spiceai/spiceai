// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use cayenne_metastore_model::{cite_check, run, runs};

// Checking clones and drops a state per transition; allocation dominates the
// system allocator's profile.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<(), String> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("list") => {
            for r in runs() {
                println!("{r:?}");
            }
        }
        Some("run") => run(args.get(1).ok_or("Usage: run <name>")?)?,
        Some("quick") => {
            let mut failed = false;
            for name in [
                "remote-maintenance-minimal",
                "read-transaction-widened-1",
                "position-delete",
                "read-rule2",
                "read-widened-rule2-3",
                "read-widened-delivery-1",
                "cold-promotion",
                "read-progress",
                "read-same-key",
                "source-positions",
                "commits-restart-turnover",
                "unknown-action-refusal",
                "naming",
                "object-names",
                "reads-linearizable",
                "reads-bounded",
                "table-seq",
                "delete-coverage",
                "pk-index-checkpoint",
                "seq-order-2-exhaust",
                "seq-order-two-writers-2-exhaust",
                "commits-retry",
                "commits-retry-expiry",
                "standby",
                "commits-etag",
                "commits-commit-focus-copy-2-exhaust",
                "commits-storage-2-exhaust",
                "window-storage-6-exhaust",
                "concurrent-floor-aligned-6-exhaust",
                "retention",
                "window-txn-0-exhaust",
                "concurrent-txn-overlap-0-exhaust",
                "concurrent-txn-disjoint-0-exhaust",
            ] {
                if let Err(error) = run(name) {
                    eprintln!("{error}");
                    failed = true;
                }
            }
            if failed {
                return Err("Unexpected outcomes; see RESULT lines".into());
            }
        }
        Some("all") => {
            let mut failed = false;
            for r in runs() {
                if let Err(e) = run(r.name) {
                    eprintln!("{e}");
                    failed = true;
                }
            }
            if failed {
                return Err("Unexpected outcomes; see RESULT lines".into());
            }
        }
        Some("cite-check") => {
            let mut docs = Vec::new();
            for path in &args[1..] {
                let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
                docs.push((path.clone(), text));
            }
            let problems = cite_check(&docs);
            for p in &problems {
                println!("{p}");
            }
            if !problems.is_empty() {
                return Err(format!("{} citation problems", problems.len()));
            }
            println!("CITATIONS ok: {} documents", docs.len());
        }
        _ => {
            return Err(
                "Usage: cayenne-metastore-model list | run <name> | quick | all | cite-check <doc>…"
                    .into(),
            );
        }
    }
    Ok(())
}
