---
name: verify-cayenne-concurrent-update
description: Prove Cayenne autocommit same-key UPDATE does not delete the row. Use against a running or freshly launched spiced, file and memory mode, 2x200 and 16x200.
---

# Verify Cayenne concurrent same-key UPDATE

Silent data loss: concurrent autocommit `UPDATE … SET amount = amount + 1 WHERE id = 50` on a Cayenne `write_mode: acceleration` table returned HTTP 200 with a mix of `count: 1` and `count: 0`, and `id=50` disappeared (`count(*)` 1000 → 999).

## Success

After `N` updates with `T` threads, in both `MODE=file` and `MODE=memory`:

- `id=50` is present
- `count(*)=1000`
- `amount = start + (number of count=1 responses)`
- no statement returns `count: 0` without a conflict error
- every HTTP status is 200, or a 409 with a retryable conflict (no silent 0)

Minimum arms: `2 200` and `16 200`, file and memory.

## Harness

From the repository root, after `pip install pyarrow requests` (or an equivalent venv):

```bash
# File mode (default)
.cursor/skills/verify-cayenne-concurrent-update/30_cayenne_concurrent_update_row_loss.sh 2 200
.cursor/skills/verify-cayenne-concurrent-update/30_cayenne_concurrent_update_row_loss.sh 16 200

# Memory mode
MODE=memory .cursor/skills/verify-cayenne-concurrent-update/30_cayenne_concurrent_update_row_loss.sh 2 200
MODE=memory .cursor/skills/verify-cayenne-concurrent-update/30_cayenne_concurrent_update_row_loss.sh 16 200
```

The script starts its own `spiced` on `8393/50393/9393`, loads 1000 parquet rows into `o_cayenne`, runs the updates, prints before/after, checks the success criteria, and tears down. It generates `data/orders.parquet` next to the script via `00_setup_data.py` on first run.

`spiced` must be the binary under test (`make install-dev` or `cargo build -p spiced`). Set `SPICED=` to pin a binary. Record the commit SHA with every run.

## In-crate gate

The deterministic engine test (no HTTP) must fail on a plan-time source capture and pass on the execution-time lock:

```bash
cargo test -p cayenne --lib --features partition-table-provider \
  an_update_planned_before_a_same_key_write_must_not_delete_the_row \
  concurrent_same_key_autocommit_updates_keep_the_row
```

Watch the gap test fail on trunk first (second pre-built plan leaves `id=7` missing, or value ≠ 12), then pass on the branch (`[(7, 12)]`).

## Record

Keep the verbatim command, SHA, and stdout for each arm (trunk before, branch after). Do not summarize a count without pasting it.