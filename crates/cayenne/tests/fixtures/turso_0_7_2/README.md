# Turso 0.7.2 on-disk fixtures

These files were written by Turso **0.7.2** (`tools/turso-072-fixture-gen`)
and are opened by `turso_0_7_2_open_compat_test.rs` with Turso **0.8.1**.

Matching format constants is not this check. The test copies each fixture into
a temp directory, opens it with `turso::Builder::new_local` (the same engine
the accelerator, dataset checkpoint, and Cayenne metastore use), and asserts
the rows 0.7.2 wrote.

| Stem | What it stands for | Sidecars |
| --- | --- | --- |
| `accelerator-mvcc` | File-mode accelerator table; rows still in the MVCC `.db-log` (the crash / unclean-shutdown case) | `.db` + `.db-log` |
| `accelerator-checkpointed` | Same table after `PRAGMA mvcc_checkpoint_threshold = 0`; rows live in the B-tree | `.db` only |
| `checkpoint` | `spice_sys_dataset_checkpoint` (Turso dataset checkpoint) | `.db` + `.db-log` |
| `cayenne-metastore` | Cayenne Turso metastore (`cayenne_table` + `cayenne_inlined_data`) | `.db` + `.db-log` |

The `.fixture` suffix keeps the repo `.gitignore` (`*.db`, `*.db-log`) from
dropping the checked-in copies. `MANIFEST.txt` is the expected row values.

## Regenerate

From `tools/turso-072-fixture-gen` (not a workspace member — cargo would
otherwise unify `turso` to 0.8.1):

```bash
cargo run --release
```
