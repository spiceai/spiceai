---
name: verify-integer-overflow
description: Reproduce BIGINT SUM / + / - / * overflow on a running spiced (file, arrow, cayenne) and compare trunk wrap vs branch error.
---

# Verify integer overflow errors

Use this when checking that `SUM` / `+` / `-` / `*` on integers fail instead of wrapping. The
runtime path under test is the one `spiced` actually serves (HTTP `/v1/sql` and Flight), not a
unit-test `SessionContext`.

## What "working" means

On a built `spiced` from this branch, each of `t_fed`, `t_arrow`, and `t_cayenne` must:

1. Return an error whose text contains `overflow` for `SELECT SUM(big) FROM <table>` (never a number).
2. Return an error whose text contains `overflow` for `SELECT big + 1 FROM <table> WHERE id = 1`.
3. Return `3` for `SELECT SUM(big) FROM <table> WHERE id IN (3, 4)`.
4. Return an error whose text contains `overflow` for `SELECT SUM(DISTINCT big) FROM <table>`.
5. Return `3` for `SELECT SUM(DISTINCT big) FROM <table> WHERE id IN (3, 4)`.
6. Return an error whose text contains `overflow` for window `SUM(DISTINCT big)` over `id IN (1, 3)`.

On trunk, the overflow queries return a wrapped integer and no error.

## Layout

The helper `scripts/verify.sh` in this skill directory:

1. Writes a 4-row parquet (`id`, `big`, `i`) next to a spicepod.
2. Refuses to start if `http://${HTTP}/v1/ready` already responds, so a leftover
   instance cannot be queried as this `SPICED`.
3. Starts `spiced --http 127.0.0.1:18490 --flight 127.0.0.1:18491` in that directory.
4. Polls `GET /v1/ready` (bounded loop; each request uses `--max-time 2`).
5. Posts each SQL to `/v1/sql` and prints the body.

```bash
# After `cargo build -p spiced --no-default-features` (file / arrow / cayenne are always linked)
export SPICED=/workspace/target/debug/spiced
/workspace/.cursor/skills/verify-integer-overflow/scripts/verify.sh /tmp/integer-overflow-verify
```

`SPICED` may be relative (`target/debug/spiced`); the script resolves it to an
absolute path before `cd` into the spicepod directory.

Do not kill an unrelated `spiced`. The script records its own PID and stops only that process.

## Trunk vs branch

Run the same script against a `spiced` built from `trunk` (before) and from this branch (after).
Keep the command, the commit SHA, and the verbatim HTTP bodies. A unit-test wrap on a vanilla
`SessionContext` is supporting evidence for the DataFusion kernel; it does not replace the
running-`spiced` check.

## Ports

| Endpoint | This skill |
| --- | --- |
| HTTP `/v1/sql`, `/v1/ready`, `/v1/datasets?status=true` | `127.0.0.1:18490` |
| Arrow Flight | `127.0.0.1:18491` |
