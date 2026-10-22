# JSON file connector

A JSON/NDJSON file dataset binds object keys by name. Reordering keys keeps values with their names. Renaming a registered key returns `NULL` on a federated read (one `WARN`) and fails an acceleration refresh so the last loaded data is kept. Schema inference samples records; the drift check reads every record so a sparse key later in the file is not treated as a rename.

## Sub-features

- `json-reorder` rewrites the same objects with keys in a different order and keeps `sum(amount_cents)=300`, `sum(user_id)=30`.
- `json-rename` renames `amount_cents` to `amt`. Federated reads return `NULL` for `amount_cents` and one `WARN`. An Arrow refresh fails; status is `Error`; accelerated sums stay `300`/`30`.
- `json-restore` puts the original key back. Federated sums return; a refresh succeeds; status is `Ready`.

## How to get to it (user POV)

- Configure a file-connector dataset `from: file:data/events.jsonl` with `file_format: jsonl`.
- Query it with `POST /v1/sql`.
- For the accelerated copy, call `POST /v1/datasets/json_acc/acceleration/refresh` after the file changes.
- Inspect status with `GET /v1/datasets?status=true`.

## Driving it with verify-file-connector.sh

Preconditions:

- `spiced` is healthy at `http://127.0.0.1:18090` (`GET /v1/ready` is `200` / `ready`).
- `doctor` lists `json_fed` and `json_acc`.
- `data/events.jsonl` is the baseline (keys `event_id`, `user_id`, `region`, `amount_cents`, `ts`; sums `300`/`30`).

- **Baseline sums.** `POST /v1/sql` `SELECT sum(amount_cents) AS amount_cents, sum(user_id) AS user_id FROM json_fed` and the same for `json_acc`. Both return `300`/`30`. Both datasets are `Ready`.
- **Reorder keys.** Rewrite each line with `amount_cents` before `user_id`. Query `json_fed`. Refresh `json_acc`, then query it. Both stay `300`/`30`. Status stays `Ready`.
- **Rename the amount key.** Rewrite each line with `amt` instead of `amount_cents`. Query `json_fed`: `amount_cents` is `null`, `user_id` is `30`. The log has one `WARN` naming `'amount_cents'` and dataset `'json_fed'`. Refresh `json_acc`: the refresh ends in `Error` with a message naming `'amount_cents'`. Query `json_acc`: sums are still `300`/`30`.
- **Restore.** Rewrite the baseline objects. Query `json_fed` (`300`/`30`). Refresh `json_acc` until `Ready`. Query `json_acc` (`300`/`30`).
- **Proof.** Keep `sql-*-json_*.json`, `datasets-*.json`, `refresh-*-json_acc.*`, and the matching `WARN` / error lines from `spiced.log`.

## Gotchas

- JSON already binds by name, so a key reorder on trunk is not the bug. The trunk bug is a rename: sampled schema inference cannot prove a key absent, so an Arrow refresh overwrites the column with `NULL` and status stays `Ready`.
- Two datasets share `data/events.jsonl`. Rewrite the file once; query federated, then refresh accelerated.
- `POST .../refresh` returns before the refresh finishes. Poll status.
- After `json_acc` is `Error`, `/v1/ready` may be `503`. Federated queries still run.
- A sparse `amount_cents` that appears only after the first records is not a rename. Do not treat that file as the rename fixture.
