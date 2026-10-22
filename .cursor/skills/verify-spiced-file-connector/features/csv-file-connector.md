# CSV file connector

A CSV file dataset with `has_header` true binds columns by header name. Reordering columns keeps values with their names. Renaming a registered column returns `NULL` on a federated read (one `WARN`) and fails an acceleration refresh so the last loaded data is kept.

## Sub-features

- `csv-reorder` rewrites the same rows with columns swapped and keeps `sum(amount_cents)=300`, `sum(user_id)=30`.
- `csv-rename` renames `amount_cents` to `amt`. Federated reads return `NULL` for `amount_cents` and one `WARN`. An Arrow refresh fails; status is `Error`; accelerated sums stay `300`/`30`.
- `csv-restore` puts the original header back. Federated sums return; a refresh succeeds; status is `Ready`.

## How to get to it (user POV)

- Configure a file-connector dataset `from: file:data/events.csv` with `file_format: csv`.
- Query it with `POST /v1/sql`.
- For the accelerated copy, call `POST /v1/datasets/csv_acc/acceleration/refresh` after the file changes.
- Inspect status with `GET /v1/datasets?status=true`.

## Driving it with verify-file-connector.sh

Preconditions:

- `spiced` is healthy at `http://127.0.0.1:18090` (`GET /v1/ready` is `200` / `ready`).
- `doctor` lists `csv_fed` and `csv_acc`.
- `data/events.csv` is the baseline (`event_id,user_id,region,amount_cents,ts` with rows summing to `300`/`30`).

- **Baseline sums.** `POST /v1/sql` `SELECT sum(amount_cents) AS amount_cents, sum(user_id) AS user_id FROM csv_fed` and the same for `csv_acc`. Both return `300`/`30`. Both datasets are `Ready`.
- **Reorder columns.** Rewrite `data/events.csv` with header `event_id,amount_cents,region,user_id,ts` and the same values under those names. Query `csv_fed` immediately. Refresh `csv_acc`, then query it. Both stay `300`/`30`. Status stays `Ready`.
- **Rename the amount column.** Rewrite the header to `event_id,user_id,region,amt,ts`. Query `csv_fed`: `amount_cents` is `null`, `user_id` is `30`. The log has one `WARN` naming `'amount_cents'` and dataset `'csv_fed'`. Refresh `csv_acc`: the refresh ends in `Error` with a message naming `'amount_cents'`. Query `csv_acc`: sums are still `300`/`30`.
- **Restore.** Rewrite the baseline header and rows. Query `csv_fed` (`300`/`30`). Refresh `csv_acc` until `Ready`. Query `csv_acc` (`300`/`30`).
- **Proof.** Keep `sql-*-csv_*.json`, `datasets-*.json`, `refresh-*-csv_acc.*`, and the matching `WARN` / error lines from `spiced.log`.

## Gotchas

- Do not treat an open port as ready. Wait for `/v1/ready`.
- A positional reader (the pre-fix runtime) swaps `300`/`30` after reorder and keeps serving the fourth column as `amount_cents` after rename. Those swapped or stale numbers are the trunk bug, not a pass.
- Two datasets share `data/events.csv`. Rewrite the file once; query federated, then refresh accelerated.
- `POST .../refresh` returns before the refresh finishes. Poll status.
- After `csv_acc` is `Error`, `/v1/ready` may be `503`. Federated queries still run.
