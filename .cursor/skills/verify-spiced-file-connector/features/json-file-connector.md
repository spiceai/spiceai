# JSON file connector

A JSON/NDJSON file dataset binds object keys by name. Reordering keys keeps values with their names. Renaming a registered key returns `NULL` on a federated read (one `WARN`) and fails an acceleration refresh so the last loaded data is kept. The drift check reads every record for key presence (not type inference) so a sparse key later in the file is not treated as a rename, and an unregistered key that changes type cannot fail the refresh.

`file_format: json` (array/object) and `json_pointer` use a non-repartitioned scan wrapper. The drift guard must still see those files; a rename on those paths is the same contract as JSONL.

## Sub-features

- `json-reorder` rewrites the same objects with keys in a different order and keeps `sum(amount_cents)=300`, `sum(user_id)=30`.
- `json-rename` renames `amount_cents` to `amt`. Federated reads return `NULL` for `amount_cents` and one `WARN`. An Arrow refresh fails; status is `Error`; accelerated sums stay `300`/`30`.
- `json-restore` puts the original key back. Federated sums return; a refresh succeeds; status is `Ready`.
- `json-empty` rewrites each source to zero records (`[]`, `{"data":[]}`, blank JSONL). Federated sums are `null`/`null`. An Arrow refresh succeeds and clears the acceleration (`Ready`, `null`/`null`). This is not a rename: no records means no missing-column list.
- `json-array` is `file_format: json` over `data/events.json` (a JSON array). Same reorder / rename / restore / empty as JSONL.
- `json-pointer` is `file_format: json` plus `json_pointer: /data` over `data/events_pointer.json` (`{"data":[...]}`). Same reorder / rename / restore / empty as JSONL.
- `json-unregistered-type-change` adds an unregistered key after registration and changes its type (`extra: 1` then `extra: {}`). Federated sums stay `300`/`30`. An Arrow refresh succeeds (`Ready`). The normal scan ignores unregistered fields; the drift probe must not type-merge them.
- `json-flatten-null-parent` with `flatten_json: true` rewrites a nested `user` object to `null` in every record. Federated `user.name` is `NULL`. An Arrow refresh succeeds (`Ready`) instead of treating the flattened key `user` as a rename of `user.name`. Proof is `json_refresh_allows_a_flattened_object_that_becomes_null` and `create_listing_table_allows_json_flatten_null_parent_refresh`. The HTTP helper does not rewrite that fixture.

## How to get to it (user POV)

- Configure a file-connector dataset `from: file:data/events.jsonl` with `file_format: jsonl`, or `from: file:data/events.json` with `file_format: json`, or `from: file:data/events_pointer.json` with `file_format: json` and `json_pointer: /data`.
- Query it with `POST /v1/sql`.
- For the accelerated copy, call `POST /v1/datasets/<name>/acceleration/refresh` after the file changes.
- Inspect status with `GET /v1/datasets?status=true`.

## Driving it with verify-file-connector.sh

Preconditions:

- `spiced` is healthy at `http://127.0.0.1:18090` (`GET /v1/ready` is `200` / `ready`).
- `doctor` lists `json_fed`, `json_acc`, `json_array_fed`, `json_array_acc`, `json_ptr_fed`, and `json_ptr_acc`.
- `data/events.jsonl`, `data/events.json`, and `data/events_pointer.json` are the baseline (keys `event_id`, `user_id`, `region`, `amount_cents`, `ts`; sums `300`/`30`).

- **Baseline sums.** `POST /v1/sql` `SELECT sum(amount_cents) AS amount_cents, sum(user_id) AS user_id FROM <dataset>` for each JSON dataset. All return `300`/`30`. All are `Ready`.
- **Reorder keys.** Rewrite each source with `amount_cents` before `user_id`. Query the federated datasets. Refresh the accelerated datasets, then query them. All stay `300`/`30`. Status stays `Ready`.
- **Rename the amount key.** Rewrite each source with `amt` instead of `amount_cents`. Query each federated dataset: `amount_cents` is `null`, `user_id` is `30`. The log has one `WARN` per federated dataset naming `'amount_cents'`. Refresh each accelerated dataset: the refresh ends in `Error` with a message naming `'amount_cents'`. Query each accelerated dataset: sums are still `300`/`30`.
- **Restore.** Rewrite the baseline objects. Query federated (`300`/`30`). Refresh accelerated until `Ready`. Query accelerated (`300`/`30`).
- **Empty.** Rewrite JSONL to blank lines, `events.json` to `[]`, and `events_pointer.json` to `{"data":[]}`. Query each federated JSON dataset: `null`/`null`. Refresh each accelerated JSON dataset until `Ready`. Query them: `null`/`null`. CSV stays populated (`300`/`30`).
- **Proof.** Keep `sql-*-json_*.json`, `datasets-*.json`, `refresh-*-json_*acc.*`, and the matching `WARN` / error lines from `spiced.log`. The WARN example path starts with `/`.

## Gotchas

- JSON already binds by name, so a key reorder on trunk is not the bug. The trunk bug is a rename: sampled schema inference cannot prove a key absent, so an Arrow refresh overwrites the column with `NULL` and status stays `Ready`.
- Two datasets share `data/events.jsonl`. Rewrite the file once; query federated, then refresh accelerated.
- `POST .../refresh` returns before the refresh finishes. Poll status.
- After `json_acc` is `Error`, `/v1/ready` may be `503`. Federated queries still run.
- A sparse `amount_cents` that appears only after the first records is not a rename. Do not treat that file as the rename fixture.
- An unregistered key that changes type after registration (`extra: 1` then `extra: {}`) is not a rename. The listing test `create_listing_table_allows_json_unregistered_field_type_change_on_refresh` is the proof; the HTTP helper does not rewrite that fixture.
- A flattened object that becomes null (`{"user": {"name": ...}}` → `{"user": null}`) is not a rename. The drift probe skips that null parent of `user.name`; the reconstructed nested struct is nullable so the scan loads `NULL` for the child. The HTTP helper does not rewrite that fixture.
