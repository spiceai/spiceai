# File-connector verification map

This directory is the maintained source for verifying user-visible file-connector behavior in `spiced`. Read this index before driving, then use the matching feature file as the recipe.

## Baseline preconditions

- Build `spiced` with `CC=cc CXX=g++ cargo build -p spiced --no-default-features`.
- Seed an isolated work directory with `helpers/verify-file-connector.sh seed`.
- Launch that work directory's spicepod and wait for `GET /v1/ready` → `200` / `ready`.
- Run `helpers/verify-file-connector.sh doctor` and require the pid, binary, four datasets, and a reachable `/v1/ready`. A transport failure is refused (`000` / curl error). HTTP 503 after an expected refresh Error is still HTTP and is accepted.
- Never drive an instance this run did not start.

## Driving conventions

- HTTP SQL is the harness: `POST /v1/sql` with aliased `sum(amount_cents)` and `sum(user_id)`.
- Federated datasets (`csv_fed`, `json_fed`, `json_array_fed`, `json_ptr_fed`) see file rewrites on the next query.
- Arrow-accelerated datasets (`csv_acc`, `json_acc`, `json_array_acc`, `json_ptr_acc`) see file rewrites only after `POST /v1/datasets/<name>/acceleration/refresh` completes.
- Capture `/v1/datasets?status=true` and the `spiced` log at every step.
- Restore the baseline files at the end of a mutating journey. Keep proof artifacts.

## Proof and skip reporting

- Proof is the query response, dataset status, and matching `WARN` / refresh-error log lines — not a unit test.
- A refresh that returns `201` is not done; poll status until it leaves `Refreshing`.
- A refresh POST that fails transport or returns non-2xx is not accepted; stop before polling so a leftover `Ready` or an older task-history row cannot look like completion.
- After a refresh is expected to fail, `/v1/ready` may be `503`. Still query over HTTP.
- Report an unreachable path with the command and the unmet precondition.

## Features

- [CSV file connector](./csv-file-connector.md) covers header reorder, column rename, restore, cluster codec name-binding (including listed `FileGeneration`), and mid-scan generation pinning (If-Match when an ETag is listed) on federated and Arrow-accelerated CSV.
- [JSON file connector](./json-file-connector.md) covers object-key reorder, column rename, restore, populated-to-empty, an unregistered key that changes type after registration, and a flattened object that becomes null, on federated and Arrow-accelerated JSONL, `file_format: json` (array), and `json_pointer`.
