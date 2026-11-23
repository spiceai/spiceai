# Scenario scripts

The YAML scenarios that `spicescript` runs against a prebuilt `spiced`. The
harness, the script format, every step and how to run them are described in
[`tools/spicescript/README.md`](../../tools/spicescript/README.md).

```bash
cargo build -p spicescript
target/debug/spicescript --jobs 8 --spiced trunk=target/release/spiced test/spicescript
```

The merge queue runs every script here except those tagged `tags: [long]`
(`--exclude-tag long`), plus any `long` or known-failing script the PR adds
or changes. An edit to a script's `tags:` line alone does not count: it moves
the script between runs without changing what it does.
`trunk_tests.yml` runs every script after merge, on trunk. Use
`--exclude-tag long` for a quick local run. Scripts tagged `macos-smoke` also
run against the macOS build, in E2E Test CI's `spicescript: macOS smoke` job;
their sources must be `files` or `mock-http`, since those runners have no
Docker.

## Layout

| Path | Holds |
| --- | --- |
| `*.yaml` | Scripts that pass; CI runs them against the commit's `spiced`. |
| `variants.yaml` | The shared variants scripts name instead of writing the patch (`cayenne: cayenne-file`), for every script here and in the directories below. Not a script. |
| `known-failing/<group>/` | Scripts that reproduce open bugs, grouped by the component a fix would touch (one issue per group). The merge queue runs one only when the PR adds or changes it; `trunk_tests.yml` runs them all after merge, on trunk, and requires each to fail exactly as recorded, and on demand against an E2E Test CI build (`gh workflow run trunk_tests.yml -f run_id=<run>`). A script moves up to `test/spicescript/` once its bug is fixed. |
| `snapshots/` | Scripts that need a `spiced` built with the `snapshots` feature; `snapshots/known-failing/<group>/` holds those that reproduce open bugs. CI runs neither: its `spiced` is built without the feature. |
| `spicebench/` | Scripts that need the `spicebench` feature, such as the Cayenne catalog connector; `spicebench/known-failing/<group>/` holds those that reproduce open bugs. CI runs neither: it does not pass `spicebench/` to `spicescript`, and its `spiced` is built without the feature. |
| `from-release/` | Scripts that only mean something with `--from` a released `spiced`, because they start on behavior the build under test no longer has, such as the Turso Cayenne metastore. CI does not run them: it passes neither `from-release/` nor `--from`. |
| `external/` | Scripts that need data from outside the repository, such as SQLite's sqllogictest corpus. |
| `live/` | Scripts that call a real provider (OpenAI, Anthropic, Databricks, the Hugging Face Hub), with credentials from the environment that each names under `requires-env:`; one whose variables are unset is listed as not run. The default run skips the directory; `trunk_tests.yml`'s `spicescript: live` job mints each token from its GitHub OIDC token right before running them, since the token expires about 5 minutes later, and runs them with `--require-all-env`, so a missing credential fails it. |
| `corpus/` | SQL corpora that `corpus` steps run, and the Python scripts that generate the literal matrices. |
| `seed/` | Source fixtures loaded by `source-exec: { file }`. |
| `slt/`, `slt-sqlite/` | sqllogictest files. |
| `recordings/` | Run times per script (`durations.json`), in every directory with scripts, and the expected failures (`expected-failures.json`). `update_durations.py` rewrites the run times from recent CI runs, which `--partition` balances the CI shards by. |
