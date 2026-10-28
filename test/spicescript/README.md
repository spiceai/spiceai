# Scenario scripts

The YAML scenarios that `spicescript` runs against a prebuilt `spiced`. The
harness, the script format, every step and how to run them are described in
[`tools/spicescript/README.md`](../../tools/spicescript/README.md).

```bash
cargo build -p spicescript
target/debug/spicescript --jobs 8 --spiced trunk=target/release/spiced test/spicescript
```

## Layout

| Path | Holds |
| --- | --- |
| `*.yaml` | Scripts that pass; CI runs them against the commit's `spiced`. |
| `releases.json` | The Spice releases scripts upgrade from: `from: X.Y.x` resolves to the newest X.Y release listed. Add each OSS release when it ships. Not a script. |
| `variants.yaml` | The shared variants scripts name instead of writing the patch (`cayenne: cayenne-file`), for every script here and in the directories below. Not a script. |
| `known-failing/<group>/` | Scripts that reproduce open bugs, grouped by the component a fix would touch (one issue per group). A script moves up to `test/spicescript/` once its bug is fixed. |
| `snapshots/` | Scripts that need a `spiced` built with the `snapshots` feature. |
| `spicebench/` | Scripts that need the `spicebench` feature, such as the Cayenne catalog connector; `spicebench/known-failing/<group>/` holds those that reproduce open bugs. CI runs neither: it does not pass `spicebench/` to `spicescript`, and its `spiced` is built without the feature. |
| `external/` | Scripts that need data from outside the repository, such as SQLite's sqllogictest corpus. |
| `corpus/` | SQL corpora that `corpus` steps run, and the Python scripts that generate the literal matrices. |
| `seed/` | Source fixtures loaded by `source-exec: { file }`. |
| `slt/`, `slt-sqlite/` | sqllogictest files. |
| `recordings/` | Run times per script (`durations.json`), in every directory with scripts, and the expected failures (`expected-failures.json`). |
