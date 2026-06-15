# Fork branch naming convention

Spice maintains forks of several upstream crates (DataFusion, arrow-rs,
datafusion-ballista, datafusion-federation, iceberg-rust, …). For forks whose
upstream version evolves **independently of DataFusion** — most notably
[`spiceai/iceberg-rust`](https://github.com/spiceai/iceberg-rust) — a single
branch name needs to convey two things at once: which upstream release the
branch tracks, and which DataFusion major it is built against. We frequently
keep more than one of these lines alive simultaneously (e.g. `trunk` on a newer
DataFusion while `release/2.0` stays on the previous one), so the name has to
disambiguate both axes.

## Convention

```
spiceai-<upstream-version>-df-<datafusion-major>
```

- `<upstream-version>` — the upstream crate version the branch tracks
  (e.g. `0.9.1`). When the branch sits on a post-release `main` snapshot, use
  the nearest released version it is at or ahead of.
- `<datafusion-major>` — the DataFusion major the branch builds against
  (e.g. `53`).

### Current `spiceai/iceberg-rust` branches

| Branch | Iceberg | DataFusion | Consumed by |
|---|---|---|---|
| `spiceai-0.9.1-df-53` | 0.9.1 | 53 | `trunk` |
| `spiceai-0.9.1-df-52` | 0.9.1 | 52 | `release/2.0` |

The legacy names `spiceai-0.9.0` (→ `spiceai-0.9.1-df-53`) and `spiceai-52`
(→ `spiceai-0.9.1-df-52`) are retained as aliases pointing at the same commits;
prefer the new names for any new work.

> Note: `spiceai-0.9.0` was a misnomer — that line tracks a *post*-0.9.1
> `main` snapshot (it carries the `arrow/reader` refactor), so its crate
> version was corrected from `0.9.0` to `0.9.1` when it was renamed.

## When upgrading DataFusion

A DataFusion upgrade creates a new `-df-<n>` line for each affected fork. Cut
the new branch from the current line, port the fork's patches, and name it per
the convention above (carrying the same `<upstream-version>` unless the upstream
crate is also being bumped). Forks that are versioned in lockstep with
DataFusion (DataFusion itself, arrow-rs, ballista) keep their existing
`spiceai-<n>` / `spiceai-<n>.<m>` naming.
