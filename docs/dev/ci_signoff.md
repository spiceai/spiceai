# CI Sign-off (developer attestation)

Spice runs the fast quality checks on the **developer's machine**, not on every
pull-request push. You run one command locally to attest your change, and a
single lightweight check validates that attestation on the PR. The heavy, full
test suite then runs **once in the merge queue**, on the actual merged result,
as the required gate.

This is our take on 37signals' [move of CI back to developer
machines](https://world.hey.com/dhh/we-re-moving-continuous-integration-back-to-developer-machines-3ac6c611),
built on the same mechanism as their open-source
[`basecamp/gh-signoff`](https://github.com/basecamp/gh-signoff). The difference:
their sign-off is the whole gate; ours only gates *entry* to the merge queue, so
the full suite still runs on the merged code before it can reach `trunk`.

## Why

Previously every PR push ran the whole suite — lint, build, unit tests,
integration tests, E2E — and then the merge queue ran much of it *again* on
merge. That is slow feedback and a lot of duplicated CI. With sign-off:

- **Fast local feedback.** `make signoff` runs lint + unit tests on your
  hardware, which is typically faster than waiting for a remote runner.
- **No duplicated CI.** The full suite runs once, in the queue, on the merged
  commit — not on every push and again on merge.
- **Same safety.** Nothing reaches `trunk` without the full suite passing on the
  exact commit that will be merged.

## The flow

```
        ┌── you ───────────────┐      ┌── GitHub ──────────────────────────────┐
        │                      │      │                                        │
  edit → commit → push → make signoff │→ Attestation check ─(green + review)─┐  │
        │        (lint+test, posts    │  (validates the sign-off on the PR)  │  │
        │         a `signoff` status) │                                      ▼  │
        │                      │      │                              merge queue │
        │                      │      │                  full suite runs on the  │
        │                      │      │                  merged result (required)│
        │                      │      │                                      │   │
        │                      │      │                          all green → merge to trunk
        └──────────────────────┘      └────────────────────────────────────────┘
```

## For contributors

### Sign off on your change

From a clean checkout, with your branch pushed and up to date:

```bash
make signoff          # runs `make lint-rust` + `make build-cli nextest`, then attests
```

On success it posts a `signoff` commit status on your current `HEAD`. Open or
refresh the PR and the **Attestation** check turns green, which — together with a
review — lets a maintainer add the PR to the merge queue.

The sign-off is bound to the **exact commit** you pushed. If you push a new
commit, the old sign-off no longer applies and you must run `make signoff` again.

Options:

```bash
scripts/signoff -f            # sign off even with an uncommitted/unpushed tree
scripts/signoff --no-verify   # attest without running the checks (honor system)
scripts/signoff status        # is HEAD signed off?
scripts/signoff --help        # full usage
```

### "No developer sign-off found for &lt;sha&gt;"

The Attestation check couldn't find a green `signoff` status for the PR's head
commit. Make sure the commit under review is pushed, then run `make signoff`
again. A new push always needs a fresh sign-off.

### External contributors (forks)

Posting a commit status requires write access to this repository, so
contributors working from a fork can't sign off on their own PR. A maintainer
reviews the change and signs off on your behalf (or pushes a small follow-up and
signs off). This mirrors the trusted-committer model — the full suite in the
merge queue is still the real gate.

## For maintainers

### What runs where

| Stage | Trigger | Checks |
| --- | --- | --- |
| Local | `make signoff` | `make lint-rust`, `make build-cli nextest` |
| Pull request | `pull_request` | **Attestation** (validates the `signoff` status) + PR hygiene (`enforce-pull-with-spice`) |
| Merge queue | `merge_group` | the full required suite (below) + advisory niche checks |

Required checks in the merge queue (the `trunk` ruleset):

- `Attestation`
- `enforce-pull-with-spice`
- `Rust Lint`
- `Build and Test`
- `Build (release profile)`
- `Integration Tests (part 1/2/3)`
- `ADBC Integration Tests`
- `Features Check`
- `Check Rust Licenses`
- `E2E Test CI` (a summary "gate" job over the whole E2E matrix)

Advisory checks that also run on `merge_group` but don't block (they can be
promoted to required with a gate job later): `integration tests (llms)`,
`Elasticsearch Integration Tests`, `Helm Lint`.

`Attestation` is the only status produced on `pull_request`, so it (plus reviews)
gates entry to the queue. Every required check is produced on `merge_group`, so
the queue enforces the full suite on the merged commit. `check_changes` still
lets docs-only merges skip the build and report success.

### Enabling it (one-time rollout)

Because this changes the checks that gate `trunk`, and the gating PR itself stops
producing the old PR checks, roll it out deliberately:

1. Review and merge the PR that adds `scripts/signoff` and moves the workflows to
   `merge_group`. That PR intentionally no longer produces the old required
   checks (`Rust Lint`, `Build and Test`, …) on the pull request, so merge it
   with an admin merge (or briefly relax the `trunk` required checks), since the
   full suite still runs for it in the merge queue.
2. Immediately run `scripts/signoff install --yes` so `trunk` requires
   `Attestation` plus the full merge-queue suite.
3. From then on, contributors run `make signoff`, the PR gates on `Attestation`,
   and the queue runs the full suite.

### Configure the ruleset

The required-checks list lives in the `trunk` ruleset and is applied with the
same script developers use:

```bash
scripts/signoff check          # show what trunk currently requires
scripts/signoff install        # dry run: preview the required-checks change
scripts/signoff install --yes  # apply it (needs admin on the repo)
```

`install` reads the current ruleset and swaps only the required-status-checks
list — the merge-queue settings, required approvals, and other rules are left
untouched. The single source of truth for the required list is the
`REQUIRED_CHECKS` array in [`scripts/signoff`](/scripts/signoff); if you add a
required merge-queue check, add its (stable) job name there and re-run `install`.

> **Every required check must be produced by a workflow that triggers on
> `merge_group`.** A required check that never runs in the queue will stall it.
> Matrix workflows (E2E, LLMs) use a summary "gate" job with a stable name so one
> ruleset entry can require the whole matrix.

### Rollback

Sign-off is enabled by the ruleset, not the workflows, so you can revert the gate
without touching code: edit the `trunk` ruleset's required status checks back to
the previous list (drop `Attestation` and re-add the heavy checks) and re-add the
`pull_request` triggers to the workflows in `.github/workflows/`. Because the
heavy jobs still exist and still run on `merge_group`, the queue keeps working
throughout.
