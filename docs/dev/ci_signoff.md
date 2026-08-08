# CI Sign-off (developer attestation)

Spice runs the fast quality checks on the **developer's machine**, not on every
pull-request push. You run one command locally to attest your change, and a
single lightweight check — **Attestation** — validates it on the PR (only the
`enforce-pull-with-spice` PR-hygiene check runs alongside it). The heavy, full
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

- **Fast local feedback.** `make signoff` target-lints changed crates first,
  then runs full lint + unit tests on your hardware — faster fail-first than a
  remote runner, and faster than workspace lint alone when a change is wrong.
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

From a clean Git checkout or JJ workspace, with your branch/bookmark pushed
and up to date:

```bash
make signoff          # targeted crate lint + tests → full lint + unit tests, then attests
```

`make signoff` first diffs the branch against `trunk`. If that diff has no
Rust-affecting files ([the list below](#branches-with-no-rust-changes-are-fast-tracked)),
Rust lint/build/unit tests are skipped and the sign-off status is
still posted (docs/YAML-only changes — and for a branch in this
repository **Attestation** fast-tracks the PR anyway, so you don't need to run
this at all; the fast-track is same-repo only, so a docs-only PR from a fork
still needs a maintainer sign-off). Otherwise it maps changed files to workspace
crates and runs, in order:

1. `make lint-rust PACKAGES="…" FEATURES="…"` — lint the crates you touched
2. `make nextest-packages PACKAGES="…" FEATURES="…"` — their unit tests
3. `make lint-rust` — the full workspace lint
4. `make nextest verify-cli` — all unit tests, then assert the `spice` binary exists

Steps 1-2 exist to fail fast: a lint or test failure in the crate you edited is
the likeliest outcome, and step 3 is by far the longest, so covering your own
crates first turns a late failure into an early one.
`SIGNOFF_SKIP_TARGETED_LINT=1` and `SIGNOFF_SKIP_TARGETED_TESTS=1` opt out.
Remote sign-off sets both: fail-fast is worth an extra resolve of the changed
crates' graph only while someone is watching the output, and nobody watches a
self-hosted runner — dispatch `signoff.yml` with `run_targeted_prechecks=true`
to get them back.

**The scoped steps build each crate the way the workspace builds it.** The
features come from a `cargo metadata` resolve of the whole workspace,
package-qualified (`runtime/debezium,…`), rather than from the crate's own
defaults. That matters because a crate's defaults are not what it is ever built
with: `runtime` declares 54 features and **no `default`**, so a bare
`cargo clippy -p runtime` compiles it with *zero* features, while the workspace
resolve gives it 35 — `spiced`'s defaults unify them in. Linting and testing a
configuration no real build produces is what makes a scoped pre-lint fail on code
the full gate accepts, and it guarantees a cache miss against the gate that
follows. The resolve is derived on every run, so there is no feature list to
drift. If `cargo metadata` or `python3` is unavailable the steps fall back to
package defaults rather than failing.

The CLI is built with the same profile as the lint and test passes
(`build-cli-dev`), not a release build: a release build shares no artifacts with
them and would recompile its whole dependency graph just to prove the binary
links. The merge queue's required `Build (release profile)` job builds the CLI,
`spiced`, and the release install for real.

On success it posts a `signoff` commit status on your current `HEAD`. If the
**Attestation** check already ran and failed before the sign-off existed, the
script re-runs that job via the `gh` CLI so it turns green without you having
to open or refresh the PR. (If no run is found yet — e.g. before the PR's
first CI run — or the rerun call fails, it falls back to prompting you to
open/refresh the PR yourself.) That, together with a review, lets a
maintainer add the PR to the merge queue.

The refresh only fires while the commit it signed off is **still the head of an open
PR**. If you pushed while a long sign-off was running, it says so, names the heads
of every open PR that contains the commit, and refreshes nothing:

```
  1111111111aa is not the head of any open PR that contains it (PR heads: 2222222222bb) — not refreshing 'Attestation'.
```

That is deliberate. `pr.yml`'s concurrency group resolves its SHA term to the
literal `any-sha` on a `pull_request` event, so every attempt for a PR shares one
group — re-running the old commit's run would cancel the current head's in-flight
one, and a re-run evaluates its *original* event payload, so the verdict it
published would be for the superseded commit. The stale sign-off had nothing to
propagate anyway (its status is on a commit no longer under review), so skipping
loses nothing. Sign off again on the new head.

The sign-off is normally bound to the **exact commit** you pushed. If you push a
code change, the old sign-off no longer applies and you must run `make signoff`
again. The only exception is merging the PR's base branch. **Attestation** walks
up to 100 successive merge commits on the first-parent chain to find a sign-off.
`HEAD` must merge the current base commit, each older merged base must appear in
order on the current base branch's first-parent history, and every merge tree
must exactly match Git's conflict-free automatic result. A conflict resolution,
amended merge, octopus merge, or merge from another branch still requires a new
sign-off.
`scripts/signoff status` reports only a commit's own status; the inheritance
check runs in the PR's **Attestation** workflow.

A sign-off that **failed** on `HEAD` disqualifies that commit. **Attestation**
rejects it before anything else, so an earlier sign-off cannot be inherited past
it and the fast-track paths below cannot route around it: a merge of the base
can be textually clean and still be semantically broken, and a sign-off run
against the merge is exactly what finds that out. Fix what it reported and run
`make signoff` again — the new status replaces the failed one. A `pending`
status is not a verdict and does not block inheritance.

### Reverts are fast-tracked

A pull request that only reverts commits already on the base branch passes
**Attestation** automatically — no local `make signoff` needed. Undoing a change
that already landed (and so already passed the full suite on its way in) is
low-risk, and the merge queue still re-runs the whole suite on the merged
result before it can reach `trunk`.

The **Attestation** workflow fast-tracks a PR when **all** of the following hold:

- Every commit the PR introduces is a Git revert — it carries the
  `This reverts commit <sha>.` footer that `git revert` writes. A single
  non-revert commit disqualifies the PR and it needs a normal sign-off.
- Each reverted commit is already on the base branch (an ancestor of the base
  tip). You can't fast-track "reverting" something that never merged.
- The PR is from the same repository, not a fork. Fork contributors can't
  self-sign-off anyway (posting a status needs write access), so a fork revert
  still takes a maintainer sign-off — this keeps the trusted-committer boundary.

To fast-track, just open the revert PR the normal way (e.g. `git revert <sha>`
keeps the footer intact); the check passes on its own. If you amend a revert
with extra changes, or squash the footer out of the message, it falls back to
requiring a sign-off.

### Branches with no Rust changes are fast-tracked

A pull request whose diff contains no Rust-affecting path passes **Attestation**
automatically. Those are exactly the branches `make signoff` skips every Rust
check for, so requiring it would attest a run that did no work. Docs, workflow
YAML, and spicepods land here. Renames are checked on both sides, so moving a
`.rs` file to a non-Rust path still requires a sign-off, and a diff at GitHub's
3000-file listing cap is treated as unknown rather than assumed clean. Same-repo
only, like the other fast-tracks.

Rust-affecting means Rust sources, the Cargo/toolchain config, **and the config
files the gate itself reads**:

- `*.rs`, `Cargo.toml`, `Cargo.lock`, `rust-toolchain[.toml]`, `.cargo/`
- `.ci/clippy.toml` (the config `make lint-rust` uses via `CLIPPY_CONF_DIR`) and
  the root `clippy.toml`; `[.]rustfmt.toml`
- `.config/nextest.toml` — retries, slow-test timeouts, test groups
- `layers.toml`, `scripts/check_crate_layers.py`, and
  `scripts/check_rust_gate_paths.py` — the no-compile guards it runs
- the root `Makefile` — it holds every `-Dclippy::…` flag the gate enforces

The merge queue still runs the full suite on the merged result — its
`check_changes` gate applies the same reasoning there.

Three lists encode that set: `RUST_AFFECTING_PATH_PATTERN` in `scripts/signoff`,
`rustAffecting` in `.github/workflows/pr.yml` (which must classify the same
paths), and the `code_changes` filter in `.github/actions/check-code-changes`
(a deliberate superset — it is the shared "did any code change" default, so it
also gates integration and E2E, and it only has to *cover* the set). A path
missing from all three lands on trunk having never been linted, built, or
tested, so `make lint-rust` runs `scripts/check_rust_gate_paths.py`. It derives
what must be gated from what the `lint-rust` recipe reads and from the tracked
config-file names, rather than from a list someone has to remember, and fails
when the three drift. Change them together.

### Dependabot bumps are fast-tracked

A Dependabot pull request that is still exactly the one commit Dependabot pushed
passes **Attestation** automatically — no local `make signoff` needed, so it can
go straight into the merge queue on review. There is no human-authored change to
attest, and the merge queue still runs the whole suite on the merged result
before it can reach `trunk`.

The **Attestation** workflow fast-tracks a PR when **all** of the following hold:

- Dependabot opened the pull request (`dependabot[bot]`).
- The PR introduces exactly one commit.
- That commit is attributed to the `dependabot[bot]` account, was committed by
  GitHub itself (its `web-flow` identity, `noreply@github.com`), and carries a
  verified signature. All three matter: Dependabot's branches live in this
  repository, so anyone with write access can push over them, and an author line
  is plain commit metadata that anyone can write. Signature verification attests
  the *committer*, not the author — so a person with a registered signing key
  could sign a commit they authored under Dependabot's name and have it verify.
  What they cannot do is claim GitHub's own committer identity, which Dependabot
  gets because its commits are created through GitHub's API.
- The PR is from the same repository, not a fork (Dependabot's branches always
  are — this keeps the trusted-committer boundary the same as for reverts).

**A bump that needs a fix needs a sign-off.** If the merge queue rejects the
bump and you push a commit to make it build — or merge the base branch into the
branch — the PR has more than Dependabot's one commit and falls back to
requiring `make signoff` like any other change. (Dependabot rebases its own
branch rather than merging, so routine base updates keep it at a single commit.)

Options:

```bash
scripts/signoff -f            # sign off even with an uncommitted/unpushed tree
scripts/signoff --no-verify   # attest without running the checks (honor system)
scripts/signoff status        # does HEAD have its own sign-off?
scripts/signoff --help        # full usage
```

### Remote sign-off (self-hosted runner)

When you can't (or don't want to) run the checks on your machine, dispatch the
**Remote Sign-off** GitHub Actions workflow on a self-hosted runner:

```bash
make signoff-remote                 # current branch
# equivalent to:
gh workflow run signoff.yml -f branch=<your-branch>
gh run watch --workflow signoff.yml
```

Sign-off runs only where it is accountable: your machine, or the Actions
runner. It deliberately never SSHes into ad-hoc hosts — the LAN lab boxes
double as benchmark machines, and a workspace build there mid-run silently
corrupts the measurement.

The Actions workflow:

0. Resolves the dispatch input to a commit, in a small GitHub-hosted `resolve`
   job, so the sign-off job can key its concurrency group on that commit
1. Checks out your branch (full history) and fetches `trunk`
2. Skips Rust checks when the branch has no Rust-affecting files. The targeted
   pre-lint and unit tests are off here (`run_targeted_prechecks` turns them on,
   with the GitHub compare API as a fallback when merge-base isn't available)
3. Runs full `make lint-rust` + `make nextest verify-cli` when Rust is affected
4. Posts pending → success/failure `signoff` statuses (skipping the pending when
   the commit is already signed off), then re-runs **Attestation** if needed

The checks run under a 353-minute budget, inside a 358-minute job budget, so a
run that overruns fails as a failed step rather than being terminated at the
runner pool's ~360-minute wall (which reports as `cancelled`, with no failed
step and no chance to clean up). A run that ends without a verdict leaves the
commit at `pending` either way, and never at a failure. Where the two endings
differ is only which status the handler finds:

| Ending | Handler | Effect |
| --- | --- | --- |
| Budget expired, runner died | `Resolve an incomplete sign-off` → `clear-pending` | Restates the `pending`, replacing the "in progress" wording with why the run ended |
| Externally cancelled, evicted by a re-dispatch | `Correct the sign-off status…` → `correct-cancelled` | Repairs a `failure`/`error` into `pending`; leaves an already-`pending` status as it is |

Either way, re-dispatch against the same HEAD to try again.

The state stays `pending` because nothing judged the diff. `failure` is the one
state that reads as a code failure: `pr.yml` rejects it, **Attestation** rejects it
on the head commit ([#12362](https://github.com/spiceai/spiceai/issues/12362)), and
a red `signoff` never self-clears, so the commit stays disqualified until someone
re-dispatches by hand ([#12741](https://github.com/spiceai/spiceai/issues/12741)).

`pr.yml` does not reject a `pending` — it is not a verdict — so Attestation decides
on its own terms rather than being forced red by a dead run. For an ordinary head
that means red, because HEAD really is not signed off, pointing at re-running
sign-off instead of at a defect in the diff. A head that `pr.yml` already clears —
a fast-tracked one, or one whose inheritance chain of verified base merges reaches
a `success` — stays green, which is the same latitude a cancelled run has always had.

A dormant `pending` is still distinguishable from a live one: `scripts/signoff
status` reports any non-success as not signed off, and `scripts/signoff mine` takes
its ⟳ from the in-flight run list, not from the status.

That restatement is the workflow's job, not the dying script's. Being signalled is
how a run learns its budget expired: `run_checks` returns `make`'s status and bash
reports a killed child as 128+N, so the script classifies that as reaching **no
verdict** and publishes nothing at all, saying why in its step summary. Treating it
as a check result instead would assert a code failure nothing established — on a
suite the log shows still passing, and over a `signoff=success` an earlier run may
already have earned ([#12518](https://github.com/spiceai/spiceai/issues/12518),
[#12520](https://github.com/spiceai/spiceai/issues/12520)). Declining to write is
what leaves the `pending` for `Resolve an incomplete sign-off` to describe honestly,
and leaves an existing success alone. The correction handlers cannot cover this case
after the fact: both are gated on `cancelled()`, and a budget expiry is a *failed*
step by the design above.

Only one sign-off runs per commit. Both dispatch forms — `-f branch=<branch>` and
`-f pr_number=<N>` — resolve to the same commit before the sign-off job starts,
and its concurrency group is keyed on that commit, so a second dispatch for a
commit already being signed off evicts the first rather than duplicating 1-4
hours of identical work and racing it for the `signoff` status (#12472). Two open
PRs that happen to share a head commit collapse into one run for the same reason.
Dispatching after the branch tip has *moved* is a different commit, so it starts
a fresh run (and the branch-keyed group evicts the run on the stale commit).

Re-dispatching against a HEAD that is *already* signed off leaves that success in
place: the run skips the in-progress `pending` and only replaces the status once
it has a verdict of its own, so a run that never finishes cannot cost you an
attestation. `mine` still shows ⟳ while it runs — that comes from the run list,
not the commit status.

A re-dispatch that *does* fail posts `signoff=failure` and then re-runs
**Attestation** so the required check reflects that verdict. The status alone
would not close the gate — **Attestation** is the required check and `pr.yml`
does not run on commit-status changes — so the failure path forces the re-run
even when the check is already green, which is the one case the success path
deliberately skips. If the status post itself fails, the run says so and leaves
**Attestation** showing the previous sign-off; re-dispatch or push to move it.

Two cases the re-run cannot cover, both reported by the run rather than hidden:
an **Attestation** run that is still in flight may already have read the status
this verdict replaced, so it can finish green and needs re-running by hand; and
a HEAD that only merges the base branch can *inherit* an earlier sign-off, which
the failing status on HEAD does not veto ([#12357](https://github.com/spiceai/spiceai/issues/12357)).

A branch whose sign-off keeps running out of budget is contending for the pool
rather than doing anything wrong. Remote runs already skip the branch-scoped
pre-checks; `-f run_targeted_prechecks=true` adds them back at the cost of a
longer run.

Requires write access to the repository (same as local sign-off — fork
contributors still need a maintainer to sign off). The lab SSH path also needs
SSH key access to the host and `gh` auth on that machine.

### A cancelled Remote Sign-off is not a failed one

`signoff.yml` sets `concurrency: signoff-<pr|branch>` with `cancel-in-progress`, so
re-dispatching sign-off for a branch cancels the run before it. A cancelled job is
killed by signal, which makes the in-flight `run_checks` return non-zero — and the
dying script posts `signoff=failure`, "Sign-off checks failed after Ns", about a
branch nothing actually judged.

So the workflow corrects the record on its way out. When the job ends cancelled it
runs:

```bash
scripts/signoff correct-cancelled [<sha>] [<owner/repo>]
```

which rewrites the status to `pending` — not `success`, because HEAD really is not
signed off, and not `error`, because `pending` is the state Attestation already
describes as "re-dispatch sign-off" rather than as a defect in the diff.

**It rewrites only `failure` and `error`.** A cancelled run can find a legitimate
`success` on the commit two ways, and overwriting either would throw away checks
that passed and cost another 1-4 hour run:

- Its own `Sign off` step completed, and the cancel signal landed in the window
  before the correction ran.
- A second run signed the same commit off. The concurrency group keys on the
  dispatch *input*, so `-f branch=my-branch` and `-f pr_number=123` for that same
  branch are different groups: they do not cancel each other, and either can be
  cancelled after the other has succeeded.

A read failure is likewise not treated as "no status" — that would license the
overwrite the guard exists to prevent. Every path exits 0, so the correction can
never add a second failure to a job that was already cancelled.

If you see `signoff=pending` with "Remote sign-off was cancelled", nothing is
wrong with the branch: dispatch sign-off again. `scripts/test_signoff_cancelled_correction.sh`
covers both directions.

The correction runs through `.trusted-ci/signoff`, so it can only work if that copy
understands the subcommand the workflow calls. Both trusted helpers are therefore read
out of the object database at the workflow file's own commit (`github.sha`) rather than
copied from the checked-out tree — the tree is the *branch under test*, which for a
branch older than the helper is a script the workflow's own call sites have outrun. That
is what left cancelled runs on older branches with a `signoff=failure` they could not
withdraw (#12657). One consequence worth knowing: a branch that changes `scripts/signoff`
or `.github/actions/**` is still signed off with the dispatch ref's version of them. To
exercise a branch's own helper changes, dispatch on that branch —
`gh workflow run signoff.yml --ref <branch> -f branch=<branch>` — which puts the workflow
and the helpers on the same commit again. `scripts/test_signoff_trusted_helpers.sh` covers
the extraction.

### Jujutsu workspaces

`make signoff` also works in non-colocated JJ workspaces (where there is a
`.jj` directory but no `.git` directory). Commit/bookmark your change and push
it normally before signing off:

```bash
jj commit -m "describe the change"
jj bookmark set my-branch -r @-
jj git push --bookmark my-branch
make signoff
```

After `jj commit`, JJ normally leaves an empty working-copy commit (`@`) on top
of the bookmarked commit. The sign-off tool recognizes that layout and attests
the pushed parent (`@-`). If `@` itself is bookmarked and pushed, it attests `@`
instead. In either case, the selected commit must exactly match a bookmark tip
on the configured remote.

The JJ remote defaults to `origin`. Set `SIGNOFF_REMOTE=<remote>` when the PR
branch is pushed elsewhere. Colocated JJ repositories continue to use the Git
path, so existing Git behavior is unchanged.

### "No developer sign-off found for &lt;sha&gt;"

The Attestation check couldn't find an applicable green `signoff` status. It
checks the PR's head commit first, then walks backward through clean, unmodified
base merges on the first-parent chain. Make sure the commit under review is
pushed, then run `make signoff` again. Any new code or manual merge resolution
needs a fresh sign-off.

### "Runner out of disk — checks did not complete"

The sign-off runner's work volume filled up, so the run stopped before finishing
its judgement of your branch. **Re-dispatch it.** If it recurs on the same
runner, that machine needs space reclaimed — `target/` is shared across every
branch that pool signs off, so it grows without bound. If instead it follows
*your branch* from runner to runner, suspect the diff: a new build script,
a dependency bump, or a feature expansion can consume the volume by itself.

Sign-off refuses to start when the volume has less than 25 GiB free, and a run
whose build reports running out of disk is reported as an infrastructure failure
rather than a check failure. Without that, the failure is nearly impossible to
read correctly: the linker dies with `errno=28` thousands of lines after nextest
has already reported every test passing, on a crate the branch never touched,
with no `-->` source pointer anywhere in the log.

The floor is checked twice: once as the job's first step after checkout
(`scripts/signoff preflight-disk`), so an already-full runner is turned away
before the toolchain setup rather than after it, and again inside the run. A stop
at the early step has no commit status to explain itself — the `pending` status is
posted later — so it annotates the run instead.

A remote run watches its own build output for that error, and a watched run's
verdict is final **in both directions**. It has to be: by the time anything
measures free space again, cargo has unlinked the partial binaries it was
writing and the volume can look healthy — and conversely, on a shared pool
another run can drag the volume under any threshold while your branch is failing
for its own reasons. Measured free space is consulted only when nothing watched,
which is how a local run gets a classification at all. The verdict resets per
build step, so only the step that actually failed speaks.

"Final in both directions" applies only to a watcher that actually reported. The
watcher exits with a reserved status to say it saw the error, so a watcher that
exits any *other* non-zero way — no `awk` on the runner, a signal — is a watcher
that reached no verdict rather than one that saw nothing. That disarms the watch
and hands classification back to the free-space backstop; treating its silence as
"not disk" would blame the branch for the volume just as surely as the unwritable
marker did.

Classification's highest-ranked answer sits above every other signal: a run that
was *signalled* judged nothing at all, where the disk and cache cases all describe
a `make` that returned. That ordering matters when a budget expires on a tight
volume — calling it disk would send you to reclaim space that was never the
problem. See the budget paragraph above for what a signalled run publishes, and
the cache section below for the fourth answer.

The watch keeps its answer in a shell variable, not a file. Recording it on disk
needed an allocation at the one moment allocation is failing — and on macOS
`$TMPDIR` and the workspace are usually the same APFS container, so there was no
reliably writable place to put it. An unwritable marker read as "not a disk
failure", which blamed the branch for the volume.

Set `SIGNOFF_MIN_FREE_GIB` to change the floor. Locally both checks only warn and
the output is not watched — your own disk is yours to manage.

### "Compiler cache unreachable — checks did not complete"

The pool's runners compile through `sccache` (`RUSTC_WRAPPER`, configured by
`.github/actions/setup-sccache`) against an S3-compatible endpoint on the host.
When that endpoint stops answering, sccache's server cannot start and *every*
`rustc` invocation from that moment on dies before compiling anything:

```
sccache: error: Server startup failed: cache storage failed to read: …
  error sending request for url (http://127.0.0.1:8333/sccache/…/.sccache_check):
  tcp connect error: Connection refused (os error 61)
error: process didn't exit successfully: `sccache …/rustc -vV` (exit status: 2)
```

**Re-dispatch it.** Your branch was never compiled, so nothing in that run is a
statement about it. If it recurs on the same runner, that machine's sccache
storage service is what needs attention.

The same watcher that reads the out-of-disk signature reads this one, so the same
rules apply: only sccache's own `error:` channel counts (a build that merely
*mentions* sccache and then fails to compile is still a check failure), the
verdict is only worth something on a run that armed the watch, and disk outranks
cache when both appear — a volume at zero can break the cache endpoint too, and
reclaiming space is then the remedy that fixes both. Unlike disk there is no
after-the-fact backstop: the endpoint may well be answering again by the time the
run ends, so the only evidence is what the build said while it was failing.

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
| Local | `make signoff` | skip Rust if no Rust-affecting files in the branch diff; else targeted `make lint-rust PACKAGES=… FEATURES=…` + `make nextest-packages PACKAGES=… FEATURES=…` (features from the workspace resolve), full `make lint-rust`, `make nextest verify-cli` |
| Remote | `make signoff-remote` | the same checks via the self-hosted `signoff.yml` workflow, without the targeted pre-checks (`run_targeted_prechecks=true` restores them); posts `signoff` |
| Pull request | `pull_request` | **Attestation** (validates the sign-off, or auto-passes a branch with no Rust-affecting files, a pure revert, or a single-commit Dependabot bump) + PR hygiene; merge-queue check names report lightweight skipped/passthrough results |
| Merge queue | `merge_group` | the full required suite (below) + advisory niche checks |
| Manual dispatch of `pr.yml` | `workflow_dispatch` | the heavy jobs, and **`Attestation` fails on purpose** — a dispatch carries no pull request payload, so there is no sign-off to read and no verdict it can honestly reach |

`Attestation` failing on a dispatch is deliberate: it is the only gate a pull
request has, so a green one that inspected nothing would satisfy that gate
outright. Do not dispatch `pr.yml` to report a missing `Attestation` on a PR —
it cannot, and the failure it posts lands on that head commit. Push to the
branch, or re-run the `pull_request`-triggered `pr` run, so the event fires with
its payload. To re-run the *sign-off* itself, dispatch `signoff.yml` instead.

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

GitHub uses one required-checks list both before queue entry and on the merge
queue commit. Therefore every required workflow triggers on both `pull_request`
and `merge_group`. On a pull request, `Attestation` is the only *quality* work
(the `enforce-pull-with-spice` hygiene check also runs); the expensive required
jobs report skipped or lightweight passthrough results under their stable check
names. On `merge_group`, those same names run the real suite, so any failure
blocks the merge. `check_changes` still lets docs-only merge groups skip work
and report success.

### Enabling it (one-time rollout)

Because this changes the checks that gate `trunk`, roll it out deliberately:

1. Review and merge the PR that adds `scripts/signoff`. Its pull-request
   workflows still report every currently required check name, allowing it to
   enter the merge queue normally; the queue runs the real suite.
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

> **Every required check must be produced under the same stable name on both
> the PR head (`pull_request` or `pull_request_target`) and `merge_group`.** A
> missing PR check blocks queue entry; a missing merge-group check stalls the
> queue. Matrix workflows use a summary
> "gate" job with a stable name so one ruleset entry can require the whole matrix.

### Rollback

Sign-off is enabled by the ruleset, not the workflows, so you can revert the gate
without touching code: edit the `trunk` ruleset's required status checks back to
the previous list (drop `Attestation` and re-add the heavy checks), then restore
the heavy jobs on `pull_request` by removing their PR-only skip conditions.
Because the jobs continue to run on `merge_group`, the queue keeps working
throughout.
