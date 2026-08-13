# Stacked pull requests

A **stacked PR** is a branch based on another open branch rather than on `trunk`, so
a large change can be reviewed as a sequence of small PRs. Stacking is supported and
encouraged for work that would otherwise land as one unreviewable PR.

The mechanical steps live in `scripts/restack_stacked_branch.sh`, covered by
`scripts/test_restack_stacked_branch.sh` — every case there is a way one of those steps
once failed silently. This document is the reasoning around them: which mechanism to
use, which commit is which, and why the audit is not optional.

**Stacking never requires a force push.** A rebase is never *required* either — merging
always works — but once the parent lands, an unpushed child is cleaner to rebase, so
the restack step below picks between the two. This document covers (1) the workflow
while the parent is open, (2) what changes when the parent squash-merges, (3) the two
ways to restack and how to choose, and (4) the audit that catches the one failure mode
that is otherwise silent.

> **The rule stacking interacts with:** never force-push (see `CLAUDE.md`). Rebasing
> is not itself forbidden — `git pull --rebase` is the recommended way to take
> upstream commits. What is forbidden is *rewriting history you have already pushed*,
> because that requires a force push. That distinction decides which restack
> mechanism you use below.

Three commits get referred to throughout, and they are not the same one:

- **Fork point** — the newest `trunk` commit reachable from the child, which is what
  `git merge-base <child> origin/trunk` reports, before and after the parent lands.
  Usually that is where the parent branched from, but it advances if the parent merges
  a newer `trunk` and the child takes that update.
- **Parent head** (`$PARENT_HEAD`) — the parent branch's *final* tip, the state `trunk`
  squashed. Only the squash-equivalence check in step 1 uses this.
- **Stack base** (`$STACKBASE`) — the **newest parent commit contained in the child**:
  the parent's tip at the split, or the parent's tip as of your last parent → child
  merge if you took later parent commits. Everything after it on the child is the
  child's own work, which is what makes it the right three-way base and the right
  reference for the audit.

Derive them rather than assuming they are the same commit — the parent branch usually
gains review-fix commits after the child splits off:

```bash
STACKBASE=$(scripts/restack_stacked_branch.sh stack-base <parent-pr> --child <child>)
PARENT_HEAD=$(gh pr view <parent-pr> --json headRefOid -q .headRefOid)   # step 1 only
```

The script fetches the tracking refs and the PR ref (the parent branch is deleted, but
`refs/pull/<n>/head` survives) and derives the stack base with `merge-base`. Name the
child, or check it out first: derived from `trunk` the answer is the fork point, which
is the wrong boundary everywhere it is used. The script refuses to return a commit
`trunk` already contains rather than let that pass.

> **Fetch the tracking refs, not just the PR ref.** `git fetch origin <refspec>` leaves
> its result in `FETCH_HEAD` and does **not** update `origin/trunk`. Every comparison,
> rebase, and merge below is against `origin/trunk`, so a stale local ref silently
> restacks onto a commit that predates the parent's squash — the wrong base, with no
> error. The script does both fetches; do the same if you work by hand.

Using `$PARENT_HEAD` as the stack base is wrong whenever the child never took the
parent's last commits: the audit would read that parent work as child deletions, and
the three-way merge would treat it as content you removed and drop it. If the two
differ, `git log --oneline "$STACKBASE".."$PARENT_HEAD"` is parent work the child never
carried — expect it to arrive with the merge, and do not read it as child intent.

---

## While the parent PR is open

1. Branch the child off the parent branch, not off `trunk`.
2. Open the child PR with its **base set to the parent branch**. Reviewers then see
   only the child's own diff.
3. When the parent gains commits (review fixes, for example), merge the parent branch
   into the child and push normally:

   ```bash
   # An explicit refspec for the same reason as trunk below: a bare fetch obeys
   # remote.origin.fetch, and a --single-branch clone would leave this stale.
   git fetch origin "+refs/heads/<parent>:refs/remotes/origin/<parent>"
   git checkout <child>
   git merge origin/<parent>
   git push
   ```

Nothing here needs a rebase. Keep stacks shallow — each extra level multiplies the
restack work below — and prefer landing the parent before starting a third level.

Do **not** merge `trunk` into the child at this stage. `BEHIND` does not block the
merge queue, and a `trunk` merge discards a passing `make signoff` attestation, so it
costs a full re-signoff for nothing.

---

## What changes when the parent squash-merges

`trunk` takes squash merges (`delete_branch_on_merge` is on), so when the parent
lands:

- The parent's individual commits **never appear on `trunk`**. `trunk` gets one new
  commit whose parent is **whatever `trunk`'s tip was at merge time** and whose tree is
  that tip plus the parent branch's net diff. If `trunk` moved after the fork point,
  that tree is *not* equal to the parent branch's tree — it also carries the
  intervening commits.
- The parent branch is deleted, and GitHub **retargets the child PR's base to
  `trunk`** automatically.
- The child's merge base is still the **fork point**, so until you restack, the child's
  PR diff includes the parent's changes as well as its own.

That last point makes the restack mandatory rather than cosmetic. Because the merge
base is the fork point — older than the stack base — the parent's changes now appear
on *both* sides of the comparison, and git handles the three cases differently:

| Case, child vs. squashed parent | What git does |
| --- | --- |
| A file only the parent touched | Identical content on both sides — resolves silently and correctly. |
| A file the parent **and** child both edited | Frequently a **spurious conflict**: git cannot align the duplicated parent hunks against a base that predates them. |
| A file the parent **added** and the child **deleted** | **Silently restored, with no conflict reported.** At the fork point the file does not exist, and it does not exist in your tree either, so git sees no change on your side and applies trunk's add unopposed. |

The third row is the dangerous one. It hit #12891, stacked on the branch that landed as
#12661: restacking it after the parent squash-merged produced 13 reported conflicts,
plus `crates/runtime-table/src/table_layers.rs` — half of the registry #12891 was
written to delete — put back with no mention in the conflict list.

---

## Restacking: pick by whether the child has been pushed

### Child not yet pushed → rebase onto `trunk`

Clean history, none of the hazards above, and no force push because nothing has been
published yet:

```bash
git checkout <child>
git rebase --onto origin/trunk "$STACKBASE"   # the derived stack base, not the parent's final head
git push -u origin <child>                    # first push, not a force push
```

The rebase drops the parent's commits (already on `trunk` as the squash commit) and
replays only the child's. Conflicts, if any, are real ones resolved per commit, and
adds/deletes are preserved.

### Child already pushed → merge `trunk`, then audit

Rewriting a pushed branch needs a force push, so merge instead — and treat the audit
as part of the merge, not an optional check.

Start the merge with `--no-commit`. A conflict-free merge otherwise commits itself
immediately, which is exactly the case where the silent resurrection happens and the
one where you most need the worktree held open for the audit:

```bash
git checkout <child>
git status --porcelain             # must be empty before you start — see below
PRE=$(git rev-parse HEAD)          # the child's pre-merge tip; needed by the audit
                                   # $PARENT_HEAD / $STACKBASE derived as above
git merge --no-ff --no-commit origin/trunk
test -f "$(git rev-parse --git-dir)/MERGE_HEAD" || echo "the merge did not start — stop"
```

`HEAD` stays at `$PRE` for every step below; the merge result lives in the index and
worktree until you commit it in step 5.

**Start from a clean index, and confirm the merge actually began.** A staged change
makes `git merge` refuse outright — even one to a file the merge does not touch — and
it refuses *quietly* enough to be missed: exit 2, no `MERGE_HEAD`, and the steps below
carry on regardless. Step 5's `git commit` then turns that staged change into an
ordinary commit, so you push a branch that never took `trunk` plus a commit nobody
reviewed, and the audit reports nothing because nothing was resurrected. Checking
`git status` afterwards cannot see it either: by then the tree is clean. An *unstaged*
change to a path the merge does not touch is harmless — the merge proceeds and a
worktree-only edit stays out of the commit — but git also aborts when an unstaged change
sits on a path the merge would overwrite, so the reliable rule is simply to begin clean
rather than to reason about which kind you have.

**1. Look at whether the parent's edits survived the squash** — against `$PARENT_HEAD`,
not `$STACKBASE`, since the squash reflects the parent's final state. Restrict the
comparison to the paths the parent touched, since `trunk` will have moved elsewhere:

```bash
git diff --stat "$PARENT_HEAD" origin/trunk -- <paths the parent touched>
```

This is an inspection, not a test: `origin/trunk` also carries the commits it took
before and after the squash, so a difference here may be somebody else's change to a
file the parent happened to touch rather than a change to the parent itself. Read it,
and only worry if the parent's own edits are missing or altered.

**2. Re-resolve each conflicted file three-way against the stack base**, not against
the base git chose. Git's own attempt conflicted all 13 files in the case above; all
13 resolved cleanly this way:

```bash
scripts/restack_stacked_branch.sh resolve "$STACKBASE" <path>
```

It resolves and stages the path when the merge is clean (exit 0); writes the correctly
based conflict into the worktree and deliberately leaves it **unstaged** when there is a
real conflict, so the path stays unmerged until you agree with it (exit 1); and refuses
outright, touching nothing, when the path needs a person (exit 2).

That last case is not rare, and each variety of it is a way this went wrong silently
before the script existed:

- **A mode disagreement.** Copying content leaves the destination's mode alone, so a
  `100644` vs `100755` conflict resolved to your mode and quietly dropped an executable
  bit `trunk` added — and a symlink destination is *followed*, so resolving a conflicted
  `CLAUDE.md` that way overwrote `.github/copilot-instructions.md` while the link itself
  still looked untouched.
- **A missing side.** The parent rewrites a file, the child deletes it, `trunk` squashes
  the parent: there is then no stage 2 while stage 3 matches `$STACKBASE` exactly, so a
  naive three-way merge is `empty + base + base` — clean, empty, and it stages a 0-byte
  file where you meant a deletion.
- **A path absent from `$STACKBASE`.** `$STACKBASE` is deliberately newer than git's
  stage 1, so a path can be genuinely conflicted and still not exist at that base.

**3. Audit for silent damage** — this is the step that catches the resurrection, and the
one not to skip:

```bash
scripts/restack_stacked_branch.sh audit "$STACKBASE" "$PRE" --parent-head "$PARENT_HEAD"
```

Pass `--parent-head`: without it the stack base can only be checked by ancestry, and an
*earlier* parent commit passes that test while leaving everything the parent added after
it out of the comparison entirely — so the merge could restore a file the child deleted
and the audit would have nothing to say. It reports when it could not make that check.

It compares what the child *intended* against what the merge actually staged, and exits
non-zero on any `RESURRECTED`, `LOST`, `DISCARDED`, or `REVIEW` path — including one that
is simply still unmerged, since nothing has decided that either. Run it again after every
correction.

It checks that the merge in progress is the one you think it is: the side being merged
must be `origin/trunk`, or whatever `--trunk <rev>` names. Auditing some other merge would
compare against the wrong side, and the parent count in step 5 cannot tell the difference
— a merge of anything has two parents.

`DISCARDED` is the same failure as `RESURRECTED`, in content or file mode rather than
existence — a reverted executable bit counts, and every blob involved is identical: when
the child's edit *reverts* something the parent did, the child's side matches the fork
point, so the merge sees nothing to preserve and keeps `trunk`'s version. No conflict is
reported, and the path is a modification, so listing added and deleted paths alone would
never surface it.

The check is a comparison against the merge that *should* have happened — the staged
content against a three-way merge from the stack base — rather than against either input,
because a file can lose a revert and keep an unrelated edit in the same commit, leaving a
result that matches neither side. `REVIEW` is the case where that correct merge conflicts:
git resolved the path without asking only because it used the older base, so nobody has
actually decided what belongs there.

Resolving a `REVIEW` path does not change what the audit sees — the inputs are the same
commits either way, and a considered decision looks exactly like git's silent one in the
index. Say so explicitly once you have made the call, so the re-run can finish:

```bash
scripts/restack_stacked_branch.sh audit "$STACKBASE" "$PRE" --accept <path>
```

`--accept` applies only to `REVIEW`. A `RESURRECTED`, `LOST`, or `DISCARDED` path is not
ambiguous — nothing was decided there, something was lost — so it stays a finding until
the tree is corrected, whatever you pass.

Two properties are worth knowing, because both were bugs that reported success:

- It compares against `$PRE`, the pre-merge tip. A resurrected file is no longer a
  deletion in the merge *result*, so comparing against a committed merge reports success
  no matter what happened. (If you have already committed the merge, `$PRE` is `HEAD^1`.)
- It tests the **index**, not the filesystem, because `git commit` records the index.
  `rm` a resurrected file without staging the removal and a filesystem test reports it
  gone while the index still carries it — clean audit, wrong commit.

Then check the whole shape: `git diff --stat "$STACKBASE" "$PRE"` (intent) against
`git diff --stat --cached origin/trunk` (result) should agree file for file, apart from
other commits `trunk` took meanwhile. Investigate anything in one list but not the
other. `--cached` for the same reason as above — it compares `trunk` against the index,
which is what the merge commit will record; without it an unstaged correction makes the
stats look right while the commit still carries the wrong content.

**4. Take `trunk`'s `Cargo.lock` wholesale** rather than resolving it by hand, then
let `cargo metadata` re-add the branch's own crates:

```bash
git restore --source=origin/trunk -- Cargo.lock &&
  cargo metadata --format-version 1 >/dev/null &&
  git add -- Cargo.lock
```

`restore`, not `checkout` — `git checkout <tree> -- <path>` writes the **index** as well
as the worktree, so `trunk`'s lockfile would already be staged before `cargo metadata`
ran, and chaining would protect nothing. `git restore --source` touches only the
worktree, which leaves the `git add` as the single point where anything is staged, and
the chain then means a failed resolve stages nothing at all.

`cargo metadata` only re-adds what the branch's *manifests* require, so any lockfile
change the branch made without a manifest change — a `cargo update` of a transitive
dependency, say — is discarded here and has to be re-applied by hand. Check with
`git diff --stat "$STACKBASE" "$PRE" -- Cargo.lock` before taking `trunk`'s copy.

**5. Commit, push, then re-run `make signoff`.** Sign-off attests the *pushed* HEAD
from a clean checkout (`docs/dev/ci_signoff.md`), and the merge is a new commit, so the
previous attestation no longer covers it:

```bash
git commit                                # completes the --no-commit merge
git rev-list --parents -n1 HEAD | wc -w   # 3 = a real merge, 2 = no merge happened
git status --short                        # must be clean before signing off
git push
make signoff
```

Count the parents *of the commit you just made*, rather than trusting it to have merged
anything: two words means an ordinary commit and the `--no-commit` merge never
happened. `git status` cannot tell you that — it is clean either way.

---

## Checklist

- [ ] Child PR based on the parent branch while the parent is open.
- [ ] No `trunk` merge into the child before the parent lands.
- [ ] Stack base derived with `git merge-base`, not assumed to be the parent's final head.
- [ ] After the parent lands: rebased if unpushed, `merge --no-ff --no-commit` if pushed.
- [ ] Conflicts re-resolved against the stack base with `restack_stacked_branch.sh resolve`.
- [ ] `restack_stacked_branch.sh audit` clean before the merge is committed.
- [ ] `Cargo.lock` taken from `trunk` and re-resolved by cargo.
- [ ] Merge committed and pushed, then `make signoff` re-run on the new pushed HEAD.
