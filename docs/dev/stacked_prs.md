# Stacked pull requests

A **stacked PR** is a branch based on another open branch rather than on `trunk`, so
a large change can be reviewed as a sequence of small PRs. Stacking is supported and
encouraged for work that would otherwise land as one unreviewable PR.

Stacking never requires a rebase, and never requires a force push. This document
covers (1) the workflow while the parent is open, (2) what changes when the parent
squash-merges, (3) the two ways to restack and how to choose, and (4) the audit that
catches the one failure mode that is otherwise silent.

> **The rule stacking interacts with:** never force-push (see `CLAUDE.md`). Rebasing
> is not itself forbidden — `git pull --rebase` is the recommended way to take
> upstream commits. What is forbidden is *rewriting history you have already pushed*,
> because that requires a force push. That distinction decides which restack
> mechanism you use below.

---

## While the parent PR is open

1. Branch the child off the parent branch, not off `trunk`.
2. Open the child PR with its **base set to the parent branch**. Reviewers then see
   only the child's own diff.
3. When the parent gains commits (review fixes, for example), merge the parent branch
   into the child and push normally:

   ```bash
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
  commit whose *content* equals the parent branch, with the pre-split commit as its
  parent.
- The parent branch is deleted, and GitHub **retargets the child PR's base to
  `trunk`** automatically.
- The child's merge base is still the **pre-split** commit, so until you restack, the
  child's PR diff includes the parent's changes as well as its own.

That last point makes the restack mandatory rather than cosmetic. Because the merge
base predates the split, the parent's changes now appear on *both* sides of the
comparison, and git handles the three cases differently:

| Case, child vs. squashed parent | What git does |
| --- | --- |
| A file only the parent touched | Identical content on both sides — resolves silently and correctly. |
| A file the parent **and** child both edited | Frequently a **spurious conflict**: git cannot align the duplicated parent hunks against a base that predates them. |
| A file the parent **added** and the child **deleted** | **Silently restored, with no conflict reported.** From the pre-split base the file is absent on your side too, so trunk's add applies unopposed. |

The third row is the dangerous one. It hit `ben/issue-12614-spice-layer` (stacked on
the branch that landed as #12661): 13 reported conflicts, plus
`crates/runtime-table/src/table_layers.rs` — half of the registry that PR existed to
delete — restored with no mention in the conflict list.

---

## Restacking: pick by whether the child has been pushed

### Child not yet pushed → rebase onto `trunk`

Clean history, none of the hazards above, and no force push because nothing has been
published yet:

```bash
git checkout <child>
git rebase --onto origin/trunk <parent-tip>   # <parent-tip> = parent's head SHA at the split
git push -u origin <child>                    # first push, not a force push
```

Get `<parent-tip>` from the merged parent PR:

```bash
gh pr view <parent-pr> --json headRefOid -q .headRefOid
```

The rebase drops the parent's commits (already on `trunk` as the squash commit) and
replays only the child's. Conflicts, if any, are real ones resolved per commit, and
adds/deletes are preserved.

### Child already pushed → merge `trunk`, then audit

Rewriting a pushed branch needs a force push, so merge instead — and treat the audit
as part of the merge, not an optional check.

```bash
git checkout <child>
PRE=$(git rev-parse HEAD)          # the child's pre-merge tip; needed by the audit
STACKBASE=<parent-tip>             # as above
git merge origin/trunk
```

**1. Confirm trunk's squash really matches the stack base** — if it does not, the
parent changed during merge and every assumption below needs rechecking:

```bash
git diff --stat "$STACKBASE" origin/trunk -- <paths the parent touched>
```

**2. Re-resolve each conflicted file three-way against the stack base**, not against
the base git chose. Git's own attempt conflicted all 13 files in the case above; all
13 resolved cleanly this way:

```bash
git show "$STACKBASE":<path> > /tmp/base
git merge-file -p --diff3 <ours> /tmp/base <theirs>
```

**3. Audit for silent damage** — run this before committing the merge. It compares
what the child *intended* against what is on disk:

```bash
# Files the child deleted that the merge brought back
git diff --name-only --diff-filter=D "$STACKBASE" "$PRE" |
  while read -r f; do [ -e "$f" ] && echo "RESURRECTED $f"; done

# Files the child added that the merge dropped
git diff --name-only --diff-filter=A "$STACKBASE" "$PRE" |
  while read -r f; do [ -e "$f" ] || echo "LOST $f"; done
```

Both must print nothing. The comparison has to use `$PRE`, the pre-merge tip — a
resurrected file is no longer a deletion in the merge result, so comparing against
the post-merge `HEAD` reports success no matter what happened. (After the merge
commit exists, `$PRE` is `HEAD^1`.)

Then check the whole shape: `git diff --stat "$STACKBASE" "$PRE"` (intent) against
`git diff --stat origin/trunk HEAD` (result) should agree file for file, apart from
other commits `trunk` took meanwhile. Investigate anything in one list but not the
other.

**4. Take `trunk`'s `Cargo.lock` wholesale** rather than resolving it by hand, then
let `cargo metadata` re-add the branch's own crates:

```bash
git checkout origin/trunk -- Cargo.lock
cargo metadata --format-version 1 >/dev/null
```

**5. Re-run `make signoff`** — the merge produced a new commit, so the previous
attestation no longer covers the pushed HEAD.

---

## Checklist

- [ ] Child PR based on the parent branch while the parent is open.
- [ ] No `trunk` merge into the child before the parent lands.
- [ ] After the parent lands: rebased if unpushed, merged if pushed.
- [ ] `RESURRECTED` / `LOST` audit clean (merge path only).
- [ ] `Cargo.lock` taken from `trunk` and re-resolved by cargo.
- [ ] `make signoff` re-run on the new pushed HEAD.
