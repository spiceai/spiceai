# Stacked pull requests

A **stacked PR** is a branch based on another open branch rather than on `trunk`, so
a large change can be reviewed as a sequence of small PRs. Stacking is supported and
encouraged for work that would otherwise land as one unreviewable PR.

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

- **Fork point** — the `trunk` commit the *parent* branched from. This is what
  `git merge-base <child> origin/trunk` reports, both before and after the parent
  lands.
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
git fetch origin                                   # refresh origin/trunk — see the warning below
git fetch origin "refs/pull/<parent-pr>/head"      # the branch is deleted; the PR ref survives
PARENT_HEAD=$(gh pr view <parent-pr> --json headRefOid -q .headRefOid)
STACKBASE=$(git merge-base HEAD "$PARENT_HEAD")
```

> **Fetch the tracking refs, not just the PR ref.** `git fetch origin <refspec>` leaves
> its result in `FETCH_HEAD` and does **not** update `origin/trunk`. Every comparison,
> rebase, and merge below is against `origin/trunk`, so a stale local ref silently
> restacks onto a commit that predates the parent's squash — the wrong base, with no
> error. Plain `git fetch origin` first.

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
   git fetch origin              # without this, origin/<parent> is stale and the merge is a no-op
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
git rebase --onto origin/trunk "$STACKBASE"   # stack base = parent's tip at the split
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
PRE=$(git rev-parse HEAD)          # the child's pre-merge tip; needed by the audit
                                   # $PARENT_HEAD / $STACKBASE derived as above
git merge --no-ff --no-commit origin/trunk
```

`HEAD` stays at `$PRE` for every step below; the merge result lives in the index and
worktree until you commit it in step 5.

**1. Confirm trunk's squash really matches the parent head** — `$PARENT_HEAD` here, not
`$STACKBASE`, since the squash reflects the parent's final state. Restrict the
comparison to the paths the parent touched, since `trunk` will have moved elsewhere;
anything reported means the parent changed during merge and the assumptions below need
rechecking:

```bash
git diff --stat "$PARENT_HEAD" origin/trunk -- <paths the parent touched>
```

**2. Re-resolve each conflicted file three-way against the stack base**, not against
the base git chose. Git's own attempt conflicted all 13 files in the case above; all
13 resolved cleanly this way. For a path with all three stages present (`git
ls-files -u <path>` shows them):

```bash
f=<path>
t=$(mktemp -d)                          # never fixed /tmp names — see below
if git ls-files --stage -- ":(literal)$f" | awk '{ print $1 }' | grep -qx 120000; then
  # A symlink on ANY side — never cp onto it. A conflicted symlink's blob content is
  # its target path, so recreate the link from whichever side is right, then stage it:
  #   ln -sfn "$(git show ":2:$f")" "$f" && git add -- "$f"
  echo "SYMLINK conflict on $f — resolve by hand"
else
  git show ":2:$f" > "$t/ours"          # your side
  git show ":3:$f" > "$t/theirs"        # trunk's side
  git show "$STACKBASE:$f" > "$t/base"  # the correct base — NOT git's stage 1
  if git merge-file -p --diff3 "$t/ours" "$t/base" "$t/theirs" > "$t/merged"; then
    cp "$t/merged" "$f" && git add -- "$f"                 # clean — accept it
  else
    grep -n '^<<<<<<<\|^|||||||\|^>>>>>>>' "$t/merged"     # real conflict — by hand first
  fi
fi
rm -rf "$t"                             # once you are done with this path
```

The intermediates go in a private `mktemp -d`, never fixed `/tmp` paths: `> /tmp/ours`
follows a pre-existing symlink at that name and truncates whatever it points at, so a
pasted snippet can silently destroy a file outside the repo on a shared machine.

Three more things about that block. `merge-file -p` only writes the merged text to
stdout —
it neither updates the worktree file nor stages it, so the `cp`/`git add` are what
actually resolve the path. The staging must be *guarded* by the exit status (0 clean,
else the number of conflicts left): an unguarded `cp` stages conflict markers the moment
someone pastes the block.

And the symlink branch has to be **control flow, not a warning** — a printed warning
does not stop the `cp` that follows it — testing **every stage**, not the first line.
`git ls-files --stage` lists an unmerged path as stages 1 (base), 2 (ours), 3 (theirs)
in that order, so the first line is the *base's* mode: a path that was a regular file at
the base and is a symlink on your side would fall through to the text merge. `cp` then
follows the link and writes *through* it, so resolving a conflicted `CLAUDE.md` that way
silently overwrites `.github/copilot-instructions.md` while the link itself still looks
untouched. Any `120000` among the stages means resolve it by hand — a symlink on one
side and a regular file on the other is a type conflict that wants a human anyway.

**3. Audit for silent damage** — this is the step that catches the resurrection. It
compares what the child *intended* against what the merge actually put on disk:

```bash
# Files the child deleted that the merge brought back
git diff --name-only -z --no-renames --diff-filter=D "$STACKBASE" "$PRE" |
  while IFS= read -r -d '' f; do
    git ls-files --error-unmatch -- ":(literal)$f" >/dev/null 2>&1 && echo "RESURRECTED $f"
  done

# Files the child added that the merge dropped
git diff --name-only -z --no-renames --diff-filter=A "$STACKBASE" "$PRE" |
  while IFS= read -r -d '' f; do
    git ls-files --error-unmatch -- ":(literal)$f" >/dev/null 2>&1 || echo "LOST $f"
  done
```

Both must print nothing. Four details the checks depend on:

- The comparison uses `$PRE`, the pre-merge tip. A resurrected file is no longer a
  deletion in the merge *result*, so comparing against a committed merge reports
  success no matter what happened. (If you have already committed the merge, `$PRE` is
  `HEAD^1`.)
- `--no-renames` is required. Rename detection is on by default (even with
  `diff.renames` unset), and it reports a moved file as a single `R` entry, so its
  **old path appears under neither `D` nor `A`** — the merge could restore the old path
  and this audit would print nothing. A stacked PR that carves or moves code is mostly
  renames, which is exactly the shape of the case that motivated this document.
- `-z` and NUL-delimited reading are required for the same reason `--no-renames` is.
  Git C-quotes any path containing a tab, newline, backslash, or non-ASCII byte —
  `naïve.txt` prints as `"na\303\257ve.txt"` — and that quoted string is not the real
  path, so the lookup misses and the audit says nothing. `:(literal)` keeps a path with
  glob characters from being read as a pathspec pattern.
- The checks query the **index**, not the filesystem, because `git commit` records the
  index. They are meant to be re-run after you correct something, and that is where a
  filesystem test fails: `rm` a resurrected file without staging the removal and
  `[ -e ]` reports it gone while the index still carries it, so the audit prints a
  clean result and the merge commit contains the file anyway. Querying the index also
  removes the need to special-case a dangling symlink, which `[ -e ]` gets wrong.

Then check the whole shape: `git diff --stat "$STACKBASE" "$PRE"` (intent) against
`git diff --stat --cached origin/trunk` (result) should agree file for file, apart from
other commits `trunk` took meanwhile. Investigate anything in one list but not the
other. `--cached` for the same reason as above — it compares `trunk` against the index,
which is what the merge commit will record; without it an unstaged correction makes the
stats look right while the commit still carries the wrong content.

**4. Take `trunk`'s `Cargo.lock` wholesale** rather than resolving it by hand, then
let `cargo metadata` re-add the branch's own crates:

```bash
git checkout origin/trunk -- Cargo.lock
cargo metadata --format-version 1 >/dev/null
git add Cargo.lock
```

`cargo metadata` only re-adds what the branch's *manifests* require, so any lockfile
change the branch made without a manifest change — a `cargo update` of a transitive
dependency, say — is discarded here and has to be re-applied by hand. Check with
`git diff --stat "$STACKBASE" "$PRE" -- Cargo.lock` before taking `trunk`'s copy.

**5. Commit, push, then re-run `make signoff`.** Sign-off attests the *pushed* HEAD
from a clean checkout (`docs/dev/ci_signoff.md`), and the merge is a new commit, so the
previous attestation no longer covers it:

```bash
git commit                    # completes the --no-commit merge
git status --short            # must be clean before signing off
git push
make signoff
```

---

## Checklist

- [ ] Child PR based on the parent branch while the parent is open.
- [ ] No `trunk` merge into the child before the parent lands.
- [ ] Stack base derived with `git merge-base`, not assumed to be the parent's final head.
- [ ] After the parent lands: rebased if unpushed, `merge --no-ff --no-commit` if pushed.
- [ ] Conflicts re-resolved against the stack base, and staged (merge path only).
- [ ] `RESURRECTED` / `LOST` audit clean before the merge is committed (merge path only).
- [ ] `Cargo.lock` taken from `trunk` and re-resolved by cargo.
- [ ] Merge committed and pushed, then `make signoff` re-run on the new pushed HEAD.
