#!/usr/bin/env bash
#
# Unit tests for `scripts/restack_stacked_branch.sh`.
#
# Every case here is a defect the script had at some point, and each one failed
# the same way: by printing nothing. That is what makes them worth pinning. An
# audit that reports a clean restack when the merge reverted a deletion is worse
# than no audit, because the reader stops looking -- and the whole reason this
# tooling exists is that git already reports such a merge as conflict-free.
#
# No network and no fixtures: each case builds a throwaway repository whose
# history has the shape being tested -- fork point, parent branch, child branch,
# squash merge onto trunk -- and runs the subject against it.
#
# Usage: scripts/test_restack_stacked_branch.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/restack_stacked_branch.sh"

tests_run=0
failures=0

work_root="$(mktemp -d)"
trap 'rm -rf "$work_root"' EXIT

# Each case runs in a subshell so it can cd into its own repository, which means
# a counter incremented in there is discarded on the way out -- a harness that
# reports "all passed" while a case failed. Fail the subshell instead, and let
# the caller count. Reporting a false pass is the one thing a test for silent
# failures must not do.
fail_test() {
  echo "  FAIL: $1"
  exit 1
}

start_test() {
  tests_run=$((tests_run + 1))
  echo "- $1"
}

# A repository with the shape this tooling is about: a fork point on trunk, a
# parent branch off it, a child branch off the parent, and trunk taking the
# parent as a squash merge. Echoes "<stack_base> <pre_merge_tip>".
new_stack() {
  local dir="$1"
  mkdir -p "$dir" && cd "$dir" || exit 1
  git init --quiet --initial-branch=trunk .
  git config user.email test@example.com
  git config user.name "Test"
  git config commit.gpgsign false
}

commit_all() {
  git add --all
  git commit --quiet --message "$1"
}

squash_parent_onto_trunk() {
  git checkout --quiet trunk
  git merge --squash parent >/dev/null 2>&1
  git commit --quiet --message "squash-merge parent"
}

# ---------------------------------------------------------------------------
# audit
# ---------------------------------------------------------------------------

start_test "audit reports a path the merge restored (the case this exists for)"
(
  new_stack "$work_root/resurrect"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'registry\n' > registry.rs && commit_all "parent adds registry.rs"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes registry.rs"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" audit "$stack_base" "$pre" 2>/dev/null)
  status=$?
  [ "$status" -eq 1 ] || fail_test "expected exit 1, got $status"
  case "$output" in
    *"RESURRECTED registry.rs"*) ;;
    *) fail_test "expected RESURRECTED registry.rs, got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit sees through rename detection (a moved path is still audited)"
(
  new_stack "$work_root/rename"
  printf 'contents\n' > old_path.rs && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'contents\nparent\n' > old_path.rs && commit_all "parent edits it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git mv old_path.rs new_path.rs && commit_all "child moves it"
  pre=$(git rev-parse HEAD)
  # The merge restores the old path: git sees the child's move as a rename, so a
  # rename-detecting diff reports neither a deletion of old_path.rs nor an
  # addition of it, and the audit would print nothing.
  git checkout --quiet "$stack_base" -- old_path.rs

  output=$("$subject" audit "$stack_base" "$pre" 2>/dev/null)
  case "$output" in
    *"RESURRECTED old_path.rs"*) ;;
    *) fail_test "rename detection hid the restored path; got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit handles paths git C-quotes (newline, non-ASCII)"
(
  new_stack "$work_root/quoting"
  awkward=$(printf 'na\303\257ve\nname.rs')
  printf 'x\n' > "$awkward" && commit_all "fork point"
  git checkout --quiet -b parent
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet -- "$awkward" && commit_all "child deletes the awkward path"
  pre=$(git rev-parse HEAD)
  git checkout --quiet "$stack_base" -- "$awkward"

  output=$("$subject" audit "$stack_base" "$pre" 2>/dev/null)
  case "$output" in
    *RESURRECTED*) ;;
    *) fail_test "C-quoted path was not reported; got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit tests the index, not the worktree (unstaged correction)"
(
  new_stack "$work_root/index"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'registry\n' > registry.rs && commit_all "parent adds registry.rs"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes registry.rs"
  pre=$(git rev-parse HEAD)
  git checkout --quiet "$stack_base" -- registry.rs
  # "Correcting" it with a plain rm leaves the index entry, so the commit would
  # still carry the file. A filesystem test would call this clean.
  rm registry.rs

  output=$("$subject" audit "$stack_base" "$pre" 2>/dev/null)
  case "$output" in
    *"RESURRECTED registry.rs"*) ;;
    *) fail_test "unstaged removal read as clean; got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a path the merge dropped"
(
  new_stack "$work_root/lost"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent && printf 'p\n' > parent.rs && commit_all "parent"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'new\n' > added_by_child.rs && commit_all "child adds a file"
  pre=$(git rev-parse HEAD)
  git rm --quiet --cached added_by_child.rs >/dev/null

  output=$("$subject" audit "$stack_base" "$pre" 2>/dev/null)
  case "$output" in
    *"LOST added_by_child.rs"*) ;;
    *) fail_test "expected LOST added_by_child.rs, got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit is quiet and succeeds when the merge did what was intended"
(
  new_stack "$work_root/clean"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'registry\n' > registry.rs && commit_all "parent adds registry.rs"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes registry.rs"
  pre=$(git rev-parse HEAD)

  output=$("$subject" audit "$stack_base" "$pre" 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "expected exit 0 on a clean audit, got $status"
  case "$output" in
    *RESURRECTED* | *LOST*) fail_test "clean audit reported a finding: $output" ;;
  esac
) || failures=$((failures + 1))

# ---------------------------------------------------------------------------
# resolve
# ---------------------------------------------------------------------------

start_test "resolve refuses a symlink rather than writing through it"
(
  new_stack "$work_root/symlink"
  printf 'TARGET CONTENT\n' > target.txt
  printf 'other\n' > link_me.md && commit_all "fork point"
  git checkout --quiet -b parent
  rm link_me.md && ln -s target.txt link_me.md && commit_all "parent makes it a symlink"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  rm link_me.md && ln -s other_target.txt link_me.md && commit_all "child retargets it"
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" resolve "$stack_base" link_me.md 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 for a symlink, got $status"
  case "$output" in
    MANUAL*) ;;
    *) fail_test "expected MANUAL for a symlink, got: $output" ;;
  esac
  # The corruption this prevents: cp follows the link and rewrites its target.
  [ "$(cat target.txt)" = "TARGET CONTENT" ] ||
    fail_test "the symlink's target was overwritten"
) || failures=$((failures + 1))

start_test "resolve refuses a mode conflict rather than dropping the exec bit"
(
  new_stack "$work_root/mode"
  printf 'script\n' > run.sh && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'script\nparent\n' > run.sh && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'script\nparent\nchild\n' > run.sh && commit_all "child edits"
  squash_parent_onto_trunk
  chmod +x run.sh && commit_all "trunk marks it executable"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" resolve "$stack_base" run.sh 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 for a mode conflict, got $status"
  case "$output" in
    *"theirs=100755"*) ;;
    *) fail_test "expected the mode disagreement to be named, got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "resolve refuses a missing side rather than staging an empty file"
(
  new_stack "$work_root/missing"
  printf 'OLD\n' > p.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'PARENT VERSION\n' > p.txt && commit_all "parent rewrites p.txt"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet p.txt && commit_all "child deletes p.txt"
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # No stage 2 (the child deleted it) while stage 3 matches the stack base
  # exactly, so an unguarded three-way merge is empty + base + base: clean, and
  # empty. That stages a 0-byte file where a deletion was meant.
  output=$("$subject" resolve "$stack_base" p.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 for a missing side, got $status"
  if [ -f p.txt ] && [ ! -s p.txt ]; then
    fail_test "an empty p.txt was written where a deletion was intended"
  fi
) || failures=$((failures + 1))

start_test "resolve writes correctly based conflict markers and leaves them unstaged"
(
  new_stack "$work_root/conflict"
  printf 'l1\nl2\nl3\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\n' > f.txt && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nCHILD\n' > f.txt && commit_all "child edits line 3"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\n' > f.txt && commit_all "trunk edits line 3 differently"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" resolve "$stack_base" f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 1 ] || fail_test "expected exit 1 for a real conflict, got $status"
  case "$output" in
    CONFLICT*) ;;
    *) fail_test "expected CONFLICT, got: $output" ;;
  esac
  grep -q '^<<<<<<< ours' f.txt || fail_test "conflict markers were not written to the worktree"
  grep -q '^>>>>>>> trunk' f.txt || fail_test "markers were not labelled (mktemp paths leak otherwise)"
  # The base shown must be the stack base, not git's older one: line 2 agrees on
  # both sides against that base, so only line 3 is in conflict.
  grep -q '^PARENT$' f.txt || fail_test "the parent's line was dragged into the conflict"
  [ -n "$(git ls-files -u -- f.txt)" ] || fail_test "the path was resolved without a human"
) || failures=$((failures + 1))

start_test "resolve stages a clean three-way merge against the stack base"
(
  new_stack "$work_root/clean_resolve"
  # The two edits are kept well apart: diff3 treats adjacent changes as
  # overlapping, so neighbouring lines would conflict even though nothing about
  # the merge is genuinely ambiguous.
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > f.txt && commit_all "child edits line 8"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "trunk edits line 3"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" resolve "$stack_base" f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "expected exit 0 for a clean merge, got $status ($output)"
  # Both sides' edits survive, which is the point of using the stack base.
  grep -q '^CHILD$' f.txt || fail_test "the child's edit was lost"
  grep -q '^TRUNK$' f.txt || fail_test "trunk's edit was lost"
  [ -z "$(git ls-files -u -- f.txt)" ] || fail_test "the path was left unmerged"
) || failures=$((failures + 1))

# ---------------------------------------------------------------------------
# stack-base
# ---------------------------------------------------------------------------

start_test "stack-base derives the shared commit, not the parent's final head"
(
  origin="$work_root/origin.git"
  git init --quiet --bare --initial-branch=trunk "$origin"

  new_stack "$work_root/stackbase"
  git remote add origin "$origin"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git push --quiet origin trunk
  git checkout --quiet -b parent
  printf 'p1\n' > parent.rs && commit_all "parent commit the child takes"
  shared=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'c1\n' > child.rs && commit_all "child work"
  # The parent keeps moving after the child split off; the child never takes it.
  git checkout --quiet parent
  printf 'p2\n' > parent_later.rs && commit_all "parent commit the child never took"
  parent_head=$(git rev-parse HEAD)
  git push --quiet origin parent
  git --git-dir="$origin" update-ref refs/pull/1/head "$parent_head"
  git checkout --quiet child

  reported=$("$subject" stack-base 1 2>/dev/null)
  [ "$reported" = "$shared" ] ||
    fail_test "expected the shared commit ${shared:0:8}, got ${reported:0:8} (parent head is ${parent_head:0:8})"
) || failures=$((failures + 1))

start_test "stack-base refreshes origin/trunk rather than only FETCH_HEAD"
(
  origin="$work_root/origin2.git"
  git init --quiet --bare --initial-branch=trunk "$origin"

  new_stack "$work_root/staleref"
  git remote add origin "$origin"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git push --quiet origin trunk
  git checkout --quiet -b parent && printf 'p\n' > p.rs && commit_all "parent"
  parent_head=$(git rev-parse HEAD)
  git push --quiet origin parent
  git --git-dir="$origin" update-ref refs/pull/1/head "$parent_head"
  git checkout --quiet -b child && printf 'c\n' > c.rs && commit_all "child"

  git fetch --quiet origin
  stale=$(git rev-parse origin/trunk)

  # trunk advances elsewhere, exactly as it does while a stack is in review.
  helper="$work_root/helper"
  git clone --quiet "$origin" "$helper"
  (
    cd "$helper" || exit 1
    git config user.email test@example.com
    git config user.name "Test"
    git checkout --quiet trunk
    printf 'moved on\n' > trunk.rs
    git add --all && git commit --quiet --message "trunk moves on"
    git push --quiet origin trunk
  )

  cd "$work_root/staleref" || exit 1
  "$subject" stack-base 1 >/dev/null 2>&1
  [ "$(git rev-parse origin/trunk)" != "$stale" ] ||
    fail_test "origin/trunk was left stale, so every later comparison uses the wrong base"
) || failures=$((failures + 1))

echo
if [ "$failures" -eq 0 ]; then
  echo "All $tests_run tests passed."
  exit 0
fi
echo "$failures of $tests_run tests failed."
exit 1
