#!/usr/bin/env bash
#
# Unit tests for `scripts/restack_stacked_branch.sh`.
#
# Every case here pins a way the subject can fail while printing nothing, which
# is what makes them worth a test rather than a reading. An audit that reports a
# clean restack when the merge reverted a deletion is worse than no audit,
# because the reader stops looking -- and the whole reason this tooling exists is
# that git already reports such a merge as conflict-free.
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

# Guarded because an empty work_root turns every fixture path below into an
# absolute one — `/resurrect`, `/mode` — which a privileged run would create and
# modify at the filesystem root.
work_root="$(mktemp -d)" || exit 1
[ -n "$work_root" ] && [ -d "$work_root" ] || {
  echo "could not create a private work directory" >&2
  exit 1
}
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

# An empty repository on trunk, ready for a case to build the history it needs:
# a fork point, a parent branch off it, a child off the parent, and trunk taking
# the parent as a squash merge. Each case records the revisions it cares about.
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
# usage
# ---------------------------------------------------------------------------

start_test "usage lists every subcommand, and an unknown one is rejected"
(
  # Nothing else runs the help path, so a broken one would only be found by
  # someone reaching for it -- which is the worst moment.
  output=$(bash "$subject" --help 2>&1)
  status=$?
  [ "$status" -eq 0 ] || fail_test "--help exited $status"
  case "$output" in
    *sed*|*error*|*"command not found"*) fail_test "--help printed an error: $output" ;;
  esac
  for word in stack-base resolve audit; do
    case "$output" in
      *"$word"*) ;;
      *) fail_test "--help does not mention $word: $output" ;;
    esac
  done

  output=$(bash "$subject" not-a-subcommand 2>&1)
  status=$?
  [ "$status" -ne 0 ] || fail_test "an unknown subcommand exited 0"
) || failures=$((failures + 1))

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

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
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

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"RESURRECTED old_path.rs"*) ;;
    *) fail_test "rename detection hid the restored path; got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit handles paths git C-quotes (newline, non-ASCII)"
(
  new_stack "$work_root/quoting"
  awkward=$(printf 'na\303\257ve\nname.rs')
  printf 'x\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  # The parent has to have a commit of its own: a stack base that trunk already
  # contains is the fork point, which the audit refuses as a mis-derived base.
  printf 'x\n' > "$awkward" && commit_all "parent adds the awkward path"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet -- "$awkward" && commit_all "child deletes the awkward path"
  pre=$(git rev-parse HEAD)
  git checkout --quiet "$stack_base" -- "$awkward"

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
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

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"RESURRECTED registry.rs"*) ;;
    *) fail_test "unstaged removal read as clean; got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a child edit the merge discarded (a revert of the parent)"
(
  new_stack "$work_root/revert"
  printf 'old\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'new\n' > f.txt && commit_all "parent changes it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  # Reverting the parent's change makes the child's side match the fork point, so
  # the merge sees nothing to preserve and keeps trunk's version. No conflict is
  # reported, and the path is a modification, so neither the add nor the delete
  # filter lists it.
  printf 'old\n' > f.txt && commit_all "child deliberately reverts it"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -eq 1 ] || fail_test "expected exit 1, got $status ($output)"
  case "$output" in
    *"DISCARDED f.txt"*) ;;
    *) fail_test "the discarded revert was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a partial loss, where the result matches neither side"
(
  new_stack "$work_root/partial"
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  # Two changes in one file: a revert of the parent's line 2, and an unrelated
  # edit at line 8. The merge keeps the second and drops the first, so the staged
  # blob equals neither the child's version nor trunk's, and comparing whole
  # entries against either side sees nothing wrong.
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > f.txt && commit_all "child reverts line 2, edits line 8"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  staged=$(git show :f.txt)
  case "$staged" in
    *PARENT*) ;;
    *) fail_test "the fixture did not reproduce the partial loss" ;;
  esac
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"DISCARDED f.txt"*) ;;
    *) fail_test "a partial loss was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports an add/add the older base merged for you"
(
  new_stack "$work_root/addadd"
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  git rm --quiet f.txt && commit_all "parent deletes it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nCHILD\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "child brings it back, edited"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nTRUNK\nl8\n' > f.txt && commit_all "trunk brings it back differently"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # From the fork point the file still existed, so git treats these as two edits
  # to one file and combines them. From the stack base, where the parent had
  # deleted it, both sides add the same path and nothing has decided which wins.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"REVIEW f.txt"*) ;;
    *) fail_test "a divergent add/add was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit refuses to call a merge clean while a path is still unmerged"
(
  new_stack "$work_root/unresolved"
  printf 'a\nb\nc\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\n' > f.txt && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'a\nCHILD\nc\n' > f.txt && commit_all "child edits line 2"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'a\nTRUNK\nc\n' > f.txt && commit_all "trunk edits line 2"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  [ -n "$(git ls-files -u -- f.txt)" ] || fail_test "the fixture did not leave a conflict"
  # Leaving it for somebody else is not the same as the merge being right, and
  # the caller gates on this status.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "an unresolved path was reported clean: $output"
  case "$output" in
    *"REVIEW f.txt"*) ;;
    *) fail_test "the unresolved path was not named: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit refuses to call a merge clean while an added path is unmerged"
(
  new_stack "$work_root/unresolvedadd"
  printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent
  git rm --quiet seed.txt && commit_all "parent deletes the seed"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'child version\n' > added.txt && commit_all "child adds a file"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'trunk version\n' > added.txt && commit_all "trunk adds the same path differently"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  [ -n "$(git ls-files -u -- added.txt)" ] || fail_test "the fixture did not leave an add/add conflict"
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "an unresolved addition was reported clean: $output"
) || failures=$((failures + 1))

start_test "audit checks a resolved path the child never touched"
(
  new_stack "$work_root/resolveduntouched"
  printf 'a\nb\nc\n' > shared.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\n' > shared.txt && commit_all "parent edits it before the split"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'child\n' > c.txt && commit_all "child works elsewhere entirely"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'a\nTRUNK\nc\n' > shared.txt && commit_all "trunk edits the same line after"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Resolve it the wrong way: keep the parent's line. From the stack base this
  # merge is not ambiguous — our side equals the base, so trunk's version wins.
  printf 'a\nPARENT\nc\n' > shared.txt && git add shared.txt

  # The path is in none of the intent lists, since stack_base..pre never touched
  # it, so nothing else in the audit would look at this resolution.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a wrongly resolved untouched path was reported clean: $output"
  case "$output" in
    *"DISCARDED shared.txt"*) ;;
    *) fail_test "the untouched path was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "stack-base refuses to answer from the wrong branch"
(
  origin="$work_root/origin3.git"
  git init --quiet --bare --initial-branch=trunk "$origin"

  new_stack "$work_root/stackbase_wrongbranch"
  git remote add origin "$origin"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git push --quiet origin trunk
  git checkout --quiet -b parent
  printf 'p\n' > p.rs && commit_all "parent work"
  parent_head=$(git rev-parse HEAD)
  git push --quiet origin parent
  git --git-dir="$origin" update-ref refs/pull/1/head "$parent_head"
  git checkout --quiet -b child && printf 'c\n' > c.rs && commit_all "child work"

  # Standing on trunk, HEAD shares only the fork point with the parent, and the
  # document derives the stack base before checking the child out.
  git checkout --quiet trunk
  output=$("$subject" stack-base 1 2>&1)
  status=$?
  [ "$status" -ne 0 ] || fail_test "the fork point was returned as a stack base: $output"

  # Naming the child gives the right answer from anywhere.
  output=$("$subject" stack-base 1 --child child 2>/dev/null)
  [ "$output" = "$parent_head" ] ||
    fail_test "expected ${parent_head:0:8} with --child, got ${output:0:8}"
) || failures=$((failures + 1))

start_test "audit reports a conflict on a path the child never touched"
(
  new_stack "$work_root/untouched"
  printf 'a\nb\nc\n' > shared.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\n' > shared.txt && commit_all "parent edits it before the split"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'child\n' > c.txt && commit_all "child works elsewhere entirely"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'a\nTRUNK\nc\n' > shared.txt && commit_all "trunk edits the same line after"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # stack_base..pre contains no change to shared.txt, so it is in none of the
  # three intent lists, yet the merge left it conflicted.
  [ -n "$(git ls-files -u -- shared.txt)" ] || fail_test "the fixture did not leave a conflict"
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "an unmerged path outside the child's changes was ignored: $output"
  case "$output" in
    *"REVIEW shared.txt"*) ;;
    *) fail_test "the conflicted path was not named: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit separates a delete/modify from a silent restoration, and can accept it"
(
  new_stack "$work_root/deletemodify"
  printf 'old\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'parent\n' > f.txt && commit_all "parent rewrites it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet f.txt && commit_all "child deletes it"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'trunk moved it on\n' > f.txt && commit_all "trunk changes it after the stack base"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Resolve the delete/modify by keeping trunk's version, as a reviewer might.
  git checkout --quiet trunk -- f.txt 2>/dev/null; git add f.txt

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"REVIEW f.txt"*) ;;
    *) fail_test "a delete/modify was not distinguished from a restoration: $output" ;;
  esac
  # And the decision must be expressible, or the re-run loop cannot end.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "the decision could not be accepted: $output"
) || failures=$((failures + 1))

start_test "audit fails loudly when a tree cannot be read, rather than assuming absence"
(
  new_stack "$work_root/badtree"
  printf 'fork\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'parent\n' > f.txt && commit_all "parent changes it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet f.txt && commit_all "child deletes it"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'fork\n' > f.txt && commit_all "trunk restores the fork-point content"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # An unreadable tree returns nothing, which reads as "trunk deleted it too" and
  # would let a surviving deletion pass as agreed.
  stub_dir="$work_root/badtree_stub"
  mkdir -p "$stub_dir"
  real_git=$(command -v git)
  cat >"$stub_dir/git" <<STUB
#!/usr/bin/env bash
if [ "\$1" = "ls-tree" ]; then
  echo "fatal: bad object" >&2
  exit 128
fi
exec "$real_git" "\$@"
STUB
  chmod +x "$stub_dir/git"

  output=$(PATH="$stub_dir:$PATH" "$subject" audit "$stack_base" "$pre" --trunk trunk 2>&1)
  status=$?
  [ "$status" -ne 0 ] || fail_test "an unreadable tree exited 0: $output"
  case "$output" in
    *"audit clean"*) fail_test "an unreadable tree reported clean: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a surviving deletion that trunk had changed"
(
  new_stack "$work_root/deletionkept"
  printf 'fork\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'parent\n' > f.txt && commit_all "parent changes it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet f.txt && commit_all "child deletes it"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  # Trunk changes it back to the fork-point content, which is invisible from the
  # fork point: git sees trunk as unchanged and keeps the deletion without asking.
  printf 'fork\n' > f.txt && commit_all "trunk restores the fork-point content"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -e f.txt ] && fail_test "the fixture did not keep the deletion"

  # From the stack base, trunk's version differs, so this is a delete/modify that
  # nobody decided — and the path is absent from the result, so a check that only
  # looked for restored files would pass it.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a delete/modify resolved by the older base was reported clean: $output"
  case "$output" in
    *"REVIEW f.txt"*) ;;
    *) fail_test "the path was not named: $output" ;;
  esac
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "the decision could not be accepted: $output"
) || failures=$((failures + 1))

start_test "audit works the same from a subdirectory as from the root"
(
  new_stack "$work_root/subdir"
  mkdir -p nested
  printf 'old\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'registry\n' > registry.rs && commit_all "parent adds registry.rs at the root"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes it"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -e registry.rs ] || fail_test "the fixture did not restore the deleted file"

  # Pathspecs are matched relative to the current directory, so a root-relative
  # name looked up from here would find nothing and the restoration would look
  # like agreement.
  mkdir -p nested && cd nested || exit 1
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "run from a subdirectory, the audit reported clean: $output"
  case "$output" in
    *"RESURRECTED registry.rs"*) ;;
    *) fail_test "the restored path was not reported from a subdirectory: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit does not confuse a directory prefix with the path itself"
(
  new_stack "$work_root/prefix"
  printf 'a file, not a directory\n' > dir && printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'p\n' > p.txt && commit_all "parent work"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  # Replace the file with a directory of the same name: a legitimate change, and
  # one where a pathspec for `dir` also matches `dir/file`.
  git rm --quiet dir && mkdir -p dir && printf 'child\n' > dir/file && commit_all "child makes dir a directory"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -f dir/file ] || fail_test "the fixture did not keep the directory"
  [ ! -f dir ] || fail_test "the fixture still has a file called dir"

  # The child deleted `dir`, and the index contains only `dir/file`. Asking with a
  # pathspec would say `dir` is present and call this a resurrection.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "a file-to-directory replacement was reported: $output"
) || failures=$((failures + 1))

start_test "audit rejects an earlier parent commit when it knows the parent head"
(
  new_stack "$work_root/stalebase"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'p1\n' > p1.txt && commit_all "parent commit one"
  earlier=$(git rev-parse HEAD)
  printf 'registry\n' > registry.rs && commit_all "parent commit two adds registry.rs"
  parent_head=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes registry.rs"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -e registry.rs ] || fail_test "the fixture did not restore the deleted file"

  # The earlier parent commit is an ancestor of the child and absent from trunk,
  # so ancestry alone accepts it — and registry.rs, added after it, appears in no
  # intent diff at all.
  # Ancestry cannot tell: it says so rather than implying the base was verified.
  output=$("$subject" audit "$earlier" "$pre" --trunk trunk 2>&1)
  case "$output" in
    *"checked only by ancestry"*) ;;
    *) fail_test "an unverifiable base was not called out: $output" ;;
  esac
  output=$("$subject" audit "$earlier" "$pre" --trunk trunk --parent-head "$parent_head" 2>&1)
  status=$?
  [ "$status" -ne 0 ] || fail_test "the parent head did not expose the wrong base: $output"
  case "$output" in
    *"is not the newest commit"*) ;;
    *) fail_test "expected the base to be named as wrong, got: $output" ;;
  esac
  # And the right base with the same check still works.
  output=$("$subject" audit "$parent_head" "$pre" --trunk trunk --parent-head "$parent_head" 2>/dev/null)
  case "$output" in
    *"RESURRECTED registry.rs"*) ;;
    *) fail_test "the correct base did not report the resurrection: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit refuses the fork point in place of the stack base"
(
  new_stack "$work_root/forkpointarg"
  printf 'old\n' > keep.txt && commit_all "fork point"
  fork_point=$(git rev-parse HEAD)
  git checkout --quiet -b parent
  printf 'registry\n' > registry.rs && commit_all "parent adds registry.rs"
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes registry.rs"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -e registry.rs ] || fail_test "the fixture did not restore the deleted file"

  # registry.rs exists at neither the fork point nor the child's tip, so with the
  # fork point as the base every intent list is empty and the restoration is
  # invisible. It is also the confusion the document is largely about.
  output=$("$subject" audit "$fork_point" "$pre" --trunk trunk 2>&1)
  status=$?
  [ "$status" -ne 0 ] || fail_test "the fork point was accepted as a stack base: $output"
  case "$output" in
    *"audit clean"*) fail_test "a resurrection went unreported with the wrong base: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit will not report clean when handed the wrong pre-merge tip"
(
  new_stack "$work_root/wrongpre"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'registry\n' > registry.rs && commit_all "parent adds registry.rs"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes registry.rs"
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -e registry.rs ] || fail_test "the fixture did not restore the deleted file"

  # The stack base passed as both arguments: every intent diff is empty, and an
  # empty list looks exactly like nothing to report.
  output=$("$subject" audit "$stack_base" "$stack_base" --trunk trunk 2>&1)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a wrong pre-merge tip reported clean over a resurrection: $output"
) || failures=$((failures + 1))

start_test "audit will not audit a merge of something other than trunk"
(
  new_stack "$work_root/othermerge"
  printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent && printf 'p\n' > p.txt && commit_all "parent"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child && printf 'c\n' > c.txt && commit_all "child adds a file"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  # An unrelated branch, merged instead of trunk. It touches nothing the child
  # touched, so every per-path check would pass and the resulting commit has two
  # parents, which is all the documented parent count looks at.
  git checkout --quiet -b sidebranch trunk
  printf 'unrelated\n' > unrelated.txt && commit_all "an unrelated branch"
  git checkout --quiet child
  git merge --no-ff --no-commit sidebranch >/dev/null 2>&1

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "auditing a merge of something else reported clean: $output"
  case "$output" in
    *INCOMPLETE*) ;;
    *) fail_test "the wrong merge was not called out: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit lets an add/add settled by removing the path be accepted"
(
  new_stack "$work_root/addadd_rm"
  printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'p\n' > p.txt && commit_all "parent work"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'child version\n' > added.txt && commit_all "child adds a file"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'trunk version\n' > added.txt && commit_all "trunk adds the same path differently"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Settle the add/add by deciding the path should not exist at all.
  git rm --quiet -f added.txt >/dev/null 2>&1

  # Absent from the result, but not a plain loss: trunk had its own version, so
  # this was ambiguous and a person resolved it.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"REVIEW added.txt"*) ;;
    *) fail_test "an add/add settled by removal was not treated as a decision: $output" ;;
  esac
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept added.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "the resolution could not be accepted: $output"
) || failures=$((failures + 1))

start_test "audit reports a collision when the merge kept your addition instead"
(
  new_stack "$work_root/dircollision_kept"
  printf 'fork version\n' > dir && printf 'seed\n' > seed.txt && commit_all "fork point has a file called dir"
  git checkout --quiet -b parent
  git rm --quiet dir && commit_all "parent removes it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  mkdir -p dir && printf 'child\n' > dir/file && commit_all "child adds dir/file"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  # Trunk puts the fork's file back, unchanged, so from the fork point trunk
  # looks untouched and the merge keeps the child's directory without asking.
  printf 'fork version\n' > dir && commit_all "trunk restores the file called dir"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -f dir/file ] || fail_test "the fixture did not keep the child's directory"
  [ -z "$(git ls-files -u)" ] || fail_test "the fixture left a conflict, so nothing was silent"

  # dir/file is intact, and trunk has no entry by that name, so the pair is only
  # visible by asking about dir itself.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a file-versus-directory collision was reported clean: $output"
  case "$output" in
    *"REVIEW dir/file"*) ;;
    *) fail_test "the collision was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a lost directory-to-file replacement as a loss"
(
  new_stack "$work_root/dir_to_file"
  printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent
  mkdir -p thing && printf 'inside\n' > thing/inner && commit_all "parent adds a directory"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet -r thing && printf 'now a file\n' > thing && commit_all "child makes it a file"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # A wrong resolution that keeps trunk's directory. Trunk never changed it, so
  # from the stack base the child's file wins and this is a loss.
  git rm --quiet --cached thing >/dev/null 2>&1 || true
  rm -rf thing && mkdir -p thing && printf 'inside\n' > thing/inner && git add thing
  for artifact in 'thing~trunk' 'thing~HEAD'; do
    git rm --quiet --cached -- "$artifact" >/dev/null 2>&1 || true
    rm -rf -- "$artifact"
  done
  [ -z "$(git ls-files -u)" ] || fail_test "the fixture left something unmerged: $(git ls-files -u)"

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept thing 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "--accept excused a lost replacement: $output"
  case "$output" in
    *"LOST thing"*) ;;
    *) fail_test "expected a plain loss, got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit survives trunk turning a file the child edited into a directory"
(
  new_stack "$work_root/file_to_dir"
  printf 'a\nb\nc\n' > thing && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\n' > thing && commit_all "parent edits it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'a\nCHILD\nc\n' > thing && commit_all "child edits it"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git rm --quiet thing && mkdir -p thing && printf 'inside\n' > thing/inner &&
    commit_all "trunk turns it into a directory"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Settle it by keeping trunk's directory, as a reviewer might.
  git rm --quiet --cached thing >/dev/null 2>&1 || true
  rm -rf thing && mkdir -p thing && printf 'inside\n' > thing/inner && git add thing
  for artifact in 'thing~trunk' 'thing~HEAD'; do
    git rm --quiet --cached -- "$artifact" >/dev/null 2>&1 || true
    rm -rf -- "$artifact"
  done
  [ -z "$(git ls-files -u)" ] || fail_test "the fixture left something unmerged: $(git ls-files -u)"

  # A directory where the child has a file cannot be three-way merged: reading it
  # as a blob would abort, and neither choice is deterministic.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>&1)
  status=$?
  case "$output" in
    *"fatal"*|*"cat-file"*) fail_test "the audit crashed on a directory: $output" ;;
  esac
  [ "$status" -ne 0 ] || fail_test "a file-versus-directory conflict was reported clean: $output"
  case "$output" in
    *"REVIEW thing"*) ;;
    *) fail_test "expected a decision, got: $output" ;;
  esac
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept thing 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "the resolution could not be accepted: $output"
) || failures=$((failures + 1))

start_test "audit will not let a pre-existing blocker excuse a lost replacement"
(
  new_stack "$work_root/blocker_unchanged"
  printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'parent version\n' > dir && commit_all "parent adds a file called dir"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet dir && mkdir -p dir && printf 'child\n' > dir/file &&
    commit_all "child replaces it with a directory"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # A wrong resolution that puts trunk's file back and drops the child's
  # directory. Trunk never touched dir, so from the stack base the child's
  # replacement simply wins: this is a loss, not a judgement call.
  rm -rf dir && printf 'parent version\n' > dir
  git rm --quiet -r --cached dir >/dev/null 2>&1 || true
  git add dir
  # git parks trunk's copy at dir~trunk during the collision; clear it so the loss
  # below is the only thing left to report.
  git rm --quiet --cached -- 'dir~trunk' >/dev/null 2>&1 || true
  rm -f -- 'dir~trunk'
  [ -z "$(git ls-files -u)" ] || fail_test "the fixture left something unmerged: $(git ls-files -u)"

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept dir/file 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "--accept excused a lost replacement: $output"
  case "$output" in
    *"LOST dir/file"*) ;;
    *) fail_test "expected a plain loss, got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit treats a file-versus-directory collision as a decision"
(
  new_stack "$work_root/dircollision"
  printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'p\n' > p.txt && commit_all "parent work"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  mkdir -p dir && printf 'child\n' > dir/file && commit_all "child adds dir/file"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'trunk put a file here\n' > dir && commit_all "trunk adds a file called dir"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Settle it by keeping trunk's file at dir, so the child's addition is gone.
  # git parks trunk's version at dir~trunk to avoid the collision, so that has to
  # be cleaned up as part of the resolution too.
  git rm --quiet -r --cached dir >/dev/null 2>&1 || true
  rm -rf dir && printf 'trunk put a file here\n' > dir && git add dir
  git rm --quiet --cached -- 'dir~trunk' >/dev/null 2>&1 || true
  rm -f -- 'dir~trunk'
  [ -z "$(git ls-files -u)" ] || fail_test "the fixture left something unmerged: $(git ls-files -u)"

  # Looked up by its exact name, dir/file is simply absent from trunk, so this
  # reads as a plain loss unless the collision above it is noticed.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"REVIEW dir/file"*) ;;
    *) fail_test "a file-versus-directory collision was not treated as a decision: $output" ;;
  esac
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept dir/file 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "the resolution could not be accepted: $output"
) || failures=$((failures + 1))

start_test "audit still reports a child-only addition the merge dropped"
(
  new_stack "$work_root/addadd_lost"
  printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'p\n' > p.txt && commit_all "parent work"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'child only\n' > mine.txt && commit_all "child adds a file trunk never had"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  git rm --quiet --cached mine.txt >/dev/null

  # Trunk never had this path, so nothing was ambiguous: it is a plain loss and
  # --accept must not touch it.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept mine.txt 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "--accept silenced a dropped addition: $output"
  case "$output" in
    *"LOST mine.txt"*) ;;
    *) fail_test "the dropped addition was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a rename of ours standing against a deletion on trunk"
(
  new_stack "$work_root/renamedelete"
  printf 'keep\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'line one\nline two\nline three\nline four\n' > f.txt && commit_all "parent adds f.txt"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git mv f.txt g.txt && commit_all "child moves it to g.txt"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git rm --quiet f.txt && commit_all "trunk deletes it"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # The intent diffs are read with --no-renames, so the move looks like a
  # deletion of f.txt plus an addition of g.txt: f.txt then appears deleted by
  # both sides, and g.txt looks like an ordinary child-only addition.
  [ -e g.txt ] || fail_test "the fixture did not keep the moved file"
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a rename against a deletion was reported clean: $output"
  case "$output" in
    *"REVIEW f.txt"*) ;;
    *) fail_test "the rename/delete was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a deletion trunk carried away under a new name"
(
  new_stack "$work_root/renamed"
  printf 'keep\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'registry contents\n' > registry.rs && commit_all "parent adds registry.rs"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes registry.rs"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git mv registry.rs registry_moved.rs && commit_all "trunk renames it instead"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # The old path is gone, so the deletion looks honoured, while the content the
  # child deleted is back under another name.
  [ -e registry.rs ] && fail_test "the fixture kept the old path"
  [ -e registry_moved.rs ] || fail_test "the fixture did not restore the content elsewhere"
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a rename of a deleted path was reported clean: $output"
  case "$output" in
    *"REVIEW registry.rs"*) ;;
    *) fail_test "the renamed-away deletion was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit treats a restored old path as a decision when trunk renamed it"
(
  new_stack "$work_root/restored_rename"
  printf 'keep\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'line one\nline two\nline three\nline four\n' > f.txt && commit_all "parent adds f.txt"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet f.txt && commit_all "child deletes f.txt"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git mv f.txt g.txt && commit_all "trunk renames it to g.txt"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Somebody settles the delete-versus-rename by putting the old path back.
  git checkout --quiet "$stack_base" -- f.txt && git add f.txt

  # Trunk has no f.txt, so this looks like both sides agreeing it should go and
  # the old path coming back for no reason — but trunk moved it rather than
  # deleting it, so keeping f.txt is a decision somebody can make.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"REVIEW f.txt"*) ;;
    *) fail_test "a restored path against a trunk rename was not a decision: $output" ;;
  esac
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "the decision could not be accepted: $output"
) || failures=$((failures + 1))

start_test "audit calls a restoration a restoration when both sides deleted the path"
(
  new_stack "$work_root/bothdeleted"
  printf 'old\n' > keep.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'registry\n' > registry.rs && commit_all "parent adds registry.rs"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet registry.rs && commit_all "child deletes registry.rs"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git rm --quiet registry.rs && commit_all "trunk deletes it as well"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # However it got back in, both sides had agreed it should go.
  git checkout --quiet "$stack_base" -- registry.rs && git add registry.rs

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept registry.rs 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a path both sides deleted was accepted back: $output"
  case "$output" in
    *"RESURRECTED registry.rs"*) ;;
    *) fail_test "expected an unambiguous restoration, got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit will not let --accept silence a plain resurrection"
(
  new_stack "$work_root/acceptresurrect"
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
  [ -e registry.rs ] || fail_test "the fixture did not restore the deleted file"

  # Trunk never touched this file, so nothing was ambiguous and nothing was
  # decided: the deletion was simply undone. --accept is for decisions, not for
  # silencing the finding this tooling exists to make.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept registry.rs 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "--accept silenced a resurrection: $output"
  case "$output" in
    *"RESURRECTED registry.rs"*) ;;
    *) fail_test "the resurrection was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit will not let --accept carry a dropped mode change"
(
  new_stack "$work_root/accept_mode"
  printf 'a\nb\nc\n' > run.sh && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\n' > run.sh && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'a\nCHILD\nc\n' > run.sh && commit_all "child edits line 2"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'a\nTRUNK\nc\n' > run.sh && chmod +x run.sh && commit_all "trunk edits line 2 and marks it executable"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Resolve the content by hand but leave the mode alone. The content is genuinely
  # ambiguous; the executable bit is not — only trunk touched it.
  printf 'a\nDECIDED\nc\n' > run.sh && chmod -x run.sh && git add run.sh

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept run.sh 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "--accept carried a dropped executable bit: $output"
  case "$output" in
    *"DISCARDED run.sh"*) ;;
    *) fail_test "expected the mode to be reported, got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit will not let --accept silence a discarded edit"
(
  new_stack "$work_root/acceptdiscard"
  printf 'old\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'new\n' > f.txt && commit_all "parent changes it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'old\n' > f.txt && commit_all "child reverts it"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # The correct merge is unambiguous here: the child's revert simply lost.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept f.txt 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "--accept silenced a discarded edit: $output"
) || failures=$((failures + 1))

start_test "audit will not accept a path that is still unmerged"
(
  new_stack "$work_root/acceptunmerged"
  printf 'a\nb\nc\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\n' > f.txt && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'a\nCHILD\nc\n' > f.txt && commit_all "child edits line 2"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'a\nTRUNK\nc\n' > f.txt && commit_all "trunk edits line 2"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -n "$(git ls-files -u -- f.txt)" ] || fail_test "the fixture did not leave a conflict"

  # Accepting is a statement about a decision that was staged. Nothing is staged
  # here, so it must not be honoured.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept f.txt 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "an unmerged path was accepted: $output"
) || failures=$((failures + 1))

start_test "audit accepts a path a person decided, so the re-run loop can finish"
(
  new_stack "$work_root/accept"
  printf 'l1\nl2\nl3\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\n' > f.txt && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nCHILD\nl3\n' > f.txt && commit_all "child edits line 2 too"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'l1\nTRUNK\nl3\n' > f.txt && commit_all "trunk edits line 2 as well"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Resolve it the way a person would, and stage that decision.
  printf 'l1\nDECIDED BY A HUMAN\nl3\n' > f.txt && git add f.txt

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"REVIEW f.txt"* | *"DISCARDED f.txt"*) ;;
    *) fail_test "a hand-resolved conflict was not raised at all: $output" ;;
  esac
  # Without a way to accept it the audit could never come back clean, so the
  # documented "re-run until clean" loop would not terminate.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "accepting the path did not clear the audit: $output ($status)"
  case "$output" in
    *"ACCEPTED f.txt"*) ;;
    *) fail_test "the accepted path was not named: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit accepts a path whose name contains a newline"
(
  new_stack "$work_root/acceptnl"
  awkward=$(printf 'two\nlines.txt')
  printf 'a\nb\nc\n' > "$awkward" && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\n' > "$awkward" && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'a\nCHILD\nc\n' > "$awkward" && commit_all "child edits line 2"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'a\nTRUNK\nc\n' > "$awkward" && commit_all "trunk edits line 2"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # Decide it by hand and stage that, which is the only situation --accept is
  # for: an ambiguous path, resolved by a person.
  printf 'a\nDECIDED\nc\n' > "$awkward" && git add -- ":(literal)$awkward"

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *REVIEW*) ;;
    *) fail_test "the fixture did not produce an ambiguous finding: $output" ;;
  esac
  # Delimited text cannot hold this path: it would be split into two records,
  # clearing neither the finding nor anything else predictable.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept "$awkward" 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "accepting a path with a newline did not clear it: $output"

  # The other direction, which is the dangerous one: delimited text would let a
  # path named after one line of this one clear a finding about the whole path.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept two 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "accepting an unrelated path cleared this finding: $output"
) || failures=$((failures + 1))

start_test "resolve suggests a removal when the child is the side that deleted"
(
  new_stack "$work_root/rmadvice"
  printf 'OLD\n' > p.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'PARENT VERSION\n' > p.txt && commit_all "parent rewrites it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  git rm --quiet p.txt && commit_all "child deletes it"
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" resolve "$stack_base" p.txt 2>/dev/null)
  # There is no --ours entry to check out when the child deleted the path, so
  # advising checkout --ours would hand over a command that cannot work.
  case "$output" in
    *"git rm --"*) ;;
    *) fail_test "expected a removal to be suggested, got: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a path you changed that trunk deleted"
(
  new_stack "$work_root/modifydelete"
  printf 'old\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'new\n' > f.txt && commit_all "parent changes it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'old\n' > f.txt && commit_all "child reverts it"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git rm --quiet f.txt && commit_all "trunk deletes it"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # From the fork point the child's side looks unchanged, so the deletion applies
  # without a conflict. From the stack base it is a modify/delete that nobody has
  # decided, and the path is in neither the add nor the delete list.
  [ -e f.txt ] && fail_test "the fixture did not reproduce the silent deletion"
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -eq 1 ] || fail_test "expected exit 1, got $status ($output)"
  case "$output" in
    *"REVIEW f.txt"*) ;;
    *) fail_test "a modify/delete against trunk was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit reports a discarded mode revert, where every blob is identical"
(
  new_stack "$work_root/moderevert"
  printf 'script\n' > run.sh && commit_all "fork point"
  git checkout --quiet -b parent
  chmod +x run.sh && commit_all "parent marks it executable"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  chmod -x run.sh && commit_all "child takes the executable bit back off"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # Content never changed, so comparing object ids alone sees three identical
  # blobs and reports nothing while the bit the child removed survives.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"DISCARDED run.sh"*) ;;
    *) fail_test "a mode-only revert was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit will not text-merge a symlink target"
(
  new_stack "$work_root/symlink_merge"
  # A multiline target whose differing line is in the middle. That matters: link
  # blobs have no trailing newline, so a change to the final line makes the
  # textual merge conflict of its own accord and hides the point.
  ln -s "$(printf 'a\nb\nc')" link && printf 'seed\n' > seed.txt && commit_all "fork point"
  git checkout --quiet -b parent
  rm link && ln -s "$(printf 'X\nb\nc')" link && git add -A && commit_all "parent retargets it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  rm link && ln -s "$(printf 'a\nb\nc')" link && git add -A && commit_all "child restores the first line"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  rm link && ln -s "$(printf 'a\nB\nc')" link && git add -A && commit_all "trunk has that change and another"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -z "$(git ls-files -u)" ] || fail_test "the fixture left a conflict, so nothing was silent"

  # From the fork point our side looks unchanged, so trunk's target is staged
  # without a word. git never merges link targets, so a correctly based merge asks
  # a person -- and the reason has to say so rather than describing a text
  # conflict, which is a different claim about a different kind of object.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a changed symlink was reported clean: $output"
  case "$output" in
    *"REVIEW link: both sides changed this symlink"*) ;;
    *) fail_test "expected the link to be judged as a link, got: $output" ;;
  esac
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk --accept link 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "the decision could not be accepted: $output"
) || failures=$((failures + 1))

start_test "audit reports a discarded type change (symlink back to a regular file)"
(
  new_stack "$work_root/typerevert"
  printf 'target\n' > target.txt
  printf 'regular\n' > p.md && commit_all "fork point"
  git checkout --quiet -b parent
  rm p.md && ln -s target.txt p.md && git add -A && commit_all "parent makes it a symlink"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  rm p.md && printf 'regular\n' > p.md && git add -A && commit_all "child puts the file back"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # git calls this a type change, so it appears under neither M nor A nor D.
  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  case "$output" in
    *"DISCARDED p.md"*) ;;
    *) fail_test "a discarded type change was not reported: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit will not mistake an earlier merge on the child for this one"
(
  new_stack "$work_root/childmerge"
  printf 'old\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent && printf 'new\n' > f.txt && commit_all "parent changes it"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child && printf 'child\n' > c.txt && commit_all "child work"
  # The parent takes a review fix after the child split off, and the child merges
  # it — exactly what the documented workflow says to do while the parent PR is
  # open. The child's tip is then a merge commit whose second parent is the
  # parent branch, so HEAD^2 resolves to something that is not trunk.
  git checkout --quiet parent && printf 'review fix\n' > fix.txt && commit_all "parent review fix"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet child
  git merge --no-ff --no-edit parent >/dev/null 2>&1
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  git checkout --quiet child
  # No trunk merge started. HEAD^2 exists from the child's own merge, and taking
  # it for trunk would let the audit claim a check it never made.

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>&1)
  status=$?
  # Callers gate on the status, so an audit that could not check everything must
  # not report success however clearly it explains itself.
  [ "$status" -ne 0 ] || fail_test "an incomplete audit exited 0: $output"
  case "$output" in
    *"only additions and deletions were checked"*) ;;
    *) fail_test "an unrelated merge was mistaken for this one: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit does not cry wolf when both sides' edits survive the merge"
(
  new_stack "$work_root/nofalse"
  # The three edits are kept far apart. Adjacent ones conflict even when nothing
  # about the merge is ambiguous, and a conflicted path would make this case pass
  # without ever comparing a merged result.
  printf 'a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\n' > f.txt && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'a\nPARENT\nc\nd\ne\nf\ng\nh\ni\nj\nk\nCHILD\n' > f.txt && commit_all "child edits line 12"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'a\nPARENT\nc\nd\nTRUNK\nf\ng\nh\ni\nj\nk\nl\n' > f.txt && commit_all "trunk edits line 5"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ -z "$(git ls-files -u -- f.txt)" ] ||
    fail_test "the fixture conflicts, so it never exercises a merged comparison"

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "a merge that kept both edits was reported: $output"
  case "$output" in
    *DISCARDED*) fail_test "false positive on a normally merged edit: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit says so when it cannot check modifications, rather than implying it did"
(
  new_stack "$work_root/nomerge"
  printf 'old\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent && printf 'new\n' > f.txt && commit_all "parent"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child && printf 'old\n' > f.txt && commit_all "child reverts"
  pre=$(git rev-parse HEAD)
  # No merge started, so there is no other side to compare against.

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>&1)
  status=$?
  # Callers gate on the status, so an audit that could not check everything must
  # not report success however clearly it explains itself.
  [ "$status" -ne 0 ] || fail_test "an incomplete audit exited 0: $output"
  case "$output" in
    *"only additions and deletions were checked"*) ;;
    *) fail_test "a narrowed audit did not say so: $output" ;;
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

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
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
  printf 'parent\n' > p.rs && commit_all "parent adds p.rs"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'child\n' > c.rs && commit_all "child adds its own file"
  pre=$(git rev-parse HEAD)
  squash_parent_onto_trunk
  printf 'moved on\n' > t.rs && commit_all "trunk moves on elsewhere"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" audit "$stack_base" "$pre" --trunk trunk 2>/dev/null)
  status=$?
  [ "$status" -eq 0 ] || fail_test "expected exit 0 on a clean audit, got $status ($output)"
  case "$output" in
    *RESURRECTED* | *LOST* | *DISCARDED* | *REVIEW* | *INCOMPLETE*)
      fail_test "clean audit reported a finding: $output" ;;
  esac
) || failures=$((failures + 1))

# ---------------------------------------------------------------------------
# resolve
# ---------------------------------------------------------------------------

start_test "audit fails loudly on an unusable revision instead of reporting clean"
(
  new_stack "$work_root/badrev"
  printf 'old\n' > keep.txt && commit_all "fork point"
  pre=$(git rev-parse HEAD)

  output=$("$subject" audit does-not-exist "$pre" --trunk trunk 2>&1)
  status=$?
  [ "$status" -ne 0 ] || fail_test "a bad revision exited 0"
  case "$output" in
    *"audit clean"*) fail_test "a bad revision reported a clean audit: $output" ;;
  esac
) || failures=$((failures + 1))

start_test "audit fails loudly when the index cannot be read, rather than saying clean"
(
  new_stack "$work_root/badindex"
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

  # A `git` whose `ls-files` fails the way a damaged index does. Treating that as
  # "not in the index" would report a clean audit for a child whose entire
  # contribution is a deletion — precisely the miss this guards against.
  stub_dir="$work_root/badindex_stub"
  mkdir -p "$stub_dir"
  real_git=$(command -v git)
  cat >"$stub_dir/git" <<STUB
#!/usr/bin/env bash
if [ "\$1" = "ls-files" ]; then
  echo "fatal: index file corrupt" >&2
  exit 128
fi
exec "$real_git" "\$@"
STUB
  chmod +x "$stub_dir/git"

  output=$(PATH="$stub_dir:$PATH" "$subject" audit "$stack_base" "$pre" --trunk trunk 2>&1)
  status=$?
  [ "$status" -ne 0 ] || fail_test "an unreadable index exited 0"
  case "$output" in
    *"audit clean"*) fail_test "an unreadable index reported a clean audit: $output" ;;
  esac
) || failures=$((failures + 1))

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

start_test "resolve keeps the executable bit both sides agreed on"
(
  new_stack "$work_root/agreedmode"
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > run.sh && chmod +x run.sh && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > run.sh && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > run.sh && commit_all "child edits"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > run.sh && commit_all "trunk edits"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  [ "$(git ls-files --stage -- run.sh | awk '$3 == 2 { print $1 }')" = 100755 ] ||
    fail_test "the fixture lost the executable bit before the merge"
  # Someone took the bit off the worktree copy; both sides still agree it is
  # executable, and copying content over it would record whatever is on disk.
  chmod -x run.sh

  "$subject" resolve "$stack_base" run.sh >/dev/null 2>&1
  status=$?
  [ "$status" -eq 0 ] || fail_test "expected a clean resolve, got $status"
  [ "$(git ls-files --stage -- run.sh | awk '{ print $1 }')" = 100755 ] ||
    fail_test "the agreed executable bit was not staged: $(git ls-files --stage -- run.sh)"
  # The worktree has to agree, or step 5's clean-status check fails and a later
  # add can restage the mode the other way.
  [ -x run.sh ] || fail_test "the worktree copy is not executable, so it disagrees with the index"
  [ -z "$(git diff --name-only -- run.sh)" ] ||
    fail_test "an unstaged mode change was left behind: $(git diff -- run.sh | head -3)"
) || failures=$((failures + 1))

start_test "resolve refuses a path whose directory is a symlink"
(
  new_stack "$work_root/symlinked_dir"
  mkdir -p real
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > real/f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > real/f.txt && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > real/f.txt && commit_all "child edits"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > real/f.txt && commit_all "trunk edits"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # Replace the directory with a link to somewhere else entirely. The final
  # component is still a regular file, so a check that only looks at it passes
  # and the write lands outside the worktree.
  mkdir -p "$work_root/symlinked_dir_elsewhere"
  printf 'OUTSIDE\n' > "$work_root/symlinked_dir_elsewhere/f.txt"
  rm -rf real && ln -s "$work_root/symlinked_dir_elsewhere" real

  output=$("$subject" resolve "$stack_base" real/f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 for a symlinked directory, got $status ($output)"
  [ "$(cat "$work_root/symlinked_dir_elsewhere/f.txt")" = "OUTSIDE" ] ||
    fail_test "a file outside the worktree was overwritten"
) || failures=$((failures + 1))

start_test "resolve refuses when the link count cannot be read"
(
  new_stack "$work_root/statless"
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > f.txt && commit_all "child edits"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "trunk edits"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  before=$(cat f.txt)

  # A stat that answers with prose rather than a number, which is what the wrong
  # implementation order produces. An unreadable count must not read as "one
  # name, safe to overwrite".
  stub_dir="$work_root/statless_stub"
  mkdir -p "$stub_dir"
  cat >"$stub_dir/stat" <<'STUB'
#!/usr/bin/env bash
echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
exit 0
STUB
  chmod +x "$stub_dir/stat"

  output=$(PATH="$stub_dir:$PATH" "$subject" resolve "$stack_base" f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 when the link count is unreadable, got $status ($output)"
  [ "$(cat f.txt)" = "$before" ] || fail_test "the file was written anyway"
) || failures=$((failures + 1))

start_test "resolve refuses a conflicted path that is a hard link"
(
  new_stack "$work_root/hardlink"
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > f.txt && commit_all "child edits"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "trunk edits"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # Another name for the same inode: copying over one truncates the other.
  ln f.txt "$work_root/hardlink_other" 2>/dev/null || exit 0   # no hard links here
  before=$(cat "$work_root/hardlink_other")

  output=$("$subject" resolve "$stack_base" f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 for a hard link, got $status ($output)"
  [ "$(cat "$work_root/hardlink_other")" = "$before" ] ||
    fail_test "the other name for the inode was rewritten"
) || failures=$((failures + 1))

start_test "resolve keeps the agreed executable bit on a real conflict too"
(
  new_stack "$work_root/conflictmode"
  printf 'a\nb\nc\n' > run.sh && chmod +x run.sh && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'a\nPARENT\nc\n' > run.sh && commit_all "parent edits line 2"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'a\nCHILD\nc\n' > run.sh && commit_all "child edits line 2"
  squash_parent_onto_trunk
  printf 'a\nTRUNK\nc\n' > run.sh && commit_all "trunk edits line 2"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  chmod -x run.sh

  output=$("$subject" resolve "$stack_base" run.sh 2>/dev/null)
  status=$?
  [ "$status" -eq 1 ] || fail_test "expected exit 1 for a real conflict, got $status ($output)"
  # The markers are written for a person to resolve, and staging that resolution
  # must not record a mode neither side asked for.
  [ -x run.sh ] || fail_test "the conflicted worktree copy is not executable"
  [ -n "$(git ls-files -u -- run.sh)" ] || fail_test "the path was resolved without a human"
) || failures=$((failures + 1))

start_test "resolve refuses a conflicted path replaced by a directory"
(
  new_stack "$work_root/worktree_dir"
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > f.txt && commit_all "child edits"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "trunk edits"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  rm -f f.txt && mkdir f.txt

  # cp would write *inside* the directory, under a name nobody is looking at.
  output=$("$subject" resolve "$stack_base" f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 for a directory, got $status ($output)"
  [ -z "$(ls -A f.txt)" ] || fail_test "something was written inside the directory"
) || failures=$((failures + 1))

start_test "resolve refuses when the worktree entry is a symlink the index knows nothing about"
(
  new_stack "$work_root/worktree_symlink"
  printf 'TARGET CONTENT\n' > target.txt
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > f.txt && commit_all "child edits"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "trunk edits"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  # Both stages are regular files; only the worktree entry is a link, so a guard
  # that reads the index alone would let cp follow it.
  rm -f f.txt && ln -s target.txt f.txt

  output=$("$subject" resolve "$stack_base" f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 for a symlinked worktree entry, got $status ($output)"
  [ "$(cat target.txt)" = "TARGET CONTENT" ] ||
    fail_test "the link's target was overwritten"
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
  # both sides against it, so only line 3 conflicts. Under the older base PARENT
  # appears inside both arms, so finding it somewhere proves nothing -- require
  # exactly one, above the first marker.
  [ "$(grep -c '^PARENT$' f.txt)" -eq 1 ] ||
    fail_test "PARENT appears $(grep -c '^PARENT$' f.txt) times, so the wrong base was used"
  [ "$(grep -n '^PARENT$' f.txt | head -1 | cut -d: -f1)" \
    -lt "$(grep -n '^<<<<<<<' f.txt | head -1 | cut -d: -f1)" ] ||
    fail_test "the parent's line was dragged inside the conflict markers"
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

start_test "resolve refuses a fatal merge-file even when it wrote partial output"
(
  new_stack "$work_root/fatal"
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > f.txt && commit_all "child edits"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > f.txt && commit_all "trunk edits"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  before=$(cat f.txt)

  # A `git` that fails `merge-file` the way an interrupted or failed run does:
  # some output on stdout, then a fatal status. Deciding by "did it write
  # anything" would copy that fragment over the conflicted file.
  stub_dir="$work_root/fatal_stub"
  mkdir -p "$stub_dir"
  real_git=$(command -v git)
  cat >"$stub_dir/git" <<STUB
#!/usr/bin/env bash
if [ "\$1" = "merge-file" ]; then
  printf 'PARTIAL OUTPUT FROM A FAILED RUN\n'
  exit 255
fi
exec "$real_git" "\$@"
STUB
  chmod +x "$stub_dir/git"

  output=$(PATH="$stub_dir:$PATH" "$subject" resolve "$stack_base" f.txt 2>/dev/null)
  status=$?
  [ "$status" -eq 2 ] || fail_test "expected exit 2 for a fatal merge-file, got $status ($output)"
  [ "$(cat f.txt)" = "$before" ] ||
    fail_test "the partial output of a failed run was written over the conflicted file"
) || failures=$((failures + 1))

start_test "resolve's recovery suggestion is valid shell for a path with a quote in it"
(
  new_stack "$work_root/quoted"
  awkward="it's[1].sh"
  # A mode conflict, because that reaches the manual branch reliably: a path the
  # parent *added* and the child deleted is restored without conflicting at all,
  # which is the resurrection the audit catches rather than something to resolve.
  printf 'script\n' > "$awkward" && commit_all "fork point"
  git checkout --quiet -b parent
  printf 'script\nparent\n' > "$awkward" && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'script\nparent\nchild\n' > "$awkward" && commit_all "child edits"
  squash_parent_onto_trunk
  chmod +x "$awkward" && commit_all "trunk marks it executable"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1

  output=$("$subject" resolve "$stack_base" "$awkward" 2>/dev/null)
  suggestion=${output#*take one side whole: }
  [ "$suggestion" != "$output" ] || fail_test "no recovery suggestion was printed: $output"
  # The suggestion is meant to be pasted, and this is the branch that handles
  # exactly the paths a naive quoting scheme breaks on.
  printf '%s\n' "$suggestion" > "$work_root/suggestion.sh"
  bash -n "$work_root/suggestion.sh" 2>/dev/null ||
    fail_test "the suggested command is not valid shell: $suggestion"
) || failures=$((failures + 1))

start_test "resolve stages only the named path when the name contains glob characters"
(
  new_stack "$work_root/globby"
  # Both names exist, and the first one read as a pathspec pattern matches the
  # second: staging by filename rather than by literal pathspec would sweep an
  # unrelated file into the merge commit.
  printf 'l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\n' > 'weird[1].txt'
  printf 'unrelated\n' > weird1.txt
  commit_all "fork point"
  git checkout --quiet -b parent
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nl8\n' > 'weird[1].txt' && commit_all "parent edits"
  stack_base=$(git rev-parse HEAD)
  git checkout --quiet -b child
  printf 'l1\nPARENT\nl3\nl4\nl5\nl6\nl7\nCHILD\n' > 'weird[1].txt' && commit_all "child edits"
  squash_parent_onto_trunk
  printf 'l1\nPARENT\nTRUNK\nl4\nl5\nl6\nl7\nl8\n' > 'weird[1].txt' && commit_all "trunk edits"
  git checkout --quiet child
  git merge --no-ff --no-commit trunk >/dev/null 2>&1
  # An uncommitted change to the collateral file: if it gets staged, the merge
  # commit carries an edit nobody reviewed.
  printf 'modified but not staged\n' > weird1.txt

  "$subject" resolve "$stack_base" 'weird[1].txt' >/dev/null 2>&1
  status=$?
  # An unresolved path is listed by `diff --cached` too, so without these the
  # case would pass on a resolve that staged nothing at all.
  [ "$status" -eq 0 ] || fail_test "resolve did not succeed: exit $status"
  [ -z "$(git ls-files -u -- ':(literal)weird[1].txt')" ] ||
    fail_test "the named path is still unmerged, so nothing was staged"
  git diff --cached --name-only | grep -qx 'weird1.txt' &&
    fail_test "an unrelated path matching the glob was staged"
  git diff --cached --name-only | grep -qFx 'weird[1].txt' ||
    fail_test "the named path was not staged"
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
  # Narrow the refspec the way `git clone --single-branch` does: origin/trunk
  # exists but a bare `git fetch origin` will never touch it again.
  git config remote.origin.fetch '+refs/heads/child:refs/remotes/origin/child'

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
