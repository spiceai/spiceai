#!/usr/bin/env bash
#
# Restack a stacked branch after its parent squash-merges into trunk, and audit
# what that merge actually did.
#
# `trunk` takes squash merges, so a parent branch's commits never appear on it.
# A child branched from that parent therefore has a merge base older than the
# point it split from -- and git resolves the resulting merge against that older
# base. Files only the parent touched merge silently and correctly; files both
# branches edited conflict spuriously; and files the parent added that the child
# deleted are restored with no conflict reported at all. That last one is what
# this exists for: restacking #12891 after its parent #12661 landed put
# crates/runtime-table/src/table_layers.rs back -- half of the registry #12891
# was written to delete -- and the conflict list never mentioned it.
#
# Two subcommands do the mechanical parts, both of which have failed in ways that
# print nothing:
#
#   resolve  Re-resolve one conflicted path three-way against the stack base
#            rather than the base git chose.
#   audit    Report paths the merge resurrected or lost, against what the child
#            intended.
#
# The reasoning -- when to rebase instead, what the stack base is, why the audit
# is not optional -- is in docs/dev/stacked_prs.md. This script is the part worth
# testing rather than retyping; scripts/test_restack_stacked_branch.sh covers
# every failure mode named below, each of which was a real defect.
#
# Usage:
#   scripts/restack_stacked_branch.sh stack-base <parent-pr>
#   scripts/restack_stacked_branch.sh resolve <stack-base> <path>
#   scripts/restack_stacked_branch.sh audit <stack-base> <pre-merge-tip>

set -uo pipefail

usage() {
  sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

die() {
  echo "$*" >&2
  exit 2
}

# `git fetch origin <refspec>` leaves its result in FETCH_HEAD and does not
# update origin/trunk, yet every comparison, rebase, and merge here is against
# origin/trunk. A stale tracking ref restacks onto a commit predating the
# parent's squash -- the wrong base, with no error -- so refresh both.
#
# The stack base is the newest parent commit contained in the child, which is
# not the parent PR's final head whenever the parent took commits the child
# never merged. Using the head instead would read that parent work as child
# deletions and let the three-way merge drop it.
cmd_stack_base() {
  local parent_pr="${1:-}"
  [ -n "$parent_pr" ] || die "usage: stack-base <parent-pr>"

  git fetch --quiet origin || die "could not fetch origin"
  git fetch --quiet origin "refs/pull/${parent_pr}/head" ||
    die "could not fetch refs/pull/${parent_pr}/head"

  local parent_head
  parent_head=$(git rev-parse FETCH_HEAD) || die "could not resolve FETCH_HEAD"
  git merge-base HEAD "$parent_head" ||
    die "no common commit between HEAD and PR #${parent_pr}"
}

# Exit status:
#   0  resolved cleanly and staged
#   1  conflict markers written to the worktree, deliberately left unstaged
#   2  needs manual resolution; the worktree is untouched
cmd_resolve() {
  local stack_base="${1:-}" path="${2:-}"
  [ -n "$stack_base" ] && [ -n "$path" ] || die "usage: resolve <stack-base> <path>"

  # Compare our mode against trunk's, not the first line `git ls-files --stage`
  # prints -- that is stage 1, the base's mode. `cp` copies content and leaves
  # the destination's mode alone, which breaks two ways: a symlink destination
  # is followed and written *through* (resolving CLAUDE.md that way overwrites
  # .github/copilot-instructions.md while the link still looks untouched), and a
  # 100644-vs-100755 disagreement silently drops an executable bit. Comparing
  # stage 2 to stage 3 covers both, plus a missing stage, in one test.
  local ours_mode theirs_mode
  ours_mode=$(git ls-files --stage -- ":(literal)$path" | awk '$3 == 2 { print $1 }')
  theirs_mode=$(git ls-files --stage -- ":(literal)$path" | awk '$3 == 3 { print $1 }')

  if [ "${ours_mode:-none}" != "${theirs_mode:-none}" ] || [ "$ours_mode" = 120000 ]; then
    # The suggestion is meant to be pasted, and this branch exists for the awkward
    # paths, so escape rather than wrap in quotes: a path containing a single
    # quote would otherwise produce a command that is not valid shell at all.
    local quoted
    quoted=$(printf '%q' ":(literal)$path")
    echo "MANUAL $path: ours=${ours_mode:-none} theirs=${theirs_mode:-none} (mode, symlink, or missing stage)"
    echo "  take one side whole: git checkout --ours -- $quoted && git add -- $quoted"
    return 2
  fi

  # A private directory, never fixed /tmp names: those would follow a
  # pre-existing symlink at that path and truncate whatever it points at.
  local tmp rc
  tmp=$(mktemp -d) || die "could not create a private temp directory"
  resolve_with_tmp "$stack_base" "$path" "$tmp"
  rc=$?
  rm -rf "$tmp"
  return "$rc"
}

resolve_with_tmp() {
  local stack_base="$1" path="$2" tmp="$3"

  # A redirection creates its file before git runs, so a failed `git show`
  # leaves an empty file that merge-file reads as "this side is empty" rather
  # than "this side is missing" -- which can merge cleanly and stage the wrong
  # content. Reachable here: the parent rewrites a file, the child deletes it,
  # trunk squashes the parent, so there is no stage 2 while stage 3 matches the
  # stack base exactly. That merges to an empty file where a deletion was meant.
  # The stack base being newer than git's stage 1 is also why the third
  # extraction can fail on a genuinely three-stage conflict.
  if ! git show ":2:$path" > "$tmp/ours" 2>/dev/null ||
     ! git show ":3:$path" > "$tmp/theirs" 2>/dev/null ||
     ! git show "$stack_base:$path" > "$tmp/base" 2>/dev/null; then
    echo "MANUAL $path: a side could not be extracted (missing stage, or absent from $stack_base)"
    return 2
  fi

  local merge_status
  git merge-file -p --diff3 \
    -L ours -L "base ($stack_base)" -L trunk \
    "$tmp/ours" "$tmp/base" "$tmp/theirs" > "$tmp/merged" 2>/dev/null
  merge_status=$?

  if [ "$merge_status" -eq 0 ]; then
    # `:(literal)` because git add takes a pathspec, not a filename: a conflicted
    # path containing *, ? or [] would otherwise stage every other path it matches.
    cp -- "$tmp/merged" "$path" && git add -- ":(literal)$path" || die "could not stage $path"
    echo "RESOLVED $path"
    return 0
  fi

  # merge-file reports conflicts as a count in 1..127 and fatal errors above that.
  # Only the first range means the output is a conflict worth keeping: it is the
  # correctly based conflict this step exists to produce, so it goes to the
  # worktree unstaged, leaving the path unmerged until a human agrees. Discarding
  # it would send the reader back to git's spurious-base conflict. Deciding by
  # "did it write anything" instead would copy the partial output of an
  # interrupted or failed run over the file.
  if [ "$merge_status" -ge 1 ] && [ "$merge_status" -le 127 ]; then
    cp -- "$tmp/merged" "$path" || die "could not write $path"
    echo "CONFLICT $path: correctly based markers written, left unstaged"
    return 1
  fi

  echo "MANUAL $path: merge-file failed (status $merge_status); worktree left untouched"
  return 2
}

# Is the path in the index? 0 yes, 1 no -- and anything else is a broken
# repository rather than an answer. `git ls-files --error-unmatch` returns 1 for
# an absent path but 128 for a fatal problem such as an unreadable index, and
# reading that as "absent" would report `audit clean` for a child whose work is
# entirely deletions: the same false negative the audit exists to prevent.
path_in_index() {
  local candidate="$1" status
  git ls-files --error-unmatch -- ":(literal)$candidate" >/dev/null 2>&1
  status=$?
  case "$status" in
    0) return 0 ;;
    1) return 1 ;;
    *) die "git ls-files failed with status $status while checking $candidate" ;;
  esac
}

# Exit status: 0 if the merge did what the child intended, 1 otherwise.
#
# The comparison is against the pre-merge tip, and membership is tested in the
# *index*, because `git commit` records the index. Both matter on a re-run after
# a correction: `rm` a resurrected file without staging the removal and a
# filesystem test reports it gone while the index still carries it, so the audit
# says clean and the commit contains the file anyway.
#
# `--no-renames` is not optional either. Rename detection is on by default and
# reports a move as a single `R` entry, so the old path lands under neither
# filter -- and a stack that carves or moves code is mostly renames. `-z` for
# the same class of reason: git C-quotes any path containing a tab, newline,
# backslash, or non-ASCII byte, and that quoted string names no file.
cmd_audit() {
  local stack_base="${1:-}" pre="${2:-}"
  [ -n "$stack_base" ] && [ -n "$pre" ] || die "usage: audit <stack-base> <pre-merge-tip>"

  # Both revisions are checked first, and each diff is written to a file whose
  # exit status is tested, because a process substitution discards it: a
  # mistyped or unreachable revision would otherwise feed the loops zero paths
  # and this would print `audit clean` — the precise false negative the audit
  # exists to prevent. The diffs cannot go through a variable instead, since a
  # shell cannot hold the NUL bytes that keep odd filenames intact.
  git rev-parse --verify --quiet "$stack_base^{commit}" >/dev/null ||
    die "not a commit: $stack_base"
  git rev-parse --verify --quiet "$pre^{commit}" >/dev/null ||
    die "not a commit: $pre"

  local tmp
  tmp=$(mktemp -d) || die "could not create a private temp directory"

  if ! git diff --name-only -z --no-renames --diff-filter=D \
         "$stack_base" "$pre" -- > "$tmp/deleted"; then
    rm -rf "$tmp"
    die "could not diff $stack_base..$pre for deletions"
  fi
  if ! git diff --name-only -z --no-renames --diff-filter=A \
         "$stack_base" "$pre" -- > "$tmp/added"; then
    rm -rf "$tmp"
    die "could not diff $stack_base..$pre for additions"
  fi

  local findings=0 path

  while IFS= read -r -d '' path; do
    if path_in_index "$path"; then
      echo "RESURRECTED $path"
      findings=$((findings + 1))
    fi
  done < "$tmp/deleted"

  while IFS= read -r -d '' path; do
    if ! path_in_index "$path"; then
      echo "LOST $path"
      findings=$((findings + 1))
    fi
  done < "$tmp/added"

  rm -rf "$tmp"

  if [ "$findings" -ne 0 ]; then
    echo "audit found $findings path(s) the merge changed against the branch's intent" >&2
    return 1
  fi
  echo "audit clean"
  return 0
}

main() {
  local subcommand="${1:-}"
  shift || true

  case "$subcommand" in
    stack-base) cmd_stack_base "$@" ;;
    resolve) cmd_resolve "$@" ;;
    audit) cmd_audit "$@" ;;
    -h | --help | help | "") usage ;;
    *)
      echo "unknown subcommand: $subcommand" >&2
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
