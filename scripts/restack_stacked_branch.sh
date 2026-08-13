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
# Two subcommands do the mechanical parts, both of which fail silently when done
# by hand:
#
#   resolve  Re-resolve one conflicted path three-way against the stack base
#            rather than the base git chose.
#   audit    Report paths the merge resurrected, lost, or discarded the content
#            of, against what the child intended.
#
# The reasoning -- when to rebase instead, what the stack base is, why the audit
# is not optional -- is in docs/dev/stacked_prs.md. This script is the part worth
# testing rather than retyping: scripts/test_restack_stacked_branch.sh pins every
# failure mode named below, all of which report success while doing the wrong
# thing, so nothing else would catch them.
#
# Paths are repository-relative, as git's own diffs report them; the commands move
# to the worktree root before doing anything.
#
# Usage:
#   scripts/restack_stacked_branch.sh stack-base <parent-pr> [--child <ref>]
#   scripts/restack_stacked_branch.sh resolve <stack-base> <path>
#   scripts/restack_stacked_branch.sh audit <stack-base> <pre-merge-tip>
#            [--trunk <rev>] [--parent-head <rev>] [--accept <path>]...

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
  local parent_pr="${1:-}" child="HEAD"
  [ -n "$parent_pr" ] || die "usage: stack-base <parent-pr> [--child <ref>]"
  shift
  while [ $# -gt 0 ]; do
    case "$1" in
      --child)
        shift
        [ -n "${1:-}" ] || die "--child needs a ref"
        child="$1"
        shift
        ;;
      *) die "unknown option for stack-base: $1" ;;
    esac
  done

  # An explicit refspec rather than a bare `git fetch origin`, which obeys
  # `remote.origin.fetch`: a --single-branch clone narrows that to the checked-out
  # branch, so origin/trunk would stay stale or missing while this claimed to have
  # refreshed it -- and everything downstream would then use the wrong base.
  git fetch --quiet origin "+refs/heads/trunk:refs/remotes/origin/trunk" ||
    die "could not fetch trunk from origin"
  git fetch --quiet origin "refs/pull/${parent_pr}/head" ||
    die "could not fetch refs/pull/${parent_pr}/head"

  local parent_head base ancestry
  parent_head=$(git rev-parse FETCH_HEAD) || die "could not resolve FETCH_HEAD"
  base=$(git merge-base "$child" "$parent_head") ||
    die "no common commit between $child and PR #${parent_pr}"

  # Run from trunk rather than from the child and this returns the fork point,
  # which is the wrong boundary everywhere it is used -- a rebase from it replays
  # the parent's commits as the child's. A commit trunk already contains cannot
  # be a stack base, so say so here instead of leaving it to be discovered later.
  git merge-base --is-ancestor "$base" "refs/remotes/origin/trunk"
  ancestry=$?
  case "$ancestry" in
    1) ;;
    0) die "$child shares only ${base} with PR #${parent_pr}, and trunk already contains it: check out the child branch, or pass --child <ref>" ;;
    *) die "could not tell whether ${base} is contained in origin/trunk (status $ancestry)" ;;
  esac
  printf '%s\n' "$base"
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

  # The stages say what git recorded; the worktree says what cp would write
  # into. A conflicted path replaced on disk by a symlink still has two
  # regular-file stages, and copying onto it follows the link and rewrites
  # whatever it points at. A directory, fifo, socket or device is no better: cp
  # would write inside it, block on it, or overwrite it.
  local worktree_oddity=""
  if [ -L "$path" ]; then
    worktree_oddity="a symlink"
  elif [ -e "$path" ] && [ ! -f "$path" ]; then
    worktree_oddity="not a regular file"
  else
    worktree_oddity=$(unsafe_worktree_path "$path") || worktree_oddity=""
  fi
  if [ -n "$worktree_oddity" ] ||
     [ "${ours_mode:-none}" != "${theirs_mode:-none}" ] || [ "$ours_mode" = 120000 ]; then
    # The suggestion is meant to be pasted, and this branch exists for the awkward
    # paths, so escape rather than wrap in quotes: a path containing a single
    # quote would otherwise produce a command that is not valid shell at all.
    local quoted
    quoted=$(printf '%q' ":(literal)$path")
    if [ -n "$worktree_oddity" ]; then
      echo "MANUAL $path: the worktree entry is $worktree_oddity, whatever the index says"
    else
      echo "MANUAL $path: ours=${ours_mode:-none} theirs=${theirs_mode:-none} (mode, symlink, or missing stage)"
    fi
    if [ -z "$ours_mode" ]; then
      # There is no --ours entry to check out; the child deleted this path, so
      # the command that keeps the child's side is a removal.
      echo "  keep your deletion: git rm -- $quoted"
    else
      echo "  take one side whole: git checkout --ours -- $quoted && git add -- $quoted"
    fi
    return 2
  fi

  # A private directory, never fixed /tmp names: those would follow a
  # pre-existing symlink at that path and truncate whatever it points at.
  local tmp rc
  tmp=$(mktemp -d) || die "could not create a private temp directory"
  resolve_with_tmp "$stack_base" "$path" "$tmp" "$ours_mode"
  rc=$?
  rm -rf "$tmp"
  return "$rc"
}

resolve_with_tmp() {
  local stack_base="$1" path="$2" tmp="$3" agreed_mode="$4"

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
    # cp keeps an existing destination's permissions, and gives a new one the
    # temporary file's, so the executable bit both sides agreed on can be lost or
    # invented here. Set it from the stages before staging.
    cp -- "$tmp/merged" "$path" || die "could not write $path"
    git add -- ":(literal)$path" || die "could not stage $path"
    # Set the recorded mode through git rather than chmod: the executable bit is
    # the only part git keeps, and BSD chmod has no -- to protect a path that
    # begins with a dash.
    case "$agreed_mode" in
      100755) git update-index --chmod=+x -- "$path" ||
        die "could not restore the executable bit on $path" ;;
      100644) git update-index --chmod=-x -- "$path" ||
        die "could not clear the executable bit on $path" ;;
    esac
    # --chmod touches the index alone, which would leave the worktree disagreeing
    # with it: an unstaged mode change that fails the clean-status check in step 5
    # and that a later `git add` could restage the wrong way round. Write the file
    # back out of the index so the two match.
    git checkout-index -f -- "$path" || die "could not re-materialize $path"
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
    # The mode both sides agreed on has to survive here too, or staging the
    # resolved conflict later records whatever the worktree happened to have.
    # chmod rather than update-index, because this path stays unmerged on
    # purpose; ./ because BSD chmod has no -- for a name beginning with a dash.
    case "$agreed_mode" in
      100755) chmod +x "./$path" || die "could not restore the executable bit on $path" ;;
      100644) chmod -x "./$path" || die "could not clear the executable bit on $path" ;;
    esac
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
  index_fatal_check "$1"
  git rev-parse --verify --quiet ":0:$1" >/dev/null 2>&1
}

# The commit being merged in, which the modification check compares against:
# MERGE_HEAD while the merge is open, the second parent once it is committed.
#
# HEAD^2 is only this merge's other side when HEAD is the merge *of the pre-merge
# tip*. A child that merged its parent while the parent PR was open is itself a
# merge commit, so without that check an older merge's second parent would be
# mistaken for trunk and the audit would claim a full check it did not make.
merge_other_side() {
  local pre_tip="$1" git_dir first
  git_dir=$(git rev-parse --git-dir) || return 1
  if [ -f "$git_dir/MERGE_HEAD" ]; then
    # During a --no-commit merge HEAD is the pre-merge tip by definition, so if
    # it is not, the caller named the wrong commit. That matters more than it
    # looks: a wrong pre-merge tip empties the intent diffs, and empty lists are
    # indistinguishable from nothing to report.
    [ "$(git rev-parse --verify --quiet HEAD)" = \
      "$(git rev-parse --verify --quiet "$pre_tip")" ] || return 1
    git rev-parse --verify --quiet MERGE_HEAD
    return $?
  fi
  first=$(git rev-parse --verify --quiet "HEAD^1") || return 1
  [ "$first" = "$(git rev-parse --verify --quiet "$pre_tip")" ] || return 1
  git rev-parse --verify --quiet "HEAD^2"
}

# "<mode> <oid>" for a path in a commit, and in the index at stage 0. Mode and
# object together, because a revert can be entirely in the mode: parent adds +x,
# child takes it away, and every blob involved is byte-identical while the
# executable bit the child removed survives in the result.
# Both capture git's output before parsing it, so a failed read is a failure
# rather than an empty answer: through a pipe the status would be awk's, and an
# unreadable index or object would look exactly like a path that is not there.
tree_entry() {
  local out
  out=$(git ls-tree "$1" -- ":(literal)$2") || return 1
  # An absent path must stay empty: printf would emit a blank line, and awk turns
  # that into " ", which reads as present everywhere downstream.
  [ -n "$out" ] || return 0
  printf '%s\n' "$out" | awk '{ print $1 " " $3 }'
}

index_entry() {
  local want="$1" oid record fields stage mode=""
  index_fatal_check "$want"
  oid=$(git rev-parse --verify --quiet ":0:$want") || return 0
  # The mode has to come from ls-files, read NUL-delimited: without -z git
  # C-quotes a path containing a newline and no exact comparison would match. The
  # pathspec can also have matched children, so take only this exact path.
  while IFS= read -r -d '' record; do
    [ "${record#*$'\t'}" = "$want" ] || continue
    fields=${record%%$'\t'*}
    stage=${fields##* }
    [ "$stage" = 0 ] || continue
    mode=${fields%% *}
    break
  done < <(git ls-files --stage -z -- ":(literal)$want")
  [ -n "$mode" ] || return 0
  printf '%s %s' "$mode" "$oid"
}

# Did trunk rename this path away rather than delete it? Prints the new name.
# A path missing from trunk is not proof that trunk agreed it should go: renaming
# it keeps the content under another name, and from the older base -- where the
# path never existed -- that lands as a clean addition while the child's deletion
# looks honoured.
# Sets RENAMED_DEST rather than printing it: command substitution strips trailing
# newlines, and a path may end in one, so `g` and `g\n` would compare equal and a
# rename/rename conflict would look like both sides agreeing.
renamed_to() {
  local map="$1" want="$2" status old new
  RENAMED_DEST=""
  [ -n "$map" ] && [ -s "$map" ] || return 1
  while IFS= read -r -d '' status; do
    IFS= read -r -d '' old || return 1
    IFS= read -r -d '' new || return 1
    case "$status" in
      R*) [ "$old" = "$want" ] && { RENAMED_DEST="$new"; return 0; } ;;
      *) ;;
    esac
  done < "$map"
  return 1
}

# The nearest ancestor of this path that exists on <rev> as something other than
# a directory, in BLOCKING_ANCESTOR. A child that adds dir/file while trunk puts a
# file or a link at dir cannot have both, and from the stack base that is a
# collision somebody has to settle -- while the addition itself, looked up by its
# exact name, simply looks absent from trunk.
blocking_ancestor() {
  local rev="$1" target="$2" dir="${2%/*}" entry mode
  BLOCKING_ANCESTOR=""
  while [ -n "$dir" ] && [ "$dir" != "$target" ]; do
    entry=$(tree_entry "$rev" "$dir") || return 1
    if [ -n "$entry" ]; then
      mode=${entry%% *}
      if [ "$mode" != 040000 ]; then
        BLOCKING_ANCESTOR="$dir"
        return 0
      fi
    fi
    case "$dir" in
      */*) dir="${dir%/*}" ;;
      *) dir="" ;;
    esac
  done
  return 1
}

# Anything above the final component that git cannot see: a symlinked ancestor
# means a write to this path lands outside the worktree entirely, and a hard link
# means truncating an inode somebody else's file also points at.
unsafe_worktree_path() {
  local target="$1" dir="${1%/*}" links=""
  while [ -n "$dir" ] && [ "$dir" != "$target" ]; do
    if [ -L "$dir" ]; then
      printf 'under %s, which is a symlink' "$dir"
      return 0
    fi
    case "$dir" in
      */*) dir="${dir%/*}" ;;
      *) dir="" ;;
    esac
  done
  if [ -f "$target" ] && [ ! -L "$target" ]; then
    # GNU form first: its -c takes a format, while BSD stat rejects -c outright.
    # The other order is unsafe, because GNU -f means "report filesystem status"
    # and takes no format, so %l would be read as another path to stat -- and a
    # repository containing a file called %l would make that succeed and return
    # filesystem prose. The ./ prefix keeps a path starting with a dash from
    # being read as an option, which BSD stat has no -- to prevent.
    links=$(stat -c %h "./$target" 2>/dev/null) ||
      links=$(stat -f %l "./$target" 2>/dev/null) || links=""
    case "$links" in
      1) ;;
      "" | *[!0-9]*)
        # No usable link count. Refuse rather than assume one name.
        printf 'a path whose link count could not be read'
        return 0
        ;;
      *) printf 'a hard link with %s names' "$links"; return 0 ;;
    esac
  fi
  return 1
}

# Sets RENAME_AMBIGUITY when a path missing from trunk is missing because it moved
# rather than because trunk agreed it should go -- on either side. Both sides
# moving it to the same place is not ambiguous, and neither is neither.
rename_ambiguity() {
  local path="$1" trunk_dest=""
  RENAME_AMBIGUITY=""
  if renamed_to "$RESTACK_RENAMES" "$path"; then
    trunk_dest="$RENAMED_DEST"
    if renamed_to "$RESTACK_CHILD_RENAMES" "$path" &&
       [ "$RENAMED_DEST" = "$trunk_dest" ]; then
      return 1
    fi
    RENAME_AMBIGUITY="trunk renamed it to $trunk_dest"
    return 0
  fi
  if renamed_to "$RESTACK_CHILD_RENAMES" "$path"; then
    RENAME_AMBIGUITY="you renamed it to $RENAMED_DEST and trunk deleted it"
    return 0
  fi
  return 1
}

# Report an ambiguous path, unless a person has said they decided it. Only cases
# a correctly based merge could not settle by itself come through here: a
# deterministic loss is never something --accept may silence.
accept_or_review() {
  if path_accepted "$1"; then
    echo "ACCEPTED $1"
    return 0
  fi
  echo "REVIEW $1: $2"
  return 1
}

# Was this path explicitly accepted by a person on the command line? The list is
# an array rather than delimited text because a path may itself contain a
# newline, which this command supports everywhere else.
# A pathspec matches a whole directory, so `-- :(literal)dir` succeeds when the
# index holds only dir/file. These two ask about the exact path instead, through
# the :0: index syntax, while still separating "not there" from "could not look".
index_fatal_check() {
  local status
  git ls-files --stage -- ":(literal)$1" >/dev/null 2>&1
  status=$?
  case "$status" in
    0 | 1) return 0 ;;
    *) die "git ls-files failed with status $status while checking $1" ;;
  esac
}

path_accepted() {
  local candidate="$1" entry
  for entry in ${RESTACK_ACCEPTED+"${RESTACK_ACCEPTED[@]}"}; do
    [ "$entry" = "$candidate" ] && return 0
  done
  return 1
}

# A path the child added needs its content checked, not only its presence. If
# trunk added the same path with different content, the merge from the stack
# base is an add/add conflict -- but from the older fork point, where the file
# still existed, git can combine both sides' edits and call it resolved.
audit_addition() {
  local path="$1" stack_base="$2" pre="$3" other="$4"
  local staged_entry mine_entry theirs_entry
  staged_entry=$(index_entry "$path") || die "could not read the index entry for $path"
  mine_entry=$(tree_entry "$pre" "$path") || die "could not read $pre:$path"
  theirs_entry=$(tree_entry "$other" "$path") || die "could not read $other:$path"

  local unmerged
  unmerged=$(git ls-files -u -- ":(literal)$path") || die "could not read the index for $path"
  [ -z "$unmerged" ] || return 0
  [ -n "$staged_entry" ] && [ -n "$mine_entry" ] || return 0

  if [ -n "$theirs_entry" ] && [ "$mine_entry" != "$theirs_entry" ]; then
    accept_or_review "$path" "you and trunk each added this path differently, which nothing has decided"
    return $?
  fi
  if [ "$staged_entry" != "$mine_entry" ]; then
    echo "DISCARDED $path: the staged version is not the one you added"
    return 1
  fi

  # The addition can be intact and still be half of a collision: trunk may have
  # put a file where one of this path's directories has to be. Looked up by its
  # own name there is no trunk entry at all, so the pair only shows up by asking
  # about the ancestors -- and if the stack base already had the same thing
  # there, the collision is not something this merge introduced.
  if [ -n "$other" ] && blocking_ancestor "$other" "$path"; then
    local ancestor="$BLOCKING_ANCESTOR" trunk_ancestor base_ancestor
    trunk_ancestor=$(tree_entry "$other" "$ancestor") || die "could not read $other:$ancestor"
    base_ancestor=$(tree_entry "$stack_base" "$ancestor") || die "could not read $stack_base:$ancestor"
    if [ "$trunk_ancestor" != "$base_ancestor" ]; then
      accept_or_review "$path" "trunk put something other than a directory at $ancestor, so this addition and trunk cannot both exist"
      return $?
    fi
  fi
  return 0
}

# Does the staged result for one path match what a merge based on the stack base
# would have produced? Prints a finding and returns 1 if not.
#
# Comparing the staged entry against trunk's would only catch a change lost
# whole. A child that reverts one of the parent's hunks and edits the same file
# elsewhere keeps the second edit and loses the revert, leaving a result that
# matches neither side -- so the comparison has to be against the correct merge,
# not against either input.
audit_modification() {
  local path="$1" stack_base="$2" pre="$3" other="$4" tmp="$5"
  local staged_entry mine_entry theirs_entry base_entry
  staged_entry=$(index_entry "$path") || die "could not read the index entry for $path"
  mine_entry=$(tree_entry "$pre" "$path") || die "could not read $pre:$path"
  theirs_entry=$(tree_entry "$other" "$path") || die "could not read $other:$path"
  base_entry=$(tree_entry "$stack_base" "$path") || die "could not read $stack_base:$path"

  # A missing entry is not automatically somebody else's problem. The add and
  # delete lists classify stack_base..pre, so they say nothing about what trunk
  # did: a path the child modified can be absent from trunk, or from the result,
  # and neither list would mention it.
  local unmerged
  unmerged=$(git ls-files -u -- ":(literal)$path") || die "could not read the index for $path"
  # Reported by the index-wide scan, which also catches paths the child never
  # touched. Acceptance cannot apply: nothing has been staged to accept.
  [ -z "$unmerged" ] || return 0
  if [ -z "$theirs_entry" ]; then
    # You changed it, trunk deleted it. From the stack base that is a
    # modify/delete conflict; from the older base the deletion applies unopposed.
    accept_or_review "$path" "you changed it and trunk deleted it, which nothing has decided"
    return $?
  fi
  if [ -z "$staged_entry" ]; then
    echo "DISCARDED $path: your change is gone, and so is the path"
    return 1
  fi
  [ -n "$mine_entry" ] && [ -n "$base_entry" ] || return 0

  local staged_mode staged_oid mine_mode mine_oid theirs_mode theirs_oid base_mode base_oid
  read -r staged_mode staged_oid <<< "$staged_entry"
  read -r mine_mode mine_oid <<< "$mine_entry"
  read -r theirs_mode theirs_oid <<< "$theirs_entry"
  read -r base_mode base_oid <<< "$base_entry"

  git cat-file blob "$mine_oid" > "$tmp/m.ours" 2>/dev/null &&
    git cat-file blob "$base_oid" > "$tmp/m.base" 2>/dev/null &&
    git cat-file blob "$theirs_oid" > "$tmp/m.theirs" 2>/dev/null &&
    git cat-file blob "$staged_oid" > "$tmp/m.staged" 2>/dev/null ||
    die "could not read the blobs for $path"

  local expected_status
  git merge-file -p --diff3 -L ours -L base -L trunk \
    "$tmp/m.ours" "$tmp/m.base" "$tmp/m.theirs" > "$tmp/m.expected" 2>/dev/null
  expected_status=$?

  if [ "$expected_status" -eq 0 ]; then
    if ! cmp -s "$tmp/m.expected" "$tmp/m.staged"; then
      echo "DISCARDED $path: the staged content is not what a merge from the stack base gives"
      return 1
    fi
  elif [ "$expected_status" -ge 1 ] && [ "$expected_status" -le 127 ]; then
    # git resolved this without a conflict only because it used the older base.
    accept_or_review "$path" "a merge from the stack base conflicts here, so the staged result was never decided"
    return $?
  else
    accept_or_review "$path" "the expected merge could not be computed (binary content?)"
    return $?
  fi

  local expected_mode
  if [ "$mine_mode" != "$base_mode" ] && [ "$theirs_mode" != "$base_mode" ] &&
     [ "$mine_mode" != "$theirs_mode" ]; then
    accept_or_review "$path" "both sides changed the file mode, to $mine_mode and $theirs_mode"
    return $?
  fi
  if [ "$mine_mode" != "$base_mode" ]; then
    expected_mode="$mine_mode"
  else
    expected_mode="$theirs_mode"
  fi
  if [ "$staged_mode" != "$expected_mode" ]; then
    echo "DISCARDED $path: staged mode $staged_mode, expected $expected_mode"
    return 1
  fi
  return 0
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
  [ -n "$stack_base" ] && [ -n "$pre" ] || die "usage: audit <stack-base> <pre-merge-tip> [--trunk <rev>] [--parent-head <rev>] [--accept <path>]..."
  shift 2

  # A path this reports as REVIEW stays reported: the inputs do not change when
  # you resolve it, so re-running would keep failing and the "run it again until
  # it is clean" loop could never end for a genuine conflict. --accept records
  # that a person decided this path, which is the one thing the audit cannot
  # infer -- git's silent old-base resolution and a considered human one look
  # identical in the index.
  local trunk_ref="origin/trunk" parent_head_ref=""
  RESTACK_ACCEPTED=()
  while [ $# -gt 0 ]; do
    case "$1" in
      --parent-head)
        shift
        [ -n "${1:-}" ] || die "--parent-head needs a revision"
        parent_head_ref="$1"
        shift
        ;;
      --trunk)
        shift
        [ -n "${1:-}" ] || die "--trunk needs a revision"
        trunk_ref="$1"
        shift
        ;;
      --accept)
        shift
        [ -n "${1:-}" ] || die "--accept needs a path"
        RESTACK_ACCEPTED+=("$1")
        shift
        ;;
      *) die "unknown option for audit: $1" ;;
    esac
  done

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
  # M and T together: a type change (a file the child turned back into a symlink,
  # or the reverse) is reported as T and would otherwise appear in none of these
  # three lists at all.
  if ! git diff --name-only -z --no-renames --diff-filter=MT \
         "$stack_base" "$pre" -- > "$tmp/modified"; then
    rm -rf "$tmp"
    die "could not diff $stack_base..$pre for modifications"
  fi

  local trunk_rev
  trunk_rev=$(git rev-parse --verify --quiet "$trunk_ref^{commit}") ||
    die "cannot resolve $trunk_ref; pass --trunk <rev> to say what this merge brings in"

  # Naming commits is not enough. The stack base must be somewhere on the child's
  # history, and it must not already be part of trunk: a commit trunk contains is
  # the fork point (or older), not the parent's tip, and passing that is the very
  # confusion this tooling exists to prevent. It also hides the failure it was
  # written for -- a path the parent added and the child deleted exists at
  # neither the fork point nor the child's tip, so no intent diff mentions it and
  # the merge can restore it in silence.
  # --is-ancestor answers with 0 or 1 and uses other statuses for failures, so
  # the two must not be lumped together: an ancestry read that fails is not a
  # licence to continue with a base this refuses to accept.
  local ancestry
  git merge-base --is-ancestor "$stack_base" "$pre"
  ancestry=$?
  case "$ancestry" in
    0) ;;
    1) die "$stack_base is not an ancestor of $pre, so it cannot be this branch's stack base" ;;
    *) die "could not tell whether $stack_base is an ancestor of $pre (status $ancestry)" ;;
  esac
  git merge-base --is-ancestor "$stack_base" "$trunk_rev"
  ancestry=$?
  case "$ancestry" in
    1) ;;
    0) die "$stack_base is already contained in $trunk_ref, so it is the fork point rather than the parent's tip; derive it with: $(basename "${BASH_SOURCE[0]}") stack-base <parent-pr>" ;;
    *) die "could not tell whether $stack_base is contained in $trunk_ref (status $ancestry)" ;;
  esac

  # Ancestry alone cannot tell the parent's tip from an earlier parent commit:
  # both are ancestors of the child and absent from a squash-merged trunk. An
  # earlier one leaves its own additions out of every intent diff, so the merge
  # can restore what the child deleted and nothing here would notice. Given the
  # parent's head, the answer is exact.
  if [ -z "$parent_head_ref" ]; then
    echo "note: no --parent-head given, so the stack base was checked only by ancestry" >&2
  fi
  if [ -n "$parent_head_ref" ]; then
    local parent_head_rev expected_base
    parent_head_rev=$(git rev-parse --verify --quiet "$parent_head_ref^{commit}") ||
      die "cannot resolve $parent_head_ref"
    expected_base=$(git merge-base "$pre" "$parent_head_rev") ||
      die "no common commit between $pre and $parent_head_ref"
    [ "$(git rev-parse --verify --quiet "$stack_base^{commit}")" = "$expected_base" ] ||
      die "$stack_base is not the newest commit of $parent_head_ref contained in $pre; that is $expected_base"
  fi

  local other incomplete=0
  if other=$(merge_other_side "$pre"); then
    # An active merge is not necessarily *this* merge. Merging anything else and
    # then auditing would otherwise compare against the wrong side and could
    # report clean, and the parent count on the resulting commit would look right
    # too, since it is a merge either way.
    if [ "$other" != "$trunk_rev" ]; then
      echo "INCOMPLETE: the merge in progress brings in ${other} rather than $trunk_ref, so modifications were not checked"
      incomplete=1
      other=""
    fi
  else
    # Outside a merge there is no other side to compare against, so the question
    # this command answers -- did the merge do what you intended -- has no answer
    # yet. Callers gate on the exit status, so it must not be success: a partial
    # audit reporting clean is the failure this whole helper is arguing against.
    echo "INCOMPLETE: no merge in progress or committed, so only additions and deletions were checked"
    incomplete=1
    other=""
  fi

  # Renames trunk made since the stack base, so a deleted path that is missing
  # from trunk can be told apart from one trunk carried elsewhere.
  RESTACK_RENAMES="$tmp/renames"
  : > "$RESTACK_RENAMES"
  if [ -n "$other" ] &&
     ! git diff -z --name-status --find-renames --diff-filter=R \
         "$stack_base" "$other" -- > "$RESTACK_RENAMES"; then
    rm -rf "$tmp"
    die "could not list renames between $stack_base and $other"
  fi
  # And the child's own renames: the intent diffs are read with --no-renames, so
  # a move shows up there as a deletion plus an addition. That is what the audit
  # wants for a path trunk left alone, but it hides a rename standing against a
  # deletion or an edit on trunk's side, which no correctly based merge settles.
  RESTACK_CHILD_RENAMES="$tmp/child_renames"
  if ! git diff -z --name-status --find-renames --diff-filter=R \
         "$stack_base" "$pre" -- > "$RESTACK_CHILD_RENAMES"; then
    rm -rf "$tmp"
    die "could not list renames between $stack_base and $pre"
  fi

  local findings=0 path

  # Any unmerged path means the merge is undecided, and the three intent lists
  # only cover paths the child changed. One it inherited untouched can conflict
  # all the same -- the parent changed a line before the split, trunk changed it
  # after -- and would otherwise sit unresolved while this reported clean.
  if ! git ls-files -u -z > "$tmp/unmerged"; then
    rm -rf "$tmp"
    die "could not read unmerged entries from the index"
  fi
  local record last_unmerged="" theirs_now base_now theirs_add mine_add theirs_here
  while IFS= read -r -d '' record; do
    path=${record#*$'\t'}
    [ "$path" = "$last_unmerged" ] && continue
    last_unmerged="$path"
    echo "REVIEW $path: still unmerged, so nothing has decided it"
    findings=$((findings + 1))
  done < "$tmp/unmerged"

  while IFS= read -r -d '' path; do
    if ! path_in_index "$path"; then
      # An unmerged path has no stage 0 either, and the index-wide scan above has
      # already reported it.
      [ -z "$(git ls-files -u -- ":(literal)$path")" ] || continue
      # The deletion survived, which is right unless trunk changed the file after
      # the stack base: then the correct merge is a delete/modify, and the older
      # base only made it look settled -- trunk's change is invisible from there
      # if it happens to restore the fork-point content.
      [ -n "$other" ] || continue
      theirs_now=$(tree_entry "$other" "$path") || die "could not read $other:$path"
      base_now=$(tree_entry "$stack_base" "$path") || die "could not read $stack_base:$path"
      if [ -z "$theirs_now" ]; then
        # Trunk deleted it, and so did we -- unless either side's missing entry is
        # really a move, in which case the stack base sees a rename against a
        # deletion and nothing has settled which wins.
        if rename_ambiguity "$path"; then
          accept_or_review "$path" "$RENAME_AMBIGUITY, which nothing has decided" ||
            findings=$((findings + 1))
        fi
        continue
      fi
      [ "$theirs_now" = "$base_now" ] && continue      # trunk left it alone
      accept_or_review "$path" "you deleted it and trunk changed it, which nothing has decided" ||
        findings=$((findings + 1))
      continue
    fi
    # Already reported by the scan above, and its stages are not a decision.
    [ -z "$(git ls-files -u -- ":(literal)$path")" ] || continue
    # If trunk changed the file after the stack base, a correctly based merge is
    # a delete/modify conflict rather than a silent restoration, and keeping
    # trunk's version is a decision somebody may legitimately have made.
    if [ -n "$other" ]; then
      theirs_now=$(tree_entry "$other" "$path") || die "could not read $other:$path"
      base_now=$(tree_entry "$stack_base" "$path") || die "could not read $stack_base:$path"
    else
      theirs_now=""
      base_now=""
    fi
    # Only a trunk that still *has* the path makes this a delete/modify. If trunk
    # deleted it too, both sides agreed and a staged path is an unambiguous
    # restoration, which no decision can explain away.
    if [ -n "$other" ] && [ -n "$theirs_now" ] && [ "$theirs_now" != "$base_now" ]; then
      accept_or_review "$path" "you deleted it and trunk changed it, which nothing has decided" ||
        findings=$((findings + 1))
    elif [ -n "$other" ] && [ -z "$theirs_now" ] && rename_ambiguity "$path"; then
      # Missing from trunk because it moved, not because trunk agreed it should
      # go, so keeping the old path can be somebody's decision.
      accept_or_review "$path" "$RENAME_AMBIGUITY, and the old path is back, which nothing has decided" ||
        findings=$((findings + 1))
    else
      # Unambiguous: trunk did not touch it, so nothing was decided here and the
      # deletion was simply undone. That stays a finding until it is corrected.
      echo "RESURRECTED $path"
      findings=$((findings + 1))
    fi
  done < "$tmp/deleted"

  while IFS= read -r -d '' path; do
    if ! path_in_index "$path"; then
      # Absent from the result. If trunk added the same path differently, that is
      # an add/add somebody may have settled by staging a removal, so it is a
      # decision; if trunk never had it, the child's addition was simply dropped.
      theirs_add=""
      if [ -n "$other" ]; then
        theirs_add=$(tree_entry "$other" "$path") || die "could not read $other:$path"
        mine_add=$(tree_entry "$pre" "$path") || die "could not read $pre:$path"
      fi
      if [ -n "$theirs_add" ] && [ "$theirs_add" != "$mine_add" ]; then
        accept_or_review "$path" "you and trunk each added this path differently, and the result has neither" ||
          findings=$((findings + 1))
        continue
      fi
      if [ -n "$other" ] && blocking_ancestor "$other" "$path"; then
        accept_or_review "$path" "trunk put something other than a directory at $BLOCKING_ANCESTOR, so this addition and trunk cannot both exist" ||
          findings=$((findings + 1))
        continue
      fi
      echo "LOST $path"
      findings=$((findings + 1))
      continue
    fi
    [ -n "$other" ] || continue
    audit_addition "$path" "$stack_base" "$pre" "$other" || findings=$((findings + 1))
  done < "$tmp/added"

  # A modification disappears by the same mechanism as a deletion, and just as
  # quietly: when the child's edit reverts something the parent did, the child's
  # side matches the fork point, so the merge sees no change to preserve and
  # keeps trunk's version. Neither filter above lists such a path -- it is a
  # modification, and its content is simply gone.
  #
  while [ -n "$other" ] && IFS= read -r -d '' path; do
    audit_modification "$path" "$stack_base" "$pre" "$other" "$tmp" ||
      findings=$((findings + 1))
  done < "$tmp/modified"

  # Paths where the staged result differs from trunk. The child's own work is
  # supposed to differ; a path it never touched is not, and after a conflict on
  # such a path is resolved it appears in none of the intent lists, so nothing
  # above would look at the resolution. From the stack base those merges are not
  # ambiguous at all -- our side equals the base, so trunk's version wins.
  if [ -n "$other" ]; then
    local mine_here base_here staged_here
    if ! git diff --cached --name-only -z --no-renames "$other" -- > "$tmp/vs_trunk"; then
      rm -rf "$tmp"
      die "could not compare the index against $other"
    fi
    while IFS= read -r -d '' path; do
      [ -z "$(git ls-files -u -- ":(literal)$path")" ] || continue
      mine_here=$(tree_entry "$pre" "$path") || die "could not read $pre:$path"
      base_here=$(tree_entry "$stack_base" "$path") || die "could not read $stack_base:$path"
      [ "$mine_here" = "$base_here" ] || continue        # the child changed it
      staged_here=$(index_entry "$path") || die "could not read the index entry for $path"
      theirs_here=$(tree_entry "$other" "$path") || die "could not read $other:$path"
      [ "$staged_here" = "$theirs_here" ] && continue
      echo "DISCARDED $path: you never touched it, so the result should be trunk's version"
      findings=$((findings + 1))
    done < "$tmp/vs_trunk"
  fi

  rm -rf "$tmp"

  if [ "$findings" -ne 0 ]; then
    echo "audit found $findings path(s) the merge changed against the branch's intent" >&2
    return 1
  fi
  if [ "$incomplete" -ne 0 ]; then
    echo "audit did not check modifications, so it cannot report the merge as clean" >&2
    return 1
  fi
  echo "audit clean"
  return 0
}

main() {
  local subcommand="${1:-}"
  shift || true

  # Everything here works in repository-relative paths: that is what git's diffs
  # emit, what its pathspecs are matched against relative to the current
  # directory, and what cp is handed. Run from a subdirectory and those three
  # disagree -- a root-relative name is looked up under the wrong prefix, so a
  # restored file outside that prefix looks absent from both the index and trunk
  # and the audit reports clean. Move to the root instead of hoping.
  local toplevel
  case "$subcommand" in
    -h | --help | help | "") ;;
    *)
      toplevel=$(git rev-parse --show-toplevel) ||
        die "not inside a git worktree"
      cd "$toplevel" || die "could not enter $toplevel"
      ;;
  esac

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
