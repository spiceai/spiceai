#!/usr/bin/env bash
#
# Reclaim stale space on a self-hosted runner's work volume.
#
# The pool's hosts are long-lived and nothing ever takes anything off them, so
# free space only moves one way. #12794 is what that looks like when it lands:
# two volumes below the sign-off floor at the same time (26 GiB then 1 GiB, and
# 5 GiB) while two other hosts in the same pool sat at 163 and 536 GiB. The
# recovery was an operator deleting things by hand.
#
# The preflight checks that already exist -- `scripts/preflight_build_disk.sh`
# for the build path, `scripts/signoff` for the sign-off path -- report a full
# volume at the point it is detectable instead of minutes later from inside the
# compiler. Neither reclaims anything. This does.
#
# What it does NOT do is bound a single run: `/opt/github-runner-01` went from
# 26 GiB to 1 GiB in 6.2 hours of active building, and no periodic sweep
# prevents that. This bounds the baseline a long-lived host accumulates between
# runs, which is a different problem with the same symptom.
#
# ## Why deleting here is safe when deleting by path alone is not
#
# Several runner instances share one volume, so a sweep that walks the volume
# can delete a live build out from under a sibling. Two rules avoid it, and
# every candidate below satisfies both:
#
#   1. Nothing outside this runner instance's own work root is ever a
#      candidate. A job owns its instance for its whole duration, so while this
#      runs, that instance has no other job. Sibling instances keep their own
#      work roots on the same volume and are never walked.
#   2. Nothing modified within the age threshold is ever a candidate. The
#      workspace this job is using is rewritten by the checkout that put this
#      script there, so an age filter cannot select it -- but the rule is
#      enforced explicitly as well rather than relying on that.
#
# Usage: scripts/reclaim_runner_disk.sh [--dry-run] [--root <dir>]
#                                       [--max-age-days <n>] [--free-gib <n>]
#
# Environment:
#   RUNNER_WORKSPACE     Set by Actions to <work-root>/<repo>. Its parent is the
#                        work root, and it is itself the one workspace this job
#                        is using, so it is excluded by name.
#   RUNNER_TEMP          This job's scratch directory. Excluded by name for the
#                        same reason.
#   RECLAIM_MAX_AGE_DAYS Default age threshold in days (default: 7), overridden
#                        by --max-age-days. A week keeps the warm build cache
#                        that makes an incremental sign-off finish in minutes
#                        rather than hours, and drops what no branch built on
#                        this host has needed since.
#   RECLAIM_FREE_GIB     Free space (GiB) at or above which build output is left
#                        alone entirely (default: 100), overridden by
#                        --free-gib. See `reclaim_stale_build_output`.

set -uo pipefail

readonly DEFAULT_MAX_AGE_DAYS=7

# Free space at or above which a host's build output is left alone entirely.
#
# Taken from the spread #12794 measured across one pool at one moment: 536 and
# 163 GiB on the healthy hosts, 26 then 1 GiB and 5 GiB on the two that broke.
# A floor between those populations reclaims where it matters and touches
# nothing where it does not -- which is the point, because a pruned cache is not
# free. Sign-offs on this pool run 2-5 hours against a 353-minute step budget,
# and a budget expiry publishes `signoff=failure`, disqualifying the commit as
# if the branch were broken. Trading that risk for space a host already has
# would be a bad trade in both directions.
readonly DEFAULT_FREE_GIB=100

# Exit status for a usage error, distinct from 1 so a caller can tell "this
# script was invoked wrongly" from "the sweep hit a problem".
readonly EXIT_USAGE=64

# The signature line every `cargo` target directory carries in its
# CACHEDIR.TAG, per the Cache Directory Tagging Specification. Requiring it is
# what makes pruning inside a workspace precise: a directory holding this file
# is build output by cargo's own declaration, so a name-based guess about which
# directories are `target/` -- and the chance of walking into source under a
# directory that merely happens to be called `target` -- does not arise.
readonly CACHEDIR_SIGNATURE='Signature: 8a477f597d28d172789f06886806bc55'

info() { printf '%s\n' "$*" >&2; }

# Report rather than remove. Set by --dry-run; defaulted here so the unit tests,
# which source this file to reach one function at a time, never meet it unset.
DRY_RUN="${DRY_RUN:-0}"

# Free space in GiB on the volume holding "$1". Prints an integer; returns
# non-zero when `df` is absent or unparseable, so a host whose df cannot be read
# still gets swept -- the reading is for the report, not for a decision.
free_gib() {
  local path="$1" free
  command -v df >/dev/null 2>&1 || return 1
  # -k for 1024-byte blocks on both macOS (whose df defaults to 512) and Linux.
  free=$(df -Pk "$path" 2>/dev/null | awk 'NR==2 { print int($4 / 1048576) }') || return 1
  [[ "$free" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$free"
}

# The work root to sweep: the parent of RUNNER_WORKSPACE. Actions sets that to
# <work-root>/<repo>, so its parent is the directory holding every workspace,
# `_temp`, and the rest of the instance's state.
work_root_from_env() {
  local workspace="${RUNNER_WORKSPACE:-}"
  [[ -n "$workspace" ]] || return 1
  printf '%s\n' "$(dirname "$workspace")"
}

# True when the directory is a cargo target directory, by its own CACHEDIR.TAG.
is_cargo_target_dir() {
  local dir="$1"
  local tag="$dir/CACHEDIR.TAG"
  [[ -d "$dir" && -f "$tag" ]] || return 1
  grep -qF "$CACHEDIR_SIGNATURE" "$tag" 2>/dev/null
}

# The workspace this job is running out of, which must survive the sweep
# whatever its age. RUNNER_WORKSPACE names it on a runner; away from one -- an
# operator running this by hand with --root -- the script's own location does,
# and answering nothing there would leave the one tree we are certain is in use
# protected only by its mtimes.
live_workspace() {
  local from_env="${RUNNER_WORKSPACE:-}"
  if [[ -n "$from_env" ]]; then
    printf '%s\n' "${from_env%/}"
    return 0
  fi
  # <repo-checkout>/scripts/this-script -> the directory holding the checkout,
  # which is what a work root lists.
  local script_dir
  script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd) || return 1
  printf '%s\n' "$(dirname "$(dirname "$script_dir")")"
}

# True when nothing in the top few levels of "$1" has been modified within
# "$2" days -- i.e. no job has used this workspace recently.
#
# Depth-limited on purpose. A checkout rewrites the repository root on every job
# that uses a workspace, so recent use is always visible within a few levels,
# and probing that far costs a directory read rather than a walk of a target
# directory holding hundreds of thousands of files.
workspace_is_stale() {
  local dir="$1" days="$2" recent
  [[ -d "$dir" ]] || return 1
  recent=$(find "$dir" -maxdepth 3 -mtime "-${days}" -print 2>/dev/null | head -n 1)
  [[ -z "$recent" ]]
}

# Remove "$1", or report it under --dry-run. Prints one line either way so the
# run log names everything the sweep touched.
remove_path() {
  local path="$1" reason="$2"
  if [[ "$DRY_RUN" == "1" ]]; then
    info "would remove (${reason}): ${path}"
    return 0
  fi
  info "removing (${reason}): ${path}"
  rm -rf "$path"
}

# Orphaned job scratch. Actions clears `_temp` between jobs on a healthy run, so
# what survives past the threshold is what a killed or timed-out job left --
# and #12794's hosts have had plenty of both.
reclaim_temp() {
  local root="$1" days="$2" entry
  # Assigned separately from `root`: bash declares every name in one `local`
  # before assigning any of them, so a later initialiser reading an earlier name
  # sees it unset and aborts the function under `set -u`.
  local temp_dir="$root/_temp"
  [[ -d "$temp_dir" ]] || return 0

  local live_temp="${RUNNER_TEMP:-}"
  live_temp="${live_temp%/}"

  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    # Whatever this job is using, whatever its age. On stock Actions RUNNER_TEMP
    # *is* `_temp`, so no entry beneath it can equal it and the age filter is
    # what actually protects the running job -- its files were written minutes
    # ago. The name check covers the other layout, where RUNNER_TEMP points at a
    # per-job directory inside `_temp`: there an idle-then-resumed runner really
    # can present the live scratch as old, and only this catches it.
    if [[ -n "$live_temp" ]]; then
      [[ "${entry%/}" == "$live_temp" || "$live_temp" == "${entry%/}"/* ]] && continue
    fi
    remove_path "$entry" "orphaned job scratch"
  done < <(find "$temp_dir" -mindepth 1 -maxdepth 1 -mtime "+${days}" 2>/dev/null)
}

# Whole workspaces for repositories nobody has built on this host lately. Each
# one holds a full checkout and its build output, so this is where a host that
# has served several repositories keeps most of its dead weight.
reclaim_workspaces() {
  local root="$1" days="$2" entry base
  local live
  live=$(live_workspace) || live=""

  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    base=$(basename "$entry")
    # Runner-internal state: `_temp` is handled above with its own exclusion,
    # and `_actions`/`_tool` hold checkouts and toolchains a running job
    # resolves lazily, so an old-but-in-use entry is reachable there in a way it
    # is not for a workspace. Left alone.
    [[ "$base" == _* ]] && continue
    [[ -n "$live" && "${entry%/}" == "$live" ]] && continue
    workspace_is_stale "$entry" "$days" || continue
    remove_path "$entry" "workspace unused for ${days}d"
  done < <(find "$root" -mindepth 1 -maxdepth 1 -type d 2>/dev/null)
}

# Stale build output inside the workspaces that survive, which on this pool is
# where the space actually is: the sweep above cannot touch the workspace this
# job is using, and that is the one holding the target directory every sign-off
# has been writing into.
#
# Pruning by file age is what `cargo sweep` does and what cargo is built to
# tolerate: a missing artifact or fingerprint is rebuilt, so the cost of being
# wrong here is a rebuild, never a wrong build. Files newer than the threshold
# stay, so the cache that makes an incremental build fast survives the sweep.
#
# Gated on free space, unlike the two above. Job scratch a killed job orphaned
# and a workspace nobody has built in a week are dead weight on any host, but a
# target directory is the warm cache an incremental build depends on, and
# pruning it on a host with 536 GiB free buys space nobody needed at the cost of
# a colder build -- which on this pool is measured in hours against a step
# budget that publishes a code-failure verdict when it expires. So a host above
# the floor keeps its cache, and one below it does not.
reclaim_stale_build_output() {
  local root="$1" days="$2" floor="$3" tag target_dir accepted
  local free
  if free=$(free_gib "$root"); then
    if (( free >= floor )); then
      info "free space is ${free} GiB, at or above the ${floor} GiB floor — leaving build output alone"
      return 0
    fi
    info "free space is ${free} GiB, below the ${floor} GiB floor — pruning stale build output"
  else
    # No reading is not a reason to skip: the hosts this exists for are the ones
    # whose volume is in trouble, and a host whose `df` cannot be read is not
    # evidence of a healthy one.
    info "could not measure free space — pruning stale build output anyway"
  fi

  # Collected in full before any of it is swept, rather than filtered as it
  # arrives. `find` emits a directory's entries in the order the filesystem
  # returns them, not sorted and not ancestors-first: a nested target directory
  # is reported before its ancestor whenever `package/` happens to precede
  # `CACHEDIR.TAG` in the parent's listing. Deciding nesting against a partial
  # list therefore sweeps the same files twice and reports two directories where
  # there is one.
  local -a found=()
  while IFS= read -r tag; do
    [[ -n "$tag" ]] || continue
    target_dir=$(dirname "$tag")
    is_cargo_target_dir "$target_dir" || continue
    found+=("$target_dir")
  done < <(find "$root" -type f -name CACHEDIR.TAG 2>/dev/null)

  local -i pruned=0
  for target_dir in ${found[@]+"${found[@]}"}; do
    # Nested inside another one found anywhere in the sweep -- cargo puts one
    # under `target/package/` for a packaged crate, and a vendored checkout can
    # carry its own -- so it is already covered by its ancestor.
    local nested=0
    for accepted in ${found[@]+"${found[@]}"}; do
      [[ "$target_dir" == "$accepted"/* ]] && { nested=1; break; }
    done
    (( nested )) && continue

    if [[ "$DRY_RUN" == "1" ]]; then
      local count
      count=$(find "$target_dir" -type f -mtime "+${days}" 2>/dev/null | wc -l | tr -d ' ')
      info "would prune ${count} file(s) older than ${days}d from: ${target_dir}"
      pruned+=1
      continue
    fi

    info "pruning files older than ${days}d from: ${target_dir}"
    find "$target_dir" -type f -mtime "+${days}" -delete 2>/dev/null
    # Directories the prune emptied. Left behind they cost nothing but noise,
    # and cargo recreates whatever it needs.
    find "$target_dir" -mindepth 1 -type d -empty -delete 2>/dev/null
    pruned+=1
  done

  info "build output directories swept: ${pruned}"
}

usage() {
  info "usage: $0 [--dry-run] [--root <dir>] [--max-age-days <n>] [--free-gib <n>]"
}

main() {
  DRY_RUN=0
  local root=""
  local days="${RECLAIM_MAX_AGE_DAYS:-$DEFAULT_MAX_AGE_DAYS}"
  local floor="${RECLAIM_FREE_GIB:-$DEFAULT_FREE_GIB}"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --dry-run) DRY_RUN=1; shift ;;
      --root) root="${2:-}"; shift 2 ;;
      --max-age-days) days="${2:-}"; shift 2 ;;
      --free-gib) floor="${2:-}"; shift 2 ;;
      -h|--help) usage; return 0 ;;
      *) info "unknown argument: $1"; usage; return "$EXIT_USAGE" ;;
    esac
  done

  if [[ ! "$days" =~ ^[0-9]+$ ]]; then
    info "--max-age-days must be a non-negative integer, got: ${days}"
    return "$EXIT_USAGE"
  fi

  if [[ ! "$floor" =~ ^[0-9]+$ ]]; then
    info "--free-gib must be a non-negative integer, got: ${floor}"
    return "$EXIT_USAGE"
  fi

  if [[ -z "$root" ]]; then
    if ! root=$(work_root_from_env); then
      info "no work root: pass --root, or run where RUNNER_WORKSPACE is set"
      return "$EXIT_USAGE"
    fi
  fi

  if [[ ! -d "$root" ]]; then
    info "work root is not a directory: ${root}"
    return "$EXIT_USAGE"
  fi

  local before after
  before=$(free_gib "$root") || before=""
  [[ -n "$before" ]] && info "free space before: ${before} GiB"
  info "sweeping ${root} for entries older than ${days}d (dry-run: ${DRY_RUN})"

  reclaim_temp "$root" "$days"
  reclaim_workspaces "$root" "$days"
  reclaim_stale_build_output "$root" "$days" "$floor"

  after=$(free_gib "$root") || after=""
  if [[ -n "$before" && -n "$after" ]]; then
    info "free space after: ${after} GiB (reclaimed $((after - before)) GiB)"
  elif [[ -n "$after" ]]; then
    info "free space after: ${after} GiB"
  fi

  return 0
}

# Sourced by the unit tests to reach one function at a time; run as a program
# everywhere else.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
