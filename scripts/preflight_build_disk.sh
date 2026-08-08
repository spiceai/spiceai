#!/usr/bin/env bash
#
# Refuse a build the runner's volumes cannot hold, before `make` writes a byte.
#
# Without this the condition surfaces from inside the compiler, several minutes
# in, as `No space left on device (os error 28)` against whichever crate was
# writing at the time — usually one the branch never touched, and often behind a
# `cc-rs`/sccache frame that reads as a compiler-cache fault instead (#12794).
# `scripts/signoff` already refuses on the same grounds for the sign-off path;
# this is the same reading for the build path, so a full host is reported the
# same way whichever job lands on it.
#
# Fatal only on a self-hosted runner. GitHub-hosted images ship with far less
# free space than a self-hosted host needs, and builds complete there today, so
# a floor tuned for the pool must not fail them: hosted runners get the reading
# and nothing else.
#
# Usage: scripts/preflight_build_disk.sh <path> [<path>...]
#
# Environment:
#   BUILD_MIN_FREE_GIB   Free space (GiB) each measured volume must have before
#                        the build starts (default: 10). A volume below this
#                        cannot finish a release build of this workspace, so
#                        the floor names a certainty rather than a preference.
#                        Set it to 0 to report the readings and refuse nothing,
#                        which is the escape hatch if the floor is ever wrong
#                        for a host: it needs no change to this script.
#   RUNNER_ENVIRONMENT   Set by Actions to `self-hosted` or `github-hosted`.
#                        Anything else — an unset value, a local run — reports
#                        without failing.

set -uo pipefail

readonly DEFAULT_MIN_FREE_GIB=10

# Exit status for "the runner cannot hold this build". Distinct from 1 so a
# caller can tell the refusal from this script failing to run at all.
readonly EXIT_DISK_EXHAUSTED=70

info() { printf '%s\n' "$*" >&2; }

# "<device> <free_gib>" for the volume holding $1, from one `df`. Reporting both
# together is what lets two of the build's paths on one volume be recognised as
# one volume without asking twice.
#
# Fails rather than guessing: an unmeasurable volume must read as "unknown",
# never as "full", because refusing a build on a reading we do not have is the
# worse error.
measure_volume() {
  local path="$1" reading free
  command -v df >/dev/null 2>&1 || return 1
  # -P: POSIX one-line-per-filesystem output, so the fields are reliably placed
  # even when a long device name would otherwise wrap.
  # -k: 1024-byte blocks on macOS (whose df defaults to 512) as well as Linux.
  reading=$(df -Pk "$path" 2>/dev/null | awk 'NR==2 { print $1, int($4 / 1048576) }') || return 1
  free="${reading##* }"
  [[ "$free" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$reading"
}

# Actions surfaces. Both no-op outside a workflow, so the script runs the same
# way from a shell.
annotate() {
  echo "::error title=Runner out of disk::$*"
}

append_step_summary() {
  [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] || return 0
  printf '%s\n' "$*" >>"$GITHUB_STEP_SUMMARY" 2>/dev/null || true
}

main() {
  if [[ $# -eq 0 ]]; then
    info "usage: ${0##*/} <path> [<path>...]"
    return 2
  fi

  local min="${BUILD_MIN_FREE_GIB:-$DEFAULT_MIN_FREE_GIB}"
  if [[ ! "$min" =~ ^[0-9]+$ ]]; then
    info "warning: BUILD_MIN_FREE_GIB='${min}' is not an integer — using ${DEFAULT_MIN_FREE_GIB}"
    min="$DEFAULT_MIN_FREE_GIB"
  fi
  # Force base 10: bash arithmetic reads a leading zero as octal, so a
  # well-meaning `BUILD_MIN_FREE_GIB=08` is an arithmetic error rather than 8.
  min=$((10#$min))

  # Plain strings rather than arrays: this runs under whichever bash the runner
  # provides, and bash 3.2 — still /bin/bash on macOS — errors on `${#arr[@]}`
  # for an empty array under `set -u`.
  local seen_ids="" detail="" short_count=0
  local path reading free id

  for path in "$@"; do
    [[ -n "$path" ]] || continue
    # Measure something that exists: a target directory the build has not
    # created yet would otherwise report nothing, which is exactly the
    # first-build-on-a-fresh-runner case. The nearest existing ancestor is on
    # the same volume the directory will be created on.
    while [[ ! -e "$path" && "$path" != "/" && "$path" != "." && "$path" != *: ]]; do
      local parent="${path%/*}"
      [[ "$parent" != "$path" ]] || break
      path="${parent:-/}"
    done

    if ! reading=$(measure_volume "$path"); then
      info "note: could not measure free space on ${path} — not treating that as full"
      continue
    fi
    id="${reading%% *}"
    free="${reading##* }"

    # One physical volume reported once, however many of the build's paths land
    # on it — the usual case, where target/ and TMPDIR share a disk.
    case "${seen_ids}" in
      *"|${id}|"*) continue ;;
    esac
    seen_ids="${seen_ids}|${id}|"

    info "free space on ${path}: ${free} GiB (floor ${min} GiB)"
    if (( free < min )); then
      detail="${detail}${path} has ${free} GiB free; "
      short_count=$((short_count + 1))
    fi
  done

  if (( short_count == 0 )); then
    return 0
  fi

  local runner="${RUNNER_NAME:-this runner}"
  detail="${detail%; }"

  # A hosted runner is reported and allowed through: its image is smaller than
  # any floor a self-hosted host needs, and these builds pass there today.
  if [[ "${RUNNER_ENVIRONMENT:-}" != "self-hosted" ]]; then
    info "warning: below the ${min} GiB build floor (${detail}) — building anyway on a ${RUNNER_ENVIRONMENT:-non-self-hosted} runner"
    return 0
  fi

  info "Not enough disk to build on ${runner}: ${detail}, below the ${min} GiB floor."
  info "  The build did not start, so the branch was not evaluated."
  info "  Reclaim space on this runner — stale _work checkouts, target/ trees and the local sccache directory accumulate, and several runner instances share the volume."
  annotate "${detail}, below the ${min} GiB floor on ${runner}. The build did not start and the branch was not evaluated: reclaim space on this runner and re-run."
  append_step_summary "### Preflight: not enough disk to build"$'\n'"On \`${runner}\`: ${detail}, below the **${min} GiB** floor, so the build did not start and **the branch was not evaluated**. This is an infrastructure failure, not a code failure: reclaim space on this runner (\`target/\` and the local sccache directory are shared across branches, and several runner instances share the volume) and re-run. Adjust the floor with \`BUILD_MIN_FREE_GIB\`."$'\n'
  return "$EXIT_DISK_EXHAUSTED"
}

main "$@"
