#!/usr/bin/env bash
#
# Unit tests for `scripts/reclaim_runner_disk.sh`.
#
# The sweep deletes things on a shared, long-lived volume, so both directions
# are the bug. Deleting too little leaves the condition #12794 describes -- a
# host that only ever fills, until every job that lands on it fails on a write.
# Deleting too much takes a live build out from under the job using it, or
# throws away the warm cache that is the difference between a sign-off finishing
# in minutes and one taking hours. The exclusions are therefore tested as
# closely as the removals: this job's own scratch, this job's own workspace,
# anything modified inside the threshold, and anything a cargo target directory
# has not disowned.
#
# No runner and no network: every case is a temporary directory tree with mtimes
# set to whatever the case needs.
#
# Usage: scripts/test_reclaim_runner_disk.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/reclaim_runner_disk.sh"

tests_run=0
# Deliberately not named `failures`: a helper that shadows the suite's own
# counter makes it report "all N passed" while cases fail underneath it.
failed_assertions=0

fail_test() {
  failed_assertions=$((failed_assertions + 1))
  echo "  FAIL: $1"
}

# The subject reads the tree through `find` and swallows its stderr, because a
# host whose walk fails should not take the sweep down with it. That is right
# for the runner and wrong for a test: an environment where `find` cannot run
# would report every removal case as "nothing was removed" -- which is exactly
# what a broken sweep looks like -- and every exclusion case as passing. Refuse
# to grade the subject on an environment that cannot exercise it.
if ! find "$script_dir" -maxdepth 0 -print >/dev/null 2>&1; then
  echo "cannot run: 'find' is unavailable here, and without it every case would report a false pass" >&2
  exit 1
fi

pass_or_fail() {
  local name="$1" ok="$2" detail="$3"
  tests_run=$((tests_run + 1))
  if [[ "$ok" == "1" ]]; then
    echo "  ok: $name"
  else
    fail_test "$name: $detail"
  fi
}

# A timestamp `touch -t` accepts, "$1" days in the past. GNU and BSD `date`
# spell relative arithmetic differently and the suite has to run on both: CI is
# Linux, a developer reproducing a case is often not.
days_ago_stamp() {
  local days="$1"
  date -u -d "${days} days ago" +%Y%m%d%H%M 2>/dev/null ||
    date -u -v-"${days}"d +%Y%m%d%H%M 2>/dev/null
}

age_path() {
  local path="$1" days="$2" stamp
  stamp=$(days_ago_stamp "$days")
  [[ -n "$stamp" ]] || { echo "could not compute a timestamp ${days} days ago" >&2; exit 1; }
  touch -t "$stamp" "$path"
}

# Ages every entry in a tree, deepest first so that aging a directory is not
# undone by a later write inside it.
age_tree() {
  local root="$1" days="$2" entry
  while IFS= read -r entry; do
    age_path "$entry" "$days"
  done < <(find "$root" -depth 2>/dev/null)
}

make_cargo_target() {
  local dir="$1"
  mkdir -p "$dir"
  printf '%s\n' \
    'Signature: 8a477f597d28d172789f06886806bc55' \
    '# This file is a cache directory tag created by cargo.' >"$dir/CACHEDIR.TAG"
}

# Runs one function from the subject in a fresh shell. Sourcing reaches a single
# function without running the argument parsing a real invocation would.
# Echoes "<rc>|<output>".
call_subject() {
  local snippet="$1"
  shift
  local output rc
  # `env` rather than an assignment prefix: these come from "$@", and a word
  # that only looks like an assignment after expansion is read as the command.
  output="$(env "$@" bash -c 'source "$1"; shift; '"$snippet" _ "$subject" 2>&1)"
  rc=$?
  printf '%s|%s' "$rc" "$output"
}

echo "work_root_from_env"

result="$(call_subject 'work_root_from_env' "RUNNER_WORKSPACE=/opt/github-runner-01/_work/spiceai")"
pass_or_fail "derives the work root from RUNNER_WORKSPACE" \
  "$([[ "$result" == "0|/opt/github-runner-01/_work" ]] && echo 1 || echo 0)" \
  "got: $result"

result="$(call_subject 'work_root_from_env' "RUNNER_WORKSPACE=")"
pass_or_fail "fails when RUNNER_WORKSPACE is unset" \
  "$([[ "${result%%|*}" != "0" ]] && echo 1 || echo 0)" \
  "expected a non-zero exit, got: $result"

echo "live_workspace"

result="$(call_subject 'live_workspace' "RUNNER_WORKSPACE=/opt/github-runner-01/_work/spiceai/")"
pass_or_fail "normalises a trailing slash, so the exclusion still matches" \
  "$([[ "$result" == "0|/opt/github-runner-01/_work/spiceai" ]] && echo 1 || echo 0)" \
  "got: $result"

# Off a runner there is no RUNNER_WORKSPACE, and answering nothing would leave
# the one tree we are certain is in use protected only by its mtimes.
result="$(call_subject 'live_workspace' "RUNNER_WORKSPACE=")"
pass_or_fail "falls back to the checkout this script lives in" \
  "$([[ "$result" == "0|$(dirname "$(dirname "$script_dir")")" ]] && echo 1 || echo 0)" \
  "got: $result"

echo "is_cargo_target_dir"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

make_cargo_target "$tmp/real-target"
mkdir -p "$tmp/named-target"
mkdir -p "$tmp/wrong-signature"
printf 'Signature: not-the-cargo-one\n' >"$tmp/wrong-signature/CACHEDIR.TAG"

result="$(env bash -c 'source "$1"; is_cargo_target_dir "$2"' _ "$subject" "$tmp/real-target" 2>&1; echo "rc=$?")"
pass_or_fail "accepts a directory carrying cargo's CACHEDIR.TAG" \
  "$([[ "$result" == *"rc=0" ]] && echo 1 || echo 0)" "got: $result"

result="$(env bash -c 'source "$1"; is_cargo_target_dir "$2"' _ "$subject" "$tmp/named-target" 2>&1; echo "rc=$?")"
pass_or_fail "rejects a directory that is merely named like one" \
  "$([[ "$result" == *"rc=1" ]] && echo 1 || echo 0)" "got: $result"

result="$(env bash -c 'source "$1"; is_cargo_target_dir "$2"' _ "$subject" "$tmp/wrong-signature" 2>&1; echo "rc=$?")"
pass_or_fail "rejects a CACHEDIR.TAG some other tool wrote" \
  "$([[ "$result" == *"rc=1" ]] && echo 1 || echo 0)" "got: $result"

echo "workspace_is_stale"

mkdir -p "$tmp/stale-ws/repo/src"
touch "$tmp/stale-ws/repo/src/lib.rs" "$tmp/stale-ws/repo/Cargo.toml"
age_tree "$tmp/stale-ws" 30

mkdir -p "$tmp/fresh-ws/repo/src"
touch "$tmp/fresh-ws/repo/src/lib.rs"
age_tree "$tmp/fresh-ws" 30
# One recent file at the depth a checkout rewrites, which is the whole signal.
touch "$tmp/fresh-ws/repo/Cargo.toml"

result="$(env bash -c 'source "$1"; workspace_is_stale "$2" "$3"' _ "$subject" "$tmp/stale-ws" 7 2>&1; echo "rc=$?")"
pass_or_fail "a workspace untouched for longer than the threshold is stale" \
  "$([[ "$result" == *"rc=0" ]] && echo 1 || echo 0)" "got: $result"

result="$(env bash -c 'source "$1"; workspace_is_stale "$2" "$3"' _ "$subject" "$tmp/fresh-ws" 7 2>&1; echo "rc=$?")"
pass_or_fail "one recent file keeps a workspace off the list" \
  "$([[ "$result" == *"rc=1" ]] && echo 1 || echo 0)" "got: $result"

# A walk that errored prints nothing, which is byte-for-byte what "nothing here
# is recent" looks like — and the caller deletes the whole workspace on that
# answer. The unknown case has to land on the safe side.
failing_find_dir="$(mktemp -d)"
printf '#!/usr/bin/env bash\nexit 1\n' >"$failing_find_dir/find"
chmod +x "$failing_find_dir/find"

result="$(env "PATH=$failing_find_dir:$PATH" bash -c 'source "$1"; workspace_is_stale "$2" "$3"' _ "$subject" "$tmp/stale-ws" 7 2>&1; echo "rc=$?")"
pass_or_fail "a workspace whose walk failed is not called stale" \
  "$([[ "$result" == *"rc=1" ]] && echo 1 || echo 0)" \
  "an unreadable directory was reported as safe to delete: $result"

echo "reclaim_temp"

root="$tmp/work"
mkdir -p "$root/_temp/orphan" "$root/_temp/live" "$root/_temp/recent"
touch "$root/_temp/orphan/leftover.log"
age_tree "$root/_temp/orphan" 30
age_tree "$root/_temp/live" 30

env "RUNNER_TEMP=$root/_temp/live" bash -c 'source "$1"; reclaim_temp "$2" "$3"' _ "$subject" "$root" 7 >/dev/null 2>&1

pass_or_fail "removes scratch a killed job orphaned" \
  "$([[ ! -e "$root/_temp/orphan" ]] && echo 1 || echo 0)" "orphan survived"
pass_or_fail "keeps this job's own scratch however old it looks" \
  "$([[ -d "$root/_temp/live" ]] && echo 1 || echo 0)" "RUNNER_TEMP was removed"
pass_or_fail "keeps scratch inside the threshold" \
  "$([[ -d "$root/_temp/recent" ]] && echo 1 || echo 0)" "recent scratch was removed"

# On stock Actions RUNNER_TEMP *is* `_temp`, not a directory inside it. Nothing
# beneath it may be spared on that basis -- sparing the whole directory would
# make the sweep a no-op on every real runner, which is the shape of bug that
# passes its own tests while reclaiming nothing.
mkdir -p "$root/_temp/from-a-dead-job"
age_tree "$root/_temp/from-a-dead-job" 30
env "RUNNER_TEMP=$root/_temp" bash -c 'source "$1"; reclaim_temp "$2" "$3"' _ "$subject" "$root" 7 >/dev/null 2>&1
pass_or_fail "still sweeps when RUNNER_TEMP names the whole _temp directory" \
  "$([[ ! -e "$root/_temp/from-a-dead-job" ]] && echo 1 || echo 0)" \
  "the sweep spared everything because RUNNER_TEMP is _temp itself"

echo "reclaim_workspaces"

mkdir -p "$root/otherrepo/otherrepo" "$root/spiceai/spiceai" "$root/busyrepo/busyrepo" "$root/_actions/some/action"
touch "$root/otherrepo/otherrepo/Cargo.toml" "$root/spiceai/spiceai/Cargo.toml" "$root/busyrepo/busyrepo/Cargo.toml"
age_tree "$root/otherrepo" 30
age_tree "$root/busyrepo" 30
age_tree "$root/_actions" 30
# The live workspace is aged too, to prove the exclusion is by name and does not
# lean on the checkout that would refresh it in a real run.
age_tree "$root/spiceai" 30
touch "$root/busyrepo/busyrepo/Cargo.toml"

env "RUNNER_WORKSPACE=$root/spiceai" bash -c 'source "$1"; reclaim_workspaces "$2" "$3"' _ "$subject" "$root" 7 >/dev/null 2>&1

pass_or_fail "removes a workspace no job has used inside the threshold" \
  "$([[ ! -e "$root/otherrepo" ]] && echo 1 || echo 0)" "stale workspace survived"
pass_or_fail "keeps the workspace this job is using, whatever its age" \
  "$([[ -d "$root/spiceai/spiceai" ]] && echo 1 || echo 0)" "the live workspace was removed"
pass_or_fail "keeps a workspace something touched inside the threshold" \
  "$([[ -d "$root/busyrepo/busyrepo" ]] && echo 1 || echo 0)" "a recently used workspace was removed"
pass_or_fail "leaves runner-internal state alone" \
  "$([[ -d "$root/_actions/some/action" ]] && echo 1 || echo 0)" "_actions was removed"

echo "reclaim_stale_build_output"

target="$root/spiceai/spiceai/target"
make_cargo_target "$target"
mkdir -p "$target/debug/deps" "$target/package/inner-crate"
touch "$target/debug/deps/old.rlib" "$target/debug/deps/fresh.rlib"
make_cargo_target "$target/package/inner-crate/target"
touch "$target/package/inner-crate/target/old.rlib"

# A directory named `target` that no cargo ever wrote: source, not build output.
mkdir -p "$root/spiceai/spiceai/crates/target"
touch "$root/spiceai/spiceai/crates/target/mod.rs"

age_tree "$target/debug" 30
age_tree "$target/package" 30
age_tree "$root/spiceai/spiceai/crates/target" 30
touch "$target/debug/deps/fresh.rlib"

# A floor no real volume reaches, so every host counts as below it and the prune
# always runs: the free-space gate has its own cases below, and these are about
# which files the prune selects.
readonly ALWAYS_PRUNE_FLOOR=999999999
output="$(env bash -c 'source "$1"; reclaim_stale_build_output "$2" "$3" "$4"' _ "$subject" "$root" 7 "$ALWAYS_PRUNE_FLOOR" 2>&1)"

pass_or_fail "prunes build output older than the threshold" \
  "$([[ ! -e "$target/debug/deps/old.rlib" ]] && echo 1 || echo 0)" "stale artifact survived"
pass_or_fail "keeps build output inside the threshold, so the cache stays warm" \
  "$([[ -f "$target/debug/deps/fresh.rlib" ]] && echo 1 || echo 0)" "warm artifact was pruned"
pass_or_fail "never walks a directory merely named target" \
  "$([[ -f "$root/spiceai/spiceai/crates/target/mod.rs" ]] && echo 1 || echo 0)" "source under crates/target was deleted"
pass_or_fail "sweeps a nested target directory only through its ancestor" \
  "$([[ "$output" == *"build output directories swept: 1"* ]] && echo 1 || echo 0)" "got: $output"

echo "free-space gate on build output"

# A host with room to spare keeps its cache. The point of the gate: a pruned
# target directory costs a colder build, and on this pool a colder build is
# measured against a step budget whose expiry publishes a code-failure verdict.
gated="$tmp/gated"
make_cargo_target "$gated/spiceai/spiceai/target"
touch "$gated/spiceai/spiceai/target/old.rlib"
age_tree "$gated" 30

output="$(env bash -c 'source "$1"; reclaim_stale_build_output "$2" "$3" "$4"' _ "$subject" "$gated" 7 0 2>&1)"
pass_or_fail "leaves build output alone on a host above the floor" \
  "$([[ -f "$gated/spiceai/spiceai/target/old.rlib" ]] && echo 1 || echo 0)" \
  "the cache was pruned on a host with room"
pass_or_fail "says why it left it alone" \
  "$([[ "$output" == *"leaving build output alone"* ]] && echo 1 || echo 0)" "got: $output"

output="$(env bash -c 'source "$1"; reclaim_stale_build_output "$2" "$3" "$4"' _ "$subject" "$gated" 7 "$ALWAYS_PRUNE_FLOOR" 2>&1)"
pass_or_fail "prunes on a host below the floor" \
  "$([[ ! -e "$gated/spiceai/spiceai/target/old.rlib" ]] && echo 1 || echo 0)" \
  "the cache survived on a host that is out of room"

# A volume whose `df` cannot be read is not evidence of a healthy one -- and the
# hosts this exists for are the ones in trouble.
stub_dir="$(mktemp -d)"
printf '#!/usr/bin/env bash\nexit 1\n' >"$stub_dir/df"
chmod +x "$stub_dir/df"

unreadable="$tmp/unreadable"
make_cargo_target "$unreadable/repo/target"
touch "$unreadable/repo/target/old.rlib"
age_tree "$unreadable" 30

output="$(env "PATH=$stub_dir:$PATH" bash -c 'source "$1"; reclaim_stale_build_output "$2" "$3" "$4"' _ "$subject" "$unreadable" 7 100 2>&1)"
pass_or_fail "prunes when free space cannot be measured" \
  "$([[ ! -e "$unreadable/repo/target/old.rlib" ]] && echo 1 || echo 0)" \
  "an unreadable df was treated as a healthy host"

echo "dry run"

dry="$tmp/dryrun"
mkdir -p "$dry/_temp/orphan" "$dry/staleworkspace/repo" "$dry/spiceai/spiceai"
make_cargo_target "$dry/spiceai/spiceai/target"
touch "$dry/staleworkspace/repo/Cargo.toml" "$dry/spiceai/spiceai/target/old.rlib"
age_tree "$dry" 30

output="$(env "RUNNER_WORKSPACE=$dry/spiceai" "DRY_RUN=1" bash -c \
  'source "$1"; reclaim_temp "$2" "$3"; reclaim_workspaces "$2" "$3"; reclaim_stale_build_output "$2" "$3" "$4"' \
  _ "$subject" "$dry" 7 "$ALWAYS_PRUNE_FLOOR" 2>&1)"

pass_or_fail "removes nothing" \
  "$([[ -d "$dry/_temp/orphan" && -d "$dry/staleworkspace" && -f "$dry/spiceai/spiceai/target/old.rlib" ]] && echo 1 || echo 0)" \
  "a dry run deleted something"
pass_or_fail "still names what it would remove" \
  "$([[ "$output" == *"would remove"* && "$output" == *"would prune"* ]] && echo 1 || echo 0)" \
  "got: $output"

echo "argument handling"

result="$(env "RUNNER_WORKSPACE=" bash "$subject" 2>&1; echo "rc=$?")"
pass_or_fail "refuses to guess a work root" \
  "$([[ "$result" == *"rc=64" ]] && echo 1 || echo 0)" "got: $result"

result="$(bash "$subject" --root "$tmp" --max-age-days notanumber 2>&1; echo "rc=$?")"
pass_or_fail "rejects a non-numeric age" \
  "$([[ "$result" == *"rc=64" ]] && echo 1 || echo 0)" "got: $result"

result="$(bash "$subject" --root "$tmp" --free-gib lots 2>&1; echo "rc=$?")"
pass_or_fail "rejects a non-numeric free-space floor" \
  "$([[ "$result" == *"rc=64" ]] && echo 1 || echo 0)" "got: $result"

result="$(bash "$subject" --root "$tmp/does-not-exist" 2>&1; echo "rc=$?")"
pass_or_fail "rejects a work root that is not there" \
  "$([[ "$result" == *"rc=64" ]] && echo 1 || echo 0)" "got: $result"

result="$(bash "$subject" --root "$tmp" --unknown-flag 2>&1; echo "rc=$?")"
pass_or_fail "rejects an unknown argument rather than sweeping with a default" \
  "$([[ "$result" == *"rc=64" ]] && echo 1 || echo 0)" "got: $result"

# A value-taking flag typed last used to leave `shift 2` failing with the
# argument still in place, so the parser spun on it until the job timeout —
# a mistyped dispatch input hung the sweep instead of correcting it. Each case
# is bounded so a regression fails the suite rather than hanging it.
for flag in --root --max-age-days --free-gib; do
  result="$(bash "$subject" "$flag" 2>&1 & pid=$!; ( sleep 10; kill -9 "$pid" 2>/dev/null ) & watchdog=$!; wait "$pid"; rc=$?; kill "$watchdog" 2>/dev/null; echo "rc=$rc")"
  pass_or_fail "$flag with no value exits rather than looping" \
    "$([[ "$result" == *"rc=64" ]] && echo 1 || echo 0)" "got: $result"
done

# Bash arithmetic reads a leading zero as octal, so an unnormalised `08` is an
# arithmetic error and `0100` would quietly mean 64 — a wrong floor decides
# whether a host keeps its build cache.
octal="$tmp/octal"
make_cargo_target "$octal/repo/target"
touch "$octal/repo/target/old.rlib"
age_tree "$octal" 30

output="$(env bash -c 'source "$1"; reclaim_stale_build_output "$2" "$3" "$4"' _ "$subject" "$octal" 7 08 2>&1)"
pass_or_fail "a zero-padded floor is read as decimal, not octal" \
  "$([[ "$output" != *"error"* && "$output" != *"value too great"* && "$output" == *"GiB"* ]] && echo 1 || echo 0)" \
  "got: $output"

echo
if [[ "$failed_assertions" -eq 0 ]]; then
  echo "all ${tests_run} checks passed"
  exit 0
fi
echo "${failed_assertions} of ${tests_run} checks failed"
exit 1
