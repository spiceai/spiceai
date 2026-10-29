#!/usr/bin/env bash
#
# Tests for .github/actions/allocate-service-ports — the scripts that give each
# E2E job its own PostgreSQL, MySQL, and ClickHouse ports on a host shared by
# several runner instances.
#
# The property under test is exclusivity: a port handed to one job is not
# handed to another until that job releases it, even when both jobs' seeds
# land on the same offset and neither has bound the port yet.
#
#   bash scripts/test_e2e_service_ports.sh

set -uo pipefail

repo_root=$(cd -- "$(dirname -- "$0")/.." && pwd)
allocate="$repo_root/.github/actions/allocate-service-ports/allocate_ports.sh"
release="$repo_root/.github/actions/allocate-service-ports/release_ports.sh"

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
lock_dir="$work_dir/locks"

failures=0

pass() {
  printf '  ok    %s\n' "$1"
}

fail() {
  printf '  FAIL  %s\n' "$1"
  failures=$((failures + 1))
}

assert_eq() {
  if [ "$2" = "$3" ]; then pass "$1"; else fail "$1: expected '$2', got '$3'"; fi
}

assert_ne() {
  if [ "$2" != "$3" ]; then pass "$1"; else fail "$1: both were '$2'"; fi
}

# Runs the allocator for runner instance $1 within one run and job, as the
# matrix legs of a single job do, and prints the CI_PG_PORT it chose.
pg_port() {
  env -i PATH="$PATH" CI_SERVICE_PORT_LOCK_DIR="$lock_dir" RUNNER_NAME="$1" \
    GITHUB_RUN_ID=40802775871 GITHUB_JOB=test_quickstart_data_postgres GITHUB_RUN_ATTEMPT=1 \
    bash "$allocate" | sed -n 's/^CI_PG_PORT=//p'
}

release_for() {
  env -i PATH="$PATH" CI_SERVICE_PORT_LOCK_DIR="$lock_dir" RUNNER_NAME="$1" \
    CI_SERVICE_PORT_LOCKS="$2" bash "$release"
}

echo "allocate-service-ports"

# These two instance names seed the same offset, so before reservations both
# were handed the same port while their databases were still starting.
first=$(pg_port spiceai-macos-runner-008)
second=$(pg_port spiceai-macos-runner-049)
assert_ne "instances whose seeds collide get different ports" "$first" "$second"
assert_eq "the first instance holds its reservation" "spiceai-macos-runner-008" "$(cat "$lock_dir/$first/owner")"

again=$(pg_port spiceai-macos-runner-008)
assert_eq "an instance takes over its own leftover reservation" "$first" "$again"

release_for spiceai-macos-runner-049 "$first $second"
assert_eq "release keeps another instance's reservation" "spiceai-macos-runner-008" "$(cat "$lock_dir/$first/owner" 2>/dev/null)"
[ -d "$lock_dir/$second" ] && fail "release frees the job's own reservation" || pass "release frees the job's own reservation"

release_for spiceai-macos-runner-008 "$first"
[ -d "$lock_dir/$first" ] && fail "the holder's release frees its port" || pass "the holder's release frees its port"
assert_eq "a released port is handed out again" "$first" "$(pg_port spiceai-macos-runner-049)"

# Another instance's reservation is never taken over, however old: two jobs
# that both judged it stale would both claim it.
release_for spiceai-macos-runner-049 "$first"
mkdir -p "$lock_dir/$first" && echo spiceai-macos-runner-gone >"$lock_dir/$first/owner"
touch -t 202001010000 "$lock_dir/$first"
assert_ne "an old reservation of another instance is skipped" "$first" "$(pg_port spiceai-macos-runner-008)"
dupes=0
for _ in 1 2 3 4 5 6 7 8; do
  pg_port spiceai-macos-runner-008 >"$work_dir/a" &
  pg_port spiceai-macos-runner-049 >"$work_dir/b" &
  wait
  [ "$(cat "$work_dir/a")" = "$(cat "$work_dir/b")" ] && dupes=$((dupes + 1))
  release_for spiceai-macos-runner-008 "$(cat "$work_dir/a")"
  release_for spiceai-macos-runner-049 "$(cat "$work_dir/b")"
done
assert_eq "concurrent allocations next to an old reservation never share a port" "0" "$dupes"

# An instance runs one job at a time, so reservations it still owns when a new
# job starts are leftovers of its previous job, from another run, and are freed.
leftover=$(env -i PATH="$PATH" CI_SERVICE_PORT_LOCK_DIR="$lock_dir" RUNNER_NAME=spiceai-macos-runner-008 \
  GITHUB_RUN_ID=1 GITHUB_JOB=other GITHUB_RUN_ATTEMPT=1 bash "$allocate" | sed -n 's/^CI_PG_PORT=//p')
pg_port spiceai-macos-runner-008 >/dev/null
[ -d "$lock_dir/$leftover" ] && fail "a previous job's leftover is released" || pass "a previous job's leftover is released"

# With every candidate port taken, allocation fails rather than hand out a port
# another job holds, exports nothing, and keeps none of what it reserved.
exhausted="$work_dir/exhausted"
mkdir -p "$exhausted"
for p in $(seq 11000 11999); do mkdir "$exhausted/$p" && echo other >"$exhausted/$p/owner"; done
out=$(env -i PATH="$PATH" CI_SERVICE_PORT_LOCK_DIR="$exhausted" RUNNER_NAME=spiceai-macos-runner-008 \
  GITHUB_RUN_ID=40802775871 GITHUB_JOB=test_quickstart_data_postgres GITHUB_RUN_ATTEMPT=1 bash "$allocate" 2>&1)
rc=$?
assert_eq "exhaustion exits non-zero" "1" "$rc"
case "$out" in *CI_*=*) fail "exhaustion exports no ports" ;; *) pass "exhaustion exports no ports" ;; esac
assert_eq "exhaustion releases the ports it had reserved" "0" "$(grep -lx spiceai-macos-runner-008 "$exhausted"/*/owner 2>/dev/null | wc -l | tr -d ' ')"

if [ "$failures" -gt 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "All checks passed"
