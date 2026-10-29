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

# A reservation left by an instance that never released it stops blocking the
# port once it is older than CI_SERVICE_PORT_STALE_MINUTES (a day by default).
touch -t 202001010000 "$lock_dir/$first"
assert_eq "an abandoned reservation is taken over" "$first" "$(pg_port spiceai-macos-runner-008)"

if [ "$failures" -gt 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "All checks passed"
