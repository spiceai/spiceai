#!/usr/bin/env bash
#
# Unit tests for the compiler-cache pre-flight in the `setup-sccache` action:
# whether an unreachable cache costs the build its cache or its verdict.
#
# The action exports `RUSTC_WRAPPER=sccache`, which puts sccache in front of
# every `rustc` invocation. Storage it cannot reach therefore fails each one, and
# the job reports a compile error naming a crate rather than the cache — in the
# merge queue that false verdict dequeues the batch and cancels every sibling
# candidate with it (#12770). The pre-flight must clear the wrapper and let the
# build proceed uncached instead, and must never fail the step itself.
#
# No network: a stub `sccache` on PATH serves each condition the runner presents
# — a cache that answers, storage that refuses the connection, and a runner
# missing the binary altogether.
#
# Usage: scripts/test_sccache_preflight.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/../.github/actions/setup-sccache/verify_compiler_cache.sh"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

if [[ ! -f "$subject" ]]; then
  echo "FAIL: subject not found at $subject"
  exit 1
fi

# An `sccache` that answers the way v0.9.1 does in the named condition.
#
# The "refused" stub is deliberately asymmetric, because the real binary is: only
# commands routed through `connect_or_start_server` (`--zero-stats`,
# `--start-server`) surface unreachable storage. `--show-stats` calls
# `connect_to_server` and, when no daemon answers, reports synthesized empty stats
# and exits 0. A stub that failed every command would let a pre-flight built on
# `--show-stats` pass here and still miss the outage in production, so the stub
# has to keep that distinction for the test to mean anything.
write_sccache_stub() {
  local dir="$1" condition="$2"
  case "$condition" in
    ok)
      cat >"$dir/sccache" <<'STUB'
#!/usr/bin/env bash
echo "Compile requests 0"
exit 0
STUB
      ;;
    refused)
      cat >"$dir/sccache" <<'STUB'
#!/usr/bin/env bash
case "${1:-}" in
  --show-stats)
    # No daemon: real sccache synthesizes empty stats and succeeds regardless of
    # whether the storage behind it answers.
    echo "Compile requests 0"
    exit 0
    ;;
esac
echo "sccache: error: Server startup failed: cache storage failed to read: Unexpected (temporary) at read => send http request" >&2
echo "   error sending request for url (http://127.0.0.1:8335/sccache/sccache/.sccache_check): tcp connect error: Connection refused (os error 61)" >&2
exit 2
STUB
      ;;
    absent) return 0 ;;
    *)
      echo "unknown stub condition: $condition" >&2
      exit 1
      ;;
  esac
  chmod +x "$dir/sccache"
}

# Run the subject with `sccache` in the named condition. Sets `run_rc`,
# `run_output` and `run_env` (the lines the subject appended to $GITHUB_ENV).
call_subject() {
  local condition="$1"
  local tmp
  tmp="$(mktemp -d)"
  mkdir -p "$tmp/bin"
  write_sccache_stub "$tmp/bin" "$condition"

  : >"$tmp/github_env"
  # A PATH holding only the stub dir plus the system directories, so a real
  # sccache on the developer's machine cannot answer for the stub.
  run_output="$(PATH="$tmp/bin:/usr/bin:/bin" GITHUB_ENV="$tmp/github_env" bash "$subject" 2>&1)"
  run_rc=$?
  run_env="$(cat "$tmp/github_env")"
  rm -rf "$tmp"
}

# An unreachable cache must not fail the step: that is the false code-failure
# verdict this pre-flight exists to prevent.
test_unreachable_storage_does_not_fail_the_step() {
  tests_run=$((tests_run + 1))
  echo "test: unreachable storage does not fail the step"
  call_subject refused
  if [[ "$run_rc" -ne 0 ]]; then
    fail_test "expected exit 0 for unreachable storage, got $run_rc"
  fi
}

test_unreachable_storage_clears_the_wrapper() {
  tests_run=$((tests_run + 1))
  echo "test: unreachable storage clears RUSTC_WRAPPER so the build continues uncached"
  call_subject refused
  if ! grep -qx 'RUSTC_WRAPPER=' <<<"$run_env"; then
    fail_test "expected an empty RUSTC_WRAPPER in \$GITHUB_ENV, got: ${run_env//$'\n'/ | }"
  fi
  if ! grep -qx 'SCCACHE_SETUP=false' <<<"$run_env"; then
    fail_test "expected SCCACHE_SETUP=false in \$GITHUB_ENV, got: ${run_env//$'\n'/ | }"
  fi
}

# A `::warning::` annotation keeps only its first line, so the reason the cache
# was dropped has to survive on that line or the log explains nothing.
test_unreachable_storage_names_the_cache_on_one_line() {
  tests_run=$((tests_run + 1))
  echo "test: the warning names the cache and stays on one line"
  call_subject refused
  local warnings
  warnings="$(grep -c '^::warning::' <<<"$run_output")"
  if [[ "$warnings" -ne 1 ]]; then
    fail_test "expected exactly 1 ::warning:: line, got $warnings: ${run_output//$'\n'/ | }"
  fi
  if ! grep -q '^::warning::.*Connection refused' <<<"$run_output"; then
    fail_test "expected the warning to carry sccache's reason, got: ${run_output//$'\n'/ | }"
  fi
}

# A runner whose sccache never installed is the same class of condition as one
# whose storage is down, and must degrade the same way.
test_absent_binary_degrades_instead_of_failing() {
  tests_run=$((tests_run + 1))
  echo "test: a missing sccache binary degrades to an uncached build"
  call_subject absent
  if [[ "$run_rc" -ne 0 ]]; then
    fail_test "expected exit 0 when sccache is absent, got $run_rc"
  fi
  if ! grep -qx 'RUSTC_WRAPPER=' <<<"$run_env"; then
    fail_test "expected an empty RUSTC_WRAPPER in \$GITHUB_ENV, got: ${run_env//$'\n'/ | }"
  fi
}

# The other direction: a cache that answers must be left in front of rustc, or
# the pre-flight would silently discard the cache it is meant to protect.
test_reachable_cache_is_left_alone() {
  tests_run=$((tests_run + 1))
  echo "test: a reachable cache keeps the wrapper"
  call_subject ok
  if [[ "$run_rc" -ne 0 ]]; then
    fail_test "expected exit 0 for a reachable cache, got $run_rc"
  fi
  if [[ -n "$run_env" ]]; then
    fail_test "expected no \$GITHUB_ENV writes for a reachable cache, got: ${run_env//$'\n'/ | }"
  fi
  if grep -q '^::warning::' <<<"$run_output"; then
    fail_test "expected no warning for a reachable cache, got: ${run_output//$'\n'/ | }"
  fi
}

test_unreachable_storage_does_not_fail_the_step
test_unreachable_storage_clears_the_wrapper
test_unreachable_storage_names_the_cache_on_one_line
test_absent_binary_degrades_instead_of_failing
test_reachable_cache_is_left_alone

echo
if [[ "$failures" -eq 0 ]]; then
  echo "All $tests_run tests passed."
  exit 0
fi

echo "$failures of $tests_run tests failed."
exit 1
