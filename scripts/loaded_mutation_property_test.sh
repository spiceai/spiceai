#!/usr/bin/env bash
#
# Run the cayenne mutation-property / convergence tests under CPU load.
#
# These are deterministic correctness tests (the accelerated table MUST converge
# to a reference model), but some failures are scheduling-sensitive: they only
# surface when the machine is saturated and thread interleavings shift — exactly
# what happens in CI when the full suite runs, and NOT on an idle dev box (an
# isolated run can pass indefinitely). This harness recreates that pressure by
# oversubscribing the CPU with busy-loops, then runs the tests with retries
# DISABLED so any single-attempt failure is a hard failure instead of a masked
# flaky retry.
#
# Use it to reproduce a suspected convergence flake locally, or to smoke-test a
# change to the compaction / snapshot / restart paths before pushing.
#
# Usage:
#   scripts/loaded_mutation_property_test.sh [TEST_FILTER] [ITERATIONS]
#
#   TEST_FILTER   nextest filter (default: empty = the whole
#                 mutation_property_test binary, so all convergence tests contend
#                 with each other like CI). Pass e.g.
#                 prop_sequential_key_impl_sqlite (fails fastest) or
#                 prop_concurrent_mixed_key_sqlite to target one test.
#   ITERATIONS    number of times to run the selection (default: 10). The loop
#                 stops early and exits non-zero on the first failing iteration.
#
# Env:
#   NHOGS         CPU busy-loops (default: min(nproc, 16); use >= core count to
#                 oversubscribe).
#   OPS_SCALE     CAYENNE_PROPTEST_OPS_SCALE (default: 2; longer op histories
#                 widen race windows).
#   TEST_PKG / TEST_BIN  override the crate/binary (default: cayenne /
#                 mutation_property_test).
#
set -uo pipefail

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

FILTER="${1:-}"
ITERATIONS="${2:-10}"
OPS_SCALE="${OPS_SCALE:-2}"
TEST_PKG="${TEST_PKG:-cayenne}"
TEST_BIN="${TEST_BIN:-mutation_property_test}"

ncpu="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 8)"
NHOGS="${NHOGS:-$(( ncpu < 16 ? ncpu : 16 ))}"

run_tests() {
  # sccache can point at an unwritable dir in some sandboxes and break the C
  # build; bypass the wrappers for these runs (see CLAUDE.md).
  env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
    SCCACHE_DIR="${SCCACHE_DIR:-$HOME/.cache/sccache}" \
    RUST_BACKTRACE=1 CAYENNE_PROPTEST_OPS_SCALE="$OPS_SCALE" \
    cargo nextest run -p "$TEST_PKG" --test "$TEST_BIN" ${FILTER:+"$FILTER"} \
    --retries 0 --no-fail-fast
}

echo ">> Building $TEST_PKG::$TEST_BIN ..."
env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
  SCCACHE_DIR="${SCCACHE_DIR:-$HOME/.cache/sccache}" \
  cargo nextest run -p "$TEST_PKG" --test "$TEST_BIN" ${FILTER:+"$FILTER"} --no-run

echo ">> Spawning $NHOGS CPU hog(s) to induce contention (${ncpu} cores)..."
HOGS=()
for _ in $(seq 1 "$NHOGS"); do yes > /dev/null & HOGS+=($!); done
cleanup() { kill "${HOGS[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo ">> Running '${FILTER:-<all>}' x${ITERATIONS}, retries disabled, OPS_SCALE=$OPS_SCALE ..."
for i in $(seq 1 "$ITERATIONS"); do
  echo "=== iteration $i/$ITERATIONS ==="
  if ! run_tests; then
    echo ">>> FAILED on iteration $i (filter='${FILTER:-<all>}')"
    exit 1
  fi
done

echo ">> All $ITERATIONS iteration(s) passed. Raise NHOGS/OPS_SCALE/ITERATIONS to hunt harder."
