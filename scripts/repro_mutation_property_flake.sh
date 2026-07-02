#!/usr/bin/env bash
#
# Reproduce the cayenne mutation_property convergence "flake".
#
# These are deterministic correctness tests: the accelerated table MUST converge
# to the reference model. They intermittently fail in CI with a panic at
# mutation_property_test.rs "…convergence failed / diverged":
#
#     missing(loss)=[]                                   <- no row is ever lost
#     extra(resurrect)=[(k, v)…]                         <- a deleted key reappears
#     wrong_value(k,exp,got)=[(k, exp, -9223372036854775808)…]
#
# got = i64::MIN is the test's DUPLICATE-KEY sentinel (see read_rows() in
# crates/cayenne/tests/mutation_property_test.rs): the live `SELECT id,value`
# returned the SAME primary key more than once. i.e. the underlying bug is
# post-compaction merge-on-read returning DUPLICATE physical rows for a PK
# (a double-count), NOT data loss.
#
# The bug is scheduling-sensitive and will NOT reproduce on an idle machine
# (isolated runs pass indefinitely). CI hits it because the full test suite
# saturates the runner. This script recreates that by oversubscribing the CPU
# with busy-loops, then running the target test(s) with retries DISABLED so any
# single-attempt failure is a hard failure (never a masked "flaky" retry).
#
# Usage:
#   scripts/repro_mutation_property_flake.sh [TEST_FILTER] [MAX_ATTEMPTS]
#
#   TEST_FILTER   nextest filter (default: prop_sequential_key_impl_sqlite —
#                 fails fastest, ~5-15s, with a small reproducible op history).
#                 Use prop_concurrent_mixed_key_sqlite for the exact CI test.
#   MAX_ATTEMPTS  give up after this many green attempts (default: 20).
#
# Env:
#   NHOGS         number of CPU busy-loops (default: 24; use >= core count).
#   OPS_SCALE     CAYENNE_PROPTEST_OPS_SCALE (default: 2; longer op histories
#                 widen the race window).
#
set -uo pipefail

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

FILTER="${1:-prop_sequential_key_impl_sqlite}"
MAX_ATTEMPTS="${2:-20}"
NHOGS="${NHOGS:-24}"
OPS_SCALE="${OPS_SCALE:-2}"

# sccache in some sandboxes points at an unwritable dir and breaks the C build;
# bypass the wrappers for these runs (see CLAUDE.md).
run_test() {
  env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
    SCCACHE_DIR="${SCCACHE_DIR:-$HOME/.cache/sccache}" \
    RUST_BACKTRACE=1 CAYENNE_PROPTEST_OPS_SCALE="$OPS_SCALE" \
    cargo nextest run -p cayenne --test mutation_property_test "$FILTER" \
    --retries 0 --no-fail-fast --no-capture
}

# Build once up front so build time is not counted against the slow-timeout and
# the busy-loops do not slow compilation.
echo ">> Building test binary..."
env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
  SCCACHE_DIR="${SCCACHE_DIR:-$HOME/.cache/sccache}" \
  cargo nextest run -p cayenne --test mutation_property_test "$FILTER" --no-run

# Oversubscribe the cores so thread interleavings resemble a loaded CI box.
echo ">> Spawning $NHOGS CPU hogs to induce contention..."
HOGS=()
for _ in $(seq 1 "$NHOGS"); do yes > /dev/null & HOGS+=($!); done
cleanup() { kill "${HOGS[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo ">> Reproducing '$FILTER' (retries disabled, up to $MAX_ATTEMPTS attempts)..."
for i in $(seq 1 "$MAX_ATTEMPTS"); do
  echo "=== attempt $i/$MAX_ATTEMPTS ==="
  if ! run_test; then
    echo ">>> REPRODUCED on attempt $i (filter=$FILTER)"
    exit 1
  fi
done

echo ">> No failure in $MAX_ATTEMPTS attempts. Raise NHOGS/OPS_SCALE/MAX_ATTEMPTS and retry."
exit 0
