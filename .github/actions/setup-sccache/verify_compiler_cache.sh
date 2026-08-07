#!/usr/bin/env bash
#
# Confirm the compiler cache answers before `rustc` is wrapped in it.
#
# `sccache` wraps every `rustc` invocation, so storage it cannot reach fails each
# one, and the job reports a compile error naming a crate rather than the cache.
# A compiler cache is an optimization: when it is unreachable the build must lose
# the cache, not the verdict. Asking the server for its stats performs the same
# storage read a compile would, at a point where failing it costs only the cache.
#
# Clears `RUSTC_WRAPPER` in `$GITHUB_ENV` so the build continues uncached, and
# always exits 0: an unreachable cache is an infrastructure condition and must
# not be reported as a fault in the code under test.
#
# Usage: verify_compiler_cache.sh

set -uo pipefail

if output=$(sccache --show-stats 2>&1); then
  echo "Compiler cache is reachable."
  exit 0
fi

# One line: a `::warning::` annotation keeps only its first line, and sccache
# reports the storage error across several.
echo "::warning::Compiler cache is unreachable; building without it. sccache reported: ${output//$'\n'/ }"

if [[ -n "${GITHUB_ENV:-}" ]]; then
  {
    echo "RUSTC_WRAPPER="
    echo "SCCACHE_SETUP=false"
  } >>"$GITHUB_ENV"
fi

exit 0
