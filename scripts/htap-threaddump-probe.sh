#!/usr/bin/env bash
# Native all-thread stack-dump probe for a stalled `spiced` process.
#
# When the cold-tier promotion deadlocks, the runtime never reports ready and the
# tokio async watchdog can only see phase/progress counters — not the exact
# parked frame. This probe periodically captures NATIVE all-thread backtraces of
# the spiced process (user-space via eu-stack/gdb, plus a /proc comm+wchan
# fallback that needs no ptrace), so the stuck thread's frame (channel recv,
# spill I/O, decompress, futex, ...) and any thread-pool starvation are visible
# in the run artifacts. Best-effort: every failure is non-fatal.
set -uo pipefail

OUTDIR="${OUTDIR:-/tmp}"
INTERVAL="${THREADDUMP_INTERVAL:-60}"
START_AFTER="${THREADDUMP_START_AFTER:-240}" # skip the healthy load window
mkdir -p "$OUTDIR"

have() { command -v "$1" >/dev/null 2>&1; }

# Best-effort install a user-space unwinder once.
if ! have eu-stack && ! have gdb; then
  (sudo apt-get update -qq && sudo apt-get install -y -qq elfutils gdb) >/dev/null 2>&1 || true
fi

sleep "$START_AFTER"

while true; do
  pid="$(pgrep -x spiced 2>/dev/null | head -1)"
  if [ -n "${pid:-}" ]; then
    ts="$(date -u '+%Y%m%dT%H%M%SZ')"
    out="$OUTDIR/threaddump-${ts}.txt"
    {
      echo "=== spiced pid=${pid} ${ts} ==="
      if have eu-stack; then
        echo "--- eu-stack -p ${pid} ---"
        eu-stack -p "$pid" 2>&1 || echo "(eu-stack failed)"
      elif have gdb; then
        echo "--- gdb thread apply all bt ---"
        gdb -p "$pid" -batch -ex "set pagination off" -ex "thread apply all bt" 2>&1 || echo "(gdb failed)"
      else
        echo "(no eu-stack/gdb available)"
      fi
      echo "--- /proc/${pid}/task comm + wchan (blocked-on symbol; no ptrace needed) ---"
      for t in /proc/"$pid"/task/*; do
        [ -d "$t" ] || continue
        tid="$(basename "$t")"
        printf '%s\t%-20s\twchan=%s\n' \
          "$tid" "$(cat "$t/comm" 2>/dev/null)" "$(cat "$t/wchan" 2>/dev/null)"
      done
    } > "$out" 2>&1
    echo "threaddump -> $out"
  else
    echo "spiced not running yet"
  fi
  sleep "$INTERVAL"
done
