#!/usr/bin/env bash
# Read-only profiler for the in-cluster CH-benCH MySQL pod, run from a
# workstation while a benchmark run is in flight (requires kubectl access to
# the cluster, e.g. via `az connectedk8s proxy`).
#
# Samples, once per interval, in a single `kubectl exec` round trip:
#   - mysqld CPU: /proc/<pid>/stat utime+stime delta, reported as cores used
#     and as % of the pod's cpuset (mysqld is pinned; see MYSQLD_CPUSET)
#   - SHOW GLOBAL STATUS deltas: commits, rows written, binlog cache use and
#     disk spills, buffer-pool wait-free stalls, redo log waits, row lock
#     waits/time
#   - SHOW ENGINE INNODB STATUS: purge history list length and checkpoint age
#
# The end-of-run summary maps each signal to the server-tuning hypothesis it
# supports (CPU allocation vs io_capacity/flushing vs purge vs binlog cache).
#
# Usage:
#   profile-mysql-pod.sh [duration_s] [interval_s] [out.csv]
# Env overrides: KUBE_CONTEXT (ts-dev), NAMESPACE (dataplatform), POD (chbench-mysql-0)

set -euo pipefail

CTX="${KUBE_CONTEXT:-ts-dev}"
NS="${NAMESPACE:-dataplatform}"
POD="${POD:-chbench-mysql-0}"
DURATION="${1:-900}"
INTERVAL="${2:-10}"
OUT="${3:-mysql_profile_$(date +%Y%m%d_%H%M%S).csv}"

# Everything the sample needs, gathered in one in-pod bash invocation and
# emitted as KEY=VALUE lines. Runs as root in the container; the password
# comes from the container's own environment, not this script.
IN_POD='
set -e
# The mysql image ships no pgrep/pidof; find mysqld by scanning /proc.
PID=""
for d in /proc/[0-9]*; do
  if [ -r "$d/comm" ] && [ "$(cat "$d/comm" 2>/dev/null)" = "mysqld" ]; then
    PID=${d##*/}; break
  fi
done
[ -n "$PID" ] || { echo "err=mysqld_not_found"; exit 1; }
s=$(cat /proc/$PID/stat)
rest=${s##*) }
arr=($rest)
echo "cpu_ticks=$(( ${arr[11]} + ${arr[12]} ))"
echo "clk_tck=$(getconf CLK_TCK)"
allowed=$(grep Cpus_allowed_list /proc/$PID/status | cut -f2)
echo "cpus_allowed=$allowed"
n=0
IFS=, read -ra ranges <<< "$allowed"
for r in "${ranges[@]}"; do
  if [[ "$r" == *-* ]]; then n=$(( n + ${r#*-} - ${r%-*} + 1 )); else n=$(( n + 1 )); fi
done
echo "cpus_allowed_count=$n"
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "SHOW GLOBAL STATUS WHERE Variable_name IN (
  '\''Com_commit'\'','\''Threads_running'\'',
  '\''Binlog_cache_use'\'','\''Binlog_cache_disk_use'\'',
  '\''Innodb_buffer_pool_wait_free'\'','\''Innodb_log_waits'\'',
  '\''Innodb_row_lock_waits'\'','\''Innodb_row_lock_time'\'',
  '\''Innodb_rows_inserted'\'','\''Innodb_rows_updated'\'','\''Innodb_rows_deleted'\'',
  '\''Innodb_buffer_pool_pages_dirty'\'','\''Innodb_buffer_pool_pages_total'\'')" 2>/dev/null \
  | awk "{print tolower(\$1) \"=\" \$2}"
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null | awk "
  /History list length/ {print \"history_list=\" \$4}
  /^Log sequence number/ {print \"lsn=\" \$4}
  /^Last checkpoint at/ {print \"checkpoint=\" \$4}"
'

sample() {
  kubectl --context "$CTX" -n "$NS" exec "$POD" -- bash -c "$IN_POD" 2>/dev/null
}

get() { # get <key> from $1 (the sample text)
  printf '%s\n' "$1" | awk -F= -v k="$2" '$1==k {print $2; exit}'
}

echo "profiling $POD (context $CTX) every ${INTERVAL}s for ${DURATION}s -> $OUT"
echo "ts,cpu_cores,cpu_pct_of_cpuset,threads_running,commits_per_s,rows_written_per_s,binlog_cache_use_d,binlog_cache_disk_use_d,bp_wait_free_d,log_waits_d,row_lock_waits_d,row_lock_ms_d,history_list,checkpoint_age_gb,dirty_pages_pct" > "$OUT"

prev="" prev_t=0
start=$(date +%s)
max_cpu=0; max_ckpt=0; max_hist=0; spills=0; waitfree=0; logwaits=0
while :; do
  now=$(date +%s)
  (( now - start >= DURATION )) && break
  if ! cur=$(sample) || [ -z "$cur" ]; then
    echo "$(date +%H:%M:%S) sample failed (pod restarting?) — retrying next tick" >&2
    sleep "$INTERVAL"; continue
  fi
  t=$(date +%s)
  if [ -n "$prev" ]; then
    dt=$(( t - prev_t )); (( dt <= 0 )) && dt=1
    clk=$(get "$cur" clk_tck); ncpu=$(get "$cur" cpus_allowed_count)
    d_ticks=$(( $(get "$cur" cpu_ticks) - $(get "$prev" cpu_ticks) ))
    d_commit=$(( $(get "$cur" com_commit) - $(get "$prev" com_commit) ))
    d_rows=$(( $(get "$cur" innodb_rows_inserted) + $(get "$cur" innodb_rows_updated) + $(get "$cur" innodb_rows_deleted) \
             - $(get "$prev" innodb_rows_inserted) - $(get "$prev" innodb_rows_updated) - $(get "$prev" innodb_rows_deleted) ))
    d_bcu=$(( $(get "$cur" binlog_cache_use) - $(get "$prev" binlog_cache_use) ))
    d_bcd=$(( $(get "$cur" binlog_cache_disk_use) - $(get "$prev" binlog_cache_disk_use) ))
    d_bpw=$(( $(get "$cur" innodb_buffer_pool_wait_free) - $(get "$prev" innodb_buffer_pool_wait_free) ))
    d_lw=$(( $(get "$cur" innodb_log_waits) - $(get "$prev" innodb_log_waits) ))
    d_rlw=$(( $(get "$cur" innodb_row_lock_waits) - $(get "$prev" innodb_row_lock_waits) ))
    d_rlt=$(( $(get "$cur" innodb_row_lock_time) - $(get "$prev" innodb_row_lock_time) ))
    hist=$(get "$cur" history_list)
    ckpt_age_gb=$(awk -v l="$(get "$cur" lsn)" -v c="$(get "$cur" checkpoint)" 'BEGIN{printf "%.2f", (l-c)/1073741824}')
    dirty_pct=$(awk -v d="$(get "$cur" innodb_buffer_pool_pages_dirty)" -v t="$(get "$cur" innodb_buffer_pool_pages_total)" 'BEGIN{printf "%.1f", 100*d/t}')
    cores=$(awk -v dt="$d_ticks" -v clk="$clk" -v s="$dt" 'BEGIN{printf "%.1f", dt/clk/s}')
    cpu_pct=$(awk -v c="$cores" -v n="$ncpu" 'BEGIN{printf "%.0f", 100*c/n}')
    line="$(date +%H:%M:%S),$cores,$cpu_pct,$(get "$cur" threads_running),$(( d_commit / dt )),$(( d_rows / dt )),$d_bcu,$d_bcd,$d_bpw,$d_lw,$d_rlw,$d_rlt,$hist,$ckpt_age_gb,$dirty_pct"
    echo "$line" | tee -a "$OUT"
    awk -v c="$cpu_pct" -v m="$max_cpu" 'BEGIN{exit !(c>m)}' && max_cpu=$cpu_pct
    awk -v c="$ckpt_age_gb" -v m="$max_ckpt" 'BEGIN{exit !(c>m)}' && max_ckpt=$ckpt_age_gb
    (( hist > max_hist )) && max_hist=$hist
    spills=$(( spills + d_bcd )); waitfree=$(( waitfree + d_bpw )); logwaits=$(( logwaits + d_lw ))
  fi
  prev="$cur"; prev_t=$t
  sleep "$INTERVAL"
done

echo ""
echo "=== summary ($OUT) ==="
echo "peak mysqld CPU: ${max_cpu}% of its cpuset  -> ~100% supports the CPU-allocation idea; well below 100% points at locks/flushing"
echo "binlog cache disk spills: ${spills}          -> >0 sustained supports raising binlog_cache_size"
echo "buffer-pool wait-free stalls: ${waitfree}    -> >0 supports raising innodb_io_capacity/page_cleaners"
echo "redo log waits: ${logwaits}                  -> >0 supports larger redo / io_capacity"
echo "peak checkpoint age: ${max_ckpt} GB of 16 GB redo -> near ~14 GB means checkpoint pressure (io_capacity)"
echo "peak purge history list: ${max_hist}         -> steady growth across the run supports more purge threads"
