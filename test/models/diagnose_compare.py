#!/usr/bin/env python3
"""Bounded before/after self-test diagnostics with the scripts' system Expect."""
import json
import os
import pathlib
import signal
import subprocess
import tempfile
import threading
import time

source = pathlib.Path(__file__).resolve().parent
out = pathlib.Path(os.environ['EXPECT_DIAGNOSTIC_OUTPUT']).resolve()
out.mkdir(parents=True, exist_ok=True)
prelude = '''proc mark {what} {
    set f [open $::env(PROBE_TRACE) a]
    puts $f "expect-$what [clock milliseconds] expect-pid=[pid]"
    close $f
}
rename spawn diagnostic_spawn
proc spawn {args} {
    mark "before-spawn $args"
    uplevel 1 [list diagnostic_spawn {*}$args]
    set child [exp_pid -i $::spawn_id]
    mark "after-spawn child-pid=$child"
    set f [open [file join $::env(PROBE_PIDS) $child] w]
    puts $f "[clock milliseconds]"
    close $f
}
rename expect diagnostic_expect
proc expect {args} {
    mark before-read
    set result [uplevel 1 [list diagnostic_expect {*}$args]]
    mark after-read
    return $result
}
mark "enter executable=[info nameofexecutable] tcl=[info patchlevel] expect=[package provide Expect]"
'''

for command in [['hostname'], ['sw_vers'], ['uname', '-a'], ['expect', '-v'],
                ['/usr/bin/expect', '-v'], ['bash', '--version'], ['/bin/sh', '--version'],
                ['df', '-k', os.environ.get('TMPDIR', '/tmp')], ['sysctl', 'vm.loadavg']]:
    result = subprocess.run(command, capture_output=True, text=True)
    print('$', ' '.join(command), '\n', result.stdout, result.stderr, flush=True)
print('source', os.environ.get('GITHUB_SHA'), 'TMPDIR', os.environ.get('TMPDIR'),
      'BASH_ENV', os.environ.get('BASH_ENV'), 'ENV', os.environ.get('ENV'), flush=True)
results = []
for variant in os.environ.get('EXPECT_DIAGNOSTIC_VARIANTS', 'before after before after').split():
    iteration = len(results)
    original = (source / ('expect_test.sh' if variant == 'before' else 'expect_test_after.sh')).read_text()
    with tempfile.TemporaryDirectory(prefix='expect-13761-compare-') as directory:
        directory = pathlib.Path(directory)
        pids = directory / 'pids'
        entries = directory / 'entries'
        pids.mkdir()
        entries.mkdir()
        tag = f'iteration-{iteration}-{variant}'
        trace = out / f'{tag}.trace'
        trace.write_text(f'parent-enter {time.time_ns()}\n')
        (directory / 'driver.exp').write_text(prelude + 'source $::env(PROBE_SCRIPT)\n')
        harness = original.replace('script_dir=$(cd -- "$(dirname -- "$0")" && pwd)',
                                   'script_dir=' + str(source))
        harness = harness.replace('set -u\nmode=$1', '''printf '%s' "$$" >"$PROBE_ENTRIES/$$"
printf 'child-enter epoch=%s pid=%s\\n' "${EPOCHREALTIME:-unknown}" "$$" >>"$PROBE_TRACE"
set -u
mode=$1''', 1)
        harness = harness.replace('printf \'%s\' "$prompt"', '''printf '%s' "$$" >"$PROBE_ENTRIES/prompt-before-$$"
printf '%s' "$prompt"
printf '%s' "$$" >"$PROBE_ENTRIES/prompt-after-$$"''', 1)
        if variant == 'before':
            harness = harness.replace('env "$@" "$script_dir/$script" 2>&1)',
                                      'env "$@" PROBE_SCRIPT="$script_dir/$script" /usr/bin/expect -f "$PROBE_DRIVER" 2>&1)')
        else:
            harness = harness.replace("<<'DRIVER'\nrename spawn", "<<'DRIVER'\n" + prelude + 'rename spawn', 1)
        script = directory / 'harness.sh'
        script.write_text(harness)
        subprocess.run(['bash', '-n', str(script)], check=True)
        env = os.environ | {'PROBE_TRACE': str(trace), 'PROBE_PIDS': str(pids),
                            'PROBE_ENTRIES': str(entries), 'PROBE_DRIVER': str(directory / 'driver.exp')}
        done = threading.Event()
        sampled = set()

        def monitor():
            while not done.wait(0.1):
                for pidfile in pids.iterdir():
                    pid = int(pidfile.name)
                    stamp = pidfile.read_text().strip()
                    if not stamp:
                        continue
                    started = int(stamp) / 1000
                    if pid in sampled or time.time() - started < 0.2 or (entries / str(pid)).exists():
                        continue
                    sampled.add(pid)
                    result = subprocess.run(['ps', '-p', str(pid), '-o', 'pid,ppid,state,wchan,comm'],
                                            capture_output=True, text=True)
                    (out / f'{tag}-pid-{pid}.ps').write_text(result.stdout + result.stderr)
                    if result.returncode == 0:
                        try:
                            subprocess.run(['sample', str(pid), '1', '1', '-file',
                                            str(out / f'{tag}-pid-{pid}.sample')],
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
                        except subprocess.TimeoutExpired:
                            print('sample timed out', pid, flush=True)

        watcher = threading.Thread(target=monitor, daemon=True)
        start = time.monotonic()
        with (out / f'{tag}.log').open('wb') as log:
            proc = subprocess.Popen(['bash', str(script)], env=env, stdout=log,
                                    stderr=subprocess.STDOUT, start_new_session=True)
            watcher.start()
            try:
                status = proc.wait(timeout=180)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGTERM)
                status = proc.wait(timeout=10)
            finally:
                done.set()
                watcher.join(timeout=15)
        launches = []
        for pidfile in sorted(pids.iterdir(), key=lambda p: int(p.read_text().strip())):
            pid = pidfile.name
            started_ms = int(pidfile.read_text().strip())
            entry = entries / pid
            launch = {'pid': int(pid), 'spawn_to_entry_ms': round(entry.stat().st_mtime_ns / 1_000_000 - started_ms, 3) if entry.exists() else None}
            for event in ['prompt-before', 'prompt-after']:
                marker = entries / f'{event}-{pid}'
                launch[event + '_ms'] = round(marker.stat().st_mtime_ns / 1_000_000, 3) if marker.exists() else None
            launches.append(launch)
        result = {'iteration': iteration, 'variant': variant, 'status': status,
                  'elapsed': round(time.monotonic() - start, 3), 'sampled': list(sampled), 'launches': launches}
        results.append(result)
        print(json.dumps(result), flush=True)
        print((out / f'{tag}.log').read_text(), flush=True)
(out / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
