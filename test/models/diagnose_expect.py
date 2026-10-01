#!/usr/bin/env python3
"""Bounded diagnostic of the self-test's actual Expect/PTY/stand-in path."""
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
original = (source / 'expect_test.sh').read_text()
wrapper = '''proc mark {what} {
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
mark enter
source $env(PROBE_SCRIPT)
'''

for command in [['hostname'], ['sw_vers'], ['uname', '-a'], ['expect', '-v'],
                ['bash', '--version'], ['df', '-k', os.environ.get('TMPDIR', '/tmp')],
                ['sysctl', 'vm.loadavg'], ['sysctl', 'kern.num_tasks']]:
    result = subprocess.run(command, capture_output=True, text=True)
    print('$', ' '.join(command), '\n', result.stdout, result.stderr, flush=True)
print('source', os.environ.get('GITHUB_SHA'), 'TMPDIR', os.environ.get('TMPDIR'),
      'BASH_ENV', os.environ.get('BASH_ENV'), 'ENV', os.environ.get('ENV'), flush=True)

results = []
for iteration in range(2):
    with tempfile.TemporaryDirectory(prefix='expect-13761-diagnostic-') as directory:
        directory = pathlib.Path(directory)
        pids = directory / 'pids'
        pids.mkdir()
        trace = out / f'iteration-{iteration}.trace'
        trace.write_text(f'parent-enter {time.time_ns()}\n')
        (directory / 'driver.exp').write_text(wrapper)
        harness = original.replace('script_dir=$(cd -- "$(dirname -- "$0")" && pwd)',
                                   'script_dir=' + str(source))
        harness = harness.replace("#!/usr/bin/env bash\nset -u\nmode=$1", '''#!/usr/bin/env bash
printf 'child-enter epoch=%s pid=%s\\n' "${EPOCHREALTIME:-unknown}" "$$" >>"$PROBE_TRACE"
set -u
mode=$1''', 1)
        harness = harness.replace('turn=0\nprintf \'%s\' "$prompt"', '''turn=0
printf 'child-prompt-before epoch=%s pid=%s\\n' "${EPOCHREALTIME:-unknown}" "$$" >>"$PROBE_TRACE"
printf '%s' "$prompt"
printf 'child-prompt-after epoch=%s pid=%s\\n' "${EPOCHREALTIME:-unknown}" "$$" >>"$PROBE_TRACE"''', 1)
        harness = harness.replace('env "$@" "$script_dir/$script" 2>&1)',
                                  'env "$@" PROBE_SCRIPT="$script_dir/$script" expect -f "$PROBE_DRIVER" 2>&1)')
        script = directory / 'harness.sh'
        script.write_text(harness)
        subprocess.run(['bash', '-n', str(script)], check=True)
        env = os.environ | {'PROBE_TRACE': str(trace), 'PROBE_PIDS': str(pids),
                            'PROBE_DRIVER': str(directory / 'driver.exp')}
        done = threading.Event()
        sampled = set()

        def monitor():
            while not done.wait(0.1):
                for pidfile in pids.iterdir():
                    pid = int(pidfile.name)
                    started = int(pidfile.read_text().strip()) / 1000
                    if pid in sampled or time.time() - started < 0.2:
                        continue
                    if f'child-enter' in trace.read_text() and f'pid={pid}\n' in trace.read_text():
                        continue
                    sampled.add(pid)
                    result = subprocess.run(['ps', '-p', str(pid), '-o', 'pid,ppid,state,wchan,comm'],
                                            capture_output=True, text=True)
                    (out / f'iteration-{iteration}-pid-{pid}.ps').write_text(result.stdout + result.stderr)
                    if result.returncode == 0:
                        subprocess.run(['sample', str(pid), '1', '1', '-file',
                                        str(out / f'iteration-{iteration}-pid-{pid}.sample')],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)

        watcher = threading.Thread(target=monitor, daemon=True)
        start = time.monotonic()
        with (out / f'iteration-{iteration}.log').open('wb') as log:
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
        result = {'iteration': iteration, 'status': status,
                  'elapsed': round(time.monotonic() - start, 3), 'sampled': list(sampled)}
        results.append(result)
        print(json.dumps(result), flush=True)
        print((out / f'iteration-{iteration}.log').read_text(), flush=True)
(out / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
