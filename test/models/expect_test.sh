#!/usr/bin/env bash
#
# Tests for the expect scripts that drive the Spice REPLs in E2E Test CI, and for
# the helpers in repl_helpers.exp that they share.
#
# A REPL that exits part-way through a script must be reported as a crash — with
# the exit status, or the signal that ended it — rather than as a step that passed
# or as expect's "spawn id expN not open".
#
# And a response that is still arriving must not be reported as a failure at all:
# the waits are bounded on silence, not on how long a model takes to finish.
#
# Drives stand-in processes instead of the real `spice` binary, so it needs only
# `expect`: no Spice runtime, no model provider, no network.
#
#   ./test/models/expect_test.sh

set -u

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

failures=0
case_output=""
case_status=0

pass() {
  printf '  ok    %s\n' "$1"
}

fail() {
  printf '  FAIL  %s\n' "$1"
  failures=$((failures + 1))
}

assert_status() {
  if [ "$case_status" = "$1" ]; then
    pass "exits $1"
  else
    fail "expected exit $1, got $case_status"
    printf '%s\n' "$case_output" | sed 's/^/        | /'
  fi
}

assert_reports() {
  case $case_output in
  *"$1"*) pass "reports \"$1\"" ;;
  *)
    fail "output does not mention \"$1\""
    printf '%s\n' "$case_output" | sed 's/^/        | /'
    ;;
  esac
}

assert_silent_about() {
  case $case_output in
  *"$1"*)
    fail "output still mentions \"$1\""
    printf '%s\n' "$case_output" | sed 's/^/        | /'
    ;;
  *) pass "does not mention \"$1\"" ;;
  esac
}

# ---------------------------------------------------------------------------
# repl_helpers.exp, driven against stand-in processes
# ---------------------------------------------------------------------------

# helper_case <name> <expect-script-body> — writes the body to a script that
# sources the helpers, runs it, and leaves the result in $case_status/$case_output.
helper_case() {
  local script="$work_dir/helper_case.exp"

  printf 'case: %s\n' "$1"
  {
    printf 'source %s\n' "$script_dir/repl_helpers.exp"
    printf 'log_user 0\n'
    printf '%s\n' "$2"
  } >"$script"

  case_output=$(expect -f "$script" 2>&1)
  case_status=$?
}

# A REPL that exits while sitting idle at its prompt. Writing to the pty of an
# exited process succeeds silently, so without repl_assert_running the script
# would send its next line into the void and finish reporting success.
helper_case 'exits while idle at the prompt' '
spawn sh -c {printf "ready> "; exit 42}
set timeout 5
expect "ready> "
sleep 1
repl_assert_running "Idle check"
send_user "REACHED THE END\n"
exit 0
'
assert_status 1
assert_reports 'exited with status 42'
assert_silent_about 'REACHED THE END'
assert_silent_about 'spawn id'

# A REPL that exits instead of answering. An expect block with no eof branch ends
# without running a body, so the script would carry on and report the answer it
# never received.
helper_case 'exits instead of answering' '
spawn sh -c {printf "ready> "; read -r line; exit 7}
set timeout 5
expect "ready> "
repl_send "how many issues?\r"
expect {
    "answer" {
        send_user "MODEL ANSWERED\n"
    }
    eof {
        repl_died "Waiting for the answer"
    }
    timeout {
        send_user "TIMED OUT\n"
        exit 1
    }
}
exit 0
'
assert_status 1
assert_reports 'Waiting for the answer'
assert_reports 'exited with status 7'
assert_silent_about 'MODEL ANSWERED'

# A REPL killed by a signal — an out-of-memory kill looks like this, and the
# signal is the whole diagnosis.
helper_case 'killed by a signal' '
spawn sh -c {printf "ready> "; read -r line; kill -9 $$}
set timeout 5
expect "ready> "
repl_send "how many issues?\r"
expect {
    "answer" {
        send_user "MODEL ANSWERED\n"
    }
    eof {
        repl_died "Waiting for the answer"
    }
    timeout {
        send_user "TIMED OUT\n"
        exit 1
    }
}
exit 0
'
assert_status 1
assert_reports 'was terminated abnormally'
assert_reports 'SIGKILL'

# A REPL that has already gone away when the script sends to it. expect reports
# this as "spawn id expN not open", which names the script line rather than the
# process that left.
helper_case 'has gone away by the time the script sends' '
spawn sh -c {printf "ready> "; exit 3}
set timeout 5
expect "ready> "
expect {
    eof {}
    timeout {}
}
repl_send "how many issues?\r"
send_user "REACHED THE END\n"
exit 0
'
assert_status 1
assert_reports 'Sending "how many issues?"'
assert_reports 'exited with status 3'
assert_silent_about 'REACHED THE END'

# SPICE_REPL_IDLE_TIMEOUT overrides the built-in bound when it is a positive
# integer, and is rejected when it is not.
helper_case 'SPICE_REPL_IDLE_TIMEOUT overrides the default' '
set ::env(SPICE_REPL_IDLE_TIMEOUT) 7
set t [repl_idle_timeout]
if {$t != 7} { send_user "got $t\n"; exit 1 }
send_user "OVERRIDE_OK\n"
exit 0
'
assert_status 0
assert_reports 'OVERRIDE_OK'

helper_case 'absent SPICE_REPL_IDLE_TIMEOUT keeps the default' '
unset -nocomplain ::env(SPICE_REPL_IDLE_TIMEOUT)
set t [repl_idle_timeout]
if {$t != 90} { send_user "got $t\n"; exit 1 }
send_user "DEFAULT_OK\n"
exit 0
'
assert_status 0
assert_reports 'DEFAULT_OK'

helper_case 'rejects a non-integer SPICE_REPL_IDLE_TIMEOUT' '
set ::env(SPICE_REPL_IDLE_TIMEOUT) not-a-number
repl_idle_timeout
send_user "REACHED THE END\n"
exit 0
'
assert_status 1
assert_reports 'SPICE_REPL_IDLE_TIMEOUT must be a positive whole number of seconds'
assert_silent_about 'REACHED THE END'

# A healthy exchange still runs to completion: the helpers must not turn a
# working interaction into a failure.
helper_case 'healthy exchange' '
spawn sh -c {printf "ready> "; while read -r line; do printf "answer\r\nready> "; done}
set timeout 5
expect "ready> "
repl_send "how many issues?\r"
expect {
    "answer" {}
    eof {
        repl_died "Waiting for the answer"
    }
    timeout {
        send_user "TIMED OUT\n"
        exit 1
    }
}
expect {
    "ready> " {}
    eof {
        repl_died "Waiting for the next prompt"
    }
    timeout {
        send_user "TIMED OUT\n"
        exit 1
    }
}
repl_assert_running "Idle check"
repl_send \x03
send_user "REACHED THE END\n"
exit 0
'
assert_status 0
assert_reports 'REACHED THE END'
assert_silent_about 'no longer running'

# ---------------------------------------------------------------------------
# The E2E scripts themselves, driven against a stand-in `spice`
# ---------------------------------------------------------------------------

# A stand-in for the `spice` CLI that emulates just enough of the `chat` and
# `search` REPLs for the scripts to run, and that can be told to exit part-way
# through so the crash paths are exercised. It is input to the installed shell,
# not an executable: first exec of a freshly written script can stall before
# its interpreter starts on macOS (#13761).
cat >"$work_dir/spice.sh" <<'STAND_IN'
set -u
mode=$1
exit_before=${SPICE_FAKE_EXIT_BEFORE_TURN:-0}
exit_after=${SPICE_FAKE_EXIT_AFTER_TURN:-0}
# Silence: how long the stand-in waits before answering a turn, or before its
# first prompt. A model that has stalled and a search that is still running
# both look like this to the script.
sleep_secs=${SPICE_FAKE_SLEEP_SECONDS:-0}
initial_sleep_secs=${SPICE_FAKE_INITIAL_SLEEP_SECONDS:-0}
# A response that arrives in pieces, the way a streaming model answers: a few
# filler lines spaced apart before the line the script is looking for.
stream_lines=${SPICE_FAKE_STREAM_LINES:-0}
stream_delay=${SPICE_FAKE_STREAM_DELAY_SECONDS:-0}
# Goes quiet after answering, without exiting — a runtime that has stopped
# producing output but is still alive, which no eof branch can catch.
stall_after=${SPICE_FAKE_STALL_AFTER_TURN:-0}
# Writes the prompt in two pieces so it lands across two reads. A wait that
# consumes the stream with a greedy catch-all eats the first piece and then
# waits forever for a prompt that has already gone by.
split_prompt=${SPICE_FAKE_SPLIT_PROMPT:-0}

write_prompt() {
  if [ "$split_prompt" -ne 0 ]; then
    printf '%s' "${prompt%"> "}"
    sleep 0.3
    printf '> '
  else
    printf '%s' "$prompt"
  fi
}

# Records how the script invoked us, so a test can check that the runtime
# endpoint was passed through rather than left at the CLI default.
if [ -n "${SPICE_FAKE_ARGV_FILE:-}" ]; then
  printf '%s\n' "$*" >"$SPICE_FAKE_ARGV_FILE"
fi

prompt='chat> '
if [ "$mode" = 'search' ]; then
  prompt='search> '
fi

turn=0
if [ "$initial_sleep_secs" -gt 0 ]; then
  sleep "$initial_sleep_secs"
fi
write_prompt

while IFS= read -r line; do
  turn=$((turn + 1))

  if [ "$exit_before" -ne 0 ] && [ "$turn" -ge "$exit_before" ]; then
    exit 44
  fi

  if [ "$sleep_secs" -gt 0 ]; then
    sleep "$sleep_secs"
  fi

  emitted=0
  while [ "$emitted" -lt "$stream_lines" ]; do
    sleep "$stream_delay"
    printf 'still thinking about it, line %s\r\n' "$((emitted + 1))"
    emitted=$((emitted + 1))
  done

  if [ "$stall_after" -ne 0 ] && [ "$turn" -ge "$stall_after" ]; then
    # No answer, no prompt, no exit: the script has to notice the silence. The
    # sleep only has to outlast the bound under test, and is kept short so a
    # stand-in that outlives its pty does not sit on the runner.
    sleep 30
    exit 45
  fi

  if [ "$mode" = 'search' ]; then
    case $line in
    *error*) printf '1  a1b2  a spice runtime error occurred  0.75  spice.public.issues\r\n' ;;
    *) printf '1  c3d4  deals for friends of spice  0.66  spice.public.catalog_page\r\n' ;;
    esac
  else
    case $line in
    *datasets*) printf 'taxi_trips, github_issues and catalog_page\r\n' ;;
    *) printf -- '- 42 of them\r\n' ;;
    esac
  fi

  write_prompt

  # Leaving after the prompt is written is how a REPL that dies while sitting
  # idle looks to the script driving it.
  if [ "$exit_after" -ne 0 ] && [ "$turn" -ge "$exit_after" ]; then
    exit 44
  fi
done
STAND_IN

# Source the real E2E script and adapt only its spawn command. Keep spawn_id in
# the caller's scope so all prompt, crash and timeout checks use the real pty.
cat >"$work_dir/script_case.exp" <<'DRIVER'
rename spawn stand_in_spawn
proc spawn {command args} {
    if {$command ne "spice"} {
        error "Expected the E2E script to spawn spice, got $command"
    }
    uplevel 1 [list stand_in_spawn /bin/sh $::env(SPICE_FAKE_SCRIPT) {*}$args]
}
source $::env(SPICE_EXPECT_SCRIPT)
DRIVER

# script_case <name> <script> [env assignments...] — runs one of the E2E scripts
# against the stand-in `spice`.
script_case() {
  local name=$1
  local script=$2
  shift 2

  printf 'case: %s\n' "$name"
  case_output=$(env "$@" \
    SPICE_FAKE_SCRIPT="$work_dir/spice.sh" \
    SPICE_EXPECT_SCRIPT="$script_dir/$script" \
    /usr/bin/expect -f "$work_dir/script_case.exp" 2>&1)
  case_status=$?
}

for script in chat_01.exp chat_01_simple.exp search_01.exp; do
  script_case "$script against a healthy REPL" "$script"
  assert_status 0
  assert_silent_about 'no longer running'
  assert_silent_about 'no output from the REPL'
done

script_case 'chat_01.exp when the REPL exits before answering' chat_01.exp \
  SPICE_FAKE_EXIT_BEFORE_TURN=2
assert_status 1
assert_reports 'Waiting for the issue count'
assert_reports 'exited with status 44'
assert_silent_about 'spawn id'

script_case 'chat_01_simple.exp when the REPL exits before answering' chat_01_simple.exp \
  SPICE_FAKE_EXIT_BEFORE_TURN=1
assert_status 1
assert_reports 'Waiting for the response to'
assert_reports 'exited with status 44'
assert_silent_about 'Model returned expected response'

script_case 'search_01.exp when the REPL exits before answering' search_01.exp \
  SPICE_FAKE_EXIT_BEFORE_TURN=1
assert_status 1
assert_reports 'Searching for "Spice runtime error"'
assert_reports 'exited with status 44'
assert_silent_about 'Search returned expected result'

script_case 'chat_01.exp when the REPL exits while idle' chat_01.exp \
  SPICE_FAKE_EXIT_AFTER_TURN=3
assert_status 1
assert_reports 'Checking the chat REPL is still running'
assert_reports 'exited with status 44'

# ---------------------------------------------------------------------------
# What the wait on a REPL response is bounded by
# ---------------------------------------------------------------------------
#
# A bound on a whole response is a bound on answer length and runner speed: a
# model that is still streaming, or a search that is still running, fails it and
# ejects unrelated PRs from the merge queue (#13711). The bound is on silence: a
# response that keeps arriving is allowed to take as long as it takes, and only
# a runtime that has stopped emitting trips it.
#
# Each case sets SPICE_REPL_IDLE_TIMEOUT so the stall path can be reached in
# seconds. The streaming cases take longer in total than that bound while never
# pausing for as long as it: the shape a bound on total response time fails and
# a bound on silence passes. The stand-in's longest pause is half the bound, so
# a scheduling hiccup on a contended runner does not read as a stall.

for script in chat_01.exp chat_01_simple.exp; do
  script_case "$script when the response streams for longer than the bound" "$script" \
    SPICE_REPL_IDLE_TIMEOUT=4 \
    SPICE_FAKE_STREAM_LINES=3 \
    SPICE_FAKE_STREAM_DELAY_SECONDS=2
  assert_status 0
  assert_silent_about 'no output from the REPL'

  script_case "$script when the prompt arrives split across two reads" "$script" \
    SPICE_REPL_IDLE_TIMEOUT=5 \
    SPICE_FAKE_SPLIT_PROMPT=1
  assert_status 0
  assert_silent_about 'no output from the REPL'
done

# A search answers in one piece once the embedding model has run, so "slow" and
# "streaming" are the same thing to the script: nothing arrives until the
# result. A fixed 5s budget fails a search that is still running, as job
# 112954591167 shows.
script_case 'search_01.exp when the search takes 6s to answer' search_01.exp \
  SPICE_REPL_IDLE_TIMEOUT=10 \
  SPICE_FAKE_SLEEP_SECONDS=6
assert_status 0
assert_reports 'Search returned expected result'
assert_silent_about 'no output from the REPL'

script_case 'search_01.exp when the search produces nothing for longer than the bound' search_01.exp \
  SPICE_REPL_IDLE_TIMEOUT=2 \
  SPICE_FAKE_SLEEP_SECONDS=4
assert_status 1
assert_reports 'no output from the REPL for 2s'
assert_reports 'Searching for "Spice runtime error"'
assert_silent_about 'Search returned expected result'

# A stand-in that starts but never reaches its prompt must still time out.
script_case 'chat_01_simple.exp when nothing arrives before the initial prompt' chat_01_simple.exp \
  SPICE_REPL_IDLE_TIMEOUT=1 \
  SPICE_FAKE_INITIAL_SLEEP_SECONDS=3
assert_status 1
assert_reports 'no output from the REPL for 1s'
assert_reports 'Waiting for the initial chat prompt'
assert_silent_about 'Model returned expected response'

# A model that pauses for longer than the bound before answering is a stall,
# however short the answer that would have followed. The stand-in sleeps 3s;
# a 1s bound is enough to trip.
script_case 'chat_01_simple.exp when the model pauses for longer than the bound' chat_01_simple.exp \
  SPICE_REPL_IDLE_TIMEOUT=1 \
  SPICE_FAKE_SLEEP_SECONDS=3
assert_status 1
assert_reports 'no output from the REPL for 1s'
assert_reports 'Waiting for the response to'
assert_silent_about 'Model returned expected response'

# Output that then stops is still a stall: the per-line reset must not make the
# bound unreachable once the answer has started arriving.
script_case 'chat_01_simple.exp when the REPL streams part of an answer and then goes silent' chat_01_simple.exp \
  SPICE_REPL_IDLE_TIMEOUT=3 \
  SPICE_FAKE_STREAM_LINES=2 \
  SPICE_FAKE_STALL_AFTER_TURN=1
assert_status 1
assert_reports 'no output from the REPL for 3s'
assert_reports 'Waiting for the response to'
assert_silent_about 'Model returned expected response'

# chat_01.exp reads the same bound: a pause longer than it is reported from the
# turn that was waiting.
script_case 'chat_01.exp when the model pauses for longer than the bound' chat_01.exp \
  SPICE_REPL_IDLE_TIMEOUT=1 \
  SPICE_FAKE_SLEEP_SECONDS=3
assert_status 1
assert_reports 'no output from the REPL for 1s'
assert_reports 'Waiting for the list of datasets'
assert_silent_about 'Model confirmed access to all datasets'

script_case 'chat_01_simple.exp when the idle bound is not a positive number' chat_01_simple.exp \
  SPICE_REPL_IDLE_TIMEOUT=soon
assert_status 1
assert_reports 'SPICE_REPL_IDLE_TIMEOUT must be a positive whole number of seconds'

# An unset override interpolates to an empty string, which is what a caller that
# does not set the bound looks like from inside the script. That has to mean the
# built-in default, not a rejected value.
script_case 'chat_01_simple.exp when the idle bound is left empty' chat_01_simple.exp \
  SPICE_REPL_IDLE_TIMEOUT=''
assert_status 0
assert_silent_about 'must be a positive whole number'

# ---------------------------------------------------------------------------
# The runtime endpoint the REPL is pointed at
# ---------------------------------------------------------------------------
#
# In CI each job binds its runtime to its own port (#12419), so a REPL left on
# the CLI default would talk to whatever else holds 8090 on a shared host — or
# to nothing. These check the scripts forward the endpoint when it is set, and
# leave the default alone when it is not.

argv_file="$work_dir/spice_argv"

assert_argv() {
  local expected=$1 actual
  actual=$(cat "$argv_file" 2>/dev/null)
  if [ "$actual" = "$expected" ]; then
    pass "invokes spice as \"$expected\""
  else
    fail "expected spice args \"$expected\", got \"$actual\""
  fi
}

for script in chat_01.exp chat_01_simple.exp search_01.exp; do
  subcommand=chat
  case $script in
  search_*) subcommand=search ;;
  esac

  : >"$argv_file"
  script_case "$script forwards SPICE_HTTP_ENDPOINT" "$script" \
    SPICE_FAKE_ARGV_FILE="$argv_file" \
    SPICE_HTTP_ENDPOINT='http://127.0.0.1:21734'
  assert_status 0
  assert_argv "$subcommand --endpoint http://127.0.0.1:21734"

  : >"$argv_file"
  script_case "$script keeps the CLI default when SPICE_HTTP_ENDPOINT is unset" "$script" \
    SPICE_FAKE_ARGV_FILE="$argv_file" \
    SPICE_HTTP_ENDPOINT=''
  assert_status 0
  assert_argv "$subcommand"
done

if [ "$failures" -ne 0 ]; then
  printf '\n%s check(s) failed\n' "$failures"
  exit 1
fi

printf '\nAll checks passed\n'
