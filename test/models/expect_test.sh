#!/usr/bin/env bash
#
# Tests for the expect scripts that drive the Spice REPLs in E2E Test CI, and for
# the helpers in repl_helpers.exp that they share.
#
# A REPL that exits part-way through a script must be reported as a crash — with
# the exit status, or the signal that ended it — rather than as a step that passed
# or as expect's "spawn id expN not open".
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
# through so the crash paths are exercised.
mkdir -p "$work_dir/bin"
cat >"$work_dir/bin/spice" <<'STAND_IN'
#!/usr/bin/env bash
set -u
mode=$1
exit_before=${SPICE_FAKE_EXIT_BEFORE_TURN:-0}
exit_after=${SPICE_FAKE_EXIT_AFTER_TURN:-0}

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
printf '%s' "$prompt"

while IFS= read -r line; do
  turn=$((turn + 1))

  if [ "$exit_before" -ne 0 ] && [ "$turn" -ge "$exit_before" ]; then
    exit 44
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

  printf '%s' "$prompt"

  # Leaving after the prompt is written is how a REPL that dies while sitting
  # idle looks to the script driving it.
  if [ "$exit_after" -ne 0 ] && [ "$turn" -ge "$exit_after" ]; then
    exit 44
  fi
done
STAND_IN
chmod +x "$work_dir/bin/spice"

# script_case <name> <script> [env assignments...] — runs one of the E2E scripts
# against the stand-in `spice`.
script_case() {
  local name=$1
  local script=$2
  shift 2

  printf 'case: %s\n' "$name"
  case_output=$(PATH="$work_dir/bin:$PATH" env "$@" "$script_dir/$script" 2>&1)
  case_status=$?
}

for script in chat_01.exp chat_01_simple.exp search_01.exp; do
  script_case "$script against a healthy REPL" "$script"
  assert_status 0
  assert_silent_about 'no longer running'
  assert_silent_about 'Timeout waiting'
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
