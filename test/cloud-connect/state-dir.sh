#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The Cloud Connect state-directory rule (spiceai/spiceai#16474), checked
# against a real `spiced` in the layouts that need root to set up: directories
# owned by other accounts, and `spiced` running as another uid.
#
# Usage: sudo bash test/cloud-connect/state-dir.sh <path to a Linux spiced>
#
# Every expected outcome comes from the enhancement's specification, not from a
# run of the code under test: the exact ERROR and INFO lines, whether `spiced`
# stops, and the owner and mode of `cloud-connect/`. Exits 0 when every case
# matches, 1 when any does not, 2 when it cannot run. See README.md.

set -uo pipefail

DOCS='https://spiceai.org/docs/cli/reference/cloud'
# A deployment's spicepod is served from this name; `spiced` names it when a
# local spicepod.yaml is also present, which is how a case sees the deployment
# was read.
SERVES='this instance serves the deployed spicepod at'
LISTENING='Spice Runtime HTTP listening on'
ENROLL_ATTEMPTED='Failed to reach the Spice Cloud endpoint'
# A start that got past the state directory, whichever line comes first.
STARTED="$SERVES|$LISTENING"
# How long one `spiced` start may take to reach the line a case waits for.
START_TIMEOUT_SECONDS=${START_TIMEOUT_SECONDS:-120}

LOCAL_SPICEPOD=$'version: v1\nkind: Spicepod\nname: local-app\n'
DEPLOYED_SPICEPOD=$'version: v1\nkind: Spicepod\nname: deployed-app\n'
# A malformed identity is enough to make the instance count as enrolled, which
# is what puts it under the rule; nothing here reaches a control plane.
IDENTITY='not valid identity JSON'
ENROLLMENT_KEY="spice-enroll-$(printf 'A%.0s' {1..32})"

die() {
  echo "error: $*" >&2
  exit 2
}

[[ $# -eq 1 ]] || die "usage: $0 <path to a Linux spiced binary>"
[[ "$(uname -s)" == Linux ]] || die "this test runs on Linux"
[[ "$(id -u)" -eq 0 ]] || die "run it as root: the cases set up directories owned by other accounts and run spiced as other uids"
for tool in setpriv cmp stat; do
  command -v "$tool" >/dev/null || die "$tool is required"
done
# A downloaded artifact may have lost its executable bit; the copy below sets it.
[[ -f "$1" ]] || die "'$1' is not a spiced binary"

ROOT=$(mktemp -d) || die "cannot create a work directory"
# Every case's uid has to reach the binary and the instance directory.
chmod 0755 "$ROOT"
cp "$1" "$ROOT/spiced" || die "cannot copy '$1'"
chmod 0755 "$ROOT/spiced"
mkdir -m 0777 "$ROOT/home"
APP="$ROOT/app"
CONFIG="$APP/.spice"
STATE="$CONFIG/cloud-connect"

SPICED_PID=
cleanup() {
  if [[ -n "$SPICED_PID" ]]; then
    kill -KILL "$SPICED_PID" 2>/dev/null
    wait "$SPICED_PID" 2>/dev/null
  fi
  rm -rf "$ROOT"
}
trap cleanup EXIT

# The two ERRORs and the INFO line, as the specification writes them.
chmod_error() { # <dir> <owner uid>
  printf "ERROR spiced: Failed to start: the Spice Cloud Connect state directory '%s' is owned by user id %s and writable by other accounts, so its deployment and identity files could be replaced. Run \`chmod go-w '%s'\` as its owner, or set SPICE_CONFIG_DIR to a directory only this account can write. See: %s" \
    "$1" "$2" "$1" "$DOCS"
}
chown_error() { # <dir> <owner uid> <this uid>
  printf "ERROR spiced: Failed to start: the Spice Cloud Connect state directory '%s' is owned by user id %s, not by this account (user id %s), so that account could replace its deployment and identity files. Run \`chown -R %s '%s'\` as root, or set SPICE_CONFIG_DIR to a directory this account owns. See: %s" \
    "$1" "$2" "$3" "$3" "$1" "$DOCS"
}
repair_info() { # <dir> <from mode> <to mode>
  printf "INFO spiced::cloud_connect: Spice Cloud Connect removed group and other write access from its state directory '%s' (mode %s → %s), so that only this account can add files there. See: %s" \
    "$1" "$2" "$3" "$DOCS"
}

# A fresh instance directory with a local spicepod, and nothing else.
fresh_instance() {
  rm -rf "$APP"
  mkdir -m 0755 "$APP"
  printf '%s' "$LOCAL_SPICEPOD" >"$APP/spicepod.yaml"
  chmod 0644 "$APP/spicepod.yaml"
}

# make_dir <path> <uid> <mode>
make_dir() {
  mkdir -p "$1" && chown "$2:$2" "$1" && chmod "$3" "$1"
}

# enrolled_files <dir> <uid>: an identity and a deployment, written by <uid>.
enrolled_files() {
  printf '%s' "$IDENTITY" >"$1/identity.json"
  printf '%s' "$DEPLOYED_SPICEPOD" >"$1/spicepod-cloud-managed.yml"
  chown "$2:$2" "$1/identity.json" "$1/spicepod-cloud-managed.yml"
  chmod 0600 "$1/identity.json" "$1/spicepod-cloud-managed.yml"
}

LOG=
STATUS=
# run_spiced <uid> <wait-for pattern> [spiced args...]: start `spiced` in the
# instance directory as <uid>, and stop it once it prints a line matching the
# extended regular expression <wait-for pattern>.
# STATUS is `exited <code>` or `stopped`; LOG is everything it printed.
run_spiced() {
  local uid=$1 until=$2
  shift 2
  LOG="$ROOT/spiced.log"
  : >"$LOG"
  (
    cd "$APP" || exit 125
    exec setpriv --reuid="$uid" --regid="$uid" --clear-groups \
      env -i PATH=/usr/bin:/bin HOME="$ROOT/home" \
      SPICE_CLOUD_ENDPOINT=https://127.0.0.1:9 \
      "$ROOT/spiced" --http 127.0.0.1:0 --flight 127.0.0.1:0 "$@"
  ) >>"$LOG" 2>&1 &
  SPICED_PID=$!
  local deadline=$((SECONDS + START_TIMEOUT_SECONDS))
  while :; do
    if ! kill -0 "$SPICED_PID" 2>/dev/null; then
      wait "$SPICED_PID"
      STATUS="exited $?"
      break
    fi
    if grep -qE -- "$until" "$LOG"; then
      STATUS=stopped
      break
    fi
    if ((SECONDS >= deadline)); then
      STATUS="timed out waiting for: $until"
      break
    fi
    sleep 0.2
  done
  if [[ "$STATUS" != exited* ]]; then
    kill -TERM "$SPICED_PID" 2>/dev/null
    local grace=$((SECONDS + 10))
    while kill -0 "$SPICED_PID" 2>/dev/null && ((SECONDS < grace)); do sleep 0.2; done
    kill -KILL "$SPICED_PID" 2>/dev/null
    wait "$SPICED_PID" 2>/dev/null
  fi
  SPICED_PID=
}

CASE=
CASE_FAILURES=0
FAILED_CASES=()
begin() {
  CASE=$1
  CASE_FAILURES=0
}
fail() {
  echo "    FAIL: $*"
  CASE_FAILURES=$((CASE_FAILURES + 1))
}
finish() {
  if ((CASE_FAILURES == 0)); then
    echo "ok   $CASE"
  else
    echo "FAIL $CASE"
    echo "    spiced: $STATUS; its output:"
    sed 's/^/    | /' "$LOG" | tail -n 25
    FAILED_CASES+=("$CASE")
  fi
}

expect_refused() { # spiced stopped itself, non-zero
  [[ "$STATUS" == exited* && "$STATUS" != "exited 0" ]] ||
    fail "spiced must exit non-zero, but it was $STATUS"
}
expect_running() { # spiced reached the line the case waited for
  [[ "$STATUS" == stopped ]] || fail "spiced must keep running, but it $STATUS"
}
expect_enrolling() { # the `--token` bootstrap ran, and nothing listens before it ends
  expect_running
  expect_line "$ENROLL_ATTEMPTED"
}
expect_line() { # <exact text> [count]
  local found
  found=$(grep -cF -- "$1" "$LOG")
  if [[ -n "${2:-}" ]]; then
    [[ "$found" -eq "$2" ]] || fail "expected $2 line(s), found $found: $1"
  else
    [[ "$found" -ge 1 ]] || fail "expected the line: $1"
  fi
}
expect_no_line() { # <text>
  ! grep -qF -- "$1" "$LOG" || fail "unexpected line containing: $1"
}
expect_owner_mode() { # <path> <uid> <octal mode>
  local actual
  actual=$(stat -c '%u %a' "$1" 2>/dev/null) || {
    fail "$1 does not exist"
    return
  }
  [[ "$actual" == "$2 $3" ]] || fail "$1 is owner $(cut -d' ' -f1 <<<"$actual") mode $(cut -d' ' -f2 <<<"$actual"); expected owner $2 mode $3"
}
expect_absent() { # <path>
  [[ ! -e "$1" && ! -L "$1" ]] || fail "$1 must not exist"
}
expect_file() { # <path> <contents>
  # Byte for byte: `$(cat …)` would drop a trailing newline.
  [[ -f "$1" ]] && cmp -s "$1" <(printf '%s' "$2") ||
    fail "$1 must hold what was written before the start"
}

echo "spiced under test: $("$ROOT/spiced" --version 2>&1 | head -n 1)"

# Spec, rule table: owned by another account, and this `spiced` is not root.
# The ownership ERROR, and nothing in the directory is read.
begin "uid 1000 refuses a state directory owned by uid 0 (mode 0777)"
fresh_instance
make_dir "$CONFIG" 0 0777
make_dir "$STATE" 0 0777
enrolled_files "$STATE" 0
run_spiced 1000 "$STARTED"
expect_refused
expect_line "$(chown_error "$STATE" 0 1000)" 1
expect_no_line "$SERVES"
finish

# Spec, container layouts: a root-owned 0777 emptyDir at `.spice`. State an
# earlier version kept there moves into a `cloud-connect/` this account creates
# owner-only, and the instance starts on it. The config directory is untouched.
begin "uid 65534 in a root-owned 0777 emptyDir moves earlier state into cloud-connect/ (0700) and starts"
fresh_instance
make_dir "$CONFIG" 0 0777
enrolled_files "$CONFIG" 65534
run_spiced 65534 "$STARTED"
expect_running
expect_line "$SERVES '$STATE/spicepod-cloud-managed.yml'"
expect_owner_mode "$STATE" 65534 700
expect_owner_mode "$CONFIG" 0 777
expect_file "$STATE/identity.json" "$IDENTITY"
expect_file "$STATE/spicepod-cloud-managed.yml" "$DEPLOYED_SPICEPOD"
expect_absent "$CONFIG/identity.json"
expect_absent "$CONFIG/spicepod-cloud-managed.yml"
expect_no_line "Failed to start"
finish

# Spec: the rule applies to an instance started with `--token`. The state
# directory exists, owner-only, before the enrollment is attempted.
begin "uid 65534 in a root-owned 0777 emptyDir with --token creates cloud-connect/ (0700) and enrolls"
fresh_instance
make_dir "$CONFIG" 0 0777
run_spiced 65534 "$ENROLL_ATTEMPTED|$LISTENING" --token "$ENROLLMENT_KEY"
expect_enrolling
expect_owner_mode "$STATE" 65534 700
expect_no_line "Failed to start"
finish

# Spec, rule table: root over another account's directory that others can
# write. The `chmod go-w` ERROR names that owner.
begin "root refuses a state directory owned by uid 1000 that others can write (mode 0777)"
fresh_instance
make_dir "$CONFIG" 1000 0755
make_dir "$STATE" 1000 0777
enrolled_files "$STATE" 1000
run_spiced 0 "$STARTED"
expect_refused
expect_line "$(chmod_error "$STATE" 1000)" 1
expect_no_line "$SERVES"
expect_owner_mode "$STATE" 1000 777
finish

# Spec, rule table: root over its service account's owner-only directory is
# used as it is.
begin "root uses a state directory owned by uid 1000 (mode 0700)"
fresh_instance
make_dir "$CONFIG" 1000 0755
make_dir "$STATE" 1000 0700
enrolled_files "$STATE" 1000
run_spiced 0 "$STARTED"
expect_running
expect_line "$SERVES '$STATE/spicepod-cloud-managed.yml'"
expect_owner_mode "$STATE" 1000 700
expect_no_line "removed group and other write access"
finish

# Spec, breaking changes: old state in a config directory that is owned by
# another account and writable by others cannot be trusted. Nothing moves.
begin "uid 1001 refuses to move state out of a config directory owned by uid 1000 that others can write"
fresh_instance
make_dir "$CONFIG" 1000 0777
enrolled_files "$CONFIG" 1000
run_spiced 1001 "$STARTED"
expect_refused
expect_line "$(chmod_error "$CONFIG" 1000)" 1
expect_no_line "$SERVES"
expect_absent "$STATE"
expect_file "$CONFIG/identity.json" "$IDENTITY"
expect_owner_mode "$CONFIG" 1000 777
finish

# Spec: a root-run `spiced` keeps working over its service account's
# directory; the state it moves stays that account's.
begin "root moves state out of a config directory owned by uid 1000 into a cloud-connect/ uid 1000 owns"
fresh_instance
make_dir "$CONFIG" 1000 0755
enrolled_files "$CONFIG" 1000
run_spiced 0 "$STARTED"
expect_running
expect_line "$SERVES '$STATE/spicepod-cloud-managed.yml'"
expect_owner_mode "$STATE" 1000 700
expect_owner_mode "$STATE/identity.json" 1000 600
expect_absent "$CONFIG/identity.json"
finish

# Spec, rule table: owned by this account and writable by others. `go-w`, one
# INFO line, then used.
begin "uid 1000 removes group and other write access from its own state directory (0775 -> 0755)"
fresh_instance
make_dir "$CONFIG" 1000 0755
make_dir "$STATE" 1000 0775
enrolled_files "$STATE" 1000
run_spiced 1000 "$STARTED"
expect_running
expect_line "$(repair_info "$STATE" 0775 0755)" 1
expect_owner_mode "$STATE" 1000 755
expect_line "$SERVES '$STATE/spicepod-cloud-managed.yml'"
finish

# Spec, migration: a config directory this account owns passes even when it is
# group-writable (umask 002), and Spice never changes its mode.
begin "uid 1000 moves state out of its own group-writable config directory (0775) and leaves its mode"
fresh_instance
make_dir "$CONFIG" 1000 0775
enrolled_files "$CONFIG" 1000
run_spiced 1000 "$STARTED"
expect_running
expect_line "$SERVES '$STATE/spicepod-cloud-managed.yml'"
expect_owner_mode "$CONFIG" 1000 775
expect_owner_mode "$STATE" 1000 700
finish

# Spec: instances that never enroll are unchanged. They create nothing and log
# nothing about Cloud Connect, even in a config directory anyone can write.
begin "an instance that never enrolled creates no state directory"
fresh_instance
make_dir "$CONFIG" 0 0777
run_spiced 1000 "$LISTENING"
expect_running
expect_absent "$STATE"
expect_no_line "Cloud Connect"
finish

echo
if ((${#FAILED_CASES[@]} == 0)); then
  echo "All Cloud Connect state-directory cases passed."
  exit 0
fi
echo "${#FAILED_CASES[@]} Cloud Connect state-directory case(s) failed:"
printf '  %s\n' "${FAILED_CASES[@]}"
exit 1
