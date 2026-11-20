#!/usr/bin/env bash
# Copy one object with `mc cp`, retrying only failures of the object store's
# transport.
#
# The CI object store answering 5xx, or a connection that times out or drops,
# says nothing about the change under test, yet a bare `mc cp` turns it into a
# failed job (and, for a required job in the merge queue, a removed entry). Any
# other error, such as a missing object, a denied request or expired
# credentials, fails at once: retrying it cannot succeed and only hides the
# real message.
#
# Usage: bash .github/scripts/mc_cp.sh SOURCE TARGET
# MC_CP_ATTEMPTS (default 4) and MC_CP_DELAY_SECONDS (default 10, doubled after
# each failed attempt) bound the retries.
set -euo pipefail

attempts="${MC_CP_ATTEMPTS:-4}"
first_delay="${MC_CP_DELAY_SECONDS:-10}"
if ! [[ "$attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo "::error::MC_CP_ATTEMPTS must be a whole number of at least 1, got '$attempts'."
  exit 2
fi
if ! [[ "$first_delay" =~ ^[0-9]+$ ]]; then
  echo "::error::MC_CP_DELAY_SECONDS must be a whole number, got '$first_delay'."
  exit 2
fi
if [ "$#" -ne 2 ]; then
  echo "::error::mc_cp.sh needs exactly two arguments, SOURCE and TARGET, got $#."
  exit 2
fi
source_path="$1"
target_path="$2"

# The messages mc prints when the store or the network failed, not the request:
# an HTTP 5xx status with its reason phrase, MinIO's own "busy" and "starting"
# errors, a timeout, or a connection the peer dropped or refused. The status
# code alone is not enough, because object keys can contain digits.
transient='5(00 Internal Server Error|02 Bad Gateway|03 Service Unavailable|04 Gateway Time-?out)|Please reduce your request rate|Server not initialized|TLS handshake timeout|i/o timeout|connection reset by peer|connection refused|unexpected EOF|context deadline exceeded|net/http: request canceled'

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

delay="$first_delay"
for ((attempt = 1; ; attempt++)); do
  # Stream mc's output to the job log as it runs, and keep a copy to classify a failure.
  status=0
  mc cp "$source_path" "$target_path" 2>&1 | tee "$log" || status="${PIPESTATUS[0]}"
  [ "$status" -eq 0 ] && exit 0
  if ! grep -Eq "$transient" "$log"; then
    echo "::error::mc cp $source_path failed (exit $status), and the error is not an object store or network failure, so it was not retried."
    exit "$status"
  fi
  if [ "$attempt" -ge "$attempts" ]; then
    echo "::error::mc cp $source_path failed $attempt times with an object store or network error. The store may be down; re-run the job once it recovers."
    exit "$status"
  fi
  echo "::warning::mc cp $source_path failed with an object store or network error (attempt $attempt of $attempts); retrying in ${delay}s."
  sleep "$delay"
  delay=$((delay * 2))
done
