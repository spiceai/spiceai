#!/usr/bin/env bash
# Pull each image argument, retrying only failures of the registry transport.
#
# A registry answering 5xx, or a connection that times out or drops, says nothing
# about the change under test, yet a bare `docker pull` turns it into a failed job
# (and, in the merge queue, a removed entry). Any other error, such as an unknown
# tag or a denied pull, fails at once: retrying it cannot succeed and only hides
# the real message.
#
# Usage: bash .github/scripts/docker_pull.sh IMAGE [IMAGE...]
# DOCKER_PULL_ATTEMPTS (default 4) and DOCKER_PULL_DELAY_SECONDS (default 10,
# doubled after each failed attempt) bound the retries.
set -euo pipefail

attempts="${DOCKER_PULL_ATTEMPTS:-4}"
first_delay="${DOCKER_PULL_DELAY_SECONDS:-10}"
if ! [[ "$attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo "::error::DOCKER_PULL_ATTEMPTS must be a whole number of at least 1, got '$attempts'."
  exit 2
fi
if ! [[ "$first_delay" =~ ^[0-9]+$ ]]; then
  echo "::error::DOCKER_PULL_DELAY_SECONDS must be a whole number, got '$first_delay'."
  exit 2
fi
if [ "$#" -eq 0 ]; then
  echo "::error::docker_pull.sh needs at least one image."
  exit 2
fi

# The messages Docker prints when the registry or the network failed, not the
# request: an HTTP 5xx status, a timeout, or a connection the peer dropped.
transient='(: |status code |status )5[0-9][0-9]([^0-9]|$)|Bad Gateway|Service Unavailable|Gateway Time-?out|TLS handshake timeout|i/o timeout|connection reset by peer|connection refused|unexpected EOF|context deadline exceeded|net/http: request canceled'

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

for image in "$@"; do
  delay="$first_delay"
  for ((attempt = 1; ; attempt++)); do
    # Stream the pull to the job log as it runs, and keep a copy to classify a failure.
    status=0
    docker pull "$image" 2>&1 | tee "$log" || status="${PIPESTATUS[0]}"
    [ "$status" -eq 0 ] && break
    if ! grep -Eq "$transient" "$log"; then
      echo "::error::docker pull $image failed (exit $status), and the error is not a registry or network failure, so it was not retried."
      exit "$status"
    fi
    if [ "$attempt" -ge "$attempts" ]; then
      echo "::error::docker pull $image failed $attempt times with a registry or network error. The registry may be down; re-run the job once it recovers."
      exit "$status"
    fi
    echo "::warning::docker pull $image failed with a registry or network error (attempt $attempt of $attempts); retrying in ${delay}s."
    sleep "$delay"
    delay=$((delay * 2))
  done
done
