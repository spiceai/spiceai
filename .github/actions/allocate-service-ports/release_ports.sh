#!/usr/bin/env bash
#
# Free the port reservations allocate_ports.sh made for this job, listed in
# CI_SERVICE_PORT_LOCKS. Only reservations this runner instance still holds are
# removed: one another instance has taken over is no longer ours to free.

set -uo pipefail

LOCK_DIR="${CI_SERVICE_PORT_LOCK_DIR:-/tmp/spice-ci-service-ports}"
owner="${RUNNER_NAME:-}"

for port in ${CI_SERVICE_PORT_LOCKS:-}; do
  lock="${LOCK_DIR}/${port}"
  if [ -n "${owner}" ] && [ "$(cat "${lock}/owner" 2>/dev/null)" = "${owner}" ]; then
    rm -rf "${lock}"
  fi
done
