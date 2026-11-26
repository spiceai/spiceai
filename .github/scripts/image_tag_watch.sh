#!/usr/bin/env bash
# Check that a ghcr.io tag still names the digest a run verified, once or for as long as it runs.
#
# While the cluster app is deployed by tag, Spice Cloud resolves the tag when the app starts and
# again whenever an executor starts, so a tag pushed again in between can change what runs. The
# run deploys by digest once Spice Cloud accepts `<tag>@sha256:<digest>`; until then these checks
# make a moved tag fail the run.
#
# Usage:
#   bash .github/scripts/image_tag_watch.sh check REPOSITORY TAG DIGEST
#       exit 0 when TAG names DIGEST, 1 (saying what it names) when it does not.
#   bash .github/scripts/image_tag_watch.sh watch REPOSITORY TAG DIGEST LOG [SECONDS]
#       check every SECONDS (default 20) until killed, appending each digest TAG names other
#       than DIGEST to LOG, so a tag pushed and pushed back between two other checks is seen.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mode="${1:?usage: image_tag_watch.sh check|watch REPOSITORY TAG DIGEST [LOG [SECONDS]]}"
repository="${2:?repository}"
tag="${3:?tag}"
digest="${4:?digest}"
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "::error::'$digest' is not a sha256 digest." >&2; exit 2; }
case "$mode" in
  check)
    now="$(bash "$here/ghcr_digest.sh" "$repository" "$tag")"
    if [ "$now" != "$digest" ]; then
      echo "ghcr.io/${repository}:${tag} names ${now}, not ${digest}."
      exit 1
    fi
    ;;
  watch)
    log="${5:?log}"
    interval="${6:-20}"
    [[ "$interval" =~ ^[1-9][0-9]*$ ]] || { echo "::error::'$interval' is not a number of seconds." >&2; exit 2; }
    while :; do
      # A failed read is not a move; the checks before and after the run still read the tag.
      if now="$(bash "$here/ghcr_digest.sh" "$repository" "$tag" 2>/dev/null)" && [ "$now" != "$digest" ]; then
        echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $now" >> "$log"
      fi
      sleep "$interval"
    done
    ;;
  *)
    echo "::error::mode must be check or watch, not '$mode'." >&2
    exit 2
    ;;
esac
