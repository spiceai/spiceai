#!/usr/bin/env bash
# Copies a release image index, by digest, to a registry tag.
#
#   push_release_image.sh version SOURCE TARGET
#     A version tag (spiceai/spiceai:2.4.2) is never moved. If TARGET already
#     points to SOURCE's digest this does nothing, so a re-run resumes; if it
#     points to any other digest it fails.
#   push_release_image.sh latest SOURCE TARGET
#     A moving tag (spiceai/spiceai:latest) is pointed at SOURCE.
#
# SOURCE is a local-registry reference to the index to publish. The copy keeps
# its digest, and the tag is read back to check it.
set -euo pipefail

if [ $# -ne 3 ] || { [ "$1" != version ] && [ "$1" != latest ]; }; then
  echo "usage: $0 version|latest SOURCE TARGET" >&2
  exit 2
fi
kind=$1
source=$2
target=$3

# Prints the digest TARGET points to, or nothing when the tag does not exist.
digest_of() {
  local out
  if out=$(docker buildx imagetools inspect "$1" --format '{{json .Manifest}}' 2>&1); then
    jq -r .digest <<<"$out"
  elif grep -q ': not found' <<<"$out"; then
    return 0
  else
    echo "::error::Failed to read $1, so it is not known whether the tag already exists. Cause: $out" >&2
    return 1
  fi
}

digest=$(digest_of "$source")
if [ -z "$digest" ]; then
  echo "::error::$source does not exist." >&2
  exit 1
fi

if [ "$kind" = version ]; then
  existing=$(digest_of "$target")
  if [ "$existing" = "$digest" ]; then
    echo "$target already points to $digest."
    exit 0
  fi
  if [ -n "$existing" ]; then
    echo "::error::$target already points to $existing, not to this run's $digest. A release image tag is never moved. If it came from an earlier run of this release, re-run the failed jobs of that run instead." >&2
    exit 1
  fi
fi

docker buildx imagetools create -t "$target" "${source%:*}@${digest}"

pushed=$(digest_of "$target")
if [ "$pushed" != "$digest" ]; then
  echo "::error::$target points to '$pushed' after the push, not to $digest." >&2
  exit 1
fi
echo "$target -> $digest"
