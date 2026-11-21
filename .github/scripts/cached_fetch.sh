#!/usr/bin/env bash
# Download a file whose SHA-256 is pinned, from a cache first and from its source after.
#
# A download from the internet (a GitHub release, static.rust-lang.org) fails now and then
# for reasons that say nothing about the change under test, and a self-hosted pool asks for
# the same files hundreds of times a day. So each file is looked up by its SHA-256, in order:
#
#   1. CI_CACHE_DIR, a directory on the runner's host that outlives the job (the
#      self-hosted Macs). No retry: it is a local read.
#   2. CI_CACHE_URL, the `ci-cache` bucket of the CI object store, read anonymously over
#      HTTP as <CI_CACHE_URL>/sha256/<sha256>. CI_CACHE_ATTEMPTS tries (default 2, one
#      retry), and a 404 is a miss, not retried.
#   3. The source URLs, tried in order on every attempt. CI_SOURCE_ATTEMPTS tries (default
#      4, three retries), retrying only a failure of the network or the server (408, 429,
#      5xx): any other error, such as a 404, cannot succeed on a retry.
#
# Every copy is checked against the pinned SHA-256 before it is used. A cached copy that
# does not match is a miss, so a wrong or tampered entry costs a download from the source
# and nothing else; a source copy that does not match fails, because the pin and the
# source disagree. Only a verified file reaches DEST, and it is written to CI_CACHE_DIR
# for the next job.
#
# The bucket fills itself. When it answered that it lacks the file (404), or held the wrong
# bytes, the verified download from the source is written back to it, anonymously, so the
# next job reads it there. A write that fails is a warning, never a failed job. Nothing is
# written when the bucket could not be reached or refused the read: it would refuse or
# stall the write too. Anyone who can reach the bucket may write it, which is safe only
# because no reader uses bytes that do not match its pin.
#
# Usage: bash .github/scripts/cached_fetch.sh SHA256 DEST URL [URL...]
# CI_SOURCE_DELAY_SECONDS (default 5, doubled after each failed attempt) and
# CI_CACHE_DELAY_SECONDS (default 2) set the waits between attempts.
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "::error::cached_fetch.sh needs SHA256, DEST and at least one source URL, got $# arguments."
  exit 2
fi
sha256="$1"
dest="$2"
shift 2
sources=("$@")

if ! [[ "$sha256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "::error::cached_fetch.sh: '$sha256' is not a lowercase hex SHA-256."
  exit 2
fi
for var in CI_CACHE_ATTEMPTS CI_SOURCE_ATTEMPTS; do
  value="${!var:-}"
  if [ -n "$value" ] && ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "::error::$var must be a whole number of at least 1, got '$value'."
    exit 2
  fi
done
for var in CI_CACHE_DELAY_SECONDS CI_SOURCE_DELAY_SECONDS; do
  value="${!var:-}"
  if [ -n "$value" ] && ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "::error::$var must be a whole number, got '$value'."
    exit 2
  fi
done
cache_attempts="${CI_CACHE_ATTEMPTS:-2}"
source_attempts="${CI_SOURCE_ATTEMPTS:-4}"
cache_delay="${CI_CACHE_DELAY_SECONDS:-2}"
source_delay="${CI_SOURCE_DELAY_SECONDS:-5}"
name="$(basename "${sources[0]%%\?*}")"

file_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

dest_dir="$(dirname "$dest")"
mkdir -p "$dest_dir"
tmp="$(mktemp "$dest_dir/.cached_fetch.XXXXXX")"
trap 'rm -f "$tmp"' EXIT

# Moves the verified download to DEST, and keeps a copy in CI_CACHE_DIR. The copy is
# written under a temporary name and renamed, so a job reading the directory at the same
# time sees the whole file or none of it.
finish() {
  local origin="$1"
  if [ -n "${CI_CACHE_DIR:-}" ] && [ "$origin" != "${CI_CACHE_DIR}" ]; then
    local cached="$CI_CACHE_DIR/sha256/$sha256" part
    if mkdir -p "$CI_CACHE_DIR/sha256" 2>/dev/null &&
      part="$(mktemp "$CI_CACHE_DIR/sha256/.part.XXXXXX" 2>/dev/null)"; then
      if cp "$tmp" "$part" && mv -f "$part" "$cached"; then
        :
      else
        rm -f "$part"
        echo "::warning::Could not keep $name in $CI_CACHE_DIR; the next job downloads it again."
      fi
    fi
  fi
  mv -f "$tmp" "$dest"
  trap - EXIT
  echo "$name: verified from ${origin} (sha256 $sha256)."
}

# 1. The host's cache directory.
if [ -n "${CI_CACHE_DIR:-}" ] && [ -f "$CI_CACHE_DIR/sha256/$sha256" ]; then
  if cp "$CI_CACHE_DIR/sha256/$sha256" "$tmp" 2>/dev/null && [ "$(file_sha256 "$tmp")" = "$sha256" ]; then
    finish "$CI_CACHE_DIR"
    exit 0
  fi
  echo "::warning::The copy of $name in $CI_CACHE_DIR cannot be read or does not match its pinned SHA-256; ignoring it."
  rm -f "$CI_CACHE_DIR/sha256/$sha256" 2>/dev/null || true
fi

# curl exits that mean the network or the server failed rather than the request: could
# not resolve or connect, partial body, timeout, TLS connect error, empty reply, send or
# receive failure, HTTP/2 stream error. With --fail every HTTP error is exit 22, and the
# status code decides.
transient() {
  local status="$1" code="$2"
  case "$status" in
    6 | 7 | 18 | 28 | 35 | 52 | 55 | 56 | 92) return 0 ;;
    22) [[ "$code" =~ ^(408|429|5[0-9][0-9])$ ]] ;;
    *) return 1 ;;
  esac
}

# Fetches $1 into $tmp; sets `status` (curl's exit) and `code` (the HTTP status).
fetch() {
  status=0
  code="$(curl --fail --silent --show-error --location --connect-timeout "$2" --max-time "$3" \
    --write-out '%{http_code}' --output "$tmp" "$1")" || status=$?
}

# 2. The CI object store. cache_state records why the read ended without a verified copy:
# `missing` (404) or `wrong` (the bytes did not match) mean the verified download from the
# source should be written back.
cache_state=''
if [ -n "${CI_CACHE_URL:-}" ]; then
  url="${CI_CACHE_URL%/}/sha256/$sha256"
  for ((attempt = 1; ; attempt++)); do
    fetch "$url" 5 600
    if [ "$status" -eq 0 ]; then
      if [ "$(file_sha256 "$tmp")" = "$sha256" ]; then
        finish "the CI cache"
        exit 0
      fi
      echo "::warning::The CI cache's copy of $name does not match its pinned SHA-256; downloading it from the source instead."
      cache_state=wrong
      break
    fi
    if [ "$status" -eq 22 ] && [ "$code" = 404 ]; then
      echo "$name is not in the CI cache yet; downloading it from the source."
      cache_state=missing
      break
    fi
    if ! transient "$status" "$code" || [ "$attempt" -ge "$cache_attempts" ]; then
      echo "::warning::Could not read $name from the CI cache (curl exit $status, HTTP $code, attempt $attempt of $cache_attempts); downloading it from the source instead."
      break
    fi
    echo "Reading $name from the CI cache failed (curl exit $status, HTTP $code); retrying in ${cache_delay}s."
    sleep "$cache_delay"
  done
fi

# Writes the verified download to the bucket for the next job, when the bucket said it
# lacked the file or held the wrong bytes. One try: the job already has its file.
write_back() {
  case "$cache_state" in
    missing | wrong) ;;
    *) return 0 ;;
  esac
  local put_status=0 put_code
  put_code="$(curl --fail --silent --show-error --connect-timeout 5 --max-time 600 \
    --write-out '%{http_code}' --output /dev/null --upload-file "$tmp" \
    "${CI_CACHE_URL%/}/sha256/$sha256")" || put_status=$?
  if [ "$put_status" -eq 0 ]; then
    echo "$name: written to the CI cache for the next job."
  else
    echo "::warning::Could not write $name to the CI cache (curl exit $put_status, HTTP $put_code); the next job downloads it from the source again."
  fi
}

# 3. The source.
delay="$source_delay"
for ((attempt = 1; ; attempt++)); do
  retry=false
  for url in "${sources[@]}"; do
    fetch "$url" 30 1800
    if [ "$status" -eq 0 ]; then
      got="$(file_sha256 "$tmp")"
      if [ "$got" = "$sha256" ]; then
        write_back
        finish "$url"
        exit 0
      fi
      echo "::error::$url does not match the pinned SHA-256 of $name: expected $sha256, got $got. Either the pin is wrong or the file at the source changed; it was not used."
      exit 1
    fi
    if transient "$status" "$code"; then
      retry=true
      echo "::warning::Downloading $name from $url failed with a network or server error (curl exit $status, HTTP $code, attempt $attempt of $source_attempts)."
    else
      echo "::warning::Downloading $name from $url failed (curl exit $status, HTTP $code), which a retry cannot fix."
    fi
  done
  if [ "$retry" != true ]; then
    echo "::error::Could not download $name: no source URL answered with the file, and none failed in a way a retry could fix."
    exit 1
  fi
  if [ "$attempt" -ge "$source_attempts" ]; then
    echo "::error::Could not download $name after $attempt attempts; its sources failed with network or server errors each time. Re-run the job once they recover."
    exit 1
  fi
  echo "Retrying the download of $name in ${delay}s."
  sleep "$delay"
  delay=$((delay * 2))
done
