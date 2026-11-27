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
#   3. The source URLs, tried in order. CI_SOURCE_ATTEMPTS tries (default 4, three
#      retries), retrying only the URLs that failed with an error of the network or the
#      server (408, 429, 5xx): any other error, such as a 404, cannot succeed on a retry,
#      so that URL is not asked again.
#
# Every copy is checked against the pinned SHA-256 before it is used. A cached copy that
# does not match is a miss, so a wrong or tampered entry costs a download from the source
# and nothing else; a source copy that does not match fails, because the pin and the
# source disagree. Only a verified file reaches DEST, and it is written to CI_CACHE_DIR
# for the next job.
#
# Every copy is also bounded in size, by CI_FETCH_MAX_BYTES (default 256 MiB, three times
# the largest file pinned today, the 80 MiB rustc archive for x86_64 Linux), so a wrong
# copy cannot fill the runner's disk before its SHA-256 is checked. curl refuses a
# response that announces a larger size before writing any of it, and from curl 8.4 also
# stops one that grows past the bound without announcing its size; a CI_CACHE_DIR entry is
# read once and no further than one byte past the bound. A cached copy over the bound is a
# miss like any other wrong copy. A source copy over it fails with exit status 4, since the
# pin then needs a larger bound: a caller must not fetch the same file some other way that
# has no bound.
#
# The bucket fills itself. When it answered that it lacks the file (404), or held the wrong
# bytes, the verified download from the source is written back to it so the next job reads
# it there. The CI object store accepts writes only with OIDC credentials, so the write is
# signed (AWS SigV4, over the pinned SHA-256 of the body) with temporary credentials that
# rustfs_credentials.sh gets for this job's GitHub OIDC token. Signing needs curl 7.87 or
# later: an older curl leaves the session token and the body's hash out of the signature,
# and the store refuses it. A job without an OIDC token (one without `permissions:
# id-token: write`, or any pull request from a fork) only reads, and says so in one line,
# as does a run with CI_CACHE_READ_ONLY=true (a CI AWS role job off trunk and the trunk
# merge queue).
# A write that fails is a warning, never a failed job. Nothing is written when the bucket
# could not be reached or refused the read: it would refuse or stall the write too. The
# credentials never leave this script's process and are never printed.
#
# Usage: bash .github/scripts/cached_fetch.sh SHA256 DEST URL [URL...]
# CI_SOURCE_DELAY_SECONDS (default 5, doubled after each failed attempt) and
# CI_CACHE_DELAY_SECONDS (default 2) set the waits between attempts.
# Exit status: 0 DEST holds the verified file; 1 it could not be downloaded; 2 the
# arguments or settings are wrong; 4 a source offered more than CI_FETCH_MAX_BYTES.
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
for var in CI_CACHE_ATTEMPTS CI_SOURCE_ATTEMPTS CI_FETCH_MAX_BYTES; do
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
max_bytes="${CI_FETCH_MAX_BYTES:-268435456}"
# curl reads --max-filesize as a 64-bit count and refuses a longer number, which would
# then read as a failed download instead of a wrong setting.
if [ "${#max_bytes}" -gt 18 ]; then
  echo "::error::CI_FETCH_MAX_BYTES must have at most 18 digits, got ${#max_bytes}."
  exit 2
fi
name="$(basename "${sources[0]%%\?*}")"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
fetch_err="$(mktemp "$dest_dir/.cached_fetch_err.XXXXXX")"
trap 'rm -f "$tmp" "$fetch_err"' EXIT

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
  rm -f "$fetch_err"
  trap - EXIT
  echo "$name: verified from ${origin} (sha256 $sha256)."
}

# 1. The host's cache directory. The entry is read once, through one open file, and no
# further than one byte past the bound, so another job renaming a larger file over it
# meanwhile cannot make the copy larger. The copy is then measured and hashed.
if [ -n "${CI_CACHE_DIR:-}" ] && [ -f "$CI_CACHE_DIR/sha256/$sha256" ]; then
  cached_size=''
  if head -c "$((max_bytes + 1))" 2>/dev/null <"$CI_CACHE_DIR/sha256/$sha256" >"$tmp"; then
    cached_size="$(wc -c <"$tmp" | tr -d ' ')" || cached_size=''
  fi
  if [[ "$cached_size" =~ ^[0-9]+$ ]] && [ "$cached_size" -le "$max_bytes" ] &&
    [ "$(file_sha256 "$tmp")" = "$sha256" ]; then
    finish "$CI_CACHE_DIR"
    exit 0
  fi
  echo "::warning::The copy of $name in $CI_CACHE_DIR cannot be read, is larger than CI_FETCH_MAX_BYTES ($max_bytes bytes), or does not match its pinned SHA-256; ignoring it."
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

# Fetches $1 into $tmp; sets `status` (curl's exit) and `code` (the HTTP status). A body
# larger than max_bytes ends the transfer with status 63. curl says so with exit 63, except
# that the curl macOS ships reports it over HTTP/2 as exit 56 (a receive failure, which
# would be retried) with the same message, so the message decides too.
fetch() {
  status=0
  code="$(curl --fail --silent --show-error --location --connect-timeout "$2" --max-time "$3" \
    --max-filesize "$max_bytes" --write-out '%{http_code}' --output "$tmp" "$1" 2>"$fetch_err")" ||
    status=$?
  if [ -s "$fetch_err" ]; then
    cat "$fetch_err" >&2
    if [ "$status" -ne 0 ] && grep -qiE 'maximum (allowed )?file size' "$fetch_err"; then
      status=63
    fi
  fi
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
    if [ "$status" -eq 63 ]; then
      echo "::warning::The CI cache's copy of $name is larger than CI_FETCH_MAX_BYTES ($max_bytes bytes), so it is not the pinned file; downloading it from the source instead."
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
  if [ "${CI_CACHE_READ_ONLY:-}" = true ]; then
    echo "$name: not written to the CI cache, since this run is read-only (CI_CACHE_READ_ONLY)."
    return 0
  fi
  if [ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] || [ -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]; then
    echo "$name: not written to the CI cache, which takes writes only from a job with an OIDC token (permissions: id-token: write; a pull request from a fork never has one)."
    return 0
  fi
  local version major minor
  version="$(curl --version 2>/dev/null | awk 'NR == 1 { print $2 }')" || version=''
  IFS=. read -r major minor _ <<<"$version"
  if ! [[ "$major" =~ ^[0-9]+$ && "$minor" =~ ^[0-9]+$ ]] || ((major < 7 || (major == 7 && minor < 87))); then
    echo "::warning::Could not write $name to the CI cache: curl ${version:-of an unknown version} cannot sign the write, and 7.87 or later can. The next job downloads it from the source again."
    return 0
  fi

  # CI_CACHE_URL is the bucket's path-style URL, <endpoint>/<bucket>, and the endpoint
  # answers STS too. 900 seconds is the shortest lifetime STS grants, and outlasts the
  # PUT's own bound. Nothing here may fail the job, which already has its file.
  local endpoint="${CI_CACHE_URL%/}" err credentials access_key='' secret_key='' session_token=''
  local cred_status=0 reason=''
  endpoint="${endpoint%/*}"
  err="$(mktemp "$dest_dir/.cached_fetch_sts.XXXXXX" 2>/dev/null)" || err=/dev/null
  credentials="$(bash "$here/rustfs_credentials.sh" "$endpoint" 900 2>"$err")" || cred_status=$?
  if [ "$err" != /dev/null ]; then
    reason="$(head -n 1 "$err" 2>/dev/null)" || reason=''
    rm -f "$err"
  fi
  if [ "$cred_status" -ne 0 ]; then
    echo "::warning::Could not get credentials to write $name to the CI cache: ${reason:-rustfs_credentials.sh exited $cred_status.} The next job downloads it from the source again."
    return 0
  fi
  {
    read -r access_key
    read -r secret_key
    read -r session_token
  } <<<"$credentials" || true

  # The credentials reach curl as configuration on stdin, never on its command line, where
  # any process on the host could read them; rustfs_credentials.sh allowed no character
  # that could end a quoted value. The body's hash in x-amz-content-sha256 is signed, so
  # the store refuses bytes that are not the verified file. us-east-1 is the region the
  # store and the compiler cache sign with.
  local put_status=0 put_code
  put_code="$(printf 'user = "%s:%s"\nheader = "x-amz-security-token: %s"\n' \
    "$access_key" "$secret_key" "$session_token" |
    curl --config - --fail --silent --show-error --connect-timeout 5 --max-time 600 \
      --aws-sigv4 'aws:amz:us-east-1:s3' --header "x-amz-content-sha256: $sha256" \
      --write-out '%{http_code}' --output /dev/null --upload-file "$tmp" \
      "${CI_CACHE_URL%/}/sha256/$sha256")" || put_status=$?
  if [ "$put_status" -eq 0 ]; then
    echo "$name: written to the CI cache for the next job."
  else
    echo "::warning::Could not write $name to the CI cache (curl exit $put_status, HTTP $put_code); the next job downloads it from the source again."
  fi
}

# 3. The source. `oversized` records that a source offered more than the bound, which
# fails the fetch with exit 4 when no other source delivers the file.
delay="$source_delay"
oversized=false
for ((attempt = 1; ; attempt++)); do
  retry_urls=()
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
      retry_urls+=("$url")
      echo "::warning::Downloading $name from $url failed with a network or server error (curl exit $status, HTTP $code, attempt $attempt of $source_attempts)."
    elif [ "$status" -eq 63 ]; then
      oversized=true
      echo "::warning::$url is larger than CI_FETCH_MAX_BYTES ($max_bytes bytes), so $name was not downloaded from it; a retry cannot fix that. If the pinned file is that large, raise CI_FETCH_MAX_BYTES."
    else
      echo "::warning::Downloading $name from $url failed (curl exit $status, HTTP $code), which a retry cannot fix."
    fi
  done
  if [ "${#retry_urls[@]}" -eq 0 ] || [ "$attempt" -ge "$source_attempts" ]; then
    if [ "$oversized" = true ]; then
      echo "::error::Could not download $name: a source offered more than CI_FETCH_MAX_BYTES ($max_bytes bytes). If the pinned file is that large, raise CI_FETCH_MAX_BYTES."
      exit 4
    fi
  fi
  if [ "${#retry_urls[@]}" -eq 0 ]; then
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
  sources=("${retry_urls[@]}")
done
