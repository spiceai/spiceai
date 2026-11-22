#!/usr/bin/env bash
# Download every Hugging Face Hub file a search spicepod reads, then point the spicepod at the
# local copies.
#
# Usage: prefetch_hf_search_data.sh <spicepod.yaml> <data-dir>
#
# A dataset `from: https://huggingface.co/<path>` is downloaded to `<data-dir>/hf/<path>`, and the
# spicepod is rewritten in place to read `file:data/hf/<path>`. `testoperator run search
# --data-dir <data-dir>` links the directory into spiced's working directory as `data/`.
#
# Why: spiced's HTTPS connector reads a Parquet file over HTTP with one range request per row group,
# sends no credentials, and ignores `http_headers` for file datasets. Every leg of a search sweep
# then shares the Hub's anonymous per-IP request quota, and legs fail with `429 Too Many Requests`
# until the ready wait runs out. A whole-file download is one request, and it sends `HF_TOKEN` when
# the environment sets it.
#
# A spicepod with no Hub source is left unchanged.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <spicepod.yaml> <data-dir>" >&2
  exit 2
fi
spicepod="$1"
data_dir="$2"

if [ ! -f "$spicepod" ]; then
  echo "::error::Spicepod not found: $spicepod" >&2
  exit 1
fi

# testoperator resolves `--data-dir`, so the directory must exist even when nothing downloads.
mkdir -p "$data_dir"

hub='https://huggingface.co/'
hub_re='https://huggingface\.co/'
# Only dataset sources: a `from:` key, optionally a list item. Comments that cite the Hub page and
# `huggingface:` embedding models do not match.
from_re="^[[:space:]]*(-[[:space:]]+)?from:[[:space:]]*${hub_re}"
# The same key with the value quoted, which the download does not handle: the check at the end
# fails on it rather than let spiced read the Hub after all.
any_from_re="^[[:space:]]*(-[[:space:]]+)?from:[[:space:]]*[\"']?${hub_re}"

# The token spiced would use: `HF_TOKEN`, else the runner's token file.
token="${HF_TOKEN:-}"
token_file="${HOME}/.cache/huggingface/token"
if [ -z "$token" ] && [ -s "$token_file" ]; then
  token=$(cat "$token_file")
fi

rc=0
lines=$(grep -E "$from_re" "$spicepod") || rc=$?
if [ "$rc" -gt 1 ]; then
  echo "::error::Could not read $spicepod (grep exit $rc)." >&2
  exit 1
fi
if [ -n "$lines" ] && [ -z "$token" ]; then
  echo "::warning::No Hugging Face token is available, so the Hub files download anonymously."
fi

# The token reaches curl as a config on stdin rather than an -H argument, so it never appears in a
# process's argv; `printf` is a builtin. curl sends a custom Authorization header only to the host
# named in the URL, so the token reaches the Hub and not the CDN it redirects to.
download() {
  # `--retry` retries timeouts, 408, 429 (after its Retry-After) and 5xx, never a 401 or 404.
  # A transfer below 100 kB/s for 2 minutes counts as a timeout.
  local opts=(--fail --location --silent --show-error --retry 5 --connect-timeout 30
    --speed-limit 102400 --speed-time 120 --output "$2" "$1")
  if [ -n "$token" ]; then
    printf 'header = "Authorization: Bearer %s"\n' "$token" | curl -K - "${opts[@]}"
  else
    curl "${opts[@]}"
  fi
}

declare -A seen=()
while IFS= read -r line; do
  [ -n "$line" ] || continue
  url="${line#*from:}"
  url="${url#"${url%%[![:space:]]*}"}"
  url="${url%"${url##*[![:space:]]}"}"
  rel="${url#"$hub"}"
  # The path becomes a file under data-dir and a `file:` source, so allow only plain path
  # characters and no `.` or `..` component.
  if [[ ! "$rel" =~ ^[A-Za-z0-9._/-]+$ ]] || [[ "/$rel/" == */./* ]] || [[ "/$rel/" == */../* ]]; then
    echo "::error::$spicepod: cannot store '$url' locally. A Hub source path may hold only letters, digits, '.', '_', '-' and '/', with no '.' or '..' component." >&2
    exit 1
  fi
  if [ -n "${seen[$rel]:-}" ]; then
    continue
  fi
  seen[$rel]=1

  dest="$data_dir/hf/$rel"
  mkdir -p "$(dirname "$dest")"
  if ! download "$url" "$dest"; then
    echo "::error::Could not download $url for $spicepod." >&2
    exit 1
  fi
  # A Parquet file ends with the magic bytes PAR1. Anything else (an HTML error page, a truncated
  # body) would surface much later as a confusing dataset error in spiced.
  size=$(stat -c %s "$dest")
  if [ "$size" -lt 8 ] || [ "$(tail -c 4 "$dest")" != "PAR1" ]; then
    echo "::error::$url did not download as a Parquet file ($size bytes)." >&2
    exit 1
  fi
  echo "$rel: $size bytes"
done <<< "$lines"

# Rewrite only the `from:` values that were downloaded. `\#...#` delimits the address because the
# pattern contains `/`.
if [ "${#seen[@]}" -gt 0 ]; then
  sed -E -i "\#${from_re}# s#${hub_re}#file:data/hf/#" "$spicepod"
fi

# Fail closed: a Hub source left in place would read the Hub after all.
if grep -qE "$any_from_re" "$spicepod"; then
  echo "::error::$spicepod still reads a dataset from the Hub. Write its \`from:\` value unquoted, as https://huggingface.co/<path>." >&2
  exit 1
fi

if [ "${#seen[@]}" -eq 0 ]; then
  echo "$spicepod reads nothing from the Hugging Face Hub."
else
  echo "Downloaded ${#seen[@]} Hub file(s) for $spicepod to $data_dir/hf."
fi
