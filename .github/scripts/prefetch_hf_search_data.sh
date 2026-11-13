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
# Only dataset sources: a `from:` key, optionally a list item. Comments that cite the Hub page and
# `huggingface:` embedding models do not match.
from_re='^[[:space:]]*(-[[:space:]]+)?from:[[:space:]]*https://huggingface\.co/'

rc=0
lines=$(grep -E "$from_re" "$spicepod") || rc=$?
if [ "$rc" -eq 1 ]; then
  echo "$spicepod reads nothing from the Hugging Face Hub."
  exit 0
elif [ "$rc" -ne 0 ]; then
  echo "::error::Could not read $spicepod (grep exit $rc)." >&2
  exit 1
fi

auth=()
if [ -n "${HF_TOKEN:-}" ]; then
  # curl sends a custom Authorization header only to the host named in the URL, so the token
  # reaches the Hub and not the CDN it redirects to.
  auth=(-H "Authorization: Bearer $HF_TOKEN")
else
  echo "::warning::HF_TOKEN is not set, so the Hub files download anonymously."
fi

count=0
declare -A seen=()
while IFS= read -r line; do
  url=$(printf '%s\n' "$line" | sed -E 's/^[^:]*from:[[:space:]]*//; s/[[:space:]]+$//')
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
  # `--retry` retries timeouts, 408, 429 (after its Retry-After) and 5xx, never a 401 or 404.
  # A transfer below 100 kB/s for 2 minutes counts as a timeout.
  if ! curl --fail --location --silent --show-error \
      --retry 5 --connect-timeout 30 --speed-limit 102400 --speed-time 120 \
      "${auth[@]}" --output "$dest" "$url"; then
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
  count=$((count + 1))
done <<< "$lines"

# Rewrite only the `from:` values that were downloaded. `\#...#` delimits the address because the
# pattern contains `/`.
sed -E -i "\\#${from_re}# s#https://huggingface\\.co/#file:data/hf/#" "$spicepod"

# Fail closed: a Hub source left in place would read the Hub after all.
if grep -qE "$from_re" "$spicepod"; then
  echo "::error::$spicepod still reads from the Hub after the rewrite." >&2
  exit 1
fi

echo "Downloaded $count Hub file(s) for $spicepod to $data_dir/hf."
