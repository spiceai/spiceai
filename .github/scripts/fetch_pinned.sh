#!/usr/bin/env bash
# Download a file pinned in .github/ci-cache/pins.json, through cached_fetch.sh.
#
# NAME is a top-level entry of pins.json, which pins either one file,
#
#   "NAME": {"url": "<url>", "sha256": "<sha256>"}
#
# or one release of a tool for each target it is built for,
#
#   "NAME": {"version": "<version>", "url": "<url with {version} and {target}>",
#            "sha256": {"<target>": "<sha256>", ...}}
#
# in which case TARGET names the build. The URL and its SHA-256 go to cached_fetch.sh,
# which reads the file from the host's CI_CACHE_DIR, the CI cache or its source, and
# moves it to DEST only once its SHA-256 matches the pin. Nothing here downloads a file
# that pins.json does not pin: a NAME or TARGET without a pin fails before any request.
#
# Usage: bash .github/scripts/fetch_pinned.sh NAME DEST [TARGET]
# Exit status: cached_fetch.sh's (0 DEST holds the verified file; 1 it could not be
# downloaded; 4 a source offered more than CI_FETCH_MAX_BYTES), or 2 when the arguments
# are wrong or pins.json has no such pin.
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "::error::fetch_pinned.sh needs NAME and DEST, and TARGET for a pin with one file per target; got $# arguments."
  exit 2
fi
name="$1"
dest="$2"
target="${3:-}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pins="$here/../ci-cache/pins.json"

# Prints the pin's SHA-256, URL and version on three lines, or the reason there is none on
# stderr. -I keeps the interpreter from importing anything from the working directory.
resolved="$(python3 -I - "$pins" "$name" "$target" <<'PY'
import json
import re
import sys

path, name, target = sys.argv[1:4]
pins = json.load(open(path, encoding="utf-8"))
pin = pins.get(name)
where = ".github/ci-cache/pins.json"
if name == "description" or not isinstance(pin, dict):
    sys.exit(f"{where} has no pin named {name}.")
sha256 = pin.get("sha256")
url = pin.get("url")
version = pin.get("version", "")
if not isinstance(url, str):
    sys.exit(f"{where} entry {name} has no url.")
if isinstance(sha256, dict):
    if not target:
        sys.exit(f"{where} pins {name} once for each target, so name the target ({', '.join(sorted(sha256))}).")
    if target not in sha256:
        sys.exit(f"{where} pins no {name} {version} for {target}; it pins {', '.join(sorted(sha256)) or 'none'}.")
    sha256 = sha256[target]
    url = url.format(version=version, target=target)
elif target:
    sys.exit(f"{where} pins one {name} file, for no particular target, but {target} was asked for.")
if not isinstance(sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", sha256):
    sys.exit(f"{where} entry {name} has no lowercase hex SHA-256{' for ' + target if target else ''}.")
print(sha256)
print(url)
print(version)
PY
)" || {
  echo "::error::Could not resolve the pin for ${name}${target:+ ($target)}; the line above says why."
  exit 2
}
sha256='' url='' version=''
{ read -r sha256; read -r url; read -r version; } <<<"$resolved" || true

echo "Fetching ${name}${version:+ $version}${target:+ for $target}, pinned at sha256 $sha256."
exec bash "$here/cached_fetch.sh" "$sha256" "$dest" "$url"
