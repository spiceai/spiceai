#!/usr/bin/env bash
# Print the digest a public ghcr.io repository's tag names now, as `sha256:<hex>`.
#
# Usage: bash .github/scripts/ghcr_digest.sh REPOSITORY TAG   (REPOSITORY as `spiceai/spiceai-dev`)
# Reads the manifest's Docker-Content-Digest with an anonymous pull token. Exits 1, saying why,
# when the tag or the answer is not what it should be.
set -euo pipefail
repository="${1:?usage: ghcr_digest.sh REPOSITORY TAG}"
tag="${2:?usage: ghcr_digest.sh REPOSITORY TAG}"
[[ "$repository" =~ ^[a-z0-9]+([._-][a-z0-9]+)*(/[a-z0-9]+([._-][a-z0-9]+)*)+$ ]] || { echo "::error::'$repository' is not a ghcr.io repository." >&2; exit 1; }
[[ "$tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || { echo "::error::'$tag' is not an image tag." >&2; exit 1; }
token="$(curl -fsSL --retry 3 "https://ghcr.io/token?scope=repository:${repository}:pull" | jq -r .token)"
digest="$(curl -fsSI --retry 3 -H "Authorization: Bearer ${token}" \
  -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
  "https://ghcr.io/v2/${repository}/manifests/${tag}" | tr -d '\r' | awk -F': ' 'tolower($1) == "docker-content-digest" { print $2 }')"
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "::error::Could not read the digest of ghcr.io/${repository}:${tag}." >&2; exit 1; }
echo "$digest"
