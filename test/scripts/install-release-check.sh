#!/usr/bin/env bash
# Copyright 2026 The Spice.ai Authors
# SPDX-License-Identifier: Apache-2.0
#
# Helpers for the live jobs of .github/workflows/install_scripts_test.yml. Each writes its results
# as step outputs to $GITHUB_OUTPUT, and exits non-zero on anything it cannot read.
#
# Usage:
#   install-release-check.sh release <latest|X.Y.Z>
#       Looks the release up with gh (GH_TOKEN must be set). Outputs:
#         tag            the release tag, e.g. v2.4.2
#         version        the tag without its v, as the installers take it
#         sums           yes when the release publishes SHA256SUMS, otherwise no
#         api_requests   the GitHub API requests one install of it makes: the latest release
#                        lookup, and the release's asset list when it has no SHA256SUMS
#         cli_asset, runtime_asset   the release assets the installers download on this runner
#   install-release-check.sh budget <requests>
#       Asks GitHub, with no token, how many API requests this runner's address has left, which
#       costs none of them. Outputs run=true when at least <requests> are left, otherwise
#       run=false, with the reason in the job summary.

set -euo pipefail

fail() {
    echo "::error::$*"
    exit 1
}

release() {
    local wanted="$1" tag sums os arch
    if [[ "$wanted" == latest ]]; then
        tag=$(gh api repos/spiceai/spiceai/releases/latest --jq '.tag_name')
    else
        tag="v$wanted"
    fi
    [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]] || fail "unexpected release tag '$tag' for '$wanted'"
    sums=$(gh api "repos/spiceai/spiceai/releases/tags/$tag" --jq '[.assets[] | select(.name == "SHA256SUMS")] | length')
    case "$sums" in
        0) sums=no ;;
        1) sums=yes ;;
        *) fail "unexpected SHA256SUMS count '$sums' in release $tag" ;;
    esac
    os=$(uname | tr '[:upper:]' '[:lower:]')
    arch=$(uname -m)
    case "$arch" in
        arm64) arch=aarch64 ;;
        amd64) arch=x86_64 ;;
    esac
    case "$os" in
        mingw*|msys*|cygwin*) os=windows ;;
    esac
    {
        echo "tag=$tag"
        echo "version=${tag#v}"
        echo "sums=$sums"
        if [[ "$sums" == yes ]]; then echo "api_requests=1"; else echo "api_requests=2"; fi
        if [[ "$os" == windows ]]; then
            echo "cli_asset=spice.exe_windows_x86_64.tar.gz"
        else
            echo "cli_asset=spice_${os}_${arch}.tar.gz"
        fi
        echo "runtime_asset=spiced_${os}_${arch}.tar.gz"
    } | tee -a "$GITHUB_OUTPUT"
}

budget() {
    local needed="$1" answer remaining
    [[ "$needed" =~ ^[0-9]+$ ]] || fail "budget needs a number of requests, got '$needed'"
    answer=$(curl -fsS -H "Accept: application/vnd.github+json" https://api.github.com/rate_limit) ||
        fail "could not read https://api.github.com/rate_limit"
    remaining=$(jq -er '.resources.core.remaining' <<< "$answer") ||
        fail "no .resources.core.remaining in the /rate_limit answer: $answer"
    [[ "$remaining" =~ ^[0-9]+$ ]] || fail "unexpected remaining count '$remaining'"
    echo "Unauthenticated GitHub API requests left for this runner's address: $remaining (needed: $needed)"
    if (( remaining >= needed )); then
        echo "run=true" >> "$GITHUB_OUTPUT"
    else
        echo "run=false" >> "$GITHUB_OUTPUT"
        echo "::warning::Not run: this runner's address has $remaining unauthenticated GitHub API requests left, and the installs need $needed."
        echo "Not run: this runner's address has $remaining unauthenticated GitHub API requests left, and the installs need $needed." >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
    fi
}

case "${1:-}" in
    release) release "${2:?release needs latest or a version}" ;;
    budget) budget "${2:?budget needs a number of requests}" ;;
    *) fail "usage: $0 release <latest|X.Y.Z> | budget <requests>" ;;
esac
