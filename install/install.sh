#!/bin/sh
# Spice.ai CLI installer
# Version: 3.0.0 (2026-11-25)
#
#   curl -fsSL https://install.spiceai.org | sh                 installs the latest release
#   curl -fsSL https://install.spiceai.org | sh -s -- 2.4.3     installs release v2.4.3
#
# Run with --help for the options. The script is POSIX sh, so any sh runs it: bash, dash, zsh and
# BusyBox ash. Everything above the last line only defines functions, so a download that is cut
# short runs nothing.

set -u

SPICE_BIN=".spice/bin"

# sudo is required to write to an install directory the user cannot write to
: "${USE_SUDO:="false"}"

# Http request CLI
SPICE_HTTP_REQUEST_CLI=curl

# GitHub Organization and repo name to download release
GITHUB_ORG=spiceai
GITHUB_REPO=spiceai

# Where this installer is served, for the commands its messages suggest
SPICE_INSTALLER_URL=https://install.spiceai.org

SPICE_SYSTEM_REQUIREMENTS_URL=https://spiceai.org/docs/reference/system_requirements

# Spice CLI filename
SPICE_CLI_FILENAME=spice

# Colors, only for a terminal that wants them (https://no-color.org).
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    yellow=$(printf '\033[0;33m')
    bold=$(printf '\033[1m')
    reset=$(printf '\033[0m')
else
    yellow=""
    bold=""
    reset=""
fi

info() {
    printf '%s\n' "$@"
}

warn() {
    printf '%sWarning:%s %s\n' "$yellow" "$reset" "$1" >&2
    shift
    if [ $# -gt 0 ]; then
        printf '%s\n' "$@" >&2
    fi
}

# Prints its arguments as lines on stderr and exits.
fail() {
    printf '%s\n' "$@" >&2
    exit 1
}

usage() {
    cat <<EOF
Spice.ai CLI installer

Usage:
  curl -fsSL $SPICE_INSTALLER_URL | sh                 Install the latest release
  curl -fsSL $SPICE_INSTALLER_URL | sh -s -- VERSION   Install a release, e.g. 2.4.3 or v2.4.3
  sh install.sh [VERSION]

Environment:
  SPICE_CLI_INSTALL_DIR             Where to install spice (default: \$HOME/$SPICE_BIN)
  SPICE_INSTALL_NO_MODIFY_PATH=1    Leave the shell profiles unchanged; the installer then
                                    prints the PATH line to add yourself
  SPICE_INSTALL_ALLOW_SUDO=1        Run under sudo anyway, installing for the root user
  SPICE_INSTALL_ALLOW_UNVERIFIED=1  Install a release asset whose sha256 cannot be checked
  GITHUB_TOKEN                      Sent with GitHub API requests, which raises their rate limit
  NONINTERACTIVE=1                  Ask nothing, as when CI is set or there is no terminal
  NO_COLOR                          Print no colors

In an interactive session, after installing spice, the installer offers to add the Spice skills
plugin ($SKILLS_URL) to the AI coding agents it finds: Claude Code, Codex, GitHub
Copilot CLI and Grok Build. It never asks, and never changes an agent's configuration, when CI or
NONINTERACTIVE is set, when its output is not a terminal, or when there is no terminal to ask on.

See https://spiceai.org/docs/installation
EOF
}

# Sets REQUESTED_VERSION to the one version argument, without a leading "v", or to "latest".
parseArgs() {
    REQUESTED_VERSION=""
    while [ $# -gt 0 ]; do
        case "$1" in
            -h|--help)
                usage
                exit 0
                ;;
            --)
                ;;
            -*)
                usage >&2
                fail "" "Unknown option: $1"
                ;;
            *)
                if [ -n "$REQUESTED_VERSION" ]; then
                    usage >&2
                    fail "" "Give one version to install, not '$REQUESTED_VERSION' and '$1'."
                fi
                REQUESTED_VERSION="$1"
                ;;
        esac
        shift
    done
    case "$REQUESTED_VERSION" in
        ""|latest) REQUESTED_VERSION=latest ;;
        v*) REQUESTED_VERSION="${REQUESTED_VERSION#v}" ;;
    esac
    if [ "$REQUESTED_VERSION" != latest ] && ! isVersion "$REQUESTED_VERSION"; then
        fail "'$REQUESTED_VERSION' is not a Spice release version. Give one such as 2.4.3 or v2.4.3, or none for the latest release." \
            "Releases: https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases"
    fi
}

# Whether $1 is a release version: X.Y.Z, with an optional -prerelease.
isVersion() {
    printf '%s\n' "$1" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'
}

# Sets CLI_DIR, the absolute install directory without a trailing slash, CLI_FILE, the spice it
# installs, and CLI_DIR_EXPLICIT, whether SPICE_CLI_INSTALL_DIR chose it.
resolveInstallDir() {
    if [ -z "${HOME:-}" ]; then
        fail "HOME is not set, so the installer cannot tell where to install spice." \
            "Set HOME, or set SPICE_CLI_INSTALL_DIR to the directory to install spice into."
    fi
    if [ -n "${SPICE_CLI_INSTALL_DIR:-}" ]; then
        CLI_DIR_EXPLICIT=true
        CLI_DIR="$SPICE_CLI_INSTALL_DIR"
    else
        CLI_DIR_EXPLICIT=false
        CLI_DIR="$HOME/$SPICE_BIN"
    fi
    case "$CLI_DIR" in
        /*) ;;
        *) CLI_DIR="$(pwd)/$CLI_DIR" ;;
    esac
    while [ "$CLI_DIR" != "/" ] && [ "${CLI_DIR%/}" != "$CLI_DIR" ]; do
        CLI_DIR="${CLI_DIR%/}"
    done
    CLI_FILE="${CLI_DIR}/${SPICE_CLI_FILENAME}"
}

# Exits when the installer runs under sudo from a regular user's shell. spice installs into the
# home directory and needs no root access; under sudo it would go into root's home directory, or
# stay root-owned in the user's, and the user's own shell would not find it or could not update
# it. Plain root (containers, CI) is unaffected, and so is an install directory chosen with
# SPICE_CLI_INSTALL_DIR.
checkNotSudo() {
    [ "$(id -u)" -eq 0 ] || return 0
    [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ] || return 0
    [ "${SPICE_INSTALL_ALLOW_SUDO:-}" = "1" ] && return 0
    [ "$CLI_DIR_EXPLICIT" = "true" ] && return 0
    fail "Do not run the Spice CLI installer with sudo." \
        "spice installs into your home directory and needs no root access. Under sudo it would be" \
        "installed for root instead of for $SUDO_USER, and 'spice' would not be found in your own shell." \
        "" \
        "Run the same command without sudo:" \
        "  curl -fsSL $SPICE_INSTALLER_URL | sh" \
        "" \
        "To install spice for every user, choose a system directory instead. The installer asks for" \
        "sudo itself when it needs it:" \
        "  curl -fsSL $SPICE_INSTALLER_URL | SPICE_CLI_INSTALL_DIR=/usr/local/bin sh" \
        "" \
        "To install spice for the root user, set SPICE_INSTALL_ALLOW_SUDO=1:" \
        "  curl -fsSL $SPICE_INSTALLER_URL | sudo SPICE_INSTALL_ALLOW_SUDO=1 sh"
}

# Sets OS and ARCH to the platform to install for, and exits when Spice publishes no CLI for it.
getSystemInfo() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    case "$OS" in
        mingw*|msys*|cygwin*)
            fail "This installer is for macOS and Linux. On Windows, install the Spice CLI from PowerShell:" \
                "  iex ((New-Object System.Net.WebClient).DownloadString(\"$SPICE_INSTALLER_URL/Install.ps1\"))"
            ;;
    esac

    ARCH=$(uname -m)
    case "$ARCH" in
        armv7*) ARCH="arm" ;;
        arm64) ARCH="aarch64" ;;
        amd64) ARCH="x86_64" ;;
    esac

    # A shell running under Rosetta 2 on an Apple silicon Mac reports x86_64. The native build
    # runs there, and is the only macOS build.
    if [ "$OS" = "darwin" ] && [ "$ARCH" = "x86_64" ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = "1" ]; then
        ARCH="aarch64"
        info "This shell runs under Rosetta 2, so the installer installs the native Apple silicon build."
    fi
}

verifySupported() {
    case "${OS}-${ARCH}" in
        linux-x86_64|linux-aarch64|darwin-aarch64) ;;
        *)
            fail "${OS}-${ARCH} does not have a pre-built binary. For supported architectures, visit $SPICE_SYSTEM_REQUIREMENTS_URL#operating-systems-and-architectures"
            ;;
    esac

    # The Linux builds link against glibc. A musl system (Alpine Linux, for one) cannot load them,
    # so say so before the download instead of installing a binary that cannot start.
    if [ "$OS" = "linux" ]; then
        case "$(ldd --version 2>&1)" in
            *musl*)
                fail "The Spice CLI cannot run on this system: Spice publishes Linux builds for glibc, and this system uses musl libc (as Alpine Linux does)." \
                    "Install it on a glibc-based distribution, such as Debian, Ubuntu or Fedora. See $SPICE_SYSTEM_REQUIREMENTS_URL"
                ;;
        esac
    fi
}

# Runs its arguments as one command, through sudo when the install directory needs it. The
# arguments pass through as they are and are never parsed again, so a path holding spaces,
# quotes or `;` stays one literal argument and never runs as shell syntax.
runAsRoot() {
    if [ "$(id -u)" -ne 0 ] && [ "$USE_SUDO" = "true" ]; then
        sudo -- "$@"
    else
        "$@"
    fi
}

# Sets USE_SUDO to true when the user cannot write the install directory, or, when it does not
# exist yet, its nearest existing ancestor, which is where mkdir -p starts. A missing ~/.spice is
# not a reason to need sudo.
decideSudo() {
    if [ -d "$CLI_DIR" ]; then
        [ -w "$CLI_DIR" ] || USE_SUDO="true"
    else
        ds_parent=$(dirname "$CLI_DIR")
        while [ ! -e "$ds_parent" ]; do
            ds_parent=$(dirname "$ds_parent")
        done
        [ -w "$ds_parent" ] || USE_SUDO="true"
    fi
}

checkHttpRequestCLI() {
    if command -v curl > /dev/null 2>&1; then
        SPICE_HTTP_REQUEST_CLI=curl
    elif command -v wget > /dev/null 2>&1; then
        SPICE_HTTP_REQUEST_CLI=wget
    else
        fail "Either 'curl' or 'wget' is required" \
            "" \
            "To install curl (macOS): 'brew install curl'" \
            "To install curl (Ubuntu): 'apt install curl'"
    fi
}

# -----------------------------------------------------------------------------
# Download verification. This block is identical in install.sh and
# install-spiced.sh; test/scripts/install-scripts-test.sh fails when they differ.
# It is POSIX sh, as install.sh is: no arrays, `local`, `[[` or `echo -e`, and
# each function's variables carry its own prefix.
# -----------------------------------------------------------------------------

# Release attestations are signed by build_release.yml running on a release branch.
SPICE_ATTESTATION_SIGNER='^https://github\.com/spiceai/spiceai/\.github/workflows/build_release\.yml@refs/heads/release/'

# Saves a URL to a file, sending GITHUB_TOKEN when it is set. Fails on an HTTP error. A request
# that times out or gets a 408, 429 or 5xx is tried up to 3 more times, a second apart, so a
# brief GitHub error doesn't stop the install. curl stops retrying when the next try would start
# more than 10 seconds after the first, so a rate limit whose Retry-After asks for a longer wait
# fails at once instead of stalling the install. A 404, such as the SHA256SUMS of a release
# without one, is not retried. A connection that does not open within 15 seconds fails.
verifyFetch() {
    vf_url="$1"
    vf_out="$2"
    if [ "$SPICE_HTTP_REQUEST_CLI" = "curl" ]; then
        set -- curl --fail -sSL --connect-timeout 15 --retry 3 --retry-delay 1 --retry-max-time 10
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            set -- "$@" -H "Authorization: Bearer $GITHUB_TOKEN"
        fi
        "$@" "$vf_url" -o "$vf_out"
    else
        # -nv, not -q: on a failure wget then says why, such as a 401 for an invalid GITHUB_TOKEN.
        set -- wget -nv -T 15
        # GNU wget retries an HTTP error only when told which. BusyBox wget can't, and makes one try.
        case "$(wget --help 2>&1)" in
            *--retry-on-http-error*) set -- "$@" --tries=4 --waitretry=1 "--retry-on-http-error=408,429,500,502,503,504" ;;
        esac
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            set -- "$@" "--header=Authorization: Bearer $GITHUB_TOKEN"
        fi
        "$@" -O "$vf_out" "$vf_url"
    fi
}

# Prints the sha256 of a file with sha256sum, shasum or openssl, whichever is installed.
# The file goes in on stdin: given a path with a backslash in it, sha256sum and shasum prefix
# their output with one, and the parsed sha256 would never match.
sha256Of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum < "$1" | awk '{ print $1 }'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 < "$1" | awk '{ print $1 }'
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 < "$1" | awk '{ print $NF }'
    else
        return 1
    fi
}

# Exits, before anything is installed, unless a downloaded release asset matches the sha256
# its release records for it: the release's SHA256SUMS, or for releases published before
# SHA256SUMS, the sha256 GitHub recorded when the asset was uploaded. That fallback is taken
# only when the release's asset list confirms it has no SHA256SUMS. A release with
# SHA256SUMS also has build attestations, which are checked when the GitHub CLI is logged in.
# SPICE_INSTALL_ALLOW_UNVERIFIED=1 installs an asset that cannot be checked at all; it never
# installs one whose check fails.
#   $1 downloaded file, $2 asset name, $3 release tag, $4 empty scratch directory
verifyDownload() {
    vd_file="$1"
    vd_asset="$2"
    vd_tag="$3"
    vd_scratch="$4"
    vd_download_url="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download/${vd_tag}/SHA256SUMS"
    vd_api_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/tags/${vd_tag}"
    vd_expected=""
    vd_actual=""
    vd_source=""
    vd_reason=""
    vd_attested=false

    # What the release records comes first, so that an asset its SHA256SUMS does not list, or a
    # release with SHA256SUMS, is refused even when no hash tool works.
    if verifyFetch "$vd_download_url" "$vd_scratch/SHA256SUMS" 2>/dev/null; then
        vd_attested=true
        vd_source="SHA256SUMS of release $vd_tag"
        vd_expected=$(awk -v f="$vd_asset" '$2 == f || $2 == "*" f { print $1; exit }' "$vd_scratch/SHA256SUMS")
        if [ -z "$vd_expected" ]; then
            printf '%s\n' "Cannot install $vd_asset: the SHA256SUMS of release $vd_tag does not list it." \
                "Nothing was installed." >&2
            exit 1
        fi
    elif verifyFetch "$vd_api_url" "$vd_scratch/release.json" 2>"$vd_scratch/release.err"; then
        # Pretty-printed or not, the JSON has one key per line once each comma is a line break.
        # grep and awk read that from a file, not a pipe: under pipefail (SHELLOPTS can turn it on)
        # a reader that stops at its first match would fail the pipeline when the writer gets
        # SIGPIPE, and a listed SHA256SUMS would read as missing.
        tr ',' '\n' < "$vd_scratch/release.json" > "$vd_scratch/release.lines"
        # The release lists SHA256SUMS when it has one. Then the download failed for another
        # reason, and the GitHub digest would skip the attestation check, so stop instead.
        if grep -Eq '"name"[[:space:]]*:[[:space:]]*"SHA256SUMS"' "$vd_scratch/release.lines"; then
            printf '%s\n' "Cannot verify $vd_asset: release $vd_tag publishes SHA256SUMS, but downloading it failed ($vd_download_url)." \
                "Try again later. Nothing was installed." >&2
            exit 1
        fi
        vd_source="sha256 GitHub recorded for $vd_asset in release $vd_tag"
        vd_expected=$(awk -v f="\"$vd_asset\"" '
            /"name"[[:space:]]*:/ { found = (index($0, f) > 0); next }
            found && /"digest"[[:space:]]*:/ {
                if (match($0, /sha256:[0-9a-fA-F]+/)) { print substr($0, RSTART + 7, RLENGTH - 7) }
                exit
            }' "$vd_scratch/release.lines")
        if [ -z "$vd_expected" ]; then
            vd_reason="release $vd_tag has no SHA256SUMS, and GitHub recorded no sha256 for $vd_asset"
        fi
    else
        vd_reason="the SHA256SUMS of release $vd_tag could not be downloaded, and the GitHub API request for its asset sha256 failed (set GITHUB_TOKEN if the API rate limit was exceeded)"
        # The client's own error names the HTTP status, such as a 401 for an invalid GITHUB_TOKEN.
        vd_cause=$(awk 'NF { line = $0 } END { print line }' "$vd_scratch/release.err" 2>/dev/null)
        if [ -n "$vd_cause" ]; then
            vd_reason="$vd_reason: $vd_cause"
        fi
    fi

    if [ -z "$vd_reason" ] && { ! vd_actual=$(sha256Of "$vd_file") || [ -z "$vd_actual" ]; }; then
        # A release with SHA256SUMS can always be checked, so the opt-out does not cover it.
        if [ "$vd_attested" = "true" ]; then
            printf '%s\n' "Cannot verify $vd_asset: release $vd_tag publishes SHA256SUMS, but none of sha256sum, shasum or openssl is installed." \
                "Install one of them and run the installer again. Nothing was installed." >&2
            exit 1
        fi
        vd_reason="none of sha256sum, shasum or openssl is installed"
    fi

    if [ -n "$vd_reason" ]; then
        if [ "${SPICE_INSTALL_ALLOW_UNVERIFIED:-}" = "1" ]; then
            printf '%sWarning:%s installing %s unverified, because SPICE_INSTALL_ALLOW_UNVERIFIED=1 is set: %s.\n' \
                "$yellow" "$reset" "$vd_asset" "$vd_reason" >&2
            return 0
        fi
        printf '%s\n' "Cannot verify $vd_asset: $vd_reason." \
            "To install it without verification, run the installer with SPICE_INSTALL_ALLOW_UNVERIFIED=1, e.g.:" \
            "  curl -fsSL $SPICE_INSTALLER_URL | SPICE_INSTALL_ALLOW_UNVERIFIED=1 bash -s ${vd_tag#v}" \
            "Nothing was installed." >&2
        exit 1
    fi

    vd_expected=$(printf '%s' "$vd_expected" | tr '[:upper:]' '[:lower:]')
    if [ "$vd_actual" != "$vd_expected" ]; then
        printf '%s\n' "Checksum mismatch for $vd_asset:" \
            "  expected sha256 $vd_expected ($vd_source)" \
            "  downloaded      $vd_actual" \
            "The download is corrupt or was altered. Nothing was installed." >&2
        exit 1
    fi
    printf '%s\n' "Verified the sha256 of $vd_asset against the $vd_source."

    if [ "$vd_attested" = "true" ]; then
        if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
            if ! gh attestation verify "$vd_file" --repo "${GITHUB_ORG}/${GITHUB_REPO}" \
                --cert-identity-regex "$SPICE_ATTESTATION_SIGNER" --deny-self-hosted-runners \
                > "$vd_scratch/attestation.log" 2>&1; then
                printf '%s\n' "The build attestation of $vd_asset did not verify:" >&2
                cat "$vd_scratch/attestation.log" >&2
                printf '%s\n' "Nothing was installed." >&2
                exit 1
            fi
            printf '%s\n' "Verified the build attestation of $vd_asset."
        else
            printf '%s\n' "Skipped the build attestation check of $vd_asset: the GitHub CLI (gh) is not installed or not logged in."
        fi
    fi
}

# Sets LATEST_TAG to the tag of the latest release, read from where
# github.com/<org>/<repo>/releases/latest redirects. That is not a GitHub API request, so it needs
# no token and does not count against the API rate limit. The redirect is read, not followed.
# Fails when no release tag could be read, as from a BusyBox wget, which cannot stop at a redirect.
latestTagFromRedirect() {
    lt_url="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest"
    if [ "$SPICE_HTTP_REQUEST_CLI" = "curl" ]; then
        lt_location=$(curl -sS --connect-timeout 15 --max-time 30 -o /dev/null -w '%{redirect_url}' "$lt_url" 2>/dev/null) || lt_location=""
    else
        # wget exits non-zero at the redirect it is told not to follow; the Location it printed
        # is what counts. awk reads to the end, so no SIGPIPE fails the pipeline under pipefail.
        lt_location=$({ wget -S --max-redirect=0 -T 15 -t 1 -O /dev/null "$lt_url" 2>&1 || :; } |
            awk 'tolower($1) == "location:" { location = $2 } END { print location }')
    fi
    lt_location=$(printf '%s' "$lt_location" | tr -d '\r')
    case "$lt_location" in
        */releases/tag/v*) LATEST_TAG="${lt_location##*/releases/tag/}" ;;
        *) return 1 ;;
    esac
    printf '%s\n' "$LATEST_TAG" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'
}

# Sets ret_val to the tag of the latest release: from the github.com redirect, or, when that
# cannot be read, from the GitHub API, sending GITHUB_TOKEN when it is set.
getLatestRelease() {
    if latestTagFromRedirect; then
        ret_val="$LATEST_TAG"
        return 0
    fi

    lr_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest"
    if [ "$SPICE_HTTP_REQUEST_CLI" = "curl" ]; then
        set -- curl -s --connect-timeout 15 --max-time 60
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            set -- "$@" -H "Authorization: Bearer $GITHUB_TOKEN"
        fi
        lr_response=$("$@" "$lr_url") || lr_response=""
    else
        set -- wget -q -T 15 "--header=Accept: application/json"
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            set -- "$@" "--header=Authorization: Bearer $GITHUB_TOKEN"
        fi
        lr_response=$("$@" -O - "$lr_url") || lr_response=""
    fi
    # awk reads all of the response, so a large one cannot fail the pipeline under pipefail.
    lr_tag=$(printf '%s\n' "$lr_response" | awk '
        !found && match($0, /"tag_name"[[:space:]]*:[[:space:]]*"[^"]*"/) {
            value = substr($0, RSTART, RLENGTH)
            sub(/^"tag_name"[[:space:]]*:[[:space:]]*"/, "", value)
            sub(/"$/, "", value)
            print value
            found = 1
        }')

    # Without a tag the download URL becomes .../releases/download//<artifact>,
    # which fetches an error page and fails later as a corrupt archive.
    if [ -z "$lr_tag" ]; then
        lr_message=$(printf '%s\n' "$lr_response" | awk -F'"' '!found && /"message"/ { print $4; found = 1 }')
        printf '%s\n' "Failed to resolve the latest Spice.ai CLI release from https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest or $lr_url" >&2
        if [ -n "$lr_message" ]; then
            printf '%s\n' "GitHub API responded: $lr_message" >&2
        fi
        fail "If the GitHub API rate limit was exceeded, set GITHUB_TOKEN or pass a version to install, e.g. 'curl -fsSL $SPICE_INSTALLER_URL | sh -s -- 2.4.3'."
    fi

    ret_val="$lr_tag"
}

# Saves the release asset URL $1 to $2, sending GITHUB_TOKEN when it is set. A connection that
# does not open within 15 seconds, or a transfer slower than 1 KB/s for a minute, fails, and a
# timeout, 408, 429 or 5xx is tried up to 3 more times. A 404 is not retried.
fetchAsset() {
    fa_url="$1"
    fa_out="$2"
    if [ "$SPICE_HTTP_REQUEST_CLI" = "curl" ]; then
        set -- curl --fail -sSL --connect-timeout 15 --speed-limit 1024 --speed-time 60 --retry 3 --retry-delay 2
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            set -- "$@" -H "Authorization: Bearer $GITHUB_TOKEN"
        fi
        "$@" "$fa_url" -o "$fa_out"
    else
        set -- wget -q -T 60
        case "$(wget --help 2>&1)" in
            *--retry-on-http-error*) set -- "$@" --tries=4 --waitretry=2 "--retry-on-http-error=408,429,500,502,503,504" ;;
        esac
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            set -- "$@" "--header=Authorization: Bearer $GITHUB_TOKEN"
        fi
        "$@" -O "$fa_out" "$fa_url"
    fi
}

downloadFile() {
    LATEST_RELEASE_TAG=$1

    # Asset naming convention: spice_{os}_{arch}.tar.gz
    # Windows uses Install.ps1, which downloads spice.exe_{os}_{arch}.tar.gz.
    SPICE_CLI_ARTIFACT="${SPICE_CLI_FILENAME}_${OS}_${ARCH}.tar.gz"
    DOWNLOAD_BASE="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download"
    DOWNLOAD_URL="${DOWNLOAD_BASE}/${LATEST_RELEASE_TAG}/${SPICE_CLI_ARTIFACT}"

    SPICE_TMP_ROOT=$(mktemp -dt spice-install-XXXXXX) || fail "Failed to create a temporary directory."
    ARTIFACT_TMP_FILE="$SPICE_TMP_ROOT/$SPICE_CLI_ARTIFACT"

    info "Downloading $DOWNLOAD_URL ..."
    # `--fail` keeps an HTTP error page from being saved as the archive.
    if ! fetchAsset "$DOWNLOAD_URL" "$ARTIFACT_TMP_FILE" || [ ! -s "$ARTIFACT_TMP_FILE" ]; then
        fail "Failed to download $DOWNLOAD_URL" \
            "Check that release $LATEST_RELEASE_TAG exists: https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases"
    fi
    info "Download successful ($(du -h "$ARTIFACT_TMP_FILE" | awk '{ print $1 }'))"

    mkdir -p "$SPICE_TMP_ROOT/verify"
    verifyDownload "$ARTIFACT_TMP_FILE" "$SPICE_CLI_ARTIFACT" "$LATEST_RELEASE_TAG" "$SPICE_TMP_ROOT/verify"
}

# Runs the extracted spice from the temporary directory and exits, leaving any installed spice as
# it is, unless it runs here and reports the version that was asked for. A binary for another libc,
# an older glibc or another CPU fails here, not after it has replaced a working one.
checkExtractedRuns() {
    ce_out="$SPICE_TMP_ROOT/version.out"
    ce_first="$SPICE_TMP_ROOT/version.first"
    ce_status=0
    (cd "$SPICE_TMP_ROOT" && "$EXTRACTED_CLI" --version) > "$ce_out" 2>&1 < /dev/null || ce_status=$?
    cp "$ce_out" "$ce_first"
    # A CLI that ran but rejected --version is asked with `version`. 126, 127 and signals mean it
    # did not run at all.
    if [ "$ce_status" -ne 0 ] && [ "$ce_status" -ne 126 ] && [ "$ce_status" -ne 127 ] && [ "$ce_status" -lt 128 ]; then
        ce_status=0
        (cd "$SPICE_TMP_ROOT" && "$EXTRACTED_CLI" version) > "$ce_out" 2>&1 < /dev/null || ce_status=$?
    fi
    if [ "$ce_status" -ne 0 ]; then
        printf '%s\n' "The downloaded spice $LATEST_RELEASE_TAG does not run on this system (exit status $ce_status), so it was not installed. It printed:" >&2
        sed 's/^/  /' "$ce_first" >&2
        fail "See the supported systems: $SPICE_SYSTEM_REQUIREMENTS_URL" \
            "Nothing was installed."
    fi

    ce_reported=$(awk 'match($0, /[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?/) { print substr($0, RSTART, RLENGTH); exit }' "$ce_out")
    if [ -z "$ce_reported" ]; then
        warn "Could not read a version from the downloaded spice, which printed: $(cat "$ce_out")"
    elif [ "$ce_reported" != "${LATEST_RELEASE_TAG#v}" ]; then
        fail "The downloaded spice reports version $ce_reported, not ${LATEST_RELEASE_TAG#v}, so it was not installed." \
            "Report this at https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/issues. Nothing was installed."
    fi
}

# Extracts spice, checks that it runs, then moves it into place in one rename. The new file
# takes the name and the old one keeps running: a `spice` running from the install directory
# does not block the upgrade, as writing over it in place would on Linux ("Text file busy"), and
# a failure at any step leaves the installed spice as it was.
installFile() {
    info "Extracting archive..."
    mkdir -p "$SPICE_TMP_ROOT/extract"
    if ! tar -xzf "$ARTIFACT_TMP_FILE" -C "$SPICE_TMP_ROOT/extract" 2>/dev/null; then
        fail "Failed to extract archive" \
            "The downloaded file may be corrupted or is not a valid tar.gz archive"
    fi

    EXTRACTED_CLI="$SPICE_TMP_ROOT/extract/$SPICE_CLI_FILENAME"
    if [ ! -f "$EXTRACTED_CLI" ] || [ -L "$EXTRACTED_CLI" ]; then
        printf '%s\n' "Failed to unpack Spice CLI executable." "Expected file: $EXTRACTED_CLI" "Archive contents:" >&2
        tar -tzf "$ARTIFACT_TMP_FILE" 2>/dev/null | awk 'NR <= 10' >&2
        exit 1
    fi
    chmod 755 "$EXTRACTED_CLI"
    checkExtractedRuns

    # A plain mkdir first: it needs no sudo when the user can create the directory.
    if [ ! -d "$CLI_DIR" ] && ! mkdir -p "$CLI_DIR" 2>/dev/null; then
        USE_SUDO="true"
        runAsRoot mkdir -p "$CLI_DIR" || fail "Could not create $CLI_DIR. Nothing was installed."
    fi
    if [ -d "$CLI_FILE" ] && [ ! -L "$CLI_FILE" ]; then
        fail "$CLI_FILE is a directory, so spice cannot be installed there. Remove it, or set SPICE_CLI_INSTALL_DIR to another directory." \
            "Nothing was installed."
    fi

    # The new spice is copied next to the old one, so the rename stays on one filesystem.
    STAGED_FILE="$CLI_DIR/.$SPICE_CLI_FILENAME.install.$$"
    if ! runAsRoot cp "$EXTRACTED_CLI" "$STAGED_FILE" || ! runAsRoot chmod 755 "$STAGED_FILE"; then
        fail "Could not write $STAGED_FILE." \
            "Nothing was installed. Any spice already in $CLI_DIR is unchanged."
    fi
    if ! runAsRoot mv -f "$STAGED_FILE" "$CLI_FILE"; then
        fail "Could not move the new spice to $CLI_FILE." \
            "Nothing was installed. Any spice already in $CLI_DIR is unchanged."
    fi
    STAGED_FILE=""
    [ -x "$CLI_FILE" ] || fail "Failed to install $SPICE_CLI_FILENAME: $CLI_FILE is missing or not executable."
    info "Installed spice ${LATEST_RELEASE_TAG} into $CLI_DIR."
}

fail_trap() {
    result=$?
    trap - EXIT
    if [ "$result" != "0" ] && [ "${CLI_INSTALLED:-}" = "true" ]; then
        printf '\n%s\n' "Stopped. spice is installed in $CLI_DIR." >&2
    elif [ "$result" != "0" ]; then
        printf '%s\n' "Failed to install Spice CLI" "For support, see https://spiceai.org/docs" >&2
    fi
    cleanup
    exit "$result"
}

cleanup() {
    if [ -n "${STAGED_FILE:-}" ]; then
        runAsRoot rm -f "$STAGED_FILE" 2>/dev/null || :
        STAGED_FILE=""
    fi
    if [ -n "${SPICE_TMP_ROOT:-}" ] && [ -d "$SPICE_TMP_ROOT" ]; then
        rm -rf "$SPICE_TMP_ROOT"
    fi
}

# Prints the spice in the first PATH directory that has one, or, with $1 set to a path, the first
# spice on PATH other than that one. Relative PATH entries are skipped.
spiceOnPath() {
    so_skip="${1:-}"
    so_rest="$PATH:"
    while [ -n "$so_rest" ]; do
        so_dir="${so_rest%%:*}"
        so_rest="${so_rest#*:}"
        case "$so_dir" in
            /*) ;;
            *) continue ;;
        esac
        so_dir="${so_dir%/}"
        so_candidate="$so_dir/$SPICE_CLI_FILENAME"
        if [ "$so_candidate" != "$so_skip" ] && [ -f "$so_candidate" ] && [ -x "$so_candidate" ]; then
            printf '%s' "$so_candidate"
            return 0
        fi
    done
}

# Sets OTHER_INSTALL to a spice on PATH that this install does not replace, such as a Homebrew
# install. The shell runs whichever comes first on PATH, so two installs make `spice upgrade` and
# `brew upgrade` update different copies.
noteOtherInstall() {
    OTHER_INSTALL=""
    no_other=$(spiceOnPath "$CLI_FILE")
    [ -n "$no_other" ] || return 0
    case "$no_other" in
        /opt/homebrew/*|/usr/local/*|/home/linuxbrew/*|*/.linuxbrew/*)
            if command -v brew > /dev/null 2>&1; then
                no_remove="If it is the Homebrew install, remove it with: brew uninstall spiceai/spiceai/spice"
            else
                no_remove="Remove it unless you need both."
            fi
            ;;
        *) no_remove="Remove it unless you need both." ;;
    esac
    OTHER_INSTALL="$no_other"
    OTHER_INSTALL_REMOVE="$no_remove"
}

# -----------------------------------------------------------------------------
# PATH setup. The installer adds the install directory to PATH in the profile files that the
# user's login shell ($SHELL) reads, following the split Homebrew documents: on macOS terminals
# start login shells, and on Linux they start interactive ones. Each change is a block between
# markers, so running the installer again finds and updates it, and removing the block undoes
# it. A file is changed in place, never replaced, so a profile that is a symlink stays one.
# -----------------------------------------------------------------------------

PROFILE_BEGIN="# >>> spice installer >>>"
PROFILE_END="# <<< spice installer <<<"

# The install directory as a profile line names it: "$HOME/..." under the home directory, so the
# line survives a renamed home, and the absolute path otherwise.
profilePath() {
    case "$CLI_DIR" in
        "$HOME"/*) printf '%s' "\$HOME/${CLI_DIR#"$HOME"/}" ;;
        *) printf '%s' "$CLI_DIR" ;;
    esac
}

# The first of its arguments that exists, or nothing.
firstExisting() {
    for fe_file in "$@"; do
        if [ -e "$fe_file" ]; then
            printf '%s' "$fe_file"
            return 0
        fi
    done
}

# Sets PROFILE_SYNTAX (sh, fish or csh) and PROFILE_A and PROFILE_B, the files to add the PATH
# block to (PROFILE_B may be empty), for the user's shell.
choosePathProfiles() {
    PROFILE_SYNTAX="sh"
    PROFILE_B=""
    case "$(basename "${SHELL:-sh}")" in
        bash)
            # bash reads the first of these that exists as a login shell, so a new .bash_profile
            # would hide an existing .profile.
            cp_login=$(firstExisting "$HOME/.bash_profile" "$HOME/.bash_login" "$HOME/.profile")
            if [ "$OS" = "darwin" ]; then
                PROFILE_A="${cp_login:-$HOME/.bash_profile}"
            else
                # .bashrc for the interactive shells terminals start, and the login file for
                # login shells (ssh, a desktop session) whose login file does not read .bashrc.
                PROFILE_A="$HOME/.bashrc"
                PROFILE_B="${cp_login:-$HOME/.profile}"
            fi
            ;;
        zsh)
            if [ "$OS" = "darwin" ]; then
                PROFILE_A="${ZDOTDIR:-$HOME}/.zprofile"
            else
                PROFILE_A="${ZDOTDIR:-$HOME}/.zshrc"
            fi
            ;;
        fish)
            PROFILE_SYNTAX="fish"
            PROFILE_A="${XDG_CONFIG_HOME:-$HOME/.config}/fish/conf.d/spice.fish"
            ;;
        tcsh|csh)
            PROFILE_SYNTAX="csh"
            PROFILE_A=$(firstExisting "$HOME/.tcshrc" "$HOME/.cshrc")
            PROFILE_A="${PROFILE_A:-$HOME/.cshrc}"
            ;;
        *)
            PROFILE_A="$HOME/.profile"
            ;;
    esac
}

# The lines of the PATH block for PROFILE_SYNTAX, adding directory $1 once.
pathBlockBody() {
    case "$PROFILE_SYNTAX" in
        fish)
            printf '%s\n' "# Puts the Spice CLI on PATH. Delete this file to undo." \
                "if not contains -- \"$1\" \$PATH" \
                "    set -gx PATH \"$1\" \$PATH" \
                "end"
            ;;
        csh)
            printf '%s\n' "# Puts the Spice CLI on PATH. Remove this block to undo." \
                "setenv PATH \"$1:\${PATH}\""
            ;;
        *)
            printf '%s\n' "# Puts the Spice CLI on PATH. Remove this block to undo." \
                "case \":\${PATH}:\" in *\":$1:\"*) ;; *) export PATH=\"$1:\${PATH}\" ;; esac"
            ;;
    esac
}

# The command that adds the install directory to PATH in the running shell.
currentShellCommand() {
    case "$PROFILE_SYNTAX" in
        fish) printf '%s' "set -gx PATH \"$(profilePath)\" \$PATH" ;;
        csh) printf '%s' "setenv PATH \"$(profilePath):\${PATH}\"" ;;
        *) printf '%s' "export PATH=\"$(profilePath):\$PATH\"" ;;
    esac
}

# The PATH line an installer before the marked blocks wrote, for the default directory only.
legacyPathLine() {
    case "$PROFILE_SYNTAX" in
        fish) printf '%s' "fish_add_path \$HOME/.spice/bin" ;;
        csh) printf '%s' "setenv PATH \"\$HOME/$SPICE_BIN:\$PATH\"" ;;
        *) printf '%s' "export PATH=\"\$HOME/$SPICE_BIN:\$PATH\"" ;;
    esac
}

# Adds the PATH block to profile $1, or brings an existing one up to date. Sets PROFILE_STATE to
# added, updated, configured (already there) or unchanged (not changed, with a warning).
writePathBlock() {
    wp_file="$1"
    wp_body=$(pathBlockBody "$(profilePath)")
    wp_dir=$(dirname "$wp_file")
    if [ ! -d "$wp_dir" ] && ! mkdir -p "$wp_dir" 2>/dev/null; then
        warn "Could not create $wp_dir, so $wp_file was not changed."
        PROFILE_STATE=unchanged
        return 0
    fi

    if [ -f "$wp_file" ] && grep -qxF "$PROFILE_BEGIN" "$wp_file"; then
        if ! grep -qxF "$PROFILE_END" "$wp_file"; then
            warn "$wp_file has a '$PROFILE_BEGIN' line with no '$PROFILE_END' line after it, so it was not changed."
            PROFILE_STATE=unchanged
            return 0
        fi
        wp_current=$(awk -v b="$PROFILE_BEGIN" -v e="$PROFILE_END" '$0 == e { inside = 0 } inside { print } $0 == b { inside = 1 }' "$wp_file")
        if [ "$wp_current" = "$wp_body" ]; then
            PROFILE_STATE=configured
            return 0
        fi
        # The first block takes the new body; any later ones are dropped. The result is written
        # back through the file, not moved over it.
        if SPICE_PATH_BLOCK="$wp_body" awk -v b="$PROFILE_BEGIN" -v e="$PROFILE_END" '
            $0 == b { if (!done) { print b; print ENVIRON["SPICE_PATH_BLOCK"]; print e; done = 1 } inside = 1; next }
            inside { if ($0 == e) { inside = 0 } next }
            { print }' "$wp_file" > "$SPICE_TMP_ROOT/profile" && cat "$SPICE_TMP_ROOT/profile" > "$wp_file"; then
            PROFILE_STATE=updated
        else
            warn "Could not update $wp_file."
            PROFILE_STATE=unchanged
        fi
        return 0
    fi

    if [ "$CLI_DIR" = "$HOME/$SPICE_BIN" ] && [ -f "$wp_file" ] && grep -qxF "$(legacyPathLine)" "$wp_file"; then
        PROFILE_STATE=configured
        return 0
    fi

    # A blank line first, unless the file is new or empty.
    wp_lead=""
    if [ -s "$wp_file" ]; then
        wp_lead="\n"
    fi
    if printf "$wp_lead%s\n%s\n%s\n" "$PROFILE_BEGIN" "$wp_body" "$PROFILE_END" >> "$wp_file" 2>/dev/null; then
        PROFILE_STATE=added
    else
        warn "Could not write to $wp_file."
        PROFILE_STATE=unchanged
    fi
}

# Puts the install directory on PATH for new shells, unless it is there already, and sets
# PATH_ACTION to what happened: already, skipped, manual, changed or configured. PATH_FILES lists
# the profiles that hold the block.
configurePath() {
    PATH_FILES=""
    choosePathProfiles
    case ":$PATH:" in
        *":$CLI_DIR:"*|*":$CLI_DIR/:"*)
            PATH_ACTION=already
            return 0
            ;;
    esac
    if [ "${SPICE_INSTALL_NO_MODIFY_PATH:-}" = "1" ]; then
        PATH_ACTION=skipped
        return 0
    fi
    # A profile line quotes the directory in double quotes; one holding a quote, `$`, a backquote,
    # a backslash or a `!` would need escaping that differs between shells, and one holding a
    # colon cannot be on PATH at all. Such a directory is left to the user. The leading $HOME of
    # a directory under the home directory is written as is, so only the rest is checked.
    cf_written=$(profilePath)
    case "${cf_written#\$HOME}" in
        *[!A-Za-z0-9\ ._/@%+=,~-]*)
            PATH_ACTION=manual
            return 0
            ;;
    esac

    PATH_ACTION=configured
    for cf_file in "$PROFILE_A" "$PROFILE_B"; do
        [ -n "$cf_file" ] || continue
        writePathBlock "$cf_file"
        case "$PROFILE_STATE" in
            added|updated)
                PATH_ACTION=changed
                PATH_FILES="${PATH_FILES:+$PATH_FILES and }$cf_file"
                ;;
            configured)
                PATH_FILES="${PATH_FILES:+$PATH_FILES and }$cf_file"
                ;;
            unchanged)
                [ "$PATH_ACTION" = "changed" ] || PATH_ACTION=manual
                ;;
        esac
    done
}

# -----------------------------------------------------------------------------
# Spice skills for AI coding agents (https://github.com/spiceai/skills). The installer offers
# them only in an interactive session: a terminal to ask on, output going to it, and neither CI
# nor NONINTERACTIVE set. An unattended install, such as a CI job, a Docker build or one whose
# output is piped to a file, is never asked and never changes an agent's configuration.
# -----------------------------------------------------------------------------

SKILLS_REPO=spiceai/skills
SKILLS_URL=https://github.com/spiceai/skills

# The agents whose own CLI installs the plugin, in the order the installer lists them. Each id is
# also the agent's command. Cursor and the rest install the skills with `npx skills add`, which
# the installer does not run: it fetches and runs a package from npm.
SKILLS_AGENTS="claude codex copilot grok"

# Whether a person can answer a question. With `curl | sh`, stdin is the script, so the question
# goes to /dev/tty, which opens only when there is a controlling terminal.
isInteractive() {
    [ -z "${CI:-}" ] && [ -z "${NONINTERACTIVE:-}" ] || return 1
    [ -t 1 ] || return 1
    ( : < /dev/tty ) 2>/dev/null
}

agentName() {
    case "$1" in
        claude) printf '%s' "Claude Code" ;;
        codex) printf '%s' "Codex" ;;
        copilot) printf '%s' "GitHub Copilot CLI" ;;
        grok) printf '%s' "Grok Build" ;;
    esac
}

# The commands that install the plugin for agent $1, as the user would type them.
agentSkillsCommands() {
    case "$1" in
        claude) printf '%s\n' "claude plugin marketplace add $SKILLS_REPO" "claude plugin install spiceai@spiceai" ;;
        codex) printf '%s\n' "codex plugin marketplace add $SKILLS_REPO" "codex plugin add spiceai@spiceai" ;;
        copilot) printf '%s\n' "copilot plugin marketplace add $SKILLS_REPO" "copilot plugin install spiceai@spiceai" ;;
        grok) printf '%s\n' "grok plugin install $SKILLS_REPO --trust" ;;
    esac
}

# Installs the Spice skills plugin for agent $1 with the agent's own CLI. The marketplace step may
# report that the marketplace is already added; whether the plugin install succeeds is what
# counts, and a plugin that is already installed is a success. Sets SKILLS_INSTALLED when it is.
installAgentSkills() {
    ia_agent="$1"
    ia_name=$(agentName "$ia_agent")
    ia_log="$SPICE_TMP_ROOT/skills-$ia_agent.log"
    ia_status=0
    info "Installing the Spice skills plugin for $ia_name..."
    # stdin is /dev/null: a question from the agent's CLI fails it rather than waiting unseen.
    case "$ia_agent" in
        claude)
            claude plugin marketplace add "$SKILLS_REPO" > "$ia_log" 2>&1 < /dev/null || :
            claude plugin install spiceai@spiceai >> "$ia_log" 2>&1 < /dev/null || ia_status=$?
            ;;
        codex)
            codex plugin marketplace add "$SKILLS_REPO" > "$ia_log" 2>&1 < /dev/null || :
            codex plugin add spiceai@spiceai >> "$ia_log" 2>&1 < /dev/null || ia_status=$?
            ;;
        copilot)
            copilot plugin marketplace add "$SKILLS_REPO" > "$ia_log" 2>&1 < /dev/null || :
            copilot plugin install spiceai@spiceai >> "$ia_log" 2>&1 < /dev/null || ia_status=$?
            ;;
        grok)
            grok plugin install "$SKILLS_REPO" --trust > "$ia_log" 2>&1 < /dev/null || ia_status=$?
            ;;
    esac
    if [ "$ia_status" -ne 0 ] && grep -qi "already installed" "$ia_log"; then
        info "The Spice skills plugin is already installed for $ia_name."
        SKILLS_INSTALLED="${SKILLS_INSTALLED:+$SKILLS_INSTALLED }$ia_agent"
    elif [ "$ia_status" -ne 0 ]; then
        warn "Could not install the Spice skills plugin for $ia_name (exit status $ia_status). spice itself is installed. $ia_name printed:"
        sed 's/^/  /' "$ia_log" >&2
        printf '%s\n' "To try again, run:" >&2
        agentSkillsCommands "$ia_agent" | sed 's/^/  /' >&2
    else
        info "Installed the Spice skills plugin for $ia_name."
        SKILLS_INSTALLED="${SKILLS_INSTALLED:+$SKILLS_INSTALLED }$ia_agent"
    fi
}

# Sets SKILLS_CHOSEN to the agents an answer picks from the listed ones ($2, space-separated):
# numbers from the list, separated by commas or spaces, "a" for all, or "n" or nothing for none.
# Fails on any other answer.
chooseAgents() {
    SKILLS_CHOSEN=""
    ca_listed="$2"
    ca_answer=$(printf '%s' "$1" | tr '[:upper:],' '[:lower:] ')
    # The answer is split into its words on purpose, with globbing off so `*` stays a word.
    set -f
    # shellcheck disable=SC2086
    set -- $ca_answer
    set +f
    [ $# -gt 0 ] || return 0
    if [ $# -eq 1 ]; then
        case "$1" in
            n|no|none) return 0 ;;
            a|all) SKILLS_CHOSEN="$ca_listed"; return 0 ;;
        esac
    fi
    for ca_word in "$@"; do
        case "$ca_word" in
            ""|*[!0-9]*|????*) SKILLS_CHOSEN=""; return 1 ;;
        esac
        ca_index=0
        ca_match=""
        for ca_agent in $ca_listed; do
            ca_index=$((ca_index + 1))
            if [ "$ca_index" -eq "$ca_word" ]; then
                ca_match="$ca_agent"
            fi
        done
        if [ -z "$ca_match" ]; then
            SKILLS_CHOSEN=""
            return 1
        fi
        case " $SKILLS_CHOSEN " in
            *" $ca_match "*) ;;
            *) SKILLS_CHOSEN="${SKILLS_CHOSEN:+$SKILLS_CHOSEN }$ca_match" ;;
        esac
    done
}

# Asks which of the agents found on PATH should get the Spice skills plugin, and installs it for
# them. Without a person to ask, prints where to find the skills instead.
offerSkills() {
    SKILLS_INSTALLED=""
    os_found=""
    for os_agent in $SKILLS_AGENTS; do
        if command -v "$os_agent" > /dev/null 2>&1; then
            os_found="${os_found:+$os_found }$os_agent"
        fi
    done
    [ -n "$os_found" ] || return 0
    if ! isInteractive; then
        info "To add the Spice skills to your AI coding agents, see $SKILLS_URL#installation"
        return 0
    fi

    info "" "Spice skills teach AI coding agents to set up and use Spice: $SKILLS_URL" \
        "Install the Spice skills plugin for:"
    os_index=0
    for os_agent in $os_found; do
        os_index=$((os_index + 1))
        info "  $os_index) $(agentName "$os_agent")"
    done
    info "  a) All of these" "  n) None"
    os_tries=0
    while :; do
        printf 'Choose one or more, such as 1,2 [n]: '
        os_answer=""
        IFS= read -r os_answer < /dev/tty || os_answer=""
        if chooseAgents "$os_answer" "$os_found"; then
            break
        fi
        os_tries=$((os_tries + 1))
        if [ "$os_tries" -ge 3 ]; then
            info "No agent chosen."
            SKILLS_CHOSEN=""
            break
        fi
        info "Answer with numbers from 1 to $os_index, a for all, or n for none."
    done
    if [ -z "$SKILLS_CHOSEN" ]; then
        info "To add the Spice skills later, see $SKILLS_URL#installation"
        return 0
    fi
    for os_agent in $SKILLS_CHOSEN; do
        installAgentSkills "$os_agent"
    done
    if [ -n "$SKILLS_INSTALLED" ]; then
        info "Start a new session of your agent and ask it to set up Spice."
        case " $SKILLS_INSTALLED " in
            *" claude "*) info "  In Claude Code: /spiceai:setup" ;;
        esac
        case " $SKILLS_INSTALLED " in
            *" codex "*) info "  In Codex: \$spiceai:setup" ;;
        esac
    fi
    info "For Cursor and other agents, see $SKILLS_URL#installation"
}

installCompleted() {
    ic_first=$(spiceOnPath)
    case "$PATH_ACTION" in
        already)
            if [ "$ic_first" = "$CLI_FILE" ]; then
                info "" "$CLI_DIR is on your PATH, so 'spice' runs the new install."
            fi
            ;;
        changed)
            info "" "Added $CLI_DIR to PATH in $PATH_FILES. New terminals will find 'spice'."
            ;;
        configured)
            info "" "$PATH_FILES already puts $CLI_DIR on PATH for new terminals."
            ;;
        skipped)
            info "" "Left your shell profiles unchanged, because SPICE_INSTALL_NO_MODIFY_PATH=1 is set."
            ;;
        manual)
            info "" "Add $CLI_DIR to PATH in your shell profile to run 'spice' from any directory."
            ;;
    esac
    if [ "$PATH_ACTION" != "already" ]; then
        info "To use spice in this terminal now, run:" "  ${bold}$(currentShellCommand)${reset}"
    fi
    if [ -n "${OTHER_INSTALL:-}" ]; then
        if [ "$PATH_ACTION" = "already" ] && [ "$ic_first" != "$CLI_FILE" ]; then
            warn "Another spice is installed at $ic_first, and your PATH finds it first, so 'spice' runs that one, not $CLI_FILE." \
                "$OTHER_INSTALL_REMOVE"
        elif [ "$PATH_ACTION" = "already" ]; then
            warn "Another spice is installed at $OTHER_INSTALL, after $CLI_DIR on your PATH." \
                "$OTHER_INSTALL_REMOVE"
        else
            warn "Another spice is installed at $OTHER_INSTALL. New terminals run $CLI_FILE, which the installer puts first on PATH." \
                "$OTHER_INSTALL_REMOVE"
        fi
    fi
    info "" "Run 'spice version' to check the install. To get started with Spice.ai, visit https://spiceai.org/docs"
    CLI_INSTALLED=true
}

main() {
    trap fail_trap EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    parseArgs "$@"
    resolveInstallDir
    checkNotSudo
    getSystemInfo
    verifySupported
    checkHttpRequestCLI
    decideSudo

    if [ "$REQUESTED_VERSION" = "latest" ]; then
        info "Getting the latest Spice.ai CLI..."
        getLatestRelease
    else
        ret_val="v$REQUESTED_VERSION"
    fi

    info "Installing Spice CLI $ret_val ..."
    noteOtherInstall
    downloadFile "$ret_val"
    installFile
    configurePath
    installCompleted
    offerSkills
    cleanup
}

main "$@"
