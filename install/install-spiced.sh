#!/usr/bin/env bash

set -e

# Colors, only for a terminal that wants them (https://no-color.org).
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    yellow=$(printf '\033[0;33m')
    reset=$(printf '\033[0m')
else
    yellow=""
    reset=""
fi

SPICE_SYSTEM_REQUIREMENTS_URL=https://spiceai.org/docs/reference/system_requirements

# Spice Runtime location
: "${SPICED_INSTALL_DIR:="/usr/local/bin"}"

# sudo is required to copy binary to SPICE_INSTALL_DIR for linux
: "${USE_SUDO:="false"}"

# Http request CLI
SPICE_HTTP_REQUEST_CLI=curl

# GitHub Organization and repo name to download release
GITHUB_ORG=spiceai
GITHUB_REPO=spiceai

# Where this installer is served, for the commands its messages suggest
SPICE_INSTALLER_URL=https://install.spiceai.org/install-spiced.sh

# Spice Runtime filename
SPICED_FILENAME=spiced

# Variant options: "" (default, includes models), "cuda", "metal"
# The default distribution now includes models, so no separate variant suffix needed
: "${VARIANT:=""}"

# CUDA version: "80", "86", "87", "89", "90"
# Only used when VARIANT is "cuda"
: "${CUDA_VERSION:=""}"

# Retry configuration
MAX_RETRIES=3
RETRY_DELAY=2

SPICED_FILE="${SPICED_INSTALL_DIR}/${SPICED_FILENAME}"

getSystemInfo() {
    ARCH=$(uname -m)
    case $ARCH in
        armv7*) ARCH="arm";;
        arm64) ARCH="aarch64";;
        amd64) ARCH="x86_64";;
    esac

    OS=$(uname | tr '[:upper:]' '[:lower:]')
    
    # Handle MINGW/MSYS/Cygwin on Windows
    case "$OS" in
        mingw*|msys*|cygwin*) OS="windows";;
    esac

    # A shell running under Rosetta 2 on an Apple silicon Mac reports x86_64. The native build
    # runs there, and is the only macOS build.
    if [ "$OS" = "darwin" ] && [ "$ARCH" = "x86_64" ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = "1" ]; then
        ARCH="aarch64"
        echo "This shell runs under Rosetta 2, so the installer installs the native Apple silicon build."
    fi

    # Most linux distro needs root permission to copy the file to /usr/local/bin
    if [[ "$OS" == "linux" || "$OS" == "darwin" ]] && [ "$SPICED_INSTALL_DIR" == "/usr/local/bin" ]; then
        USE_SUDO="true"
    fi
}

verifySupported() {
    # Check for Windows first and provide helpful error message
    if [ "$OS" == "windows" ]; then
        echo ""
        echo "Error: Native Windows is not supported for the Spice runtime (spiced) in the open source version."
        echo ""
        echo "Options:"
        echo ""
        echo "  1. Run the Spice runtime inside Windows Subsystem for Linux (WSL):"
        echo "     - Install WSL: wsl --install"
        echo "     - Open a WSL terminal (e.g., Ubuntu)"
        echo "     - Run this install script again from within WSL"
        echo "     - For more information: https://docs.microsoft.com/en-us/windows/wsl/install"
        echo ""
        echo "  2. Native Windows support is available with Spice.ai Enterprise."
        echo "     - Learn more: https://spice.ai/pricing"
        echo ""
        exit 1
    fi

    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64)
    local current_osarch="${OS}-${ARCH}"

    for osarch in "${supported[@]}"; do
        if [ "$osarch" == "$current_osarch" ]; then
            # Validate CUDA variant combinations
            if [ "$VARIANT" == "cuda" ]; then
                if [ "$OS" != "linux" ]; then
                    echo "CUDA variants are only supported on Linux"
                    exit 1
                fi
                if [ -z "$CUDA_VERSION" ]; then
                    echo "CUDA_VERSION must be set when using CUDA variant (e.g., 80, 86, 87, 89, 90)"
                    exit 1
                fi
            fi
            
            # Validate Metal variant
            if [ "$VARIANT" == "metal" ] && [ "$OS" != "darwin" ]; then
                echo "Metal variants are only supported on macOS"
                exit 1
            fi

            # The Linux builds link against glibc. A musl system (Alpine Linux, for one) cannot
            # load them, so say so before the download instead of installing a binary that
            # cannot start.
            if [ "$OS" = "linux" ]; then
                case "$(ldd --version 2>&1)" in
                    *musl*)
                        echo "The Spice runtime cannot run on this system: Spice publishes Linux builds for glibc, and this system uses musl libc (as Alpine Linux does)." >&2
                        echo "Install it on a glibc-based distribution, such as Debian, Ubuntu or Fedora, or run the spiceai/spiceai Docker image. See $SPICE_SYSTEM_REQUIREMENTS_URL" >&2
                        exit 1
                        ;;
                esac
            fi

            return
        fi
    done

    echo "${current_osarch} does not have a pre-built binary. For supported architectures, visit https://spiceai.org/docs/reference/system_requirements#operating-systems-and-architectures"
    exit 1
}

# Runs its arguments as one command, through sudo when the install directory needs it. The
# arguments pass through as they are and are never parsed again, so a path holding spaces,
# quotes or `;` stays one literal argument and never runs as shell syntax.
runAsRoot() {
    if [ $EUID -ne 0 ] && [ "$USE_SUDO" = "true" ]; then
        sudo -- "$@"
    else
        "$@"
    fi
}

checkHttpRequestCLI() {
    if type "curl" 1> /dev/null 2>&1; then
        SPICE_HTTP_REQUEST_CLI=curl
    elif type "wget" 1> /dev/null 2>&1; then
        SPICE_HTTP_REQUEST_CLI=wget
    else
        echo "Either 'curl' or 'wget' is required"
        echo
        echo "To install curl (OSX): 'brew install curl'"
        echo "To install curl (Ubuntu): 'apt install curl'"
        echo
        exit 1
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

    local spiceReleaseUrl="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest"
    local latest_release=""
    local response=""
    local headers=()

    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            headers=(-H "Authorization: Bearer $GITHUB_TOKEN")
        fi
        response=$(curl -fsS "${headers[@]}" "$spiceReleaseUrl") || latestReleaseFailed
    else
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            headers=(--header="Authorization: Bearer $GITHUB_TOKEN")
        fi
        response=$(wget -nv "${headers[@]}" --header="Accept: application/json" -O - "$spiceReleaseUrl") || latestReleaseFailed
    fi

    latest_release=$(printf '%s\n' "$response" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | awk 'NR == 1')

    if [ -z "$latest_release" ]; then
        latestReleaseFailed
    fi

    ret_val=$latest_release
}

# Exits because the latest release could not be looked up, saying how to get past a GitHub API
# rate limit, the usual cause.
latestReleaseFailed() {
    echo "Failed to get latest release information" >&2
    echo "If the GitHub API rate limit was exceeded, set GITHUB_TOKEN or pass a version to install, e.g. 'curl -fsSL $SPICE_INSTALLER_URL | bash -s 1.0.0'." >&2
    exit 1
}

# Prints the version argument without a leading "v", or exits unless it is a release version:
# X.Y.Z, with an optional -prerelease.
normalizeVersion() {
    local version="${1#v}"
    if ! printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
        echo "'$1' is not a Spice release version. Give one such as 2.4.3 or v2.4.3, or none for the latest release." >&2
        echo "Releases: https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases" >&2
        exit 1
    fi
    printf '%s' "$version"
}

validateRuntimeArchive() {
    local archive_contents archive_member archive_entry

    archive_contents=$(tar -tzf "$1" 2>/dev/null) || return 1
    archive_member=$(printf '%s\n' "$archive_contents" | grep -Fx -e "$SPICED_FILENAME" -e "./$SPICED_FILENAME") || return 1
    # Require exactly one executable entry at the archive root.
    case "$archive_member" in
        "$SPICED_FILENAME"|"./$SPICED_FILENAME") ;;
        *) return 1 ;;
    esac

    archive_entry=$(tar -tvzf "$1" "$archive_member" 2>/dev/null) || return 1
    # GNU tar and BSD tar prefix regular files with '-', links with 'l' or 'h'.
    [[ "$archive_entry" == -* && "$archive_entry" != *$'\n'* ]]
}

downloadWithRetry() {
    local url="$1"
    local output="$2"
    local attempt=1
    
    while [ $attempt -le $MAX_RETRIES ]; do
        echo "Download attempt $attempt of $MAX_RETRIES..."
        
        # A connection that does not open within 15 seconds, or a transfer slower than 1 KB/s for
        # a minute, fails the attempt instead of stalling the install.
        if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
            if curl --fail -H "Accept:application/octet-stream" -SsL --connect-timeout 15 --speed-limit 1024 --speed-time 60 "$url" -o "$output" 2>/dev/null; then
                if [ -f "$output" ] && validateRuntimeArchive "$output"; then
                    return 0
                fi
            fi
        else
            if wget -q -T 60 --auth-no-challenge --header='Accept:application/octet-stream' "$url" -O "$output" 2>/dev/null; then
                if [ -f "$output" ] && validateRuntimeArchive "$output"; then
                    return 0
                fi
            fi
        fi
        
        rm -f "$output"

        if [ $attempt -lt $MAX_RETRIES ]; then
            echo "Download failed, retrying in ${RETRY_DELAY} seconds..."
            sleep $RETRY_DELAY
            RETRY_DELAY=$((RETRY_DELAY * 2))
        fi
        
        attempt=$((attempt + 1))
    done
    
    return 1
}

downloadFile() {
    LATEST_RELEASE_TAG=$1

    # Build artifact name based on variant and OS
    # Asset naming convention:
    #   Default (VARIANT=""): spiced_{os}_{arch}.tar.gz (includes models)
    #   Metal (VARIANT="metal"): spiced_metal_{os}_{arch}.tar.gz
    #   CUDA (VARIANT="cuda"): spiced_cuda_{version}_{os}_{arch}.tar.gz
    local artifact_name="${SPICED_FILENAME}"
    
    # Add variant suffix (only for non-default variants)
    if [ -n "$VARIANT" ]; then
        if [ "$VARIANT" == "cuda" ]; then
            artifact_name="${artifact_name}_cuda_${CUDA_VERSION}"
        elif [ "$VARIANT" == "metal" ]; then
            artifact_name="${artifact_name}_metal"
        else
            artifact_name="${artifact_name}_${VARIANT}"
        fi
    fi
    
    SPICED_ARTIFACT="${artifact_name}_${OS}_${ARCH}.tar.gz"
    DOWNLOAD_BASE="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download"
    DOWNLOAD_URL="${DOWNLOAD_BASE}/${LATEST_RELEASE_TAG}/${SPICED_ARTIFACT}"

    # Create the temp directory
    SPICE_TMP_ROOT=$(mktemp -dt spice-install-XXXXXX)
    ARTIFACT_TMP_FILE="$SPICE_TMP_ROOT/$SPICED_ARTIFACT"

    echo "Downloading $DOWNLOAD_URL ..."

    if ! downloadWithRetry "$DOWNLOAD_URL" "$ARTIFACT_TMP_FILE"; then
        echo "Failed to download $DOWNLOAD_URL after $MAX_RETRIES attempts"
        exit 1
    fi
    
    echo "Download completed successfully"

    mkdir -p "$SPICE_TMP_ROOT/verify"
    verifyDownload "$ARTIFACT_TMP_FILE" "$SPICED_ARTIFACT" "$LATEST_RELEASE_TAG" "$SPICE_TMP_ROOT/verify"
}

# Runs the extracted spiced from the temporary directory and exits, leaving any installed spiced
# as it is, unless it runs here and reports the version that was asked for. A binary for another
# libc, an older glibc or another CPU fails here, not after it has replaced a working one. A CUDA
# build is not run: it needs the NVIDIA driver, which an image build often installs later.
checkExtractedRuns() {
    local extracted="$1"
    local out="$SPICE_TMP_ROOT/version.out"
    local status=0 reported

    if [ "$VARIANT" == "cuda" ]; then
        echo "Skipped running the downloaded spiced: a CUDA build needs the NVIDIA driver to start."
        return 0
    fi
    (cd "$SPICE_TMP_ROOT" && "$extracted" --version) > "$out" 2>&1 < /dev/null || status=$?
    if [ "$status" -ne 0 ]; then
        echo "The downloaded spiced $LATEST_RELEASE_TAG does not run on this system (exit status $status), so it was not installed. It printed:" >&2
        sed 's/^/  /' "$out" >&2
        echo "See the supported systems: $SPICE_SYSTEM_REQUIREMENTS_URL" >&2
        echo "Nothing was installed." >&2
        exit 1
    fi
    reported=$(awk 'match($0, /[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?/) { print substr($0, RSTART, RLENGTH); exit }' "$out")
    if [ -z "$reported" ]; then
        printf '%sWarning:%s Could not read a version from the downloaded spiced, which printed: %s\n' "$yellow" "$reset" "$(cat "$out")" >&2
    elif [ "$reported" != "${LATEST_RELEASE_TAG#v}" ]; then
        echo "The downloaded spiced reports version $reported, not ${LATEST_RELEASE_TAG#v}, so it was not installed." >&2
        echo "Report this at https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/issues. Nothing was installed." >&2
        exit 1
    fi
}

# Extracts spiced, checks that it runs, then moves it into place in one rename. The new file
# takes the name and the old one keeps running: a spiced running from the install directory does
# not block the upgrade, as writing over it in place would on Linux ("Text file busy"), and a
# failure at any step leaves the installed spiced as it was.
installFile() {
    mkdir -p "$SPICE_TMP_ROOT/extract"
    if ! tar -xzf "$ARTIFACT_TMP_FILE" -C "$SPICE_TMP_ROOT/extract"; then
        echo "Failed to extract $ARTIFACT_TMP_FILE. Nothing was installed." >&2
        exit 1
    fi

    local tmp_root_spiced="$SPICE_TMP_ROOT/extract/$SPICED_FILENAME"
    if [ ! -f "$tmp_root_spiced" ] || [ -L "$tmp_root_spiced" ]; then
        echo "Failed to unpack Spice Runtime executable." >&2
        exit 1
    fi
    chmod 755 "$tmp_root_spiced"
    checkExtractedRuns "$tmp_root_spiced"

    if [ -d "$SPICED_FILE" ] && [ ! -L "$SPICED_FILE" ]; then
        echo "$SPICED_FILE is a directory, so spiced cannot be installed there. Remove it, or set SPICED_INSTALL_DIR to another directory. Nothing was installed." >&2
        exit 1
    fi

    # The new spiced is copied next to the old one, so the rename stays on one filesystem.
    STAGED_FILE="$SPICED_INSTALL_DIR/.$SPICED_FILENAME.install.$$"
    if ! runAsRoot cp "$tmp_root_spiced" "$STAGED_FILE" || ! runAsRoot chmod 755 "$STAGED_FILE" ||
        ! runAsRoot mv -f "$STAGED_FILE" "$SPICED_FILE"; then
        echo "Could not install spiced into $SPICED_INSTALL_DIR. Nothing was installed; any spiced already there is unchanged." >&2
        exit 1
    fi
    STAGED_FILE=""

    if [ -x "$SPICED_FILE" ]; then
        echo "$SPICED_FILENAME installed into $SPICED_INSTALL_DIR successfully."

        # Print variant information
        if [ -n "$VARIANT" ]; then
            echo "Variant: $VARIANT"
            if [ "$VARIANT" == "cuda" ]; then
                echo "CUDA Version: $CUDA_VERSION"
            fi
        fi
    else
        echo "Failed to install $SPICED_FILENAME" >&2
        exit 1
    fi
}

fail_trap() {
    result=$?
    if [ "$result" != "0" ]; then
        echo "Failed to install Spice Runtime"
        echo "For support, see https://spiceai.org/docs"
    fi
    cleanup
    exit $result
}

cleanup() {
    if [ -n "${STAGED_FILE:-}" ]; then
        runAsRoot rm -f "$STAGED_FILE" 2>/dev/null || true
        STAGED_FILE=""
    fi
    if [[ -d "${SPICE_TMP_ROOT:-}" ]]; then
        rm -rf "$SPICE_TMP_ROOT"
    fi
}

installCompleted() {
    echo -e "\nTo get started with Spice.ai, visit https://spiceai.org/docs"
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
trap "fail_trap" EXIT

getSystemInfo

# Only now, once getSystemInfo has decided whether the install directory needs sudo. A plain
# mkdir before that fails under `set -e` for a non-root user whose /usr/local/bin is missing.
if [ ! -d "$SPICED_INSTALL_DIR" ]; then
    runAsRoot mkdir -p "$SPICED_INSTALL_DIR"
fi

verifySupported
checkHttpRequestCLI

if [ -z "${1:-}" ] || [ "$1" = "latest" ]; then
    echo "Getting the latest Spice.ai Runtime..."
    getLatestRelease
else
    requested_version=$(normalizeVersion "$1") || exit 1
    ret_val="v$requested_version"
fi

echo "Installing Spice Runtime $ret_val ..."

downloadFile "$ret_val"
installFile
cleanup

installCompleted
