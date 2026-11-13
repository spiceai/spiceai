#!/usr/bin/env bash

set -e

# colors
yellow="\033[0;33m"
reset="\033[0m"

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
            
            return
        fi
    done

    echo "${current_osarch} does not have a pre-built binary. For supported architectures, visit https://spiceai.org/docs/reference/system_requirements#operating-systems-and-architectures"
    exit 1
}

runAsRoot() {
    local CMD="$*"

    if [ $EUID -ne 0 ] && [ "$USE_SUDO" = "true" ]; then
        CMD="sudo $CMD"
    fi

    eval "$CMD"
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
# -----------------------------------------------------------------------------

# Release attestations are signed by build_release.yml running on a release branch.
SPICE_ATTESTATION_SIGNER='^https://github\.com/spiceai/spiceai/\.github/workflows/build_release\.yml@refs/heads/release/'

# Saves a URL to a file, sending GITHUB_TOKEN when it is set. Fails on an HTTP error. A request
# that times out or gets a 408, 429 or 5xx is tried up to 3 more times, a second apart, so a
# brief GitHub error doesn't stop the install. A 404, such as the SHA256SUMS of a release
# without one, is not retried.
verifyFetch() {
    local url="$1"
    local out="$2"
    local auth=()
    local retry=()

    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            auth=(-H "Authorization: Bearer $GITHUB_TOKEN")
        fi
        curl --fail -sSL --retry 3 --retry-delay 1 "${auth[@]}" "$url" -o "$out"
    else
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            auth=(--header="Authorization: Bearer $GITHUB_TOKEN")
        fi
        # GNU wget retries an HTTP error only when told which. BusyBox wget can't, and makes one try.
        case "$(wget --help 2>&1)" in
            *--retry-on-http-error*) retry=(--tries=4 --waitretry=1 "--retry-on-http-error=408,429,500,502,503,504") ;;
        esac
        wget -q "${retry[@]}" "${auth[@]}" -O "$out" "$url"
    fi
}

# Prints the sha256 of a file with sha256sum, shasum or openssl, whichever is installed.
sha256Of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{ print $1 }'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{ print $1 }'
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$1" | awk '{ print $NF }'
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
    local file="$1"
    local asset="$2"
    local tag="$3"
    local scratch="$4"
    local download_url="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download/${tag}/SHA256SUMS"
    local api_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/tags/${tag}"
    local expected=""
    local actual=""
    local source=""
    local reason=""
    local attested=false

    if ! actual=$(sha256Of "$file") || [ -z "$actual" ]; then
        reason="none of sha256sum, shasum or openssl is installed"
    elif verifyFetch "$download_url" "$scratch/SHA256SUMS" 2>/dev/null; then
        attested=true
        source="SHA256SUMS of release $tag"
        expected=$(awk -v f="$asset" '$2 == f || $2 == "*" f { print $1; exit }' "$scratch/SHA256SUMS")
        if [ -z "$expected" ]; then
            echo "Cannot install $asset: the SHA256SUMS of release $tag does not list it."
            echo "Nothing was installed."
            exit 1
        fi
    elif verifyFetch "$api_url" "$scratch/release.json" 2>/dev/null; then
        # The release lists SHA256SUMS when it has one. Then the download failed for another
        # reason, and the GitHub digest would skip the attestation check, so stop instead.
        if tr ',' '\n' < "$scratch/release.json" | grep -Eq '"name"[[:space:]]*:[[:space:]]*"SHA256SUMS"'; then
            echo "Cannot verify $asset: release $tag publishes SHA256SUMS, but downloading it failed ($download_url)."
            echo "Try again later. Nothing was installed."
            exit 1
        fi
        source="sha256 GitHub recorded for $asset in release $tag"
        # Pretty-printed or not, the JSON has one key per line once each comma is a line break.
        expected=$(tr ',' '\n' < "$scratch/release.json" | awk -v f="\"$asset\"" '
            /"name"[[:space:]]*:/ { found = (index($0, f) > 0); next }
            found && /"digest"[[:space:]]*:/ {
                if (match($0, /sha256:[0-9a-fA-F]+/)) { print substr($0, RSTART + 7, RLENGTH - 7) }
                exit
            }')
        if [ -z "$expected" ]; then
            reason="release $tag has no SHA256SUMS, and GitHub recorded no sha256 for $asset"
        fi
    else
        reason="the SHA256SUMS of release $tag could not be downloaded, and the GitHub API request for its asset sha256 failed (set GITHUB_TOKEN if the API rate limit was exceeded)"
    fi

    if [ -n "$reason" ]; then
        if [ "${SPICE_INSTALL_ALLOW_UNVERIFIED:-}" == "1" ]; then
            echo -e "${yellow}Warning:${reset} installing $asset unverified, because SPICE_INSTALL_ALLOW_UNVERIFIED=1 is set: $reason."
            return 0
        fi
        echo "Cannot verify $asset: $reason."
        echo "To install it without verification, run the installer with SPICE_INSTALL_ALLOW_UNVERIFIED=1, e.g.:"
        echo "  curl -fsSL $SPICE_INSTALLER_URL | SPICE_INSTALL_ALLOW_UNVERIFIED=1 bash -s ${tag#v}"
        echo "Nothing was installed."
        exit 1
    fi

    expected=$(printf '%s' "$expected" | tr '[:upper:]' '[:lower:]')
    if [ "$actual" != "$expected" ]; then
        echo "Checksum mismatch for $asset:"
        echo "  expected sha256 $expected ($source)"
        echo "  downloaded      $actual"
        echo "The download is corrupt or was altered. Nothing was installed."
        exit 1
    fi
    echo "Verified the sha256 of $asset against the $source."

    if [ "$attested" == "true" ]; then
        if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
            if ! gh attestation verify "$file" --repo "${GITHUB_ORG}/${GITHUB_REPO}" \
                --cert-identity-regex "$SPICE_ATTESTATION_SIGNER" --deny-self-hosted-runners \
                > "$scratch/attestation.log" 2>&1; then
                echo "The build attestation of $asset did not verify:"
                cat "$scratch/attestation.log"
                echo "Nothing was installed."
                exit 1
            fi
            echo "Verified the build attestation of $asset."
        else
            echo "Skipped the build attestation check of $asset: the GitHub CLI (gh) is not installed or not logged in."
        fi
    fi
}

getLatestRelease() {
    local spiceReleaseUrl="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest"
    local latest_release=""
    local response=""
    local headers=()

    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            headers=(-H "Authorization: Bearer $GITHUB_TOKEN")
        fi
        response=$(curl -fsS "${headers[@]}" "$spiceReleaseUrl") || {
            echo "Failed to get latest release information"
            exit 1
        }
    else
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            headers=(--header="Authorization: Bearer $GITHUB_TOKEN")
        fi
        response=$(wget -nv "${headers[@]}" --header="Accept: application/json" -O - "$spiceReleaseUrl") || {
            echo "Failed to get latest release information"
            exit 1
        }
    fi

    latest_release=$(printf '%s\n' "$response" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')

    if [ -z "$latest_release" ]; then
        echo "Failed to get latest release information"
        exit 1
    fi

    ret_val=$latest_release
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
        
        if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
            if curl --fail -H "Accept:application/octet-stream" -SsL "$url" -o "$output" 2>/dev/null; then
                if [ -f "$output" ] && validateRuntimeArchive "$output"; then
                    return 0
                fi
            fi
        else
            if wget -q --auth-no-challenge --header='Accept:application/octet-stream' "$url" -O "$output" 2>/dev/null; then
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

installFile() {
    tar xf "$ARTIFACT_TMP_FILE" -C "$SPICE_TMP_ROOT"
    
    # Determine the extracted filename (Windows binaries extract with .exe)
    local extracted_filename="${SPICED_FILENAME}"
    if [ "$OS" == "windows" ]; then
        extracted_filename="${SPICED_FILENAME}.exe"
    fi
    
    local tmp_root_spiced="$SPICE_TMP_ROOT/$extracted_filename"

    if [ ! -f "$tmp_root_spiced" ]; then
        echo "Failed to unpack Spice Runtime executable."
        exit 1
    fi

    chmod a+x "$tmp_root_spiced"
    
    # Copy to install directory (use runAsRoot if needed)
    if [ "$USE_SUDO" = "true" ]; then
        runAsRoot cp "$tmp_root_spiced" "$SPICED_INSTALL_DIR"
    else
        cp "$tmp_root_spiced" "$SPICED_INSTALL_DIR"
    fi

    local installed_file="$SPICED_FILE"
    if [ "$OS" == "windows" ]; then
        installed_file="${SPICED_FILE}.exe"
    fi

    if [ -f "$installed_file" ]; then
        echo "$SPICED_FILENAME installed into $SPICED_INSTALL_DIR successfully."
        
        # Print variant information
        if [ -n "$VARIANT" ]; then
            echo "Variant: $VARIANT"
            if [ "$VARIANT" == "cuda" ]; then
                echo "CUDA Version: $CUDA_VERSION"
            fi
        fi
    else
        echo "Failed to install $SPICED_FILENAME"
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

mkdir -p "$SPICED_INSTALL_DIR"

getSystemInfo
verifySupported
checkHttpRequestCLI

if [ -z "$1" ]; then
    echo "Getting the latest Spice.ai Runtime..."
    getLatestRelease
else
    ret_val=v$1
fi

echo "Installing Spice Runtime $ret_val ..."

downloadFile "$ret_val"
installFile
cleanup

installCompleted
