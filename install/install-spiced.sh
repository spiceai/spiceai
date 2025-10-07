#!/usr/bin/env bash

set -e

# colors
blue="\033[0;94m"
white="\033[0;97m"
yellow="\033[0;33m"
reset="\033[0m"

# Spice Runtime location
: ${SPICED_INSTALL_DIR:="/usr/local/bin"}

# sudo is required to copy binary to SPICE_INSTALL_DIR for linux
: ${USE_SUDO:="false"}

# Http request CLI
SPICE_HTTP_REQUEST_CLI=curl

# GitHub Organization and repo name to download release
GITHUB_ORG=spiceai
GITHUB_REPO=spiceai

# Spice Runtime filename
SPICED_FILENAME=spiced

# Variant options: "" (no variant), "models", "cuda", "metal"
: ${VARIANT:="models"}

# CUDA version: "80", "86", "87", "89", "90"
# Only used when VARIANT is "cuda"
: ${CUDA_VERSION:=""}

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

    OS=$(echo $(uname)|tr '[:upper:]' '[:lower:]')
    
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
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64 windows-x86_64)
    local current_osarch="${OS}-${ARCH}"

    for osarch in "${supported[@]}"; do
        if [ "$osarch" == "$current_osarch" ]; then
            # Validate CUDA variant combinations
            if [ "$VARIANT" == "cuda" ]; then
                if [ "$OS" != "linux" ] && [ "$OS" != "windows" ]; then
                    echo "CUDA variants are only supported on Linux and Windows"
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

    $CMD
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

checkJqInstalled() {
    if ! type "jq" 1> /dev/null 2>&1; then
        echo "'jq' is required"
        echo
        echo "To install (OSX): 'brew install jq'"
        echo "To install (Ubuntu): 'apt install jq'"
        echo
        exit 1
    fi
}

getLatestRelease() {
    local spiceReleaseUrl="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest"
    local latest_release=""

    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        latest_release=$(curl -s "$spiceReleaseUrl" | grep \"tag_name\" | awk 'NR==1{print $2}' |  sed -n 's/"\(.*\)",/\1/p')
    else
        latest_release=$(wget -q --header="Accept: application/json" -O - "$spiceReleaseUrl" | grep \"tag_name\" | awk 'NR==1{print $2}' |  sed -n 's/"\(.*\)",/\1/p')
    fi

    if [ -z "$latest_release" ]; then
        echo "Failed to get latest release information"
        exit 1
    fi

    ret_val=$latest_release
}

downloadWithRetry() {
    local url="$1"
    local output="$2"
    local attempt=1
    
    while [ $attempt -le $MAX_RETRIES ]; do
        echo "Download attempt $attempt of $MAX_RETRIES..."
        
        if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
            if curl -H "Accept:application/octet-stream" -SsL "$url" -o "$output" 2>/dev/null; then
                if [ -f "$output" ]; then
                    return 0
                fi
            fi
        else
            if wget -q --auth-no-challenge --header='Accept:application/octet-stream' "$url" -O "$output" 2>/dev/null; then
                if [ -f "$output" ]; then
                    return 0
                fi
            fi
        fi
        
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

    # Build artifact name based on variant
    local artifact_name="${SPICED_FILENAME}"
    
    # Note: Windows artifacts don't include .exe in the archive name
    # The .exe extension is only for the extracted binary
    
    # Add variant suffix
    if [ -n "$VARIANT" ]; then
        if [ "$VARIANT" == "cuda" ]; then
            artifact_name="${artifact_name}_models_cuda_${CUDA_VERSION}"
        else
            artifact_name="${artifact_name}_${VARIANT}"
        fi
    fi
    
    # Add .exe suffix for Windows in the artifact name only
    if [ "$OS" == "windows" ]; then
        artifact_name="${artifact_name}.exe"
    fi
    
    SPICED_ARTIFACT="${artifact_name}_${OS}_${ARCH}.tar.gz"
    DOWNLOAD_BASE="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download"
    DOWNLOAD_URL="${DOWNLOAD_BASE}/${LATEST_RELEASE_TAG}/${SPICED_ARTIFACT}"

    # Create the temp directory
    SPICE_TMP_ROOT=$(mktemp -dt spice-install-XXXXXX)
    ARTIFACT_TMP_FILE="$SPICE_TMP_ROOT/$SPICED_ARTIFACT"

    echo "Downloading $DOWNLOAD_URL ..."

    # Get asset ID
    local parser=". | map(select(.tag_name == \"$LATEST_RELEASE_TAG\"))[0].assets | map(select(.name == \"$SPICED_ARTIFACT\"))[0].id"
    
    local releases_url="https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases"
    local asset_id
    
    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        asset_id=$(curl -H "Accept: application/vnd.github.v3.raw" -s "$releases_url" | jq "$parser")
    else
        asset_id=$(wget -q -O - "$releases_url" | jq "$parser")
    fi
    
    if [ "$asset_id" = "null" ] || [ -z "$asset_id" ]; then
        echo "ERROR: version not found $LATEST_RELEASE_TAG or artifact $SPICED_ARTIFACT not found"
        echo "Available variants: default, models, cuda (with CUDA_VERSION), metal"
        exit 1
    fi

    local download_url="https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$asset_id"
    
    if ! downloadWithRetry "$download_url" "$ARTIFACT_TMP_FILE"; then
        echo "Failed to download $DOWNLOAD_URL after $MAX_RETRIES attempts"
        exit 1
    fi
    
    echo "Download completed successfully"
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
checkJqInstalled

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
