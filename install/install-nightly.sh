#!/usr/bin/env bash
# Spice.ai nightly installer script
# Installs both spice CLI and spiced runtime from the latest nightly build
# Version: 1.0.0 (2026-01-27)
#
# Copyright 2026 Spice AI, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# colors
blue="\033[0;94m"
green="\033[0;32m"
yellow="\033[0;33m"
red="\033[0;31m"
reset="\033[0m"

# Install directories
SPICE_BIN=".spice/bin"
: "${SPICE_CLI_INSTALL_DIR:="$HOME/$SPICE_BIN"}"
: "${SPICED_INSTALL_DIR:="/usr/local/bin"}"

# sudo is required to copy binary to SPICED_INSTALL_DIR for linux
: "${USE_SUDO:="false"}"

# GitHub Organization and repo name
GITHUB_ORG=spiceai
GITHUB_REPO=spiceai
WORKFLOW_NAME="build_nightly.yml"

# Filenames
SPICE_CLI_FILENAME=spice
SPICED_FILENAME=spiced

SPICE_CLI_FILE="${SPICE_CLI_INSTALL_DIR}/${SPICE_CLI_FILENAME}"
SPICED_FILE="${SPICED_INSTALL_DIR}/${SPICED_FILENAME}"

# Retry configuration
MAX_RETRIES=3
RETRY_DELAY=2

# Specific run URL (optional)
RUN_URL=""

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Install spice CLI and spiced runtime from nightly builds."
    echo ""
    echo "Options:"
    echo "  -r, --run-url URL    Install from a specific GitHub Actions run URL"
    echo "                       Example: https://github.com/spiceai/spiceai/actions/runs/21424866199"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  GITHUB_TOKEN         Required. GitHub personal access token with 'actions:read' permission"
    echo "  SPICE_CLI_INSTALL_DIR  Directory for spice CLI (default: ~/.spice/bin)"
    echo "  SPICED_INSTALL_DIR   Directory for spiced runtime (default: /usr/local/bin)"
    echo ""
    echo "Examples:"
    echo "  # Install latest nightly build"
    echo "  GITHUB_TOKEN=ghp_xxx $0"
    echo ""
    echo "  # Install from a specific run"
    echo "  GITHUB_TOKEN=ghp_xxx $0 --run-url https://github.com/spiceai/spiceai/actions/runs/21424866199"
}

getSystemInfo() {
    ARCH=$(uname -m)
    case $ARCH in
        armv7*) ARCH="arm";;
        arm64) ARCH="aarch64";;
        amd64) ARCH="x86_64";;
    esac

    OS=$(uname | tr '[:upper:]' '[:lower:]')

    # Determine if sudo is needed based on install directory permissions
    if [[ -d "$SPICED_INSTALL_DIR" ]]; then
        # Directory exists, check if we can write to it
        if [[ ! -w "$SPICED_INSTALL_DIR" ]]; then
            USE_SUDO="true"
        fi
    else
        # Directory doesn't exist, check parent directory
        local parent_dir
        parent_dir=$(dirname "$SPICED_INSTALL_DIR")
        if [[ ! -w "$parent_dir" ]]; then
            USE_SUDO="true"
        fi
    fi
}

verifySupported() {
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64)
    local current_osarch="${OS}-${ARCH}"

    for osarch in "${supported[@]}"; do
        if [ "$osarch" == "$current_osarch" ]; then
            return
        fi
    done

    echo -e "${red}Error:${reset} ${current_osarch} does not have a pre-built nightly binary."
    echo "Supported architectures: ${supported[*]}"
    echo "For more information, visit https://spiceai.org/docs/reference/system_requirements"
    exit 1
}

runAsRoot() {
    local CMD="$*"

    if [ $EUID -ne 0 ] && [ "$USE_SUDO" = "true" ]; then
        CMD="sudo $CMD"
    fi

    eval "$CMD"
}

checkCurl() {
    if ! type "curl" 1> /dev/null 2>&1; then
        echo -e "${red}Error:${reset} 'curl' is required"
        echo ""
        echo "To install curl (macOS): 'brew install curl'"
        echo "To install curl (Ubuntu): 'apt install curl'"
        exit 1
    fi
}

checkJq() {
    if ! type "jq" 1> /dev/null 2>&1; then
        echo -e "${red}Error:${reset} 'jq' is required for parsing JSON responses"
        echo ""
        echo "To install jq (macOS): 'brew install jq'"
        echo "To install jq (Ubuntu): 'apt install jq'"
        exit 1
    fi
}

checkGitHubToken() {
    if [ -z "$GITHUB_TOKEN" ]; then
        echo -e "${red}Error:${reset} GITHUB_TOKEN environment variable is required"
        echo ""
        echo "GitHub Actions artifacts require authentication to download."
        echo "Create a personal access token with 'actions:read' permission:"
        echo "  https://github.com/settings/tokens"
        echo ""
        echo "Then set it as an environment variable:"
        echo "  export GITHUB_TOKEN=ghp_your_token_here"
        exit 1
    fi
}

gh_curl() {
    curl -H "Authorization: token $GITHUB_TOKEN" \
         -H "Accept: application/vnd.github+json" \
         -H "X-GitHub-Api-Version: 2022-11-28" \
         "$@"
}

gh_curl_download() {
    curl -H "Authorization: token $GITHUB_TOKEN" \
         -H "Accept: application/vnd.github+json" \
         -H "X-GitHub-Api-Version: 2022-11-28" \
         -L "$@"
}

getLatestNightlyRun() {
    echo "Finding latest successful nightly build..."

    local workflow_runs_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_NAME}/runs?status=success&per_page=1"

    local response
    response=$(gh_curl -s "$workflow_runs_url")

    local run_id
    run_id=$(echo "$response" | jq -r '.workflow_runs[0].id // empty')

    if [ -z "$run_id" ]; then
        echo -e "${red}Error:${reset} Could not find any successful nightly builds"
        echo "Response: $response"
        exit 1
    fi

    local run_created_at
    run_created_at=$(echo "$response" | jq -r '.workflow_runs[0].created_at // empty')

    echo "Found nightly build: run ID $run_id (created: $run_created_at)"
    ret_val=$run_id
}

getRunIdFromUrl() {
    local url="$1"

    # Extract run ID from URL like https://github.com/spiceai/spiceai/actions/runs/21424866199
    local run_id
    run_id=$(echo "$url" | grep -oE 'runs/[0-9]+' | cut -d'/' -f2)

    if [ -z "$run_id" ]; then
        echo -e "${red}Error:${reset} Could not extract run ID from URL: $url"
        echo "Expected format: https://github.com/spiceai/spiceai/actions/runs/<run_id>"
        exit 1
    fi

    echo "Using specified run ID: $run_id"
    ret_val=$run_id
}

getArtifactDownloadUrl() {
    local run_id="$1"
    local artifact_name="$2"

    local artifacts_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/runs/${run_id}/artifacts"

    local response
    response=$(gh_curl -s "$artifacts_url")

    # Check if the response contains an error message (run not found, etc.)
    local error_message
    error_message=$(echo "$response" | jq -r '.message // empty')
    if [ -n "$error_message" ]; then
        echo -e "${red}Error:${reset} GitHub API error: $error_message"
        echo "Run ID $run_id may not exist or you may not have access to it."
        exit 1
    fi

    # Check if artifacts array exists
    local artifacts_count
    artifacts_count=$(echo "$response" | jq -r '.artifacts | length // 0')
    if [ "$artifacts_count" -eq 0 ]; then
        echo -e "${red}Error:${reset} No artifacts found for run $run_id"
        echo "The run may still be in progress, or artifacts may have expired."
        exit 1
    fi

    local artifact_id
    artifact_id=$(echo "$response" | jq -r ".artifacts[] | select(.name == \"$artifact_name\") | .id // empty")

    if [ -z "$artifact_id" ]; then
        echo -e "${red}Error:${reset} Could not find artifact '$artifact_name' in run $run_id"
        echo "Available artifacts:"
        echo "$response" | jq -r '.artifacts[].name' 2>/dev/null || echo "  (none found)"
        exit 1
    fi

    # The download URL for artifacts
    ret_val="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/artifacts/${artifact_id}/zip"
}

downloadWithRetry() {
    local url="$1"
    local output="$2"
    local attempt=1
    local delay=$RETRY_DELAY

    while [ $attempt -le $MAX_RETRIES ]; do
        echo "Download attempt $attempt of $MAX_RETRIES..."

        if gh_curl_download -SsL "$url" -o "$output" 2>/dev/null; then
            if [ -f "$output" ] && [ -s "$output" ]; then
                return 0
            fi
        fi

        if [ $attempt -lt $MAX_RETRIES ]; then
            echo "Download failed, retrying in ${delay} seconds..."
            sleep $delay
            delay=$((delay * 2))
        fi

        attempt=$((attempt + 1))
    done

    return 1
}

downloadArtifact() {
    local run_id="$1"
    local artifact_name="$2"
    local output_file="$3"

    echo "Downloading artifact: $artifact_name..."

    getArtifactDownloadUrl "$run_id" "$artifact_name"
    local download_url="$ret_val"

    if ! downloadWithRetry "$download_url" "$output_file"; then
        echo -e "${red}Error:${reset} Failed to download $artifact_name after $MAX_RETRIES attempts"
        exit 1
    fi

    echo "Downloaded: $artifact_name ($(du -h "$output_file" | cut -f1))"
}

extractArtifact() {
    local zip_file="$1"
    local extract_dir="$2"
    local binary_name="$3"

    # GitHub artifacts are zip files containing the tar.gz
    echo "Extracting artifact..."

    # First unzip the GitHub artifact wrapper
    if ! unzip -q -o "$zip_file" -d "$extract_dir" 2>/dev/null; then
        echo -e "${red}Error:${reset} Failed to extract artifact zip file"
        exit 1
    fi

    # Find and extract the tar.gz inside
    local tarball
    tarball=$(find "$extract_dir" -name "*.tar.gz" -type f | head -1)

    if [ -z "$tarball" ]; then
        echo -e "${red}Error:${reset} No tar.gz file found in artifact"
        exit 1
    fi

    if ! tar xf "$tarball" -C "$extract_dir" 2>/dev/null; then
        echo -e "${red}Error:${reset} Failed to extract tarball"
        exit 1
    fi

    local extracted_binary="$extract_dir/$binary_name"
    if [ ! -f "$extracted_binary" ]; then
        echo -e "${red}Error:${reset} Binary '$binary_name' not found after extraction"
        exit 1
    fi

    chmod +x "$extracted_binary"
    echo "Extracted: $binary_name"
}

installSpiceCli() {
    local run_id="$1"

    local artifact_name="${SPICE_CLI_FILENAME}_${OS}_${ARCH}"

    # Create temp directory
    local tmp_dir
    tmp_dir=$(mktemp -dt spice-nightly-cli-XXXXXX)

    local zip_file="$tmp_dir/artifact.zip"

    downloadArtifact "$run_id" "$artifact_name" "$zip_file"
    extractArtifact "$zip_file" "$tmp_dir" "$SPICE_CLI_FILENAME"

    # Create install directory if needed
    if [ ! -d "$SPICE_CLI_INSTALL_DIR" ]; then
        mkdir -p "$SPICE_CLI_INSTALL_DIR"
    fi

    # Install the binary
    cp "$tmp_dir/$SPICE_CLI_FILENAME" "$SPICE_CLI_INSTALL_DIR/"

    if [ -f "$SPICE_CLI_FILE" ]; then
        echo -e "${green}✓${reset} $SPICE_CLI_FILENAME installed to $SPICE_CLI_INSTALL_DIR"

        # Verify the binary works
        if "$SPICE_CLI_FILE" version >/dev/null 2>&1; then
            local version
            version=$("$SPICE_CLI_FILE" version 2>&1 | head -1)
            echo "  Version: $version"
        fi
    else
        echo -e "${red}Error:${reset} Failed to install $SPICE_CLI_FILENAME"
        rm -rf "$tmp_dir"
        exit 1
    fi

    rm -rf "$tmp_dir"
}

installSpiced() {
    local run_id="$1"

    local artifact_name="${SPICED_FILENAME}_${OS}_${ARCH}"

    # Create temp directory
    local tmp_dir
    tmp_dir=$(mktemp -dt spice-nightly-spiced-XXXXXX)

    local zip_file="$tmp_dir/artifact.zip"

    downloadArtifact "$run_id" "$artifact_name" "$zip_file"
    extractArtifact "$zip_file" "$tmp_dir" "$SPICED_FILENAME"

    # Create install directory if needed
    if [ ! -d "$SPICED_INSTALL_DIR" ]; then
        runAsRoot mkdir -p "$SPICED_INSTALL_DIR"
    fi

    # Install the binary
    if [ "$USE_SUDO" = "true" ]; then
        runAsRoot cp "$tmp_dir/$SPICED_FILENAME" "$SPICED_INSTALL_DIR/"
    else
        cp "$tmp_dir/$SPICED_FILENAME" "$SPICED_INSTALL_DIR/"
    fi

    if [ -f "$SPICED_FILE" ]; then
        echo -e "${green}✓${reset} $SPICED_FILENAME installed to $SPICED_INSTALL_DIR"

        # Verify the binary works
        if "$SPICED_FILE" --version >/dev/null 2>&1; then
            local version
            version=$("$SPICED_FILE" --version 2>&1 | head -1)
            echo "  Version: $version"
        fi
    else
        echo -e "${red}Error:${reset} Failed to install $SPICED_FILENAME"
        rm -rf "$tmp_dir"
        exit 1
    fi

    rm -rf "$tmp_dir"
}

fail_trap() {
    result=$?
    if [ "$result" != "0" ]; then
        echo -e "${red}Failed to install Spice nightly build${reset}"
        echo "For support, see https://spiceai.org/docs"
    fi
    exit $result
}

configureShellPath() {
    # Skip PATH configuration when installing to system directories
    if [[ "$SPICE_CLI_INSTALL_DIR" == "/usr/local/bin" ]] || [[ "$SPICE_CLI_INSTALL_DIR" == "/usr/bin" ]]; then
        return
    fi

    # Check if PATH already contains the install directory
    if [[ ":$PATH:" == *":$SPICE_CLI_INSTALL_DIR:"* ]]; then
        return
    fi

    echo ""
    echo -e "${yellow}Note:${reset} You may need to add the Spice CLI to your PATH:"
    echo ""
    echo "  export PATH=\"\$HOME/$SPICE_BIN:\$PATH\""
    echo ""
    echo "Add this line to your shell profile (~/.bashrc, ~/.zshrc, etc.)"
}

installCompleted() {
    echo ""
    echo -e "${green}Spice.ai nightly installation complete!${reset}"
    echo ""
    echo "Installed:"
    echo "  - spice CLI: $SPICE_CLI_FILE"
    echo "  - spiced runtime: $SPICED_FILE"
    echo ""
    echo "To get started with Spice.ai, visit https://spiceai.org/docs"
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--run-url)
            RUN_URL="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo -e "${red}Error:${reset} Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

trap "fail_trap" EXIT

echo -e "${blue}Spice.ai Nightly Installer${reset}"
echo ""

# Pre-flight checks
getSystemInfo
verifySupported
checkCurl
checkJq
checkGitHubToken

# Get run ID
if [ -n "$RUN_URL" ]; then
    getRunIdFromUrl "$RUN_URL"
else
    getLatestNightlyRun
fi
run_id="$ret_val"

echo ""
echo "Installing nightly build from run ID: $run_id"
echo "  OS: $OS"
echo "  Architecture: $ARCH"
echo ""

# Install both binaries
installSpiceCli "$run_id"
echo ""
installSpiced "$run_id"

configureShellPath
installCompleted
