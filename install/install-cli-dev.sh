#!/usr/bin/env bash
# Spice.ai CLI dev installer script
# Installs the spice CLI from a dev build workflow run
# Version: 1.0.0 (2026-02-03)
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

# GitHub Organization and repo name
GITHUB_ORG=spiceai
GITHUB_REPO=spiceai
WORKFLOW_NAME="spice_cli_dev.yml"

# Filenames
SPICE_CLI_FILENAME=spice
SPICE_CLI_FILE="${SPICE_CLI_INSTALL_DIR}/${SPICE_CLI_FILENAME}"

# Retry configuration
MAX_RETRIES=3
RETRY_DELAY=2

# Specific run URL or branch
RUN_URL=""
BRANCH=""

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Install spice CLI from a dev build workflow run."
    echo ""
    echo "Options:"
    echo "  -r, --run-url URL    Install from a specific GitHub Actions run URL"
    echo "                       Example: https://github.com/spiceai/spiceai/actions/runs/12345678"
    echo "  -b, --branch BRANCH  Install from the latest successful build on a specific branch"
    echo "                       Example: --branch target-partitions-override"
    echo "  -l, --latest         Install from the latest successful spice_cli_dev workflow run (any branch)"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  GITHUB_TOKEN           Required. GitHub personal access token with 'actions:read' permission"
    echo "  SPICE_CLI_INSTALL_DIR  Directory for spice CLI (default: ~/.spice/bin)"
    echo ""
    echo "Examples:"
    echo "  # Install from a specific run"
    echo "  GITHUB_TOKEN=ghp_xxx $0 --run-url https://github.com/spiceai/spiceai/actions/runs/12345678"
    echo ""
    echo "  # Install from the latest build on a specific branch"
    echo "  GITHUB_TOKEN=ghp_xxx $0 --branch target-partitions-override"
    echo ""
    echo "  # Install from the latest dev build (any branch)"
    echo "  GITHUB_TOKEN=ghp_xxx $0 --latest"
}

getSystemInfo() {
    ARCH=$(uname -m)
    case $ARCH in
        armv7*) ARCH="arm";;
        arm64) ARCH="aarch64";;
        amd64) ARCH="x86_64";;
    esac

    OS=$(uname | tr '[:upper:]' '[:lower:]')
}

verifySupported() {
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64)
    local current_osarch="${OS}-${ARCH}"

    for osarch in "${supported[@]}"; do
        if [ "$osarch" == "$current_osarch" ]; then
            return
        fi
    done

    echo -e "${red}Error:${reset} ${current_osarch} does not have a pre-built dev CLI binary."
    echo "Supported architectures: ${supported[*]}"
    exit 1
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

getLatestDevRun() {
    echo "Finding latest successful dev CLI build..."

    local workflow_runs_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_NAME}/runs?status=success&per_page=1"

    local response
    response=$(gh_curl -s "$workflow_runs_url")

    local run_id
    run_id=$(echo "$response" | jq -r '.workflow_runs[0].id // empty')

    if [ -z "$run_id" ]; then
        echo -e "${red}Error:${reset} Could not find any successful dev CLI builds"
        echo "Response: $response"
        exit 1
    fi

    local run_created_at
    run_created_at=$(echo "$response" | jq -r '.workflow_runs[0].created_at // empty')

    local head_branch
    head_branch=$(echo "$response" | jq -r '.workflow_runs[0].head_branch // empty')

    echo "Found dev build: run ID $run_id (branch: $head_branch, created: $run_created_at)"
    ret_val=$run_id
}

getLatestDevRunForBranch() {
    local branch="$1"
    echo "Finding latest successful dev CLI build for branch: $branch..."

    # URL encode the branch name for the API call
    local encoded_branch
    encoded_branch=$(printf '%s' "$branch" | jq -sRr @uri)

    local workflow_runs_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_NAME}/runs?status=success&branch=${encoded_branch}&per_page=1"

    local response
    response=$(gh_curl -s "$workflow_runs_url")

    local run_id
    run_id=$(echo "$response" | jq -r '.workflow_runs[0].id // empty')

    if [ -z "$run_id" ]; then
        echo -e "${red}Error:${reset} Could not find any successful dev CLI builds for branch '$branch'"
        echo ""
        echo "Make sure:"
        echo "  1. The branch name is correct"
        echo "  2. The spice_cli_dev workflow has been run on this branch"
        echo "  3. The workflow completed successfully"
        exit 1
    fi

    local run_created_at
    run_created_at=$(echo "$response" | jq -r '.workflow_runs[0].created_at // empty')

    local head_branch
    head_branch=$(echo "$response" | jq -r '.workflow_runs[0].head_branch // empty')

    echo "Found dev build: run ID $run_id (branch: $head_branch, created: $run_created_at)"
    ret_val=$run_id
}

getRunIdFromUrl() {
    local url="$1"

    # Extract run ID from URL like https://github.com/spiceai/spiceai/actions/runs/12345678
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
    tmp_dir=$(mktemp -dt spice-dev-cli-XXXXXX)

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

fail_trap() {
    result=$?
    if [ "$result" != "0" ]; then
        echo -e "${red}Failed to install Spice dev CLI${reset}"
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
    echo -e "${green}Spice.ai dev CLI installation complete!${reset}"
    echo ""
    echo "Installed:"
    echo "  - spice CLI: $SPICE_CLI_FILE"
    echo ""
    echo "To get started with Spice.ai, visit https://spiceai.org/docs"
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------

USE_LATEST=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--run-url)
            RUN_URL="$2"
            shift 2
            ;;
        -b|--branch)
            BRANCH="$2"
            shift 2
            ;;
        -l|--latest)
            USE_LATEST=true
            shift
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

echo -e "${blue}Spice.ai Dev CLI Installer${reset}"
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
elif [ -n "$BRANCH" ]; then
    getLatestDevRunForBranch "$BRANCH"
elif [ "$USE_LATEST" = "true" ]; then
    getLatestDevRun
else
    echo -e "${red}Error:${reset} One of --run-url, --branch, or --latest is required"
    echo ""
    usage
    exit 1
fi
run_id="$ret_val"

echo ""
echo "Installing dev CLI from run ID: $run_id"
echo "  OS: $OS"
echo "  Architecture: $ARCH"
echo ""

# Install the CLI
installSpiceCli "$run_id"

configureShellPath
installCompleted
