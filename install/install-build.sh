#!/usr/bin/env bash
# Spice.ai version installer script
# Installs both spice CLI and spiced runtime from a build on a specific branch or commit.
#
# Usage:
#   install-version.sh              # Install latest build from trunk
#   install-version.sh <branch>     # Install latest build from a branch (e.g. release/1.111)
#   install-version.sh <sha>        # Install build for a specific commit SHA
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
: "${INSTALL_DIR:="$HOME/$SPICE_BIN"}"

# GitHub Organization and repo name
GITHUB_ORG=spiceai
GITHUB_REPO=spiceai

# Source artifact binary names (as uploaded by CI)
SPICE_CLI_FILENAME=spice
SPICED_FILENAME=spiced

# Retry configuration
MAX_RETRIES=3
RETRY_DELAY=2

# Default ref is trunk
REF="trunk"

usage() {
    echo "Usage: $0 [OPTIONS] [REF]"
    echo ""
    echo "Install spice CLI and spiced runtime from a build on a specific branch or commit."
    echo ""
    echo "Arguments:"
    echo "  REF                  Branch name or commit SHA (default: trunk)"
    echo "                       Examples: trunk, release/1.111, abc123def456..."
    echo ""
    echo "Options:"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  GITHUB_TOKEN         GitHub personal access token with 'actions:read' permission."
    echo "                       Required for downloading artifacts. Without it, only run"
    echo "                       discovery works (with lower rate limits)."
    echo "  INSTALL_DIR          Directory for binaries (default: ~/.spice/bin)"
    echo ""
    echo "Examples:"
    echo "  # Install latest build from trunk"
    echo "  GITHUB_TOKEN=ghp_xxx $0"
    echo ""
    echo "  # Install latest build from a branch"
    echo "  GITHUB_TOKEN=ghp_xxx $0 release/1.111"
    echo ""
    echo "  # Install build for a specific commit"
    echo "  GITHUB_TOKEN=ghp_xxx $0 abc123def456789..."
}

# Normalize a ref (branch name or SHA) into a filesystem-safe suffix.
# e.g. "trunk" -> "trunk", "release/1.111" -> "release-1.111", "abc123" -> "abc123"
normalize_ref() {
    local ref="$1"
    # Replace / with -
    ref="${ref//\//-}"
    # Replace any remaining non-alphanumeric chars (except - . _) with -
    echo "$ref" | sed 's/[^a-zA-Z0-9._-]/-/g'
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

    echo -e "${red}Error:${reset} ${current_osarch} does not have a pre-built binary."
    echo "Supported architectures: ${supported[*]}"
    echo "For more information, visit https://spiceai.org/docs/reference/system_requirements"
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
        echo -e "${yellow}Warning:${reset} GITHUB_TOKEN is not set."
        echo "  Unauthenticated requests are subject to lower GitHub API rate limits (60/hour)."
        echo "  Downloading artifacts requires authentication and will fail without a token."
        echo ""
        echo "  To set a token: export GITHUB_TOKEN=ghp_your_token_here"
        echo "  Create one at: https://github.com/settings/tokens (needs 'actions:read' permission)"
        echo ""
    fi
}

gh_curl() {
    local auth_args=()
    if [ -n "$GITHUB_TOKEN" ]; then
        auth_args=(-H "Authorization: token $GITHUB_TOKEN")
    fi
    curl "${auth_args[@]}" \
         -H "Accept: application/vnd.github+json" \
         -H "X-GitHub-Api-Version: 2022-11-28" \
         "$@"
}

gh_curl_download() {
    if [ -z "$GITHUB_TOKEN" ]; then
        echo -e "${red}Error:${reset} GITHUB_TOKEN is required to download artifacts."
        echo "  export GITHUB_TOKEN=ghp_your_token_here"
        exit 1
    fi
    curl -H "Authorization: token $GITHUB_TOKEN" \
         -H "Accept: application/vnd.github+json" \
         -H "X-GitHub-Api-Version: 2022-11-28" \
         -L "$@"
}

# Check if a string looks like a commit SHA (7+ hex characters)
is_commit_sha() {
    local ref="$1"
    [[ "$ref" =~ ^[0-9a-fA-F]{7,40}$ ]]
}

findRunForBranch() {
    local branch="$1"
    local artifact_name="${SPICED_FILENAME}_${OS}_${ARCH}"

    echo "Finding latest successful build on branch '$branch'..."

    local page=1
    local max_pages=5

    while [ $page -le $max_pages ]; do
        local runs_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/runs?branch=${branch}&status=success&per_page=10&page=${page}"

        local response
        response=$(gh_curl -s "$runs_url")

        local total_count
        total_count=$(echo "$response" | jq -r '.total_count // 0')
        if [ "$total_count" -eq 0 ]; then
            echo -e "${red}Error:${reset} No successful workflow runs found for branch '$branch'"
            exit 1
        fi

        local run_count
        run_count=$(echo "$response" | jq -r '.workflow_runs | length')
        if [ "$run_count" -eq 0 ]; then
            break
        fi

        # Check each run for the expected artifact
        for i in $(seq 0 $((run_count - 1))); do
            local run_id
            run_id=$(echo "$response" | jq -r ".workflow_runs[$i].id")
            local run_name
            run_name=$(echo "$response" | jq -r ".workflow_runs[$i].name")
            local run_created_at
            run_created_at=$(echo "$response" | jq -r ".workflow_runs[$i].created_at")

            # Check if this run has the artifact we need
            local artifacts_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/runs/${run_id}/artifacts?per_page=100"
            local artifacts_response
            artifacts_response=$(gh_curl -s "$artifacts_url")

            local has_artifact
            has_artifact=$(echo "$artifacts_response" | jq -r ".artifacts[] | select(.name == \"$artifact_name\") | .id // empty")

            if [ -n "$has_artifact" ]; then
                local head_sha
                head_sha=$(echo "$response" | jq -r ".workflow_runs[$i].head_sha")
                echo "Found build: run ID $run_id ($run_name, created: $run_created_at)"
                ret_val=$run_id
                RUN_HEAD_SHA="$head_sha"
                RUN_CREATED_AT="$run_created_at"
                RUN_WORKFLOW_NAME="$run_name"
                return
            fi
        done

        page=$((page + 1))
    done

    echo -e "${red}Error:${reset} No successful build with artifact '$artifact_name' found for branch '$branch'"
    echo "Searched $((max_pages * 10)) most recent successful runs."
    exit 1
}

findRunForSha() {
    local sha="$1"
    local artifact_name="${SPICED_FILENAME}_${OS}_${ARCH}"

    echo "Finding build for commit $sha..."

    local runs_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/runs?head_sha=${sha}&status=success&per_page=10"

    local response
    response=$(gh_curl -s "$runs_url")

    local total_count
    total_count=$(echo "$response" | jq -r '.total_count // 0')
    if [ "$total_count" -eq 0 ]; then
        echo -e "${red}Error:${reset} No successful workflow runs found for commit '$sha'"
        exit 1
    fi

    local run_count
    run_count=$(echo "$response" | jq -r '.workflow_runs | length')

    for i in $(seq 0 $((run_count - 1))); do
        local run_id
        run_id=$(echo "$response" | jq -r ".workflow_runs[$i].id")
        local run_name
        run_name=$(echo "$response" | jq -r ".workflow_runs[$i].name")
        local run_created_at
        run_created_at=$(echo "$response" | jq -r ".workflow_runs[$i].created_at")

        # Check if this run has the artifact we need
        local artifacts_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/runs/${run_id}/artifacts?per_page=100"
        local artifacts_response
        artifacts_response=$(gh_curl -s "$artifacts_url")

        local has_artifact
        has_artifact=$(echo "$artifacts_response" | jq -r ".artifacts[] | select(.name == \"$artifact_name\") | .id // empty")

        if [ -n "$has_artifact" ]; then
            local head_sha
            head_sha=$(echo "$response" | jq -r ".workflow_runs[$i].head_sha")
            echo "Found build: run ID $run_id ($run_name, created: $run_created_at)"
            ret_val=$run_id
            RUN_HEAD_SHA="$head_sha"
            RUN_CREATED_AT="$run_created_at"
            RUN_WORKFLOW_NAME="$run_name"
            return
        fi
    done

    echo -e "${red}Error:${reset} No successful build with artifact '$artifact_name' found for commit '$sha'"
    exit 1
}

getArtifactDownloadUrl() {
    local run_id="$1"
    local artifact_name="$2"

    local artifacts_url="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/actions/runs/${run_id}/artifacts"

    local response
    response=$(gh_curl -s "$artifacts_url")

    local error_message
    error_message=$(echo "$response" | jq -r '.message // empty')
    if [ -n "$error_message" ]; then
        echo -e "${red}Error:${reset} GitHub API error: $error_message"
        echo "Run ID $run_id may not exist or you may not have access to it."
        exit 1
    fi

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

    echo "Extracting artifact..."

    if ! unzip -q -o "$zip_file" -d "$extract_dir" 2>/dev/null; then
        echo -e "${red}Error:${reset} Failed to extract artifact zip file"
        exit 1
    fi

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
    local target_name="$2"
    local artifact_name="${SPICE_CLI_FILENAME}_${OS}_${ARCH}"

    local tmp_dir
    tmp_dir=$(mktemp -dt spice-version-cli-XXXXXX)

    local zip_file="$tmp_dir/artifact.zip"

    downloadArtifact "$run_id" "$artifact_name" "$zip_file"
    extractArtifact "$zip_file" "$tmp_dir" "$SPICE_CLI_FILENAME"

    if [ ! -d "$INSTALL_DIR" ]; then
        mkdir -p "$INSTALL_DIR"
    fi

    local target_file="$INSTALL_DIR/$target_name"
    cp "$tmp_dir/$SPICE_CLI_FILENAME" "$target_file"

    if [ -f "$target_file" ]; then
        echo -e "${green}✓${reset} $target_name installed to $INSTALL_DIR"

        if "$target_file" version >/dev/null 2>&1; then
            local version
            version=$("$target_file" version 2>&1 | head -1)
            echo "  Version: $version"
        fi
    else
        echo -e "${red}Error:${reset} Failed to install $target_name"
        rm -rf "$tmp_dir"
        exit 1
    fi

    rm -rf "$tmp_dir"
}

installSpiced() {
    local run_id="$1"
    local target_name="$2"
    local artifact_name="${SPICED_FILENAME}_${OS}_${ARCH}"

    local tmp_dir
    tmp_dir=$(mktemp -dt spice-version-spiced-XXXXXX)

    local zip_file="$tmp_dir/artifact.zip"

    downloadArtifact "$run_id" "$artifact_name" "$zip_file"
    extractArtifact "$zip_file" "$tmp_dir" "$SPICED_FILENAME"

    if [ ! -d "$INSTALL_DIR" ]; then
        mkdir -p "$INSTALL_DIR"
    fi

    local target_file="$INSTALL_DIR/$target_name"
    cp "$tmp_dir/$SPICED_FILENAME" "$target_file"

    if [ -f "$target_file" ]; then
        echo -e "${green}✓${reset} $target_name installed to $INSTALL_DIR"

        if "$target_file" --version >/dev/null 2>&1; then
            local version
            version=$("$target_file" --version 2>&1 | head -1)
            echo "  Version: $version"
        fi
    else
        echo -e "${red}Error:${reset} Failed to install $target_name"
        rm -rf "$tmp_dir"
        exit 1
    fi

    rm -rf "$tmp_dir"
}

fail_trap() {
    result=$?
    if [ "$result" != "0" ]; then
        echo -e "${red}Failed to install Spice build${reset}"
        echo "For support, see https://spiceai.org/docs"
    fi
    exit $result
}

configureShellPath() {
    if [[ "$INSTALL_DIR" == "/usr/local/bin" ]] || [[ "$INSTALL_DIR" == "/usr/bin" ]]; then
        return
    fi

    if [[ ":$PATH:" == *":$INSTALL_DIR:"* ]]; then
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
    local cli_name="$1"
    local daemon_name="$2"
    local run_id="$3"

    local run_url="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/actions/runs/${run_id}"
    local commit_url="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/commit/${RUN_HEAD_SHA}"
    local short_sha="${RUN_HEAD_SHA:0:10}"

    echo ""
    echo -e "${green}Spice.ai installation complete!${reset}"
    echo ""
    echo "Build:"
    echo "  Workflow:  $RUN_WORKFLOW_NAME"
    echo "  Run:       $run_url"
    echo "  Commit:    $commit_url"
    echo "  Built:     $RUN_CREATED_AT"
    echo ""
    echo "Installed:"
    echo "  $INSTALL_DIR/$cli_name"
    echo "  $INSTALL_DIR/$daemon_name"
    echo ""

    # Show versions
    local cli_path="$INSTALL_DIR/$cli_name"
    local daemon_path="$INSTALL_DIR/$daemon_name"
    if "$cli_path" version >/dev/null 2>&1; then
        echo "Versions:"
        echo "  $cli_name:  $($cli_path version 2>&1 | head -1)"
        if "$daemon_path" --version >/dev/null 2>&1; then
            echo "  $daemon_name: $($daemon_path --version 2>&1 | head -1)"
        fi
        echo ""
    fi

    echo "To get started with Spice.ai, visit https://spiceai.org/docs"
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo -e "${red}Error:${reset} Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            REF="$1"
            shift
            ;;
    esac
done

trap "fail_trap" EXIT

echo -e "${blue}Spice.ai Version Installer${reset}"
echo ""

# Pre-flight checks
getSystemInfo
verifySupported
checkCurl
checkJq
checkGitHubToken

# Find the right workflow run
if is_commit_sha "$REF"; then
    findRunForSha "$REF"
else
    findRunForBranch "$REF"
fi
run_id="$ret_val"

# Compute version-specific binary names
NORMALIZED_REF=$(normalize_ref "$REF")
SPICE_CLI_TARGET="${SPICE_CLI_FILENAME}-${NORMALIZED_REF}"
SPICED_TARGET="${SPICED_FILENAME}-${NORMALIZED_REF}"

echo ""
echo "Installing build from run ID: $run_id (ref: $REF)"
echo "  OS: $OS"
echo "  Architecture: $ARCH"
echo "  Binaries: $SPICE_CLI_TARGET, $SPICED_TARGET"
echo ""

# Install both binaries
installSpiceCli "$run_id" "$SPICE_CLI_TARGET"
echo ""
installSpiced "$run_id" "$SPICED_TARGET"

configureShellPath
installCompleted "$SPICE_CLI_TARGET" "$SPICED_TARGET" "$run_id"
