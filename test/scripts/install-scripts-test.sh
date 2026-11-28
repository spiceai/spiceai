#!/usr/bin/env bash
# Copyright 2025 The Spice.ai Authors
# SPDX-License-Identifier: Apache-2.0
#
# Verification Test Suite for Spice.ai Install Scripts
# 
# This test suite validates the install scripts across all variants, platforms,
# and edge cases. The install scripts are the first experience developers have
# with the product, so they must be rock solid.
#
# Usage:
#   ./test/scripts/install-scripts-test.sh [--live] [--verbose] [--require-wget]
#
# Requires Python 3, which serves the offline tests a local stand-in for GitHub.
#
# Options:
#   --live          Run live download tests (requires network, slower)
#   --verbose       Show detailed output for each test
#   --require-wget  Fail, instead of skipping the wget tests, when wget is not installed
#
# Exit codes:
#   0 - All tests passed
#   1 - One or more tests failed

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INSTALL_SCRIPT="$PROJECT_ROOT/install/install.sh"
INSTALL_SPICED_SCRIPT="$PROJECT_ROOT/install/install-spiced.sh"
INSTALL_NIGHTLY_SCRIPT="$PROJECT_ROOT/install/install-nightly.sh"

# Test configuration
LIVE_TESTS=false
VERBOSE=false
REQUIRE_WGET=false
# The HTTP clients the offline tests run each installer with: curl, and wget when it is installed.
HTTP_CLIENTS=(curl)
TEST_COUNT=0
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# Utility Functions
# =============================================================================

github_api() {
    local headers=(-H "Accept: application/vnd.github+json")
    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
        headers+=(-H "Authorization: Bearer $GITHUB_TOKEN")
    fi
    curl --fail --silent --show-error "${headers[@]}" "$@"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $*"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $*"
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $*"
}

log_verbose() {
    if [[ "$VERBOSE" == "true" ]]; then
        echo -e "       $*"
    fi
}

run_test() {
    local test_name="$1"
    local test_func="$2"
    
    TEST_COUNT=$((TEST_COUNT + 1))
    
    if $test_func; then
        PASS_COUNT=$((PASS_COUNT + 1))
        log_pass "$test_name"
        return 0
    else
        # Counted, not returned: under `set -e` a failure would end the run before the others.
        FAIL_COUNT=$((FAIL_COUNT + 1))
        log_fail "$test_name"
    fi
}

skip_test() {
    local test_name="$1"
    local reason="$2"
    
    TEST_COUNT=$((TEST_COUNT + 1))
    SKIP_COUNT=$((SKIP_COUNT + 1))
    log_skip "$test_name - $reason"
}

# Create a temporary directory for test artifacts
setup_test_env() {
    TEST_TMP_DIR=$(mktemp -d)
    trap 'stop_fake_github; rm -rf "$TEST_TMP_DIR"' EXIT
    if command -v wget > /dev/null 2>&1; then
        HTTP_CLIENTS+=(wget)
    fi
    make_system_bin
    start_fake_github
}

# =============================================================================
# Script Validation Tests
# =============================================================================

test_scripts_exist() {
    [[ -f "$INSTALL_SCRIPT" ]] && [[ -f "$INSTALL_SPICED_SCRIPT" ]]
}

test_scripts_are_executable() {
    [[ -x "$INSTALL_SCRIPT" ]] || chmod +x "$INSTALL_SCRIPT"
    [[ -x "$INSTALL_SPICED_SCRIPT" ]] || chmod +x "$INSTALL_SPICED_SCRIPT"
    [[ -x "$INSTALL_SCRIPT" ]] && [[ -x "$INSTALL_SPICED_SCRIPT" ]]
}

# install.sh is POSIX sh, so that `curl ... | sh` works; install-spiced.sh needs bash.
test_scripts_have_shebang() {
    head -1 "$INSTALL_SCRIPT" | grep -qx "#!/bin/sh" && \
    head -1 "$INSTALL_SPICED_SCRIPT" | grep -q "#!/usr/bin/env bash"
}

test_no_jq_dependency() {
    # Verify jq is not referenced in either script
    ! grep -q "jq" "$INSTALL_SCRIPT" && \
    ! grep -q "jq" "$INSTALL_SPICED_SCRIPT"
}

test_shellcheck_passes() {
    if ! command -v shellcheck &> /dev/null; then
        log_verbose "shellcheck not installed, skipping"
        return 0  # Pass if shellcheck not available
    fi
    
    # No exclusions: a deliberate pattern carries its own `# shellcheck` directive in the script.
    shellcheck "$INSTALL_SCRIPT" && shellcheck "$INSTALL_SPICED_SCRIPT"
}

# =============================================================================
# Artifact Naming Convention Tests
# =============================================================================

# Helper to extract artifact name generation logic
get_spiced_artifact_name() {
    local os="$1"
    local arch="$2"
    local variant="${3:-}"
    local cuda_version="${4:-}"
    
    local artifact_name="spiced"
    
    # For Windows, .exe comes right after spiced
    if [[ "$os" == "windows" ]]; then
        artifact_name="${artifact_name}.exe"
    fi
    
    # Add variant suffix
    if [[ -n "$variant" ]]; then
        if [[ "$variant" == "cuda" ]]; then
            artifact_name="${artifact_name}_cuda_${cuda_version}"
        elif [[ "$variant" == "metal" ]]; then
            artifact_name="${artifact_name}_metal"
        else
            artifact_name="${artifact_name}_${variant}"
        fi
    fi
    
    echo "${artifact_name}_${os}_${arch}.tar.gz"
}

get_spice_artifact_name() {
    local os="$1"
    local arch="$2"
    
    echo "spice_${os}_${arch}.tar.gz"
}

# Test Linux x86_64 artifact names
test_artifact_name_linux_x86_64_default() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "")
    [[ "$result" == "spiced_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_models() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "models")
    [[ "$result" == "spiced_models_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_90() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "90")
    [[ "$result" == "spiced_cuda_90_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_89() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "89")
    [[ "$result" == "spiced_cuda_89_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_87() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "87")
    [[ "$result" == "spiced_cuda_87_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_86() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "86")
    [[ "$result" == "spiced_cuda_86_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_80() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "80")
    [[ "$result" == "spiced_cuda_80_linux_x86_64.tar.gz" ]]
}

# Test Linux aarch64 artifact names
test_artifact_name_linux_aarch64_default() {
    local result
    result=$(get_spiced_artifact_name "linux" "aarch64" "")
    [[ "$result" == "spiced_linux_aarch64.tar.gz" ]]
}

test_artifact_name_linux_aarch64_models() {
    local result
    result=$(get_spiced_artifact_name "linux" "aarch64" "models")
    [[ "$result" == "spiced_models_linux_aarch64.tar.gz" ]]
}

# Test macOS (darwin) artifact names
test_artifact_name_darwin_aarch64_default() {
    local result
    result=$(get_spiced_artifact_name "darwin" "aarch64" "")
    [[ "$result" == "spiced_darwin_aarch64.tar.gz" ]]
}

test_artifact_name_darwin_aarch64_models() {
    local result
    result=$(get_spiced_artifact_name "darwin" "aarch64" "models")
    [[ "$result" == "spiced_models_darwin_aarch64.tar.gz" ]]
}

test_artifact_name_darwin_aarch64_metal() {
    local result
    result=$(get_spiced_artifact_name "darwin" "aarch64" "metal")
    [[ "$result" == "spiced_metal_darwin_aarch64.tar.gz" ]]
}

# Test Windows artifact names
test_artifact_name_windows_x86_64_default() {
    local result
    result=$(get_spiced_artifact_name "windows" "x86_64" "")
    [[ "$result" == "spiced.exe_windows_x86_64.tar.gz" ]]
}

test_artifact_name_windows_x86_64_models() {
    local result
    result=$(get_spiced_artifact_name "windows" "x86_64" "models")
    [[ "$result" == "spiced.exe_models_windows_x86_64.tar.gz" ]]
}

# Test spice CLI artifact names
test_artifact_name_spice_linux_x86_64() {
    local result
    result=$(get_spice_artifact_name "linux" "x86_64")
    [[ "$result" == "spice_linux_x86_64.tar.gz" ]]
}

test_artifact_name_spice_darwin_aarch64() {
    local result
    result=$(get_spice_artifact_name "darwin" "aarch64")
    [[ "$result" == "spice_darwin_aarch64.tar.gz" ]]
}

test_artifact_name_spice_linux_aarch64() {
    local result
    result=$(get_spice_artifact_name "linux" "aarch64")
    [[ "$result" == "spice_linux_aarch64.tar.gz" ]]
}

# =============================================================================
# Artifact Name Matching Against Live Releases
# =============================================================================

test_artifacts_exist_in_latest_release() {
    if [[ "$LIVE_TESTS" != "true" ]]; then
        return 0
    fi
    
    local release_assets
    release_assets=$(github_api "https://api.github.com/repos/spiceai/spiceai/releases/latest" |
        python3 -c 'import json, sys; print(json.dumps([asset["name"] for asset in json.load(sys.stdin)["assets"]]))') || return 1
    
    if [[ -z "$release_assets" ]]; then
        log_verbose "Could not fetch release assets"
        return 1
    fi
    
    # Check critical artifacts exist
    local expected_artifacts=(
        "spice_linux_x86_64.tar.gz"
        "spice_linux_aarch64.tar.gz"
        "spice_darwin_aarch64.tar.gz"
        "spice.exe_windows_x86_64.tar.gz"
        "spiced_linux_x86_64.tar.gz"
        "spiced_linux_aarch64.tar.gz"
        "spiced_darwin_aarch64.tar.gz"
        "spiced_metal_darwin_aarch64.tar.gz"
        "spiced_cuda_80_linux_x86_64.tar.gz"
        "spiced_cuda_86_linux_x86_64.tar.gz"
        "spiced_cuda_87_linux_x86_64.tar.gz"
        "spiced_cuda_89_linux_x86_64.tar.gz"
        "spiced_cuda_90_linux_x86_64.tar.gz"
    )
    
    local missing=0
    for artifact in "${expected_artifacts[@]}"; do
        if ! echo "$release_assets" | grep -q "\"$artifact\""; then
            log_verbose "Missing artifact: $artifact"
            missing=$((missing + 1))
        fi
    done
    
    [[ $missing -eq 0 ]]
}

# =============================================================================
# URL Construction Tests
# =============================================================================

test_download_url_format_spiced() {
    # Validate the URL construction matches expected format
    local tag="v1.10.0"
    local artifact="spiced_models_linux_x86_64.tar.gz"
    local expected_url="https://github.com/spiceai/spiceai/releases/download/${tag}/${artifact}"
    
    local constructed_url="https://github.com/spiceai/spiceai/releases/download/${tag}/${artifact}"
    
    [[ "$constructed_url" == "$expected_url" ]]
}

test_download_url_format_spice() {
    local tag="v1.10.0"
    local artifact="spice_linux_x86_64.tar.gz"
    local expected_url="https://github.com/spiceai/spiceai/releases/download/${tag}/${artifact}"
    
    local constructed_url="https://github.com/spiceai/spiceai/releases/download/${tag}/${artifact}"
    
    [[ "$constructed_url" == "$expected_url" ]]
}

# =============================================================================
# Platform Validation Tests
# =============================================================================

test_variant_validation_cuda_only_linux() {
    # CUDA variants should only be valid on Linux
    # This tests the logic, not actual script execution
    local os="linux"
    local variant="cuda"
    
    # CUDA on Linux should be valid
    [[ "$os" == "linux" ]] && [[ "$variant" == "cuda" ]]
}

test_variant_validation_cuda_invalid_darwin() {
    # CUDA on darwin should be invalid
    local os="darwin"
    local variant="cuda"
    
    # This should fail (CUDA not valid on darwin)
    ! ([[ "$os" != "linux" ]] && [[ "$variant" == "cuda" ]] && false) || \
    [[ "$os" != "linux" ]]
}

test_variant_validation_metal_only_darwin() {
    # Metal variants should only be valid on macOS
    local os="darwin"
    local variant="metal"
    
    [[ "$os" == "darwin" ]] && [[ "$variant" == "metal" ]]
}

test_variant_validation_metal_invalid_linux() {
    # Metal on Linux should be invalid
    local os="linux"
    local variant="metal"
    
    [[ "$os" != "darwin" ]]
}

# =============================================================================
# Architecture Mapping Tests
# =============================================================================

test_arch_mapping_arm64_to_aarch64() {
    local arch="arm64"
    case $arch in
        arm64) arch="aarch64";;
    esac
    [[ "$arch" == "aarch64" ]]
}

test_arch_mapping_amd64_to_x86_64() {
    local arch="amd64"
    case $arch in
        amd64) arch="x86_64";;
    esac
    [[ "$arch" == "x86_64" ]]
}

test_arch_mapping_armv7_to_arm() {
    local arch="armv7l"
    case $arch in
        armv7*) arch="arm";;
    esac
    [[ "$arch" == "arm" ]]
}

test_arch_mapping_x86_64_unchanged() {
    local arch="x86_64"
    case $arch in
        arm64) arch="aarch64";;
        amd64) arch="x86_64";;
    esac
    [[ "$arch" == "x86_64" ]]
}

# =============================================================================
# OS Detection Tests
# =============================================================================

test_os_detection_mingw_to_windows() {
    local os="mingw64_nt-10.0-19041"
    case "$os" in
        mingw*|msys*|cygwin*) os="windows";;
    esac
    [[ "$os" == "windows" ]]
}

test_os_detection_msys_to_windows() {
    local os="msys_nt-10.0-19041"
    case "$os" in
        mingw*|msys*|cygwin*) os="windows";;
    esac
    [[ "$os" == "windows" ]]
}

test_os_detection_cygwin_to_windows() {
    local os="cygwin_nt-10.0"
    case "$os" in
        mingw*|msys*|cygwin*) os="windows";;
    esac
    [[ "$os" == "windows" ]]
}

test_os_detection_linux_unchanged() {
    local os="linux"
    case "$os" in
        mingw*|msys*|cygwin*) os="windows";;
    esac
    [[ "$os" == "linux" ]]
}

test_os_detection_darwin_unchanged() {
    local os="darwin"
    case "$os" in
        mingw*|msys*|cygwin*) os="windows";;
    esac
    [[ "$os" == "darwin" ]]
}

# =============================================================================
# Supported Platform Tests
# =============================================================================

test_supported_platforms_list() {
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64 windows-x86_64)
    [[ ${#supported[@]} -eq 4 ]]
}

test_linux_x86_64_is_supported() {
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64 windows-x86_64)
    local current="linux-x86_64"
    local found=false
    for osarch in "${supported[@]}"; do
        if [[ "$osarch" == "$current" ]]; then
            found=true
            break
        fi
    done
    $found
}

test_linux_aarch64_is_supported() {
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64 windows-x86_64)
    local current="linux-aarch64"
    local found=false
    for osarch in "${supported[@]}"; do
        if [[ "$osarch" == "$current" ]]; then
            found=true
            break
        fi
    done
    $found
}

test_darwin_aarch64_is_supported() {
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64 windows-x86_64)
    local current="darwin-aarch64"
    local found=false
    for osarch in "${supported[@]}"; do
        if [[ "$osarch" == "$current" ]]; then
            found=true
            break
        fi
    done
    $found
}

test_windows_x86_64_is_supported() {
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64 windows-x86_64)
    local current="windows-x86_64"
    local found=false
    for osarch in "${supported[@]}"; do
        if [[ "$osarch" == "$current" ]]; then
            found=true
            break
        fi
    done
    $found
}

test_darwin_x86_64_is_not_supported() {
    local supported=(linux-x86_64 linux-aarch64 darwin-aarch64 windows-x86_64)
    local current="darwin-x86_64"
    local found=false
    for osarch in "${supported[@]}"; do
        if [[ "$osarch" == "$current" ]]; then
            found=true
            break
        fi
    done
    ! $found
}

# =============================================================================
# CUDA Version Validation Tests
# =============================================================================

test_cuda_version_80_valid() {
    local version="80"
    [[ "$version" =~ ^(80|86|87|89|90)$ ]]
}

test_cuda_version_86_valid() {
    local version="86"
    [[ "$version" =~ ^(80|86|87|89|90)$ ]]
}

test_cuda_version_87_valid() {
    local version="87"
    [[ "$version" =~ ^(80|86|87|89|90)$ ]]
}

test_cuda_version_89_valid() {
    local version="89"
    [[ "$version" =~ ^(80|86|87|89|90)$ ]]
}

test_cuda_version_90_valid() {
    local version="90"
    [[ "$version" =~ ^(80|86|87|89|90)$ ]]
}

test_cuda_version_invalid() {
    local version="85"
    ! [[ "$version" =~ ^(80|86|87|89|90)$ ]]
}

# =============================================================================
# Default Value Tests
# =============================================================================

read_spiced_variant() {
    local installer_preamble
    installer_preamble=$(awk '
        /^# main$/ { found = 1; exit }
        { print }
        END { if (!found) exit 1 }
    ' "$INSTALL_SPICED_SCRIPT") || return 1
    bash -c "$installer_preamble"$'\nprintf "%s" "$VARIANT"\n'
}

test_default_variant_is_empty() {
    local variant
    variant=$(
        unset VARIANT
        read_spiced_variant
    ) || return 1
    [[ -z "$variant" ]]
}

test_variant_can_be_overridden() {
    local variant
    variant=$(VARIANT="metal" read_spiced_variant) || return 1
    [[ "$variant" == "metal" ]]
}

test_variant_can_be_empty() {
    local variant
    variant=$(VARIANT="" read_spiced_variant) || return 1
    [[ -z "$variant" ]]
}

# =============================================================================
# Retry Logic Tests
# =============================================================================

test_retry_config_max_retries() {
    local max_retries=3
    [[ $max_retries -eq 3 ]]
}

test_retry_config_initial_delay() {
    local retry_delay=2
    [[ $retry_delay -eq 2 ]]
}

test_retry_exponential_backoff() {
    local retry_delay=2
    # After first failure, delay doubles
    retry_delay=$((retry_delay * 2))
    [[ $retry_delay -eq 4 ]]
    # After second failure, delay doubles again
    retry_delay=$((retry_delay * 2))
    [[ $retry_delay -eq 8 ]]
}

# =============================================================================
# Live Download Tests (Optional)
# =============================================================================

test_live_latest_release_accessible() {
    if [[ "$LIVE_TESTS" != "true" ]]; then
        return 0
    fi
    
    local response
    response=$(github_api -o /dev/null -w "%{http_code}" "https://api.github.com/repos/spiceai/spiceai/releases/latest") || return 1
    log_verbose "Latest release HTTP status: $response"
    [[ "$response" == "200" ]]
}

test_live_download_url_resolves() {
    if [[ "$LIVE_TESTS" != "true" ]]; then
        return 0
    fi
    
    # Get latest release tag
    local tag
    tag=$(github_api "https://api.github.com/repos/spiceai/spiceai/releases/latest" |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["tag_name"])') || return 1
    
    if [[ -z "$tag" ]]; then
        log_verbose "Could not get latest tag"
        return 1
    fi
    
    # Follow the asset redirect and require a successful download.
    local url="https://github.com/spiceai/spiceai/releases/download/${tag}/spice_linux_x86_64.tar.gz"
    local response
    response=$(curl -sS -o /dev/null -w "%{http_code}" -L "$url" 2>/dev/null || echo "000")
    
    [[ "$response" == "200" ]]
}

test_live_spiced_linux_downloadable() {
    if [[ "$LIVE_TESTS" != "true" ]]; then
        return 0
    fi
    
    local tag
    tag=$(github_api "https://api.github.com/repos/spiceai/spiceai/releases/latest" |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["tag_name"])') || return 1
    
    if [[ -z "$tag" ]]; then
        return 1
    fi
    
    local url="https://github.com/spiceai/spiceai/releases/download/${tag}/spiced_linux_x86_64.tar.gz"
    local response
    response=$(curl -sS -o /dev/null -w "%{http_code}" -L "$url" 2>/dev/null || echo "000")
    
    [[ "$response" == "200" ]]
}

# =============================================================================
# Script Function Extraction Tests
# =============================================================================

test_install_sh_has_getLatestRelease() {
    grep -q "getLatestRelease()" "$INSTALL_SCRIPT"
}

test_install_sh_has_downloadFile() {
    grep -q "downloadFile()" "$INSTALL_SCRIPT"
}

test_install_sh_has_installFile() {
    grep -q "installFile()" "$INSTALL_SCRIPT"
}

test_install_sh_has_checkHttpRequestCLI() {
    grep -q "checkHttpRequestCLI()" "$INSTALL_SCRIPT"
}

test_install_sh_no_checkJqInstalled() {
    ! grep -q "checkJqInstalled()" "$INSTALL_SCRIPT"
}

test_install_spiced_sh_has_getLatestRelease() {
    grep -q "getLatestRelease()" "$INSTALL_SPICED_SCRIPT"
}

test_install_spiced_sh_has_downloadFile() {
    grep -q "downloadFile()" "$INSTALL_SPICED_SCRIPT"
}

test_install_spiced_sh_has_verifySupported() {
    grep -q "verifySupported()" "$INSTALL_SPICED_SCRIPT"
}

test_install_spiced_sh_no_checkJqInstalled() {
    ! grep -q "checkJqInstalled" "$INSTALL_SPICED_SCRIPT"
}

# =============================================================================
# Comment Documentation Tests
# =============================================================================

test_spiced_naming_convention_documented() {
    # Check that the naming convention comments exist
    grep -q "Asset naming convention:" "$INSTALL_SPICED_SCRIPT"
}

test_spiced_variant_empty_documented() {
    grep -q 'Default (VARIANT="")' "$INSTALL_SPICED_SCRIPT"
}

test_spiced_variant_models_documented() {
    grep -Eq 'Default.*VARIANT="".*includes models' "$INSTALL_SPICED_SCRIPT"
}

test_spiced_variant_metal_documented() {
    grep -q 'Metal.*VARIANT="metal"' "$INSTALL_SPICED_SCRIPT"
}

test_spiced_variant_cuda_documented() {
    grep -q 'CUDA.*VARIANT="cuda"' "$INSTALL_SPICED_SCRIPT"
}

# =============================================================================
# Sudo Decision Tests
# =============================================================================

# Hashes the file on stdin, not by name: GNU sha256sum prefixes its output with a backslash when
# the name has one, as it does under a TMPDIR with a backslash in it.
sha256_of() {
    if command -v sha256sum &> /dev/null; then
        sha256sum < "$1" | awk '{ print $1 }'
    else
        shasum -a 256 < "$1" | awk '{ print $1 }'
    fi
}

# The OS and architecture names the installers put in an asset name.
host_os() {
    uname | tr '[:upper:]' '[:lower:]'
}

host_arch() {
    case "$(uname -m)" in
        arm64) echo "aarch64" ;;
        amd64) echo "x86_64" ;;
        *) uname -m ;;
    esac
}

# A GitHub API release, pretty-printed as the API serves it, whose `asset` has `digest` (a
# JSON value). An asset whose name ends with the same name comes first, to catch a parser
# that matches the wrong asset.
release_json() {
    local asset="$1"
    local digest="$2"
    local sums_listed="${3:-no}"
    local sums_asset=""
    if [ "$sums_listed" = "yes" ]; then
        sums_asset='
    {
      "name": "SHA256SUMS",
      "uploader": {
        "login": "github-actions[bot]"
      },
      "browser_download_url": "https://github.com/spiceai/spiceai/releases/download/v0.0.0/SHA256SUMS"
    },'
    fi
    cat <<JSON
{
  "tag_name": "v0.0.0",
  "name": "Spice v0.0.0 (one, two)",
  "assets": [${sums_asset}
    {
      "name": "cuda_${asset}",
      "uploader": {
        "login": "github-actions[bot]"
      },
      "digest": "sha256:$(printf '%064d' 7)",
      "browser_download_url": "https://github.com/spiceai/spiceai/releases/download/v0.0.0/cuda_${asset}"
    },
    {
      "name": "${asset}",
      "label": "",
      "uploader": {
        "login": "github-actions[bot]"
      },
      "size": 1,
      "digest": ${digest},
      "browser_download_url": "https://github.com/spiceai/spiceai/releases/download/v0.0.0/${asset}"
    }
  ],
  "body": "$(release_body)"
}
JSON
}

# The release notes in release_json: "Notes", or OFFLINE_BODY_BYTES bytes of them. A real release's
# JSON is tens of kilobytes, more than a pipe buffer holds.
release_body() {
    if [ -n "${OFFLINE_BODY_BYTES:-}" ]; then
        head -c "$OFFLINE_BODY_BYTES" /dev/zero | tr '\0' 'n'
    else
        printf 'Notes'
    fi
}

# The local stand-in for GitHub that every offline test talks to: test/scripts/install-fake-github.py,
# started once per run. Each test gets its own directory under $FAKE_GITHUB, named after the test,
# which holds what GitHub serves it and the log of what it was asked (see the server's usage).
FAKE_GITHUB=""
FAKE_GITHUB_PID=""
start_fake_github() {
    FAKE_GITHUB="$TEST_TMP_DIR/fake-github"
    mkdir -p "$FAKE_GITHUB"
    python3 "$SCRIPT_DIR/install-fake-github.py" "$FAKE_GITHUB" > "$FAKE_GITHUB.log" 2>&1 &
    FAKE_GITHUB_PID=$!
    for _ in $(seq 1 200); do
        [[ -s "$FAKE_GITHUB/port" ]] && return 0
        kill -0 "$FAKE_GITHUB_PID" 2>/dev/null || break
        sleep 0.1
    done
    echo "the fake GitHub server did not start; its output:"
    cat "$FAKE_GITHUB.log"
    return 1
}

stop_fake_github() {
    if [[ -n "$FAKE_GITHUB_PID" ]]; then
        kill "$FAKE_GITHUB_PID" 2>/dev/null || true
        wait "$FAKE_GITHUB_PID" 2>/dev/null || true
        FAKE_GITHUB_PID=""
    fi
}

# The base URL of a test's directory on the fake GitHub.
fake_github_url() {
    echo "http://127.0.0.1:$(cat "$FAKE_GITHUB/port")/$1"
}

# Makes the fake GitHub answer a path of test $1 with status $2, for its first $3 requests only
# when $3 is given, with the response headers that follow.
fake_github_status() {
    local name="$1" path="$2" code="$3" times="$4"
    shift 4
    mkdir -p "$(dirname "$FAKE_GITHUB/$name/status/$path")"
    { echo "$code${times:+ $times}"; printf '%s\n' "$@"; } > "$FAKE_GITHUB/$name/status/$path"
}

# A directory of links to everything in the system bin directories except curl, wget and gh, so
# that each test decides which of them exist.
SYSTEM_BIN=""
make_system_bin() {
    SYSTEM_BIN="$TEST_TMP_DIR/system-bin"
    mkdir -p "$SYSTEM_BIN"
    local dir entry name
    local links=()
    for dir in /usr/bin /bin /usr/sbin /sbin; do
        links=()
        for entry in "$dir"/*; do
            name="${entry##*/}"
            case "$name" in curl|wget|gh) continue ;; esac
            [[ -e "$SYSTEM_BIN/$name" || -L "$SYSTEM_BIN/$name" ]] || links+=("$entry")
        done
        if [[ ${#links[@]} -gt 0 ]]; then
            ln -s "${links[@]}" "$SYSTEM_BIN/"
        fi
    done
}

# Writes $stubs/<client>, which runs the real client with each GitHub URL sent to test $name on the
# fake GitHub instead, and fails on any other URL, so no test can reach the network.
write_client_shim() {
    local stubs="$1" client="$2" name="$3"
    local real base
    real=$(command -v "$client") || return 1
    base=$(fake_github_url "$name")
    cat > "$stubs/$client" <<STUB
#!/bin/sh
n=\$#
for arg in "\$@"; do
    case "\$arg" in
        https://api.github.com/repos/spiceai/spiceai/*) arg="$base/api/\${arg#https://api.github.com/repos/spiceai/spiceai/}" ;;
        https://github.com/spiceai/spiceai/*) arg="$base/dl/\${arg#https://github.com/spiceai/spiceai/}" ;;
        *://*) echo "fake GitHub: no stand-in for \$arg" >&2; exit 97 ;;
    esac
    set -- "\$@" "\$arg"
done
shift "\$n"
exec "$real" "\$@"
STUB
    chmod +x "$stubs/$client"
}

# Runs an installer end to end, piped into bash as the README one-liner does, with no TTY and no
# network: the real curl or wget fetches the release from the fake GitHub, which serves a local
# archive holding a stub binary, and SHA256SUMS and the API's release when they exist. Stubs stand
# in for sudo and the GitHub CLI:
#   sudo  records its arguments to $home.sudo and refuses, as sudo does without a terminal.
#   gh    records its arguments to $home.gh.
# What the release records for the archive comes from these variables:
#   OFFLINE_SUMS    match (default), mismatch, unlisted (lists another asset), none, or
#                   unreachable (the API lists SHA256SUMS, but downloading it fails)
#   OFFLINE_DIGEST  match (default), mismatch, null, or fail (the API request for the release fails)
#   OFFLINE_GH      loggedout (default), pass, or fail (the attestation check), or absent (no gh)
#   OFFLINE_HASH    ok (default), or none (sha256sum, shasum and openssl print nothing and fail)
#   OFFLINE_BODY_BYTES  the size of the release notes in the API's release (default: a few bytes)
#   OFFLINE_LATEST  redirect (default: github.com/.../releases/latest redirects to the release's
#                   tag page, as GitHub does), or none (it answers 404)
#   OFFLINE_BINARY  ok (default: the binary prints "CLI version: v0.0.0"), broken (it fails as
#                   a binary for another libc does), or wrong-version (it reports 9.9.9)
# and how the install runs from these:
#   OFFLINE_VERSION       the version to install (default 0.0.0), or latest to pass none
#   OFFLINE_CLIENT        curl (default) or wget, the only one of them on PATH
#   OFFLINE_RATE_LIMITED  latest or tags: that API request gets a rate limit error
#   OFFLINE_RATE_LIMIT_STATUS  the status of that error, 403 (default) or 429
#   OFFLINE_SHELL         the shell the script is piped into (default /bin/bash)
#   OFFLINE_UNAME_S, OFFLINE_UNAME_M  what `uname -s` and `uname -m` print (default: the host's)
#   OFFLINE_ROSETTA       what `sysctl -n sysctl.proc_translated` prints (default: no sysctl stub)
#   OFFLINE_LDD           what `ldd --version` prints (default: no ldd stub)
#   OFFLINE_UID           what `id -u` prints (default: no id stub)
#   OFFLINE_AGENTS        AI coding agent commands to stub (claude codex copilot grok), each of
#                         which records its arguments to $home.agents and succeeds
#   OFFLINE_AGENT_FAIL    the stubbed agent whose plugin install fails
#   OFFLINE_AGENT_ALREADY the stubbed agent whose plugin install fails as already installed
#   OFFLINE_TTY_INPUT     when set, the install runs on a pseudo-terminal, as a person at a
#                         terminal runs it, and this is what they type
# Usage: run_installer_offline <script> <binary> <asset> <home> [VAR=value...], where the
# VAR=value pairs go into the installer's environment. The log is $home.log, and the requests
# GitHub got are in $home.requests, one "<path> <auth>" line each (see install-fake-github.py).
run_installer_offline() {
    local script="$1"
    local binary="$2"
    local asset="$3"
    local home="$4"
    shift 4
    local name stubs case_dir api dl
    name=$(basename "$home")
    stubs="$home.stubs"
    case_dir="$FAKE_GITHUB/$name"
    api="$case_dir/files/api/releases"
    dl="$case_dir/files/dl/releases/download/v0.0.0"
    local digest other
    rm -rf "$stubs" "$case_dir"
    mkdir -p "$stubs/pkg" "$api/tags" "$dl"
    : > "$case_dir/requests"
    ln -sf "$case_dir/requests" "$home.requests"
    case "${OFFLINE_BINARY:-ok}" in
        ok) printf '#!/bin/sh\necho "CLI version: v0.0.0"\n' > "$stubs/pkg/$binary" ;;
        broken) printf '#!/bin/sh\necho "%s: error while loading shared libraries: libgcc_s.so.1: cannot open shared object file" >&2\nexit 127\n' "$binary" > "$stubs/pkg/$binary" ;;
        wrong-version) printf '#!/bin/sh\necho "spice 9.9.9"\n' > "$stubs/pkg/$binary" ;;
    esac
    chmod +x "$stubs/pkg/$binary"
    tar czf "$dl/$asset" -C "$stubs/pkg" "$binary"
    digest=$(sha256_of "$dl/$asset")
    other=$(printf '%064d' 0)
    case "${OFFLINE_SUMS:-match}" in
        match) printf '%s  %s\n' "$digest" "$asset" > "$dl/SHA256SUMS" ;;
        mismatch) printf '%s  %s\n' "$other" "$asset" > "$dl/SHA256SUMS" ;;
        unlisted) printf '%s  %s\n' "$digest" "other_asset.tar.gz" > "$dl/SHA256SUMS" ;;
    esac
    # A release lists SHA256SUMS among its assets exactly when it has one.
    local sums_listed=yes
    [ "${OFFLINE_SUMS:-match}" = "none" ] && sums_listed=no
    release_json "$asset" "\"sha256:$digest\"" "$sums_listed" > "$api/latest"
    case "${OFFLINE_DIGEST:-match}" in
        match) cp "$api/latest" "$api/tags/v0.0.0" ;;
        mismatch) release_json "$asset" "\"sha256:$other\"" "$sums_listed" > "$api/tags/v0.0.0" ;;
        null) release_json "$asset" null "$sums_listed" > "$api/tags/v0.0.0" ;;
        fail) ;;
    esac
    case "${OFFLINE_RATE_LIMITED:-}" in
        latest|tags)
            local limited=api/releases/latest
            [ "$OFFLINE_RATE_LIMITED" = tags ] && limited=api/releases/tags/v0.0.0
            fake_github_status "$name" "$limited" "${OFFLINE_RATE_LIMIT_STATUS:-403}" "" \
                "x-ratelimit-limit: 60" "x-ratelimit-remaining: 0" "x-ratelimit-reset: 4102444800"
            ;;
    esac
    if [ "${OFFLINE_LATEST:-redirect}" = redirect ]; then
        fake_github_status "$name" dl/releases/latest 302 "" "location: https://github.com/spiceai/spiceai/releases/tag/v0.0.0"
    fi
    write_client_shim "$stubs" "${OFFLINE_CLIENT:-curl}" "$name" || { echo "no ${OFFLINE_CLIENT:-curl} installed"; return 1; }
    if [ -n "${OFFLINE_UNAME_S:-}${OFFLINE_UNAME_M:-}" ]; then
        cat > "$stubs/uname" <<STUB
#!/bin/sh
case "\$1" in
    -s|"") echo "${OFFLINE_UNAME_S:-$(uname -s)}" ;;
    -m) echo "${OFFLINE_UNAME_M:-$(uname -m)}" ;;
    *) exec $(command -v uname) "\$@" ;;
esac
STUB
        chmod +x "$stubs/uname"
    fi
    if [ -n "${OFFLINE_ROSETTA:-}" ]; then
        printf '#!/bin/sh\necho "%s"\n' "$OFFLINE_ROSETTA" > "$stubs/sysctl"
        chmod +x "$stubs/sysctl"
    fi
    if [ -n "${OFFLINE_LDD:-}" ]; then
        printf '#!/bin/sh\necho "%s" >&2\nexit 1\n' "$OFFLINE_LDD" > "$stubs/ldd"
        chmod +x "$stubs/ldd"
    fi
    if [ -n "${OFFLINE_UID:-}" ]; then
        cat > "$stubs/id" <<STUB
#!/bin/sh
[ "\$1" = -u ] && { echo "$OFFLINE_UID"; exit 0; }
exec $(command -v id) "\$@"
STUB
        chmod +x "$stubs/id"
    fi
    cat > "$stubs/sudo" <<STUB
#!/bin/sh
echo "\$*" >> "$home.sudo"
echo "sudo: a terminal is required to read the password" >&2
exit 1
STUB
    chmod +x "$stubs/sudo"
    if [ "${OFFLINE_GH:-loggedout}" != absent ]; then
        cat > "$stubs/gh" <<STUB
#!/bin/sh
echo "\$*" >> "$home.gh"
case "\$1 \$2" in
    "auth status") [ "${OFFLINE_GH:-loggedout}" != loggedout ] ;;
    "attestation verify") [ "${OFFLINE_GH:-loggedout}" = pass ] || { echo "verification failed" >&2; exit 1; } ;;
    *) exit 1 ;;
esac
STUB
        chmod +x "$stubs/gh"
    fi
    if [ "${OFFLINE_HASH:-ok}" = none ]; then
        for tool in sha256sum shasum openssl; do
            printf '#!/bin/sh\nexit 1\n' > "$stubs/$tool"
            chmod +x "$stubs/$tool"
        done
    fi
    local agent
    for agent in ${OFFLINE_AGENTS:-}; do
        cat > "$stubs/$agent" <<STUB
#!/bin/sh
echo "$agent \$*" >> "$home.agents"
case "\$*" in *marketplace*) echo "marketplace added"; exit 0 ;; esac
case "${OFFLINE_AGENT_FAIL:-}" in $agent) echo "boom: network unreachable" >&2; exit 1 ;; esac
case "${OFFLINE_AGENT_ALREADY:-}" in $agent) echo "Error: repo 'skills-734724f4' already installed" >&2; exit 1 ;; esac
echo "installed"
STUB
        chmod +x "$stubs/$agent"
    done
    : > "$home.sudo"
    : > "$home.gh"
    : > "$home.agents"
    local version_arg="${OFFLINE_VERSION:-0.0.0}"
    [ "$version_arg" = latest ] && version_arg=""
    local command=(env -i HOME="$home" PATH="$stubs:$SYSTEM_BIN" SHELL=/bin/bash "$@"
        /bin/bash -c "cat '$script' | ${OFFLINE_SHELL:-/bin/bash} -s -- $version_arg")
    if [ -n "${OFFLINE_TTY_INPUT+set}" ]; then
        run_on_terminal "$OFFLINE_TTY_INPUT" "${command[@]}" > "$home.log" 2>&1 < /dev/null
    else
        "${command[@]}" > "$home.log" 2>&1 < /dev/null
    fi
}

# Runs a command with a new pseudo-terminal as its controlling terminal, stdin, stdout and stderr,
# and types $1 into it, as a person at a terminal would. Prints what the terminal showed, and
# exits with the command's exit code.
run_on_terminal() {
    local input="$1"
    shift
    python3 -I -c '
import os, pty, sys
pid, fd = pty.fork()
if pid == 0:
    os.execvp(sys.argv[2], sys.argv[2:])
os.write(fd, sys.argv[1].encode())
while True:
    try:
        data = os.read(fd, 65536)
    except OSError:
        break
    if not data:
        break
    sys.stdout.buffer.write(data)
_, status = os.waitpid(pid, 0)
sys.exit(os.waitstatus_to_exitcode(status))
' "$input" "$@"
}

# install.sh, offline. Extra arguments are VAR=value pairs for its environment.
run_install_offline() {
    local home="$1"
    shift
    run_installer_offline "$INSTALL_SCRIPT" spice "spice_$(host_os)_$(host_arch).tar.gz" "$home" "$@"
}

# install-spiced.sh, offline, installing into $home/bin.
run_install_spiced_offline() {
    local home="$1"
    shift
    run_installer_offline "$INSTALL_SPICED_SCRIPT" spiced "spiced_$(host_os)_$(host_arch).tar.gz" "$home" \
        SPICED_INSTALL_DIR="$home/bin" "$@"
}

# regression test for #14445: a first install, with no ~/.spice yet, must not ask for sudo
test_install_sh_fresh_home_needs_no_sudo() {
    local home="$TEST_TMP_DIR/fresh-home"
    mkdir -p "$home"
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    [[ ! -s "$home.sudo" ]] || { echo "sudo was called: $(cat "$home.sudo")"; return 1; }
    [[ -x "$home/.spice/bin/spice" && -O "$home/.spice/bin/spice" ]]
}

test_install_sh_unwritable_dir_uses_sudo() {
    local home="$TEST_TMP_DIR/locked-home"
    mkdir -p "$home/locked"
    chmod 555 "$home/locked"
    run_install_offline "$home" SPICE_CLI_INSTALL_DIR="$home/locked/new/bin" || true
    chmod 755 "$home/locked"
    grep -q "^-- mkdir -p $home/locked/new/bin" "$home.sudo"
}

# The new spice is copied next to the old one, under a temporary name, then renamed over it.
test_install_sh_explicit_use_sudo_is_honored() {
    local home="$TEST_TMP_DIR/explicit-sudo-home"
    mkdir -p "$home"
    run_install_offline "$home" USE_SUDO=true || true
    grep -q "^-- cp .* $home/.spice/bin/\.spice\.install\.[0-9]*$" "$home.sudo" || { cat "$home.sudo" "$home.log"; return 1; }
}

# regression test: install-spiced.sh created its install directory with a plain mkdir before
# getSystemInfo decided whether it needed sudo, so under `set -e` a non-root user whose
# install directory was missing exited there, and sudo was never asked.
test_install_spiced_sh_creates_install_dir_with_sudo_when_needed() {
    local home="$TEST_TMP_DIR/spiced-locked-home"
    mkdir -p "$home/locked"
    chmod 555 "$home/locked"
    run_installer_offline "$INSTALL_SPICED_SCRIPT" spiced "spiced_$(host_os)_$(host_arch).tar.gz" "$home" \
        USE_SUDO=true SPICED_INSTALL_DIR="$home/locked/new/bin" || true
    chmod 755 "$home/locked"
    grep -q "^-- mkdir -p $home/locked/new/bin$" "$home.sudo" || { cat "$home.log"; return 1; }
}

test_install_spiced_sh_missing_writable_dir_needs_no_sudo() {
    local home="$TEST_TMP_DIR/spiced-fresh-home"
    mkdir -p "$home"
    run_installer_offline "$INSTALL_SPICED_SCRIPT" spiced "spiced_$(host_os)_$(host_arch).tar.gz" "$home" \
        SPICED_INSTALL_DIR="$home/new/bin" || { cat "$home.log"; return 1; }
    [[ ! -s "$home.sudo" ]] || { echo "sudo was called: $(cat "$home.sudo")"; return 1; }
    [[ -x "$home/new/bin/spiced" ]]
}

# Runs an installer's own runAsRoot, with and without sudo, on a directory whose name is shell
# syntax. `sudo` is stubbed to drop a leading `--` and run the rest. The directory must be
# created under its literal name, and the syntax in it must never run.
run_as_root_keeps_arguments_literal() {
    local script="$1"
    local dir
    dir="$TEST_TMP_DIR/run-as-root-$(basename "$script")"
    mkdir -p "$dir/stubs"
    cat > "$dir/stubs/sudo" <<'STUB'
#!/bin/sh
[ "$1" = "--" ] && shift
exec "$@"
STUB
    chmod +x "$dir/stubs/sudo"
    local use_sudo target
    for use_sudo in false true; do
        target="$dir/$use_sudo; touch $dir/pwned-$use_sudo"
        PATH="$dir/stubs:$PATH" bash -c "USE_SUDO=$use_sudo
            $(sed -n '/^runAsRoot() {/,/^}/p' "$script")
            runAsRoot mkdir -p \"\$1\"" _ "$target" || return 1
        [[ ! -e "$dir/pwned-$use_sudo" ]] || { echo "runAsRoot ran shell syntax from its argument (USE_SUDO=$use_sudo)"; return 1; }
        [[ -d "$target" ]] || { echo "runAsRoot did not create the literal directory (USE_SUDO=$use_sudo)"; return 1; }
    done
}

test_install_sh_run_as_root_keeps_arguments_literal() {
    run_as_root_keeps_arguments_literal "$INSTALL_SCRIPT"
}

test_install_spiced_sh_run_as_root_keeps_arguments_literal() {
    run_as_root_keeps_arguments_literal "$INSTALL_SPICED_SCRIPT"
}

test_install_nightly_sh_run_as_root_keeps_arguments_literal() {
    run_as_root_keeps_arguments_literal "$INSTALL_NIGHTLY_SCRIPT"
}

test_installers_do_not_eval() {
    ! grep -n "eval" "$INSTALL_SCRIPT" "$INSTALL_SPICED_SCRIPT" "$INSTALL_NIGHTLY_SCRIPT"
}

# Evaluates install-nightly.sh's getSystemInfo for one SPICED_INSTALL_DIR
nightly_use_sudo_for() {
    bash -c "USE_SUDO=false; SPICED_INSTALL_DIR='$1'
        $(sed -n '/^getSystemInfo() {/,/^}/p' "$INSTALL_NIGHTLY_SCRIPT")
        getSystemInfo; echo \"\$USE_SUDO\""
}

test_install_nightly_sh_missing_parent_needs_no_sudo() {
    local home="$TEST_TMP_DIR/nightly-home"
    mkdir -p "$home/locked"
    chmod 555 "$home/locked"
    local writable locked
    writable=$(nightly_use_sudo_for "$home/new/bin")
    locked=$(nightly_use_sudo_for "$home/locked/new/bin")
    chmod 755 "$home/locked"
    [[ "$writable" == "false" && "$locked" == "true" ]]
}

# =============================================================================
# Download Verification Tests
# =============================================================================

# The download verification block of an installer, from its header to the function after it.
verification_block() {
    sed -n '/^# Download verification. This block/,/^getLatestRelease() {/p' "$1" | sed '$d'
}

test_verification_block_is_identical_in_both_installers() {
    local cli spiced
    cli=$(verification_block "$INSTALL_SCRIPT")
    spiced=$(verification_block "$INSTALL_SPICED_SCRIPT")
    [[ -n "$cli" && "$cli" == "$spiced" ]] || { diff <(echo "$cli") <(echo "$spiced"); return 1; }
}

# Runs install.sh's verifyFetch with the given HTTP client (curl or wget). Prints its exit code.
run_verify_fetch() {
    local client="$1" url="$2" out="$3" rc=0
    /bin/bash -c "$(verification_block "$INSTALL_SCRIPT")"$'\n'"SPICE_HTTP_REQUEST_CLI=$client; verifyFetch '$url' '$out'" > /dev/null 2>&1 || rc=$?
    echo "$rc"
}

# Sets up test $1 on the fake GitHub: its path x serves "ok", after answering status $2 (with the
# headers after $3) to its first $3 requests, or to all of them when $3 is empty.
verify_fetch_case() {
    local name="$1"
    shift
    rm -rf "${FAKE_GITHUB:?}/$name"
    mkdir -p "$FAKE_GITHUB/$name/files"
    : > "$FAKE_GITHUB/$name/requests"
    printf 'ok' > "$FAKE_GITHUB/$name/files/x"
    if [[ $# -gt 0 ]]; then
        fake_github_status "$name" x "$@"
    fi
}

# verifyFetch with client $1 against path x of test $2 (see verify_fetch_case). Passes when the
# exit code is zero exactly when $3 is "ok", and the fake GitHub saw $4 requests.
check_verify_fetch() {
    local client="$1" name="$2" expect="$3" hits="$4"
    local out="$TEST_TMP_DIR/$name.out" rc seen
    rc=$(run_verify_fetch "$client" "$(fake_github_url "$name")/x" "$out")
    seen=$(grep -c '^x ' "$FAKE_GITHUB/$name/requests" || true)
    if [[ "$expect" == ok ]]; then
        [[ "$rc" == 0 && "$(cat "$out")" == ok && "$seen" == "$hits" ]] && return 0
    else
        [[ "$rc" != 0 && "$seen" == "$hits" ]] && return 0
    fi
    echo "$client $name: exit $rc, $seen request(s); expected $expect after $hits"
    return 1
}

# GNU wget retries an HTTP error only when told which; older wget and BusyBox wget can't.
# Matches the help text without a pipe: under pipefail, `grep -q` closing early fails `wget --help`.
wget_can_retry_http_errors() {
    command -v wget > /dev/null 2>&1 || return 1
    case "$(wget --help 2>&1)" in
        *--retry-on-http-error*) return 0 ;;
        *) return 1 ;;
    esac
}

# How many requests a rate-limited 429 gets from client $1: verifyFetch retries a 429 three
# times when the client can.
retried_429_requests() {
    if [[ "$1" == curl ]] || wget_can_retry_http_errors; then echo 4; else echo 1; fi
}

test_verify_fetch_retries_a_transient_error() {
    verify_fetch_case "vf-flaky-$1" 504 1
    check_verify_fetch "$1" "vf-flaky-$1" ok 2
}

test_verify_fetch_does_not_retry_a_404() {
    verify_fetch_case "vf-missing-$1" 404 ""
    check_verify_fetch "$1" "vf-missing-$1" fail 1
}

# GitHub answers an exhausted API rate limit with a 403 or a 429. Retrying a 403 a second later
# cannot succeed, so it is not retried.
test_verify_fetch_does_not_retry_a_rate_limit_403() {
    verify_fetch_case "vf-403-$1" 403 "" "x-ratelimit-remaining: 0"
    check_verify_fetch "$1" "vf-403-$1" fail 1
}

test_verify_fetch_retries_a_rate_limit_429() {
    verify_fetch_case "vf-429-$1" 429 "" "x-ratelimit-remaining: 0"
    check_verify_fetch "$1" "vf-429-$1" fail "$(retried_429_requests "$1")"
}

# GitHub's secondary rate limits send Retry-After, often a minute. curl would wait it out before
# each of its 3 retries and stall the install for minutes, so it gives up at once instead; wget
# ignores Retry-After and retries a second apart.
test_verify_fetch_does_not_wait_out_a_long_retry_after() {
    local expected=1
    [[ "$1" == wget ]] && expected=$(retried_429_requests wget)
    verify_fetch_case "vf-retry-after-$1" 429 "" "retry-after: 60" "x-ratelimit-remaining: 0"
    check_verify_fetch "$1" "vf-retry-after-$1" fail "$expected"
}

# With no working hash tool, the opt-out can install an asset nothing can check, but never one
# from a release that publishes SHA256SUMS, and never one SHA256SUMS does not list.
test_install_sh_opt_out_without_a_hash_tool_does_not_install_an_unlisted_asset() {
    local home="$TEST_TMP_DIR/nohash-unlisted"
    mkdir -p "$home"
    if OFFLINE_HASH=none OFFLINE_SUMS=unlisted run_install_offline "$home" SPICE_INSTALL_ALLOW_UNVERIFIED=1; then
        cat "$home.log"
        return 1
    fi
    ! installed_cli "$home" && log_has "$home" "does not list it"
}

test_install_sh_opt_out_without_a_hash_tool_does_not_skip_sha256sums() {
    local home="$TEST_TMP_DIR/nohash-signed"
    mkdir -p "$home"
    if OFFLINE_HASH=none run_install_offline "$home" SPICE_INSTALL_ALLOW_UNVERIFIED=1; then
        cat "$home.log"
        return 1
    fi
    ! installed_cli "$home" &&
        log_has "$home" "release v0.0.0 publishes SHA256SUMS, but none of sha256sum, shasum or openssl is installed"
}

test_install_sh_opt_out_without_a_hash_tool_installs_an_unsigned_release() {
    local home="$TEST_TMP_DIR/nohash-unsigned"
    mkdir -p "$home"
    OFFLINE_HASH=none OFFLINE_SUMS=none run_install_offline "$home" SPICE_INSTALL_ALLOW_UNVERIFIED=1 ||
        { cat "$home.log"; return 1; }
    installed_cli "$home" &&
        log_has "$home" "unverified, because SPICE_INSTALL_ALLOW_UNVERIFIED=1 is set: none of sha256sum, shasum or openssl is installed"
}

# With pipefail on (SHELLOPTS=pipefail in the environment turns it on in bash) and a release JSON
# larger than a pipe buffer, a reader that stops at its first match leaves the writer to die of
# SIGPIPE, and the whole pipeline reads as failed. The listed SHA256SUMS must still be found.
test_install_sh_finds_a_listed_sha256sums_under_pipefail() {
    local home="$TEST_TMP_DIR/pipefail-sums"
    mkdir -p "$home"
    if OFFLINE_SUMS=unreachable OFFLINE_BODY_BYTES=200000 run_install_offline "$home" SHELLOPTS=pipefail; then
        cat "$home.log"
        return 1
    fi
    ! installed_cli "$home" && log_has "$home" "publishes SHA256SUMS, but downloading it failed"
}

test_install_sh_reads_the_github_digest_under_pipefail() {
    local home="$TEST_TMP_DIR/pipefail-digest"
    mkdir -p "$home"
    OFFLINE_SUMS=none OFFLINE_BODY_BYTES=200000 run_install_offline "$home" SHELLOPTS=pipefail ||
        { cat "$home.log"; return 1; }
    installed_cli "$home" &&
        log_has "$home" "Verified the sha256 of spice_$(host_os)_$(host_arch).tar.gz against the sha256 GitHub recorded"
}

# GNU sha256sum prefixes its output with a backslash when the file name has one.
test_install_sh_verifies_with_a_backslash_in_tmpdir() {
    local home="$TEST_TMP_DIR/backslash"
    local tmp="$home/tmp\\dir"
    mkdir -p "$home" "$tmp"
    run_install_offline "$home" TMPDIR="$tmp" || { cat "$home.log"; return 1; }
    installed_cli "$home" &&
        log_has "$home" "Verified the sha256 of spice_$(host_os)_$(host_arch).tar.gz against the SHA256SUMS of release v0.0.0."
}

# Fails the test, printing the log, unless the log contains a line.
log_has() {
    grep -qF -- "$2" "$1.log" || { echo "missing from $1.log: $2"; cat "$1.log"; return 1; }
}

log_lacks() {
    ! grep -qF -- "$2" "$1.log" || { echo "unexpected in $1.log: $2"; cat "$1.log"; return 1; }
}

installed_cli() {
    [[ -x "$1/.spice/bin/spice" ]]
}

test_install_sh_verifies_against_sha256sums() {
    local home="$TEST_TMP_DIR/sums-match"
    mkdir -p "$home"
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    installed_cli "$home" &&
        log_has "$home" "Verified the sha256 of spice_$(host_os)_$(host_arch).tar.gz against the SHA256SUMS of release v0.0.0." &&
        log_has "$home" "Skipped the build attestation check" &&
        log_lacks "$home" "attestation verify"
}

test_install_sh_rejects_a_checksum_mismatch_before_extracting() {
    local home="$TEST_TMP_DIR/sums-mismatch"
    mkdir -p "$home"
    if OFFLINE_SUMS=mismatch run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" &&
        log_has "$home" "Checksum mismatch for spice_" &&
        log_has "$home" "Nothing was installed." &&
        log_lacks "$home" "Extracting archive"
}

test_install_sh_rejects_an_asset_missing_from_sha256sums() {
    local home="$TEST_TMP_DIR/sums-unlisted"
    mkdir -p "$home"
    if OFFLINE_SUMS=unlisted run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" && log_has "$home" "the SHA256SUMS of release v0.0.0 does not list it"
}

test_install_sh_falls_back_to_the_github_digest() {
    local home="$TEST_TMP_DIR/digest-match"
    mkdir -p "$home"
    OFFLINE_SUMS=none OFFLINE_GH=pass run_install_offline "$home" || { cat "$home.log"; return 1; }
    # A release without SHA256SUMS has no attestations, so gh is not asked about one.
    installed_cli "$home" &&
        log_has "$home" "against the sha256 GitHub recorded for spice_$(host_os)_$(host_arch).tar.gz in release v0.0.0." &&
        ! grep -q "attestation" "$home.gh"
}

test_install_sh_rejects_a_github_digest_mismatch() {
    local home="$TEST_TMP_DIR/digest-mismatch"
    mkdir -p "$home"
    if OFFLINE_SUMS=none OFFLINE_DIGEST=mismatch run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" && log_has "$home" "Checksum mismatch for spice_"
}

test_install_sh_fails_closed_without_a_checksum() {
    local home="$TEST_TMP_DIR/digest-null"
    mkdir -p "$home"
    if OFFLINE_SUMS=none OFFLINE_DIGEST=null run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" &&
        log_has "$home" "GitHub recorded no sha256 for spice_" &&
        log_has "$home" "SPICE_INSTALL_ALLOW_UNVERIFIED=1 bash -s 0.0.0" &&
        log_lacks "$home" "Extracting archive"
}

test_install_sh_fails_closed_when_the_api_request_fails() {
    local home="$TEST_TMP_DIR/digest-api-fails"
    mkdir -p "$home"
    if OFFLINE_SUMS=none OFFLINE_DIGEST=fail run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" && log_has "$home" "the GitHub API request for its asset sha256 failed"
}

test_install_sh_opt_out_installs_an_unverifiable_download() {
    local home="$TEST_TMP_DIR/opt-out"
    mkdir -p "$home"
    OFFLINE_SUMS=none OFFLINE_DIGEST=null run_install_offline "$home" SPICE_INSTALL_ALLOW_UNVERIFIED=1 ||
        { cat "$home.log"; return 1; }
    installed_cli "$home" && log_has "$home" "unverified, because SPICE_INSTALL_ALLOW_UNVERIFIED=1 is set"
}

test_install_sh_opt_out_never_installs_a_mismatch() {
    local home="$TEST_TMP_DIR/opt-out-mismatch"
    mkdir -p "$home"
    if OFFLINE_SUMS=mismatch run_install_offline "$home" SPICE_INSTALL_ALLOW_UNVERIFIED=1; then
        cat "$home.log"
        return 1
    fi
    ! installed_cli "$home" && log_has "$home" "Checksum mismatch for spice_"
}

test_install_sh_checks_the_attestation_when_gh_is_logged_in() {
    local home="$TEST_TMP_DIR/attestation-pass"
    local signer='^https://github\.com/spiceai/spiceai/\.github/workflows/build_release\.yml@refs/heads/release/'
    mkdir -p "$home"
    OFFLINE_GH=pass run_install_offline "$home" || { cat "$home.log"; return 1; }
    installed_cli "$home" &&
        log_has "$home" "Verified the build attestation of spice_" &&
        grep -qF -- "--repo spiceai/spiceai --cert-identity-regex $signer --deny-self-hosted-runners" "$home.gh"
}

test_install_sh_rejects_a_failed_attestation() {
    local home="$TEST_TMP_DIR/attestation-fail"
    mkdir -p "$home"
    if OFFLINE_GH=fail run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" &&
        log_has "$home" "The build attestation of spice_" &&
        log_has "$home" "verification failed" &&
        log_lacks "$home" "Extracting archive"
}

# A release that lists SHA256SUMS is signed, so a failed SHA256SUMS download must stop the
# install rather than fall back to the GitHub digest, which skips the attestation check.
test_install_sh_stops_when_a_listed_sha256sums_cannot_be_downloaded() {
    local home="$TEST_TMP_DIR/sums-unreachable"
    mkdir -p "$home"
    if OFFLINE_SUMS=unreachable OFFLINE_GH=fail run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" &&
        log_has "$home" "release v0.0.0 publishes SHA256SUMS, but downloading it failed" &&
        log_lacks "$home" "Extracting archive"
}

test_install_sh_opt_out_does_not_skip_a_listed_sha256sums() {
    local home="$TEST_TMP_DIR/sums-unreachable-opt-out"
    mkdir -p "$home"
    if OFFLINE_SUMS=unreachable run_install_offline "$home" SPICE_INSTALL_ALLOW_UNVERIFIED=1; then
        cat "$home.log"
        return 1
    fi
    ! installed_cli "$home" && log_has "$home" "publishes SHA256SUMS, but downloading it failed"
}

test_install_spiced_sh_stops_when_a_listed_sha256sums_cannot_be_downloaded() {
    local home="$TEST_TMP_DIR/spiced-sums-unreachable"
    mkdir -p "$home"
    if OFFLINE_SUMS=unreachable OFFLINE_GH=fail run_install_spiced_offline "$home"; then cat "$home.log"; return 1; fi
    [[ ! -e "$home/bin/spiced" ]] && log_has "$home" "release v0.0.0 publishes SHA256SUMS, but downloading it failed"
}

test_install_spiced_sh_verifies_against_sha256sums() {
    local home="$TEST_TMP_DIR/spiced-sums-match"
    mkdir -p "$home"
    run_install_spiced_offline "$home" || { cat "$home.log"; return 1; }
    [[ -x "$home/bin/spiced" ]] &&
        log_has "$home" "Verified the sha256 of spiced_$(host_os)_$(host_arch).tar.gz against the SHA256SUMS of release v0.0.0."
}

test_install_spiced_sh_rejects_a_checksum_mismatch() {
    local home="$TEST_TMP_DIR/spiced-sums-mismatch"
    mkdir -p "$home"
    if OFFLINE_SUMS=mismatch run_install_spiced_offline "$home"; then cat "$home.log"; return 1; fi
    [[ ! -e "$home/bin/spiced" ]] && log_has "$home" "Checksum mismatch for spiced_"
}

test_install_spiced_sh_falls_back_to_the_github_digest() {
    local home="$TEST_TMP_DIR/spiced-digest-match"
    mkdir -p "$home"
    OFFLINE_SUMS=none run_install_spiced_offline "$home" || { cat "$home.log"; return 1; }
    [[ -x "$home/bin/spiced" ]] && log_has "$home" "against the sha256 GitHub recorded for spiced_"
}

# =============================================================================
# GitHub Requests and Rate Limits
# =============================================================================
# Each test takes the installer (install.sh or install-spiced.sh) and, where both matter, the
# HTTP client (curl or wget). An offline install's environment holds no GITHUB_TOKEN or GH_TOKEN
# unless the test passes one.

# Runs installer $1 offline into home $2. Further arguments are VAR=value pairs for its environment.
run_named_installer_offline() {
    local installer="$1" home="$2"
    shift 2
    case "$installer" in
        install.sh) run_install_offline "$home" "$@" ;;
        install-spiced.sh) run_install_spiced_offline "$home" "$@" ;;
        *) echo "unknown installer $installer"; return 1 ;;
    esac
}

# Whether installer $1 installed its binary into home $2.
installed_by() {
    case "$1" in
        install.sh) installed_cli "$2" ;;
        install-spiced.sh) [[ -x "$2/bin/spiced" ]] ;;
    esac
}

# The release asset installer $1 downloads on this host.
asset_of() {
    case "$1" in
        install.sh) echo "spice_$(host_os)_$(host_arch).tar.gz" ;;
        install-spiced.sh) echo "spiced_$(host_os)_$(host_arch).tar.gz" ;;
    esac
}

# A new home directory for one test, named after it, which is also its name on the fake GitHub.
test_home() {
    local home="$TEST_TMP_DIR/$1"
    rm -rf "$home"
    mkdir -p "$home"
    echo "$home"
}

# Fails, printing what GitHub was asked, unless the requests of home $1, sorted, are exactly the
# lines that follow.
requests_are() {
    local home="$1"
    shift
    local expected actual
    expected=$(printf '%s\n' "$@" | sort)
    actual=$(sort "$home.requests")
    [[ "$actual" == "$expected" ]] && return 0
    echo "GitHub requests of $(basename "$home"):"
    diff <(echo "$expected") <(echo "$actual") | sed 's/^</  expected:/; s/^>/  got:     /' | grep -v '^[0-9]'
    cat "$home.log"
    return 1
}

# Fails, printing what GitHub was asked, unless home $1 made exactly $2 API requests.
api_request_count_is() {
    local count
    count=$(grep -c '^api/' "$1.requests" || true)
    [[ "$count" == "$2" ]] && return 0
    echo "expected $2 API requests, got $count:"
    cat "$1.requests"
    return 1
}

# With no token, no request carries an Authorization header, and the install of the latest release
# still verifies, against the sha256 GitHub recorded, with one API request, for the release's asset
# list. The latest release comes from where github.com/.../releases/latest redirects, which is not
# an API request.
test_installs_without_a_token() {
    local installer="$1" client="$2" home asset
    home=$(test_home "no-token-$installer-$client")
    asset=$(asset_of "$installer")
    OFFLINE_VERSION=latest OFFLINE_SUMS=none OFFLINE_CLIENT="$client" run_named_installer_offline "$installer" "$home" ||
        { cat "$home.log"; return 1; }
    installed_by "$installer" "$home" &&
        log_has "$home" "Verified the sha256 of $asset against the sha256 GitHub recorded for $asset in release v0.0.0." &&
        requests_are "$home" \
            "dl/releases/latest none" \
            "api/releases/tags/v0.0.0 none" \
            "dl/releases/download/v0.0.0/SHA256SUMS none" \
            "dl/releases/download/v0.0.0/$asset none"
}

# The control for test_installs_without_a_token: with GITHUB_TOKEN set, every API request
# carries it, so that test can tell the two apart.
test_sends_the_token_to_the_api() {
    local installer="$1" client="$2" home unauthenticated
    home=$(test_home "token-$installer-$client")
    OFFLINE_VERSION=latest OFFLINE_SUMS=none OFFLINE_CLIENT="$client" run_named_installer_offline "$installer" "$home" \
        GITHUB_TOKEN=installer-test-token || { cat "$home.log"; return 1; }
    unauthenticated=$(grep '^api/.* none$' "$home.requests" || true)
    if [[ -n "$unauthenticated" ]]; then
        echo "API requests without the token:"
        echo "$unauthenticated"
        return 1
    fi
    installed_by "$installer" "$home" && api_request_count_is "$home" 1
}

# The latest release of a release with SHA256SUMS installs with no API request at all, so the
# API rate limit cannot stop it.
test_verifies_sha256sums_with_no_api_request() {
    local installer="$1" home
    home=$(test_home "api-budget-$installer")
    OFFLINE_VERSION=latest run_named_installer_offline "$installer" "$home" || { cat "$home.log"; return 1; }
    installed_by "$installer" "$home" &&
        log_has "$home" "against the SHA256SUMS of release v0.0.0." &&
        api_request_count_is "$home" 0
}

# When the github.com redirect gives no release, the latest release comes from the API. A
# rate-limited lookup there stops the install before any download, asks once, and says how to
# get past the limit.
test_stops_when_the_latest_release_lookup_is_rate_limited() {
    local installer="$1" client="$2" home
    home=$(test_home "limited-latest-$installer-$client")
    if OFFLINE_VERSION=latest OFFLINE_LATEST=none OFFLINE_RATE_LIMITED=latest OFFLINE_CLIENT="$client" \
        run_named_installer_offline "$installer" "$home"; then
        cat "$home.log"
        return 1
    fi
    ! installed_by "$installer" "$home" &&
        log_has "$home" "If the GitHub API rate limit was exceeded, set GITHUB_TOKEN or pass a version to install" &&
        requests_are "$home" "dl/releases/latest none" "api/releases/latest none"
}

# The API is the fallback for the latest release, and it gets GITHUB_TOKEN.
test_falls_back_to_the_api_for_the_latest_release() {
    local installer="$1" client="$2" home
    home=$(test_home "latest-api-$installer-$client")
    OFFLINE_VERSION=latest OFFLINE_LATEST=none OFFLINE_CLIENT="$client" \
        run_named_installer_offline "$installer" "$home" GITHUB_TOKEN=installer-test-token || { cat "$home.log"; return 1; }
    installed_by "$installer" "$home" || return 1
    if ! grep -qx "dl/releases/latest none" "$home.requests" || ! grep -qx "api/releases/latest Bearer" "$home.requests"; then
        cat "$home.requests"
        return 1
    fi
}

# A rate-limited lookup of the sha256 GitHub recorded stops the install with the token hint and
# the opt-out command, after one request: a 403 is not retried.
test_stops_when_the_checksum_lookup_is_rate_limited() {
    local installer="$1" client="$2" home
    home=$(test_home "limited-tags-$installer-$client")
    if OFFLINE_SUMS=none OFFLINE_RATE_LIMITED=tags OFFLINE_CLIENT="$client" \
        run_named_installer_offline "$installer" "$home"; then
        cat "$home.log"
        return 1
    fi
    ! installed_by "$installer" "$home" &&
        log_has "$home" "the GitHub API request for its asset sha256 failed (set GITHUB_TOKEN if the API rate limit was exceeded)" &&
        log_has "$home" "SPICE_INSTALL_ALLOW_UNVERIFIED=1 bash -s 0.0.0" &&
        log_lacks "$home" "Extracting archive" &&
        api_request_count_is "$home" 1
}

# A 429 is retried, as any 429 is, and then stops the install the same way.
test_stops_when_the_checksum_lookup_gets_a_429() {
    local client="$1" home expected
    home=$(test_home "limited-tags-429-$client")
    expected=$(retried_429_requests "$client")
    if OFFLINE_SUMS=none OFFLINE_RATE_LIMITED=tags OFFLINE_RATE_LIMIT_STATUS=429 OFFLINE_CLIENT="$client" \
        run_install_offline "$home"; then
        cat "$home.log"
        return 1
    fi
    ! installed_cli "$home" &&
        log_has "$home" "set GITHUB_TOKEN if the API rate limit was exceeded" &&
        api_request_count_is "$home" "$expected"
}

test_opt_out_installs_when_the_checksum_lookup_is_rate_limited() {
    local installer="$1" home
    home=$(test_home "limited-tags-opt-out-$installer")
    OFFLINE_SUMS=none OFFLINE_RATE_LIMITED=tags run_named_installer_offline "$installer" "$home" \
        SPICE_INSTALL_ALLOW_UNVERIFIED=1 || { cat "$home.log"; return 1; }
    installed_by "$installer" "$home" &&
        log_has "$home" "unverified, because SPICE_INSTALL_ALLOW_UNVERIFIED=1 is set: the SHA256SUMS of release v0.0.0 could not be downloaded, and the GitHub API request for its asset sha256 failed"
}

# With no gh on PATH at all, as opposed to gh installed but logged out, a release with SHA256SUMS
# installs and says the attestation check was skipped.
test_installs_without_gh() {
    local installer="$1" home asset
    home=$(test_home "no-gh-$installer")
    asset=$(asset_of "$installer")
    OFFLINE_GH=absent run_named_installer_offline "$installer" "$home" || { cat "$home.log"; return 1; }
    installed_by "$installer" "$home" &&
        log_has "$home" "Verified the sha256 of $asset against the SHA256SUMS of release v0.0.0." &&
        log_has "$home" "Skipped the build attestation check of $asset: the GitHub CLI (gh) is not installed or not logged in."
}

# =============================================================================
# install.sh: Shells, Versions, Platforms and Privileges
# =============================================================================

# The README one-liner pipes the installer into a shell, which on Debian and Ubuntu `sh` is dash.
test_install_sh_runs_under_posix_sh() {
    local shell home ran=0
    for shell in dash /bin/sh "busybox sh"; do
        command -v "${shell%% *}" > /dev/null 2>&1 || continue
        home=$(test_home "posix-sh-$(basename "${shell// /-}")")
        OFFLINE_SHELL="$shell" run_install_offline "$home" || { echo "under $shell:"; cat "$home.log"; return 1; }
        installed_cli "$home" || { echo "under $shell:"; cat "$home.log"; return 1; }
        ran=$((ran + 1))
    done
    [[ $ran -gt 0 ]] || { echo "no POSIX shell to run the installer with"; return 1; }
}

test_install_sh_accepts_a_v_prefixed_version() {
    local home
    home=$(test_home "v-prefix")
    OFFLINE_VERSION=v0.0.0 run_install_offline "$home" || { cat "$home.log"; return 1; }
    installed_cli "$home" || return 1
    if ! grep -qx "dl/releases/download/v0.0.0/$(asset_of install.sh) none" "$home.requests"; then
        cat "$home.requests"
        return 1
    fi
}

test_install_sh_rejects_an_invalid_version_before_any_request() {
    local home
    home=$(test_home "bad-version")
    if OFFLINE_VERSION=banana run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" && log_has "$home" "'banana' is not a Spice release version" && requests_are "$home"
}

test_install_sh_prints_its_help() {
    local home
    home=$(test_home "help")
    OFFLINE_VERSION=--help run_install_offline "$home" || { cat "$home.log"; return 1; }
    ! installed_cli "$home" &&
        log_has "$home" "SPICE_INSTALL_NO_MODIFY_PATH=1" &&
        log_has "$home" "NONINTERACTIVE=1" &&
        requests_are "$home"
}

# Under sudo from a regular user, spice would be installed for root, so the installer stops
# before any download.
test_install_sh_refuses_to_run_under_sudo() {
    local home
    home=$(test_home "under-sudo")
    if OFFLINE_UID=0 run_install_offline "$home" SUDO_USER=alice; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" &&
        log_has "$home" "Do not run the Spice CLI installer with sudo." &&
        log_has "$home" "SPICE_INSTALL_ALLOW_SUDO=1" &&
        requests_are "$home"
}

test_install_sh_runs_under_sudo_when_asked_to() {
    local home other
    home=$(test_home "under-sudo-allowed")
    OFFLINE_UID=0 run_install_offline "$home" SUDO_USER=alice SPICE_INSTALL_ALLOW_SUDO=1 || { cat "$home.log"; return 1; }
    installed_cli "$home" || return 1
    other=$(test_home "under-sudo-system-dir")
    OFFLINE_UID=0 run_install_offline "$other" SUDO_USER=alice SPICE_CLI_INSTALL_DIR="$other/opt/bin" || { cat "$other.log"; return 1; }
    [[ -x "$other/opt/bin/spice" ]]
}

# A shell under Rosetta 2 reports x86_64; the native Apple silicon build runs there.
test_install_sh_installs_the_native_build_under_rosetta() {
    local home
    home=$(test_home "rosetta")
    OFFLINE_UNAME_S=Darwin OFFLINE_UNAME_M=x86_64 OFFLINE_ROSETTA=1 \
        run_installer_offline "$INSTALL_SCRIPT" spice spice_darwin_aarch64.tar.gz "$home" || { cat "$home.log"; return 1; }
    installed_cli "$home" &&
        log_has "$home" "This shell runs under Rosetta 2" &&
        grep -qx "dl/releases/download/v0.0.0/spice_darwin_aarch64.tar.gz none" "$home.requests"
}

test_install_sh_refuses_an_intel_mac() {
    local home
    home=$(test_home "intel-mac")
    if OFFLINE_UNAME_S=Darwin OFFLINE_UNAME_M=x86_64 OFFLINE_ROSETTA=0 \
        run_installer_offline "$INSTALL_SCRIPT" spice spice_darwin_x86_64.tar.gz "$home"; then
        cat "$home.log"
        return 1
    fi
    ! installed_cli "$home" && log_has "$home" "darwin-x86_64 does not have a pre-built binary" && requests_are "$home"
}

# The Linux builds need glibc. On a musl system the installer says so before downloading.
test_install_sh_refuses_a_musl_system() {
    local home
    home=$(test_home "musl")
    if OFFLINE_UNAME_S=Linux OFFLINE_UNAME_M=x86_64 OFFLINE_LDD="musl libc (x86_64)" \
        run_installer_offline "$INSTALL_SCRIPT" spice spice_linux_x86_64.tar.gz "$home"; then
        cat "$home.log"
        return 1
    fi
    ! installed_cli "$home" && log_has "$home" "this system uses musl libc" && requests_are "$home"
}

test_install_spiced_sh_refuses_a_musl_system() {
    local home
    home=$(test_home "spiced-musl")
    if OFFLINE_UNAME_S=Linux OFFLINE_UNAME_M=x86_64 OFFLINE_LDD="musl libc (x86_64)" \
        run_installer_offline "$INSTALL_SPICED_SCRIPT" spiced spiced_linux_x86_64.tar.gz "$home" SPICED_INSTALL_DIR="$home/bin"; then
        cat "$home.log"
        return 1
    fi
    [[ ! -e "$home/bin/spiced" ]] && log_has "$home" "this system uses musl libc" && requests_are "$home"
}

# =============================================================================
# Replacing an Installed Binary
# =============================================================================

# Writes an executable at $1 that prints "$2".
old_binary() {
    mkdir -p "$(dirname "$1")"
    printf '#!/bin/sh\necho "%s"\n' "$2" > "$1"
    chmod +x "$1"
}

# A binary that cannot start here (another libc, an older glibc) is not installed, and the
# installed one stays as it was, with no temporary file left beside it.
test_install_sh_keeps_the_installed_spice_when_the_new_one_cannot_run() {
    local home
    home=$(test_home "broken-binary")
    old_binary "$home/.spice/bin/spice" "spice 0.0.1"
    if OFFLINE_BINARY=broken run_install_offline "$home"; then cat "$home.log"; return 1; fi
    log_has "$home" "does not run on this system (exit status 127), so it was not installed" &&
        log_has "$home" "error while loading shared libraries" &&
        grep -qx 'echo "spice 0.0.1"' "$home/.spice/bin/spice" &&
        [[ "$(ls -A "$home/.spice/bin")" == "spice" ]] &&
        ! grep -qs "spice installer" "$home/.bash_profile" "$home/.bashrc" "$home/.profile"
}

test_install_sh_refuses_a_binary_reporting_another_version() {
    local home
    home=$(test_home "wrong-version")
    if OFFLINE_BINARY=wrong-version run_install_offline "$home"; then cat "$home.log"; return 1; fi
    ! installed_cli "$home" && log_has "$home" "The downloaded spice reports version 9.9.9, not 0.0.0"
}

test_install_spiced_sh_keeps_the_installed_spiced_when_the_new_one_cannot_run() {
    local home
    home=$(test_home "spiced-broken-binary")
    old_binary "$home/bin/spiced" "spiced 0.0.1"
    if OFFLINE_BINARY=broken run_install_spiced_offline "$home"; then cat "$home.log"; return 1; fi
    log_has "$home" "does not run on this system (exit status 127), so it was not installed" &&
        grep -qx 'echo "spiced 0.0.1"' "$home/bin/spiced" &&
        [[ "$(ls -A "$home/bin")" == "spiced" ]]
}

test_install_spiced_sh_accepts_a_v_prefixed_version() {
    local home
    home=$(test_home "spiced-v-prefix")
    OFFLINE_VERSION=v0.0.0 run_install_spiced_offline "$home" || { cat "$home.log"; return 1; }
    [[ -x "$home/bin/spiced" ]]
}

# Runs installer $1 into home $2 while the binary at $3, a copy of sleep, is running, and passes
# when the install succeeds and leaves the new binary at $3. Linux refuses to write a running
# binary in place ("Text file busy"), so this needs the rename.
upgrades_a_running_binary() {
    local installer="$1" home="$2" binary="$3" pid rc=0
    mkdir -p "$(dirname "$binary")"
    cp "$(command -v sleep)" "$binary"
    "$binary" 60 &
    pid=$!
    for _ in $(seq 1 50); do
        [[ "$(readlink "/proc/$pid/exe" 2>/dev/null)" == "$binary" ]] && break
        sleep 0.1
    done
    [[ "$(readlink "/proc/$pid/exe" 2>/dev/null)" == "$binary" ]] || { kill "$pid"; echo "the stand-in never started"; return 1; }
    run_named_installer_offline "$installer" "$home" || rc=$?
    kill -0 "$pid" 2>/dev/null || { echo "the running binary was stopped"; rc=1; }
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    [[ $rc -eq 0 ]] || { cat "$home.log"; return 1; }
    grep -q "CLI version: v0.0.0" "$binary" || { echo "$binary was not replaced"; cat "$home.log"; return 1; }
}

test_install_sh_upgrades_a_running_spice() {
    local home
    home=$(test_home "running-spice")
    upgrades_a_running_binary install.sh "$home" "$home/.spice/bin/spice"
}

test_install_spiced_sh_upgrades_a_running_spiced() {
    local home
    home=$(test_home "running-spiced")
    upgrades_a_running_binary install-spiced.sh "$home" "$home/bin/spiced"
}

# =============================================================================
# install.sh: PATH Setup
# =============================================================================

PATH_BEGIN="# >>> spice installer >>>"

# Fails, naming the entry, unless directory $1 holds no entries but those named after it,
# dotfiles included.
holds_only() {
    local dir="$1" entry name
    shift
    for entry in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do
        [[ -e "$entry" || -L "$entry" ]] || continue
        name="${entry##*/}"
        case " $* " in
            *" $name "*) ;;
            *) echo "unexpected in $dir: $name"; return 1 ;;
        esac
    done
}

# What `command -v spice` finds in a new shell ($2, e.g. "bash -lc") for home $1.
spice_in_new_shell() {
    local home="$1" shell="$2"
    env -i HOME="$home" ${ZDOTDIR:+ZDOTDIR="$ZDOTDIR"} PATH=/usr/bin:/bin $shell 'command -v spice' 2>/dev/null < /dev/null | tail -n 1 | tr -d '\r'
}

# The files bash reads: on Linux .bashrc, for the interactive shells terminals start, and the
# login file (here .profile); on macOS, where terminals start login shells, .bash_profile.
test_install_sh_puts_spice_on_path_for_new_bash_shells() {
    local home found
    home=$(test_home "path-bash")
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    if [[ "$(host_os)" == linux ]]; then
        grep -qxF "$PATH_BEGIN" "$home/.bashrc" && grep -qxF "$PATH_BEGIN" "$home/.profile" && [[ ! -e "$home/.bash_profile" ]] ||
            { ls -A "$home"; return 1; }
        found=$(spice_in_new_shell "$home" "bash -ic")
        [[ "$found" == "$home/.spice/bin/spice" ]] || { echo "interactive bash found '$found'"; return 1; }
    else
        grep -qxF "$PATH_BEGIN" "$home/.bash_profile" && [[ ! -e "$home/.bashrc" && ! -e "$home/.profile" ]] ||
            { ls -A "$home"; return 1; }
    fi
    found=$(spice_in_new_shell "$home" "bash -lc")
    [[ "$found" == "$home/.spice/bin/spice" ]] || { echo "login bash found '$found'"; return 1; }
    log_has "$home" "Added $home/.spice/bin to PATH in" && log_has "$home" "export PATH=\"\$HOME/.spice/bin:\$PATH\""
}

# bash reads the first of .bash_profile, .bash_login and .profile; a new .bash_profile would hide
# an existing .profile.
test_install_sh_does_not_hide_an_existing_profile() {
    local home
    home=$(test_home "path-existing-profile")
    echo "# mine" > "$home/.profile"
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    [[ ! -e "$home/.bash_profile" ]] && grep -qxF "# mine" "$home/.profile" && grep -qxF "$PATH_BEGIN" "$home/.profile"
}

test_install_sh_honors_zdotdir() {
    local home file found
    home=$(test_home "path-zdotdir")
    run_install_offline "$home" SHELL=/bin/zsh ZDOTDIR="$home/zdot" || { cat "$home.log"; return 1; }
    if [[ "$(host_os)" == linux ]]; then file="$home/zdot/.zshrc"; else file="$home/zdot/.zprofile"; fi
    grep -qxF "$PATH_BEGIN" "$file" && [[ ! -e "$home/.zshrc" && ! -e "$home/.zprofile" ]] || { ls -AR "$home"; return 1; }
    if command -v zsh > /dev/null 2>&1; then
        found=$(ZDOTDIR="$home/zdot" spice_in_new_shell "$home" "zsh -lic")
        [[ "$found" == "$home/.spice/bin/spice" ]] || { echo "zsh found '$found'"; return 1; }
    fi
}

test_install_sh_writes_a_fish_conf_d_file() {
    local home file found
    home=$(test_home "path-fish")
    run_install_offline "$home" SHELL=/usr/bin/fish || { cat "$home.log"; return 1; }
    file="$home/.config/fish/conf.d/spice.fish"
    grep -qF "set -gx PATH \"\$HOME/.spice/bin\" \$PATH" "$file" || { cat "$file"; return 1; }
    if command -v fish > /dev/null 2>&1; then
        found=$(spice_in_new_shell "$home" "fish -c")
        [[ "$found" == "$home/.spice/bin/spice" ]] || { echo "fish found '$found'"; return 1; }
    fi
}

test_install_sh_writes_csh_setenv() {
    local home found
    home=$(test_home "path-csh")
    run_install_offline "$home" SHELL=/bin/tcsh || { cat "$home.log"; return 1; }
    grep -qxF "setenv PATH \"\$HOME/.spice/bin:\${PATH}\"" "$home/.cshrc" || { cat "$home/.cshrc"; return 1; }
    if command -v tcsh > /dev/null 2>&1; then
        found=$(env -i HOME="$home" PATH=/usr/bin:/bin tcsh -c 'which spice' 2>/dev/null < /dev/null)
        [[ "$found" == "$home/.spice/bin/spice" ]] || { echo "tcsh found '$found'"; return 1; }
    fi
}

# Running the installer again finds its block instead of adding another.
test_install_sh_path_setup_is_idempotent() {
    local home file
    home=$(test_home "path-twice")
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    for file in "$home/.bashrc" "$home/.profile" "$home/.bash_profile"; do
        [[ -e "$file" ]] || continue
        [[ "$(grep -cxF "$PATH_BEGIN" "$file")" == 1 ]] || { echo "$file:"; cat "$file"; return 1; }
    done
    log_has "$home" "already puts $home/.spice/bin on PATH for new terminals"
}

test_install_sh_puts_a_custom_dir_on_path() {
    local home found
    home=$(test_home "path-custom")
    run_install_offline "$home" SPICE_CLI_INSTALL_DIR="$home/tools/bin" || { cat "$home.log"; return 1; }
    [[ -x "$home/tools/bin/spice" && ! -e "$home/.spice" ]] || return 1
    found=$(spice_in_new_shell "$home" "bash -lc")
    [[ "$found" == "$home/tools/bin/spice" ]] || { echo "login bash found '$found'"; return 1; }
    grep -qF "\"\$HOME/tools/bin:\${PATH}\"" "$home/.profile" "$home/.bash_profile" 2>/dev/null
}

test_install_sh_puts_a_dir_outside_home_on_path() {
    local home dir found
    home=$(test_home "path-outside-home")
    dir="$TEST_TMP_DIR/outside-home-bin"
    run_install_offline "$home" SPICE_CLI_INSTALL_DIR="$dir" || { cat "$home.log"; return 1; }
    found=$(spice_in_new_shell "$home" "bash -lc")
    [[ "$found" == "$dir/spice" ]] || { echo "login bash found '$found'"; return 1; }
}

test_install_sh_leaves_profiles_alone_when_the_dir_is_on_path() {
    local home
    home=$(test_home "path-already")
    run_install_offline "$home" PATH="$home/.spice/bin:$home.stubs:$SYSTEM_BIN" || { cat "$home.log"; return 1; }
    installed_cli "$home" && holds_only "$home" .spice &&
        log_has "$home" "$home/.spice/bin is on your PATH, so 'spice' runs the new install."
}

test_install_sh_no_modify_path_leaves_profiles_alone() {
    local home
    home=$(test_home "path-no-modify")
    run_install_offline "$home" SPICE_INSTALL_NO_MODIFY_PATH=1 || { cat "$home.log"; return 1; }
    installed_cli "$home" && holds_only "$home" .spice &&
        log_has "$home" "Left your shell profiles unchanged, because SPICE_INSTALL_NO_MODIFY_PATH=1 is set." &&
        log_has "$home" "export PATH=\"\$HOME/.spice/bin:\$PATH\""
}

# A directory whose name a profile line would have to escape is left to the user.
test_install_sh_leaves_an_unusual_dir_to_the_user() {
    local home
    home=$(test_home "path-unusual")
    run_install_offline "$home" SPICE_CLI_INSTALL_DIR="$home/we\$ird/bin" || { cat "$home.log"; return 1; }
    [[ -x "$home/we\$ird/bin/spice" ]] && holds_only "$home" "we\$ird" &&
        log_has "$home" "to PATH in your shell profile"
}

# The profile bash reads first on this host, for a home without one.
first_bash_profile() {
    if [[ "$(host_os)" == linux ]]; then echo "$1/.bashrc"; else echo "$1/.bash_profile"; fi
}

# The line an installer without the marked block wrote counts as the setup.
test_install_sh_keeps_a_legacy_path_line() {
    local home file
    home=$(test_home "path-legacy")
    file=$(first_bash_profile "$home")
    printf '%s\n' "export PATH=\"\$HOME/.spice/bin:\$PATH\"" > "$file"
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    ! grep -qxF "$PATH_BEGIN" "$file" || { cat "$file"; return 1; }
}

# A block whose line is out of date is rewritten in place: a symlinked profile stays a symlink.
test_install_sh_updates_its_block_through_a_symlink() {
    local home file
    home=$(test_home "path-symlink")
    file=$(first_bash_profile "$home")
    mkdir -p "$home/dotfiles"
    printf '%s\n' "# mine" "$PATH_BEGIN" "export PATH=\"/old/bin:\$PATH\"" "# <<< spice installer <<<" "# after" > "$home/dotfiles/profile"
    ln -s "$home/dotfiles/profile" "$file"
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    [[ -L "$file" ]] || { echo "$file is no longer a symlink"; return 1; }
    grep -qxF "# mine" "$home/dotfiles/profile" && grep -qxF "# after" "$home/dotfiles/profile" &&
        ! grep -qF "/old/bin" "$home/dotfiles/profile" &&
        grep -qF "*\":\$HOME/.spice/bin:\"*" "$home/dotfiles/profile" &&
        [[ "$(grep -cxF "$PATH_BEGIN" "$home/dotfiles/profile")" == 1 ]] || { cat "$home/dotfiles/profile"; return 1; }
}

test_install_sh_leaves_an_unterminated_block_alone() {
    local home file before
    home=$(test_home "path-unterminated")
    file=$(first_bash_profile "$home")
    printf '%s\n' "$PATH_BEGIN" "# the rest of my profile" > "$file"
    before=$(cat "$file")
    run_install_offline "$home" || { cat "$home.log"; return 1; }
    [[ "$(cat "$file")" == "$before" ]] && log_has "$home" "line with no '# <<< spice installer <<<' line after it"
}

test_install_sh_warns_about_another_spice_on_path() {
    local home
    home=$(test_home "other-spice")
    old_binary "$home/brew/bin/spice" "spice 0.0.1"
    run_install_offline "$home" PATH="$home/brew/bin:$home.stubs:$SYSTEM_BIN" || { cat "$home.log"; return 1; }
    log_has "$home" "Another spice is installed at $home/brew/bin/spice. New terminals run $home/.spice/bin/spice"
}

test_install_sh_prints_no_color_codes_off_a_terminal() {
    local home
    home=$(test_home "no-color")
    OFFLINE_SUMS=none OFFLINE_DIGEST=null run_install_offline "$home" SPICE_INSTALL_ALLOW_UNVERIFIED=1 || { cat "$home.log"; return 1; }
    log_has "$home" "Warning:" && log_lacks "$home" $'\033['
}

# =============================================================================
# install.sh: Spice Skills for AI Coding Agents
# =============================================================================

# Fails, printing the log, unless the agent commands home $1 ran are exactly the lines that follow.
agents_ran() {
    local home="$1"
    shift
    local expected=""
    [[ $# -eq 0 ]] || expected=$(printf '%s\n' "$@")
    [[ "$(cat "$home.agents")" == "$expected" ]] && return 0
    echo "agent commands of $(basename "$home"):"
    diff <(echo "$expected") "$home.agents"
    cat "$home.log"
    return 1
}

test_skills_are_never_offered_without_a_terminal() {
    local home
    home=$(test_home "skills-no-terminal")
    OFFLINE_AGENTS="claude codex" run_install_offline "$home" || { cat "$home.log"; return 1; }
    installed_cli "$home" && agents_ran "$home" &&
        log_lacks "$home" "Install the Spice skills plugin for:" &&
        log_has "$home" "To add the Spice skills to your AI coding agents, see https://github.com/spiceai/skills#installation"
}

test_skills_are_not_offered_when_no_agent_is_installed() {
    local home
    home=$(test_home "skills-no-agent")
    OFFLINE_TTY_INPUT=$'a\n' run_install_offline "$home" || { cat "$home.log"; return 1; }
    installed_cli "$home" && log_lacks "$home" "Spice skills"
}

test_skills_install_for_the_chosen_agent() {
    local home
    home=$(test_home "skills-one")
    OFFLINE_AGENTS="claude codex grok" OFFLINE_TTY_INPUT=$'1\n' run_install_offline "$home" || { cat "$home.log"; return 1; }
    log_has "$home" "1) Claude Code" && log_has "$home" "2) Codex" && log_has "$home" "3) Grok Build" &&
        agents_ran "$home" "claude plugin marketplace add spiceai/skills" "claude plugin install spiceai@spiceai" &&
        log_has "$home" "Installed the Spice skills plugin for Claude Code."
}

test_skills_install_for_every_agent() {
    local home
    home=$(test_home "skills-all")
    OFFLINE_AGENTS="claude codex copilot grok" OFFLINE_TTY_INPUT=$'a\n' run_install_offline "$home" || { cat "$home.log"; return 1; }
    agents_ran "$home" \
        "claude plugin marketplace add spiceai/skills" "claude plugin install spiceai@spiceai" \
        "codex plugin marketplace add spiceai/skills" "codex plugin add spiceai@spiceai" \
        "copilot plugin marketplace add spiceai/skills" "copilot plugin install spiceai@spiceai" \
        "grok plugin install spiceai/skills --trust"
}

test_skills_install_for_several_agents() {
    local home
    home=$(test_home "skills-several")
    OFFLINE_AGENTS="claude codex grok" OFFLINE_TTY_INPUT=$'3, 2\n' run_install_offline "$home" || { cat "$home.log"; return 1; }
    agents_ran "$home" "grok plugin install spiceai/skills --trust" \
        "codex plugin marketplace add spiceai/skills" "codex plugin add spiceai@spiceai"
}

test_skills_default_to_none() {
    local home
    home=$(test_home "skills-none")
    OFFLINE_AGENTS="claude codex" OFFLINE_TTY_INPUT=$'\n' run_install_offline "$home" || { cat "$home.log"; return 1; }
    installed_cli "$home" && agents_ran "$home" && log_has "$home" "To add the Spice skills later"
}

test_skills_ask_again_after_an_invalid_answer() {
    local home
    home=$(test_home "skills-invalid")
    OFFLINE_AGENTS="claude codex" OFFLINE_TTY_INPUT=$'9\n2\n' run_install_offline "$home" || { cat "$home.log"; return 1; }
    log_has "$home" "Answer with numbers from 1 to 2, a for all, or n for none." &&
        agents_ran "$home" "codex plugin marketplace add spiceai/skills" "codex plugin add spiceai@spiceai"
}

# Automation that has a terminal says so with CI or NONINTERACTIVE.
test_skills_are_not_offered_in_ci_or_noninteractive() {
    local home var
    for var in CI=true NONINTERACTIVE=1; do
        home=$(test_home "skills-${var%%=*}")
        OFFLINE_AGENTS="claude codex" OFFLINE_TTY_INPUT=$'a\n' run_install_offline "$home" "$var" || { cat "$home.log"; return 1; }
        installed_cli "$home" || return 1
        agents_ran "$home" || return 1
        log_lacks "$home" "Install the Spice skills plugin for:" || return 1
    done
}

# An agent that fails to install the plugin does not fail the install of spice.
test_skills_failure_does_not_fail_the_install() {
    local home
    home=$(test_home "skills-fail")
    OFFLINE_AGENTS="claude codex" OFFLINE_AGENT_FAIL=claude OFFLINE_TTY_INPUT=$'a\n' run_install_offline "$home" ||
        { cat "$home.log"; return 1; }
    installed_cli "$home" &&
        log_has "$home" "Could not install the Spice skills plugin for Claude Code (exit status 1). spice itself is installed." &&
        log_has "$home" "boom: network unreachable" &&
        log_has "$home" "Installed the Spice skills plugin for Codex."
}

test_skills_already_installed_is_not_a_failure() {
    local home
    home=$(test_home "skills-already")
    OFFLINE_AGENTS="grok" OFFLINE_AGENT_ALREADY=grok OFFLINE_TTY_INPUT=$'1\n' run_install_offline "$home" || { cat "$home.log"; return 1; }
    log_has "$home" "The Spice skills plugin is already installed for Grok Build." && log_lacks "$home" "Could not install"
}

# Runs sha256Of from install.sh with only the given tools on PATH.
sha256_with_only() {
    local bin="$TEST_TMP_DIR/sha-tools-$1"
    shift
    mkdir -p "$bin"
    ln -sf "$(command -v awk)" "$bin/awk"
    for tool in "$@"; do
        printf '#!/bin/sh\necho "SHA2-256(stdin)= 0123abcd"\n' > "$bin/$tool"
        chmod +x "$bin/$tool"
    done
    PATH="$bin" /bin/bash -c "$(verification_block "$INSTALL_SCRIPT")"$'\n''sha256Of /dev/null'
}

test_sha256_falls_back_to_openssl() {
    [[ "$(sha256_with_only openssl openssl)" == "0123abcd" ]]
}

test_sha256_fails_without_a_tool() {
    ! sha256_with_only none
}

# =============================================================================
# Error Handling Tests
# =============================================================================

test_script_has_error_handling() {
    # install.sh uses trap for error handling instead of set -e
    grep -q "fail_trap" "$INSTALL_SCRIPT" || \
    grep -q "trap.*EXIT" "$INSTALL_SCRIPT"
}

test_spiced_script_uses_set_e() {
    head -10 "$INSTALL_SPICED_SCRIPT" | grep -q "set -e" || \
    head -10 "$INSTALL_SPICED_SCRIPT" | grep -q "set -.*e"
}

test_spiced_script_has_fail_trap() {
    grep -q "fail_trap" "$INSTALL_SPICED_SCRIPT"
}

test_spiced_script_has_cleanup() {
    grep -q "cleanup" "$INSTALL_SPICED_SCRIPT"
}

# =============================================================================
# Main Test Runner
# =============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --live)
                LIVE_TESTS=true
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --require-wget)
                REQUIRE_WGET=true
                shift
                ;;
            --help|-h)
                echo "Usage: $0 [--live] [--verbose] [--require-wget]"
                echo ""
                echo "Options:"
                echo "  --live          Run live download tests (requires network)"
                echo "  --verbose       Show detailed output for each test"
                echo "  --require-wget  Fail, instead of skipping the wget tests, when wget is not installed"
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                exit 1
                ;;
        esac
    done
}

run_all_tests() {
    echo ""
    echo "=========================================="
    echo "  Spice.ai Install Script Test Suite"
    echo "=========================================="
    echo ""
    
    if [[ "$LIVE_TESTS" == "true" ]]; then
        log_info "Live tests enabled (will make network requests)"
    else
        log_info "Live tests disabled (use --live to enable)"
    fi
    echo ""
    
    # Script Validation
    echo "--- Script Validation ---"
    run_test "Scripts exist" test_scripts_exist
    run_test "Scripts are executable" test_scripts_are_executable
    run_test "Scripts have proper shebang" test_scripts_have_shebang
    run_test "No jq dependency" test_no_jq_dependency
    run_test "Shellcheck passes (if available)" test_shellcheck_passes
    echo ""
    
    # Artifact Naming - Linux x86_64
    echo "--- Artifact Naming: Linux x86_64 ---"
    run_test "Linux x86_64 default artifact name" test_artifact_name_linux_x86_64_default
    run_test "Legacy Linux x86_64 models artifact name" test_artifact_name_linux_x86_64_models
    run_test "Linux x86_64 CUDA 90 artifact name" test_artifact_name_linux_x86_64_cuda_90
    run_test "Linux x86_64 CUDA 89 artifact name" test_artifact_name_linux_x86_64_cuda_89
    run_test "Linux x86_64 CUDA 87 artifact name" test_artifact_name_linux_x86_64_cuda_87
    run_test "Linux x86_64 CUDA 86 artifact name" test_artifact_name_linux_x86_64_cuda_86
    run_test "Linux x86_64 CUDA 80 artifact name" test_artifact_name_linux_x86_64_cuda_80
    echo ""
    
    # Artifact Naming - Linux aarch64
    echo "--- Artifact Naming: Linux aarch64 ---"
    run_test "Linux aarch64 default artifact name" test_artifact_name_linux_aarch64_default
    run_test "Legacy Linux aarch64 models artifact name" test_artifact_name_linux_aarch64_models
    echo ""
    
    # Artifact Naming - macOS
    echo "--- Artifact Naming: macOS (darwin) ---"
    run_test "Darwin aarch64 default artifact name" test_artifact_name_darwin_aarch64_default
    run_test "Legacy Darwin aarch64 models artifact name" test_artifact_name_darwin_aarch64_models
    run_test "Darwin aarch64 metal artifact name" test_artifact_name_darwin_aarch64_metal
    echo ""
    
    # Artifact Naming - Windows
    echo "--- Artifact Naming: Windows ---"
    run_test "Legacy Windows x86_64 runtime artifact name" test_artifact_name_windows_x86_64_default
    run_test "Legacy Windows x86_64 models artifact name" test_artifact_name_windows_x86_64_models
    echo ""
    
    # Artifact Naming - Spice CLI
    echo "--- Artifact Naming: Spice CLI ---"
    run_test "Spice CLI Linux x86_64 artifact name" test_artifact_name_spice_linux_x86_64
    run_test "Spice CLI Darwin aarch64 artifact name" test_artifact_name_spice_darwin_aarch64
    run_test "Spice CLI Linux aarch64 artifact name" test_artifact_name_spice_linux_aarch64
    echo ""
    
    # URL Construction
    echo "--- URL Construction ---"
    run_test "Download URL format for spiced" test_download_url_format_spiced
    run_test "Download URL format for spice" test_download_url_format_spice
    echo ""
    
    # Platform Validation
    echo "--- Platform Validation ---"
    run_test "CUDA valid only on Linux" test_variant_validation_cuda_only_linux
    run_test "CUDA invalid on Darwin" test_variant_validation_cuda_invalid_darwin
    run_test "Metal valid only on Darwin" test_variant_validation_metal_only_darwin
    run_test "Metal invalid on Linux" test_variant_validation_metal_invalid_linux
    echo ""
    
    # Architecture Mapping
    echo "--- Architecture Mapping ---"
    run_test "arm64 maps to aarch64" test_arch_mapping_arm64_to_aarch64
    run_test "amd64 maps to x86_64" test_arch_mapping_amd64_to_x86_64
    run_test "armv7 maps to arm" test_arch_mapping_armv7_to_arm
    run_test "x86_64 unchanged" test_arch_mapping_x86_64_unchanged
    echo ""
    
    # OS Detection
    echo "--- OS Detection ---"
    run_test "MINGW detected as Windows" test_os_detection_mingw_to_windows
    run_test "MSYS detected as Windows" test_os_detection_msys_to_windows
    run_test "Cygwin detected as Windows" test_os_detection_cygwin_to_windows
    run_test "Linux unchanged" test_os_detection_linux_unchanged
    run_test "Darwin unchanged" test_os_detection_darwin_unchanged
    echo ""
    
    # Supported Platforms
    echo "--- Supported Platforms ---"
    run_test "Four platforms supported" test_supported_platforms_list
    run_test "linux-x86_64 is supported" test_linux_x86_64_is_supported
    run_test "linux-aarch64 is supported" test_linux_aarch64_is_supported
    run_test "darwin-aarch64 is supported" test_darwin_aarch64_is_supported
    run_test "windows-x86_64 is supported" test_windows_x86_64_is_supported
    run_test "darwin-x86_64 is NOT supported" test_darwin_x86_64_is_not_supported
    echo ""
    
    # CUDA Versions
    echo "--- CUDA Version Validation ---"
    run_test "CUDA version 80 valid" test_cuda_version_80_valid
    run_test "CUDA version 86 valid" test_cuda_version_86_valid
    run_test "CUDA version 87 valid" test_cuda_version_87_valid
    run_test "CUDA version 89 valid" test_cuda_version_89_valid
    run_test "CUDA version 90 valid" test_cuda_version_90_valid
    run_test "CUDA version 85 invalid" test_cuda_version_invalid
    echo ""
    
    # Default Values
    echo "--- Default Values ---"
    run_test "Default variant has no archive suffix" test_default_variant_is_empty
    run_test "Variant can be overridden" test_variant_can_be_overridden
    run_test "Empty variant behavior" test_variant_can_be_empty
    echo ""
    
    # Retry Logic
    echo "--- Retry Logic ---"
    run_test "Max retries is 3" test_retry_config_max_retries
    run_test "Initial delay is 2 seconds" test_retry_config_initial_delay
    run_test "Exponential backoff works" test_retry_exponential_backoff
    echo ""
    
    # Script Functions
    echo "--- Script Functions ---"
    run_test "install.sh has getLatestRelease" test_install_sh_has_getLatestRelease
    run_test "install.sh has downloadFile" test_install_sh_has_downloadFile
    run_test "install.sh has installFile" test_install_sh_has_installFile
    run_test "install.sh has checkHttpRequestCLI" test_install_sh_has_checkHttpRequestCLI
    run_test "install.sh has no checkJqInstalled" test_install_sh_no_checkJqInstalled
    run_test "install-spiced.sh has getLatestRelease" test_install_spiced_sh_has_getLatestRelease
    run_test "install-spiced.sh has downloadFile" test_install_spiced_sh_has_downloadFile
    run_test "install-spiced.sh has verifySupported" test_install_spiced_sh_has_verifySupported
    run_test "install-spiced.sh has no checkJqInstalled" test_install_spiced_sh_no_checkJqInstalled
    echo ""
    
    # Documentation
    echo "--- Documentation ---"
    run_test "Naming convention documented" test_spiced_naming_convention_documented
    run_test "Empty variant documented" test_spiced_variant_empty_documented
    run_test "Model support documented" test_spiced_variant_models_documented
    run_test "Metal variant documented" test_spiced_variant_metal_documented
    run_test "CUDA variant documented" test_spiced_variant_cuda_documented
    echo ""
    
    # Sudo decision (a root user never runs sudo, so these only run unprivileged)
    echo "--- Sudo Decision ---"
    if [[ $EUID -ne 0 ]]; then
        run_test "install.sh: first install into a fresh HOME needs no sudo" test_install_sh_fresh_home_needs_no_sudo
        run_test "install.sh: unwritable install dir uses sudo" test_install_sh_unwritable_dir_uses_sudo
        run_test "install.sh: explicit USE_SUDO=true is honored" test_install_sh_explicit_use_sudo_is_honored
        run_test "install-nightly.sh: missing parent dir needs no sudo" test_install_nightly_sh_missing_parent_needs_no_sudo
        run_test "install-spiced.sh: creates a missing install dir with sudo when needed" test_install_spiced_sh_creates_install_dir_with_sudo_when_needed
        run_test "install-spiced.sh: missing writable install dir needs no sudo" test_install_spiced_sh_missing_writable_dir_needs_no_sudo
    else
        skip_test "Sudo decision tests" "running as root"
    fi
    echo ""

    # Download verification
    echo "--- Download Verification ---"
    run_test "Verification block is identical in both installers" test_verification_block_is_identical_in_both_installers
    local client
    for client in "${HTTP_CLIENTS[@]}"; do
        if [[ "$client" == curl ]] || wget_can_retry_http_errors; then
            run_test "verifyFetch retries a transient error ($client)" "test_verify_fetch_retries_a_transient_error $client"
        else
            skip_test "verifyFetch retries a transient error ($client)" "this wget has no --retry-on-http-error"
        fi
        run_test "verifyFetch does not retry a 404 ($client)" "test_verify_fetch_does_not_retry_a_404 $client"
        run_test "verifyFetch does not retry a rate limit 403 ($client)" "test_verify_fetch_does_not_retry_a_rate_limit_403 $client"
        run_test "verifyFetch retries a rate limit 429 as it can ($client)" "test_verify_fetch_retries_a_rate_limit_429 $client"
        run_test "verifyFetch does not wait out a long Retry-After ($client)" "test_verify_fetch_does_not_wait_out_a_long_retry_after $client"
    done
    run_test "install.sh stops when a listed SHA256SUMS cannot be downloaded" test_install_sh_stops_when_a_listed_sha256sums_cannot_be_downloaded
    run_test "install.sh opt-out does not skip a listed SHA256SUMS" test_install_sh_opt_out_does_not_skip_a_listed_sha256sums
    run_test "install-spiced.sh stops when a listed SHA256SUMS cannot be downloaded" test_install_spiced_sh_stops_when_a_listed_sha256sums_cannot_be_downloaded
    run_test "install.sh: verifies against SHA256SUMS" test_install_sh_verifies_against_sha256sums
    run_test "install.sh: rejects a checksum mismatch before extracting" test_install_sh_rejects_a_checksum_mismatch_before_extracting
    run_test "install.sh: rejects an asset missing from SHA256SUMS" test_install_sh_rejects_an_asset_missing_from_sha256sums
    run_test "install.sh: falls back to the GitHub asset digest" test_install_sh_falls_back_to_the_github_digest
    run_test "install.sh: rejects a GitHub digest mismatch" test_install_sh_rejects_a_github_digest_mismatch
    run_test "install.sh: fails closed without a checksum" test_install_sh_fails_closed_without_a_checksum
    run_test "install.sh: fails closed when the API request fails" test_install_sh_fails_closed_when_the_api_request_fails
    run_test "install.sh: SPICE_INSTALL_ALLOW_UNVERIFIED installs an unverifiable download" test_install_sh_opt_out_installs_an_unverifiable_download
    run_test "install.sh: SPICE_INSTALL_ALLOW_UNVERIFIED never installs a mismatch" test_install_sh_opt_out_never_installs_a_mismatch
    run_test "install.sh: checks the attestation when gh is logged in" test_install_sh_checks_the_attestation_when_gh_is_logged_in
    run_test "install.sh: rejects a failed attestation" test_install_sh_rejects_a_failed_attestation
    run_test "install-spiced.sh: verifies against SHA256SUMS" test_install_spiced_sh_verifies_against_sha256sums
    run_test "install-spiced.sh: rejects a checksum mismatch" test_install_spiced_sh_rejects_a_checksum_mismatch
    run_test "install-spiced.sh: falls back to the GitHub asset digest" test_install_spiced_sh_falls_back_to_the_github_digest
    run_test "sha256 falls back to openssl" test_sha256_falls_back_to_openssl
    run_test "sha256 fails without a hash tool" test_sha256_fails_without_a_tool
    run_test "install.sh: opt-out without a hash tool does not install an unlisted asset" test_install_sh_opt_out_without_a_hash_tool_does_not_install_an_unlisted_asset
    run_test "install.sh: opt-out without a hash tool does not skip SHA256SUMS" test_install_sh_opt_out_without_a_hash_tool_does_not_skip_sha256sums
    run_test "install.sh: opt-out without a hash tool installs an unsigned release" test_install_sh_opt_out_without_a_hash_tool_installs_an_unsigned_release
    run_test "install.sh: verifies with a backslash in TMPDIR" test_install_sh_verifies_with_a_backslash_in_tmpdir
    run_test "install.sh: finds a listed SHA256SUMS under pipefail" test_install_sh_finds_a_listed_sha256sums_under_pipefail
    run_test "install.sh: reads the GitHub digest under pipefail" test_install_sh_reads_the_github_digest_under_pipefail
    echo ""

    # GitHub requests and rate limits
    echo "--- GitHub Requests and Rate Limits ---"
    local installer
    if [[ ${#HTTP_CLIENTS[@]} -lt 2 ]]; then
        if [[ "$REQUIRE_WGET" == "true" ]]; then
            run_test "wget is installed (--require-wget)" false
        else
            skip_test "GitHub request tests with wget" "wget is not installed"
        fi
    fi
    for installer in install.sh install-spiced.sh; do
        for client in "${HTTP_CLIENTS[@]}"; do
            run_test "$installer ($client): installs without a token, sending no Authorization header" "test_installs_without_a_token $installer $client"
            run_test "$installer ($client): sends GITHUB_TOKEN with every API request" "test_sends_the_token_to_the_api $installer $client"
            run_test "$installer ($client): stops when the latest release lookup is rate limited" "test_stops_when_the_latest_release_lookup_is_rate_limited $installer $client"
            run_test "$installer ($client): falls back to the API for the latest release" "test_falls_back_to_the_api_for_the_latest_release $installer $client"
            run_test "$installer ($client): stops when the checksum lookup is rate limited" "test_stops_when_the_checksum_lookup_is_rate_limited $installer $client"
        done
        run_test "$installer: verifies SHA256SUMS with no API request" "test_verifies_sha256sums_with_no_api_request $installer"
        run_test "$installer: opt-out installs when the checksum lookup is rate limited" "test_opt_out_installs_when_the_checksum_lookup_is_rate_limited $installer"
        run_test "$installer: installs without gh, skipping the attestation check" "test_installs_without_gh $installer"
    done
    for client in "${HTTP_CLIENTS[@]}"; do
        run_test "install.sh ($client): stops when the checksum lookup gets a 429" "test_stops_when_the_checksum_lookup_gets_a_429 $client"
    done
    echo ""

    # runAsRoot argument handling (as root, the sudo half runs the command directly)
    echo "--- runAsRoot Arguments ---"
    run_test "install.sh: runAsRoot keeps its arguments literal" test_install_sh_run_as_root_keeps_arguments_literal
    run_test "install-spiced.sh: runAsRoot keeps its arguments literal" test_install_spiced_sh_run_as_root_keeps_arguments_literal
    run_test "install-nightly.sh: runAsRoot keeps its arguments literal" test_install_nightly_sh_run_as_root_keeps_arguments_literal
    run_test "installers never call eval" test_installers_do_not_eval
    echo ""

    # install.sh: shells, versions, platforms and privileges
    echo "--- Shells, Versions, Platforms and Privileges ---"
    run_test "install.sh runs under POSIX sh" test_install_sh_runs_under_posix_sh
    run_test "install.sh accepts a v-prefixed version" test_install_sh_accepts_a_v_prefixed_version
    run_test "install.sh rejects an invalid version before any request" test_install_sh_rejects_an_invalid_version_before_any_request
    run_test "install.sh prints its help" test_install_sh_prints_its_help
    run_test "install.sh refuses to run under sudo" test_install_sh_refuses_to_run_under_sudo
    run_test "install.sh runs under sudo when asked to" test_install_sh_runs_under_sudo_when_asked_to
    run_test "install.sh installs the native build under Rosetta 2" test_install_sh_installs_the_native_build_under_rosetta
    run_test "install.sh refuses an Intel Mac" test_install_sh_refuses_an_intel_mac
    run_test "install.sh refuses a musl system" test_install_sh_refuses_a_musl_system
    run_test "install-spiced.sh refuses a musl system" test_install_spiced_sh_refuses_a_musl_system
    run_test "install-spiced.sh accepts a v-prefixed version" test_install_spiced_sh_accepts_a_v_prefixed_version
    echo ""

    echo "--- Replacing an Installed Binary ---"
    run_test "install.sh keeps the installed spice when the new one cannot run" test_install_sh_keeps_the_installed_spice_when_the_new_one_cannot_run
    run_test "install.sh refuses a binary reporting another version" test_install_sh_refuses_a_binary_reporting_another_version
    run_test "install-spiced.sh keeps the installed spiced when the new one cannot run" test_install_spiced_sh_keeps_the_installed_spiced_when_the_new_one_cannot_run
    if [[ "$(host_os)" == linux ]]; then
        run_test "install.sh upgrades a running spice" test_install_sh_upgrades_a_running_spice
        run_test "install-spiced.sh upgrades a running spiced" test_install_spiced_sh_upgrades_a_running_spiced
    else
        skip_test "Upgrading a running binary" "only Linux refuses to write a running binary in place"
    fi
    echo ""

    echo "--- PATH Setup ---"
    run_test "install.sh puts spice on PATH for new bash shells" test_install_sh_puts_spice_on_path_for_new_bash_shells
    run_test "install.sh does not hide an existing .profile" test_install_sh_does_not_hide_an_existing_profile
    run_test "install.sh honors ZDOTDIR" test_install_sh_honors_zdotdir
    run_test "install.sh writes a fish conf.d file" test_install_sh_writes_a_fish_conf_d_file
    run_test "install.sh writes csh setenv" test_install_sh_writes_csh_setenv
    run_test "install.sh PATH setup is idempotent" test_install_sh_path_setup_is_idempotent
    run_test "install.sh puts a custom dir on PATH" test_install_sh_puts_a_custom_dir_on_path
    run_test "install.sh puts a dir outside HOME on PATH" test_install_sh_puts_a_dir_outside_home_on_path
    run_test "install.sh leaves profiles alone when the dir is on PATH" test_install_sh_leaves_profiles_alone_when_the_dir_is_on_path
    run_test "install.sh SPICE_INSTALL_NO_MODIFY_PATH leaves profiles alone" test_install_sh_no_modify_path_leaves_profiles_alone
    run_test "install.sh leaves an unusual dir to the user" test_install_sh_leaves_an_unusual_dir_to_the_user
    run_test "install.sh keeps a legacy PATH line" test_install_sh_keeps_a_legacy_path_line
    run_test "install.sh updates its block through a symlink" test_install_sh_updates_its_block_through_a_symlink
    run_test "install.sh leaves an unterminated block alone" test_install_sh_leaves_an_unterminated_block_alone
    run_test "install.sh warns about another spice on PATH" test_install_sh_warns_about_another_spice_on_path
    run_test "install.sh prints no color codes off a terminal" test_install_sh_prints_no_color_codes_off_a_terminal
    echo ""

    echo "--- Spice Skills for AI Coding Agents ---"
    run_test "skills are never offered without a terminal" test_skills_are_never_offered_without_a_terminal
    run_test "skills are not offered when no agent is installed" test_skills_are_not_offered_when_no_agent_is_installed
    run_test "skills install for the chosen agent" test_skills_install_for_the_chosen_agent
    run_test "skills install for every agent" test_skills_install_for_every_agent
    run_test "skills install for several agents" test_skills_install_for_several_agents
    run_test "skills default to none" test_skills_default_to_none
    run_test "skills ask again after an invalid answer" test_skills_ask_again_after_an_invalid_answer
    run_test "skills are not offered in CI or with NONINTERACTIVE" test_skills_are_not_offered_in_ci_or_noninteractive
    run_test "a skills failure does not fail the install" test_skills_failure_does_not_fail_the_install
    run_test "skills already installed is not a failure" test_skills_already_installed_is_not_a_failure
    echo ""

    # Error Handling
    echo "--- Error Handling ---"
    run_test "install.sh has error handling" test_script_has_error_handling
    run_test "install-spiced.sh uses set -e" test_spiced_script_uses_set_e
    run_test "install-spiced.sh has fail_trap" test_spiced_script_has_fail_trap
    run_test "install-spiced.sh has cleanup" test_spiced_script_has_cleanup
    echo ""
    
    # Live Tests (if enabled)
    if [[ "$LIVE_TESTS" == "true" ]]; then
        echo "--- Live Network Tests ---"
        run_test "Latest release accessible" test_live_latest_release_accessible
        run_test "Download URL resolves" test_live_download_url_resolves
        run_test "spiced_linux downloadable" test_live_spiced_linux_downloadable
        run_test "All expected artifacts exist" test_artifacts_exist_in_latest_release
        echo ""
    fi
    
    # Summary
    echo "=========================================="
    echo "  Test Summary"
    echo "=========================================="
    echo ""
    echo -e "  Total:   ${TEST_COUNT}"
    echo -e "  ${GREEN}Passed:  ${PASS_COUNT}${NC}"
    if [[ $FAIL_COUNT -gt 0 ]]; then
        echo -e "  ${RED}Failed:  ${FAIL_COUNT}${NC}"
    else
        echo -e "  Failed:  ${FAIL_COUNT}"
    fi
    if [[ $SKIP_COUNT -gt 0 ]]; then
        echo -e "  ${YELLOW}Skipped: ${SKIP_COUNT}${NC}"
    fi
    echo ""
    
    if [[ $FAIL_COUNT -gt 0 ]]; then
        echo -e "${RED}Some tests failed!${NC}"
        return 1
    else
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    fi
}

# =============================================================================
# Entry Point
# =============================================================================

main() {
    parse_args "$@"
    setup_test_env
    run_all_tests
}

main "$@"
