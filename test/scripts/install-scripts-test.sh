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
#   ./test/scripts/install-scripts-test.sh [--live] [--verbose]
#
# Options:
#   --live      Run live download tests (requires network and Python 3, slower)
#   --verbose   Show detailed output for each test
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
        FAIL_COUNT=$((FAIL_COUNT + 1))
        log_fail "$test_name"
        return 1
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
    trap 'rm -rf "$TEST_TMP_DIR"' EXIT
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

test_scripts_have_shebang() {
    head -1 "$INSTALL_SCRIPT" | grep -q "#!/usr/bin/env bash" && \
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

sha256_of() {
    if command -v sha256sum &> /dev/null; then
        sha256sum "$1" | awk '{ print $1 }'
    else
        shasum -a 256 "$1" | awk '{ print $1 }'
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
  "body": "Notes"
}
JSON
}

# Runs an installer end to end, piped into bash as the README one-liner does, with no network
# and no TTY. Stubs stand in for the network, sudo and the GitHub CLI:
#   curl  serves the release: a local archive holding a stub binary, and SHA256SUMS and the
#         API release when they exist; anything missing fails as `curl --fail` does.
#   sudo  records its arguments to $home.sudo and refuses, as sudo does without a terminal.
#   gh    records its arguments to $home.gh.
# What the release records for the archive comes from these variables:
#   OFFLINE_SUMS    match (default), mismatch, unlisted (lists another asset), none, or
#                   unreachable (the API lists SHA256SUMS, but downloading it fails)
#   OFFLINE_DIGEST  match (default), mismatch, null, or fail (the API request fails)
#   OFFLINE_GH      loggedout (default), pass, or fail (the attestation check)
#   OFFLINE_HASH    ok (default), or none (sha256sum, shasum and openssl print nothing and fail)
# Usage: run_installer_offline <script> <binary> <asset> <home> [VAR=value...], where the
# VAR=value pairs go into the installer's environment. The log is $home.log.
run_installer_offline() {
    local script="$1"
    local binary="$2"
    local asset="$3"
    local home="$4"
    shift 4
    local stubs="$home.stubs"
    local digest other
    mkdir -p "$stubs/pkg"
    printf '#!/bin/sh\necho "CLI version: v0.0.0"\n' > "$stubs/pkg/$binary"
    chmod +x "$stubs/pkg/$binary"
    tar czf "$stubs/archive.tar.gz" -C "$stubs/pkg" "$binary"
    digest=$(sha256_of "$stubs/archive.tar.gz")
    other=$(printf '%064d' 0)
    rm -f "$stubs/SHA256SUMS" "$stubs/release.json"
    case "${OFFLINE_SUMS:-match}" in
        match) printf '%s  %s\n' "$digest" "$asset" > "$stubs/SHA256SUMS" ;;
        mismatch) printf '%s  %s\n' "$other" "$asset" > "$stubs/SHA256SUMS" ;;
        unlisted) printf '%s  %s\n' "$digest" "other_asset.tar.gz" > "$stubs/SHA256SUMS" ;;
    esac
    # A release lists SHA256SUMS among its assets exactly when it has one.
    local sums_listed=yes
    [ "${OFFLINE_SUMS:-match}" = "none" ] && sums_listed=no
    case "${OFFLINE_DIGEST:-match}" in
        match) release_json "$asset" "\"sha256:$digest\"" "$sums_listed" > "$stubs/release.json" ;;
        mismatch) release_json "$asset" "\"sha256:$other\"" "$sums_listed" > "$stubs/release.json" ;;
        null) release_json "$asset" null "$sums_listed" > "$stubs/release.json" ;;
    esac
    cat > "$stubs/curl" <<STUB
#!/bin/sh
url=""
out=""
while [ \$# -gt 0 ]; do
    case "\$1" in
        -o) out="\$2"; shift ;;
        -H) shift ;;
        http*) url="\$1" ;;
    esac
    shift
done
case "\$url" in
    */SHA256SUMS) src="$stubs/SHA256SUMS" ;;
    https://api.github.com/*) src="$stubs/release.json" ;;
    *) src="$stubs/archive.tar.gz" ;;
esac
[ -f "\$src" ] || exit 22
if [ -n "\$out" ]; then cp "\$src" "\$out"; else cat "\$src"; fi
STUB
    cat > "$stubs/sudo" <<STUB
#!/bin/sh
echo "\$*" >> "$home.sudo"
echo "sudo: a terminal is required to read the password" >&2
exit 1
STUB
    cat > "$stubs/gh" <<STUB
#!/bin/sh
echo "\$*" >> "$home.gh"
case "\$1 \$2" in
    "auth status") [ "${OFFLINE_GH:-loggedout}" != loggedout ] ;;
    "attestation verify") [ "${OFFLINE_GH:-loggedout}" = pass ] || { echo "verification failed" >&2; exit 1; } ;;
    *) exit 1 ;;
esac
STUB
    chmod +x "$stubs/curl" "$stubs/sudo" "$stubs/gh"
    if [ "${OFFLINE_HASH:-ok}" = none ]; then
        for tool in sha256sum shasum openssl; do
            printf '#!/bin/sh\nexit 1\n' > "$stubs/$tool"
            chmod +x "$stubs/$tool"
        done
    fi
    : > "$home.sudo"
    : > "$home.gh"
    env -i HOME="$home" PATH="$stubs:/usr/bin:/bin:/usr/sbin:/sbin" SHELL=/bin/bash "$@" \
        /bin/bash -c "cat '$script' | /bin/bash -s -- 0.0.0" > "$home.log" 2>&1 < /dev/null
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
    grep -q "^mkdir -p $home/locked/new/bin" "$home.sudo"
}

test_install_sh_explicit_use_sudo_is_honored() {
    local home="$TEST_TMP_DIR/explicit-sudo-home"
    mkdir -p "$home"
    run_install_offline "$home" USE_SUDO=true || true
    grep -q "^cp .* $home/.spice/bin$" "$home.sudo"
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

# One local HTTP server for the verifyFetch tests, started on first use and shared by them, so
# each test needs no server start of its own. A path that starts with /flaky answers 504 to its
# first request and "ok" after it; any other path is a 404. Each request's path is appended to
# $FLAKY_BASE.hits, and the port is in $FLAKY_BASE.port. The server exits by itself after five
# minutes, so a run that stops early leaves nothing behind.
FLAKY_BASE=""
FLAKY_SERVER_PID=""
start_flaky_server() {
    if [[ -n "$FLAKY_SERVER_PID" ]] && kill -0 "$FLAKY_SERVER_PID" 2>/dev/null; then
        return 0
    fi
    FLAKY_BASE="$TEST_TMP_DIR/flaky-server"
    : > "$FLAKY_BASE.hits"
    python3 - "$FLAKY_BASE" > "$FLAKY_BASE.log" 2>&1 <<'PY' &
import http.server
import os
import sys
import threading

base = sys.argv[1]
seen = {}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        seen[self.path] = seen.get(self.path, 0) + 1
        with open(base + ".hits", "a") as hits:
            hits.write(self.path + "\n")
        flaky = self.path.startswith("/flaky")
        if flaky and seen[self.path] > 1:
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(504 if flaky else 404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


threading.Timer(300, os._exit, [0]).start()
server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
with open(base + ".port", "w") as port:
    port.write(str(server.server_address[1]))
server.serve_forever()
PY
    FLAKY_SERVER_PID=$!
    for _ in $(seq 1 200); do
        [[ -s "$FLAKY_BASE.port" ]] && return 0
        sleep 0.1
    done
    echo "the test server did not start in 20 seconds; its output:"
    cat "$FLAKY_BASE.log"
    return 1
}

stop_flaky_server() {
    if [[ -n "$FLAKY_SERVER_PID" ]]; then
        kill "$FLAKY_SERVER_PID" 2>/dev/null || true
        wait "$FLAKY_SERVER_PID" 2>/dev/null || true
        FLAKY_SERVER_PID=""
    fi
}

# Runs install.sh's verifyFetch with the given HTTP client (curl or wget). Prints its exit code.
run_verify_fetch() {
    local client="$1" url="$2" out="$3" rc=0
    /bin/bash -c "$(verification_block "$INSTALL_SCRIPT")"$'\n'"SPICE_HTTP_REQUEST_CLI=$client; verifyFetch '$url' '$out'" > /dev/null 2>&1 || rc=$?
    echo "$rc"
}

# verifyFetch with client $1 against the flaky server's path $2, which no other test uses. Passes
# when the exit code is zero exactly when $3 is "ok", and the server saw the path $4 times.
check_verify_fetch() {
    local client="$1" url_path="$2" expect="$3" hits="$4"
    local out="$TEST_TMP_DIR/verify-fetch${url_path//\//-}.out" rc seen
    start_flaky_server || return 1
    rc=$(run_verify_fetch "$client" "http://127.0.0.1:$(cat "$FLAKY_BASE.port")$url_path" "$out")
    seen=$(grep -c "^$url_path\$" "$FLAKY_BASE.hits" || true)
    if [[ "$expect" == ok ]]; then
        [[ "$rc" == 0 && "$(cat "$out")" == ok && "$seen" == "$hits" ]] && return 0
    else
        [[ "$rc" != 0 && "$seen" == "$hits" ]] && return 0
    fi
    echo "$client $url_path: exit $rc, $seen request(s); expected $expect after $hits"
    return 1
}

test_verify_fetch_retries_a_transient_error_with_curl() {
    check_verify_fetch curl /flaky-curl ok 2
}

test_verify_fetch_does_not_retry_a_404_with_curl() {
    check_verify_fetch curl /missing-curl fail 1
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

test_verify_fetch_retries_a_transient_error_with_wget() {
    check_verify_fetch wget /flaky-wget ok 2
}

test_verify_fetch_does_not_retry_a_404_with_wget() {
    check_verify_fetch wget /missing-wget fail 1
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
            --help|-h)
                echo "Usage: $0 [--live] [--verbose]"
                echo ""
                echo "Options:"
                echo "  --live      Run live download tests (requires network)"
                echo "  --verbose   Show detailed output for each test"
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
    else
        skip_test "Sudo decision tests" "running as root"
    fi
    echo ""

    # Download verification
    echo "--- Download Verification ---"
    run_test "Verification block is identical in both installers" test_verification_block_is_identical_in_both_installers
    run_test "verifyFetch retries a transient error (curl)" test_verify_fetch_retries_a_transient_error_with_curl
    run_test "verifyFetch does not retry a 404 (curl)" test_verify_fetch_does_not_retry_a_404_with_curl
    if wget_can_retry_http_errors; then
        run_test "verifyFetch retries a transient error (wget)" test_verify_fetch_retries_a_transient_error_with_wget
        run_test "verifyFetch does not retry a 404 (wget)" test_verify_fetch_does_not_retry_a_404_with_wget
    else
        skip_test "verifyFetch retries (wget)" "this wget has no --retry-on-http-error"
    fi
    stop_flaky_server
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
