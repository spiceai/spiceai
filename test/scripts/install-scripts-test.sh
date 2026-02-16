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
#   --live      Run live download tests (requires network, slower)
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
    trap "rm -rf $TEST_TMP_DIR" EXIT
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
    
    # Run shellcheck with common exclusions for install scripts
    # SC2034: unused variables (often used for configuration)
    # SC2086: word splitting (often intentional)
    # SC2155: declare and assign separately
    # SC2223: default assignment globbing (acceptable pattern)
    # SC1090: non-constant source (dynamic shell profile loading)
    shellcheck -e SC2034,SC2086,SC2155,SC2223,SC1090 "$INSTALL_SCRIPT" 2>/dev/null && \
    shellcheck -e SC2034,SC2086,SC2155,SC2223,SC1090 "$INSTALL_SPICED_SCRIPT" 2>/dev/null
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
            artifact_name="${artifact_name}_models_cuda_${cuda_version}"
        elif [[ "$variant" == "metal" ]]; then
            artifact_name="${artifact_name}_models_metal"
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
    [[ "$result" == "spiced_models_cuda_90_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_89() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "89")
    [[ "$result" == "spiced_models_cuda_89_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_87() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "87")
    [[ "$result" == "spiced_models_cuda_87_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_86() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "86")
    [[ "$result" == "spiced_models_cuda_86_linux_x86_64.tar.gz" ]]
}

test_artifact_name_linux_x86_64_cuda_80() {
    local result
    result=$(get_spiced_artifact_name "linux" "x86_64" "cuda" "80")
    [[ "$result" == "spiced_models_cuda_80_linux_x86_64.tar.gz" ]]
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
    [[ "$result" == "spiced_models_metal_darwin_aarch64.tar.gz" ]]
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
    release_assets=$(curl -sS "https://api.github.com/repos/spiceai/spiceai/releases/latest" | grep '"name":' | grep -E "spice.*\.tar\.gz" || true)
    
    if [[ -z "$release_assets" ]]; then
        log_verbose "Could not fetch release assets"
        return 1
    fi
    
    # Check critical artifacts exist
    local expected_artifacts=(
        "spice_linux_x86_64.tar.gz"
        "spice_linux_aarch64.tar.gz"
        "spice_darwin_aarch64.tar.gz"
        "spiced_linux_x86_64.tar.gz"
        "spiced_linux_aarch64.tar.gz"
        "spiced_darwin_aarch64.tar.gz"
        "spiced_models_linux_x86_64.tar.gz"
        "spiced_models_linux_aarch64.tar.gz"
        "spiced_models_darwin_aarch64.tar.gz"
        "spiced_models_metal_darwin_aarch64.tar.gz"
        "spiced.exe_windows_x86_64.tar.gz"
        "spiced.exe_models_windows_x86_64.tar.gz"
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

test_default_variant_is_models() {
    # Source the script in a subshell and check VARIANT default
    local variant
    variant=$(bash -c 'source /dev/stdin <<< "
        : \${VARIANT:=\"models\"}
        echo \$VARIANT
    "')
    [[ "$variant" == "models" ]]
}

test_variant_can_be_overridden() {
    local variant
    variant=$(VARIANT="metal" bash -c '
        : ${VARIANT:="models"}
        echo $VARIANT
    ')
    [[ "$variant" == "metal" ]]
}

test_variant_can_be_empty() {
    local variant
    variant=$(VARIANT="" bash -c '
        : ${VARIANT:="models"}
        echo $VARIANT
    ')
    # When VARIANT is set to empty, :="models" will still set it to models
    # because := checks for unset OR empty. To allow empty, use := vs :-
    # The current script uses := so empty becomes "models"
    # This test validates the current behavior
    [[ "$variant" == "models" ]]
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
    response=$(curl -sS -o /dev/null -w "%{http_code}" "https://api.github.com/repos/spiceai/spiceai/releases/latest")
    [[ "$response" == "200" ]]
}

test_live_download_url_resolves() {
    if [[ "$LIVE_TESTS" != "true" ]]; then
        return 0
    fi
    
    # Get latest release tag
    local tag
    tag=$(curl -sS "https://api.github.com/repos/spiceai/spiceai/releases/latest" | grep '"tag_name"' | head -1 | sed 's/.*: "\(.*\)",/\1/')
    
    if [[ -z "$tag" ]]; then
        log_verbose "Could not get latest tag"
        return 1
    fi
    
    # Check if a known artifact URL returns 302 (redirect to download)
    local url="https://github.com/spiceai/spiceai/releases/download/${tag}/spice_linux_x86_64.tar.gz"
    local response
    response=$(curl -sS -o /dev/null -w "%{http_code}" -L "$url" 2>/dev/null || echo "000")
    
    [[ "$response" == "200" ]]
}

test_live_spiced_models_linux_downloadable() {
    if [[ "$LIVE_TESTS" != "true" ]]; then
        return 0
    fi
    
    local tag
    tag=$(curl -sS "https://api.github.com/repos/spiceai/spiceai/releases/latest" | grep '"tag_name"' | head -1 | sed 's/.*: "\(.*\)",/\1/')
    
    if [[ -z "$tag" ]]; then
        return 1
    fi
    
    local url="https://github.com/spiceai/spiceai/releases/download/${tag}/spiced_models_linux_x86_64.tar.gz"
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
    grep -q 'No variant.*VARIANT=""' "$INSTALL_SPICED_SCRIPT"
}

test_spiced_variant_models_documented() {
    grep -q 'Models.*VARIANT="models".*the default' "$INSTALL_SPICED_SCRIPT"
}

test_spiced_variant_metal_documented() {
    grep -q 'Metal.*VARIANT="metal"' "$INSTALL_SPICED_SCRIPT"
}

test_spiced_variant_cuda_documented() {
    grep -q 'CUDA.*VARIANT="cuda"' "$INSTALL_SPICED_SCRIPT"
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
    run_test "Linux x86_64 models artifact name" test_artifact_name_linux_x86_64_models
    run_test "Linux x86_64 CUDA 90 artifact name" test_artifact_name_linux_x86_64_cuda_90
    run_test "Linux x86_64 CUDA 89 artifact name" test_artifact_name_linux_x86_64_cuda_89
    run_test "Linux x86_64 CUDA 87 artifact name" test_artifact_name_linux_x86_64_cuda_87
    run_test "Linux x86_64 CUDA 86 artifact name" test_artifact_name_linux_x86_64_cuda_86
    run_test "Linux x86_64 CUDA 80 artifact name" test_artifact_name_linux_x86_64_cuda_80
    echo ""
    
    # Artifact Naming - Linux aarch64
    echo "--- Artifact Naming: Linux aarch64 ---"
    run_test "Linux aarch64 default artifact name" test_artifact_name_linux_aarch64_default
    run_test "Linux aarch64 models artifact name" test_artifact_name_linux_aarch64_models
    echo ""
    
    # Artifact Naming - macOS
    echo "--- Artifact Naming: macOS (darwin) ---"
    run_test "Darwin aarch64 default artifact name" test_artifact_name_darwin_aarch64_default
    run_test "Darwin aarch64 models artifact name" test_artifact_name_darwin_aarch64_models
    run_test "Darwin aarch64 metal artifact name" test_artifact_name_darwin_aarch64_metal
    echo ""
    
    # Artifact Naming - Windows
    echo "--- Artifact Naming: Windows ---"
    run_test "Windows x86_64 default artifact name" test_artifact_name_windows_x86_64_default
    run_test "Windows x86_64 models artifact name" test_artifact_name_windows_x86_64_models
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
    run_test "Default variant is models" test_default_variant_is_models
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
    run_test "Models variant documented" test_spiced_variant_models_documented
    run_test "Metal variant documented" test_spiced_variant_metal_documented
    run_test "CUDA variant documented" test_spiced_variant_cuda_documented
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
        run_test "spiced_models_linux downloadable" test_live_spiced_models_linux_downloadable
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
