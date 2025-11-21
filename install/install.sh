#!/usr/bin/env bash
# Spice.ai installer script
# Version: 2.0.0 (2025-11-21)

# colors
blue="\033[0;94m"
white="\033[0;97m"
yellow="\033[0;33m"
reset="\033[0m"

SPICE_BIN=".spice/bin"

# Spice CLI location
: ${SPICE_CLI_INSTALL_DIR:="$HOME/$SPICE_BIN"}

# sudo is required to copy binary to SPICE_INSTALL_DIR for linux
: ${USE_SUDO:="false"}

# Http request CLI
SPICE_HTTP_REQUEST_CLI=curl

# GitHub Organization and repo name to download release
GITHUB_ORG=spiceai
GITHUB_REPO=spiceai

# Spice CLI filename
SPICE_CLI_FILENAME=spice

SPICE_CLI_FILE="${SPICE_CLI_INSTALL_DIR}/${SPICE_CLI_FILENAME}"

getSystemInfo() {
    ARCH=$(uname -m)
    case $ARCH in
        armv7*) ARCH="arm";;
        arm64) ARCH="aarch64";;
        amd64) ARCH="x86_64";;
    esac

    OS=$(echo `uname`|tr '[:upper:]' '[:lower:]')

    # Determine if sudo is needed based on install directory permissions
    if [[ -d "$SPICE_CLI_INSTALL_DIR" ]]; then
        # Directory exists, check if we can write to it
        if [[ ! -w "$SPICE_CLI_INSTALL_DIR" ]]; then
            USE_SUDO="true"
        fi
    else
        # Directory doesn't exist, check parent directory
        local parent_dir=$(dirname "$SPICE_CLI_INSTALL_DIR")
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

    echo "${current_osarch} does not have a pre-built binary. For supported architectures, visit https://spiceai.org/docs/reference/system_requirements#operating-systems-and-architectures"
    exit 1
}

runAsRoot() {
    local CMD="$*"

    if [ $EUID -ne 0 -a $USE_SUDO = "true" ]; then
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

function gh_curl() {
    if [ -z "$GITHUB_TOKEN" ]
    then
        curl \
            $@
    else
        curl -H "Authorization: token $GITHUB_TOKEN" \
            $@
    fi
}

function gh_wget() {
    if [ -z "$GITHUB_TOKEN" ]
    then
        wget \
            $@
    else
        wget --header="Authorization: token $GITHUB_TOKEN" \
            $@
    fi
}

getLatestRelease() {
    local spiceReleaseUrl="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest"
    local latest_release=""

    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        latest_release=$(gh_curl -s $spiceReleaseUrl | grep \"tag_name\" | awk 'NR==1{print $2}' |  sed -n 's/\"\(.*\)\",/\1/p')
    else
        latest_release=$(gh_wget -q --header="Accept: application/json" -O - $spiceReleaseUrl | grep \"tag_name\" | awk 'NR==1{print $2}' |  sed -n 's/\"\(.*\)\",/\1/p')
    fi

    ret_val=$latest_release
}

downloadFile() {
    LATEST_RELEASE_TAG=$1

    SPICE_CLI_ARTIFACT="${SPICE_CLI_FILENAME}_${OS}_${ARCH}.tar.gz"
    SPICE_CLI_CHECKSUM="${SPICE_CLI_ARTIFACT}.sha256"
    DOWNLOAD_BASE="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download"
    DOWNLOAD_URL="${DOWNLOAD_BASE}/${LATEST_RELEASE_TAG}/${SPICE_CLI_ARTIFACT}"
    CHECKSUM_URL="${DOWNLOAD_BASE}/${LATEST_RELEASE_TAG}/${SPICE_CLI_CHECKSUM}"

    # Create the temp directory
    SPICE_TMP_ROOT=$(mktemp -dt spice-install-XXXXXX)
    ARTIFACT_TMP_FILE="$SPICE_TMP_ROOT/$SPICE_CLI_ARTIFACT"
    CHECKSUM_TMP_FILE="$SPICE_TMP_ROOT/$SPICE_CLI_CHECKSUM"

    echo "Downloading $DOWNLOAD_URL ..."

    function gh_curl_vnd() {
        gh_curl -H "Accept: application/vnd.github.v3.raw" \
          $@
    }

    parser=". | map(select(.tag_name == \"$LATEST_RELEASE_TAG\"))[0].assets | map(select(.name == \"$SPICE_CLI_ARTIFACT\"))[0].id"

    asset_id=`gh_curl_vnd -s https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases | jq "$parser"`
    if [ "$asset_id" = "null" ]; then
      echo "ERROR: version not found $VERSION"
      exit 1
    fi;

    # Download the binary
    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        gh_curl -H "Accept:application/octet-stream" -SsL \
            https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$asset_id \
            -o "$ARTIFACT_TMP_FILE"
    else
        gh_wget -q --auth-no-challenge --header='Accept:application/octet-stream' \
            https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$asset_id \
            -O "$ARTIFACT_TMP_FILE"
    fi

    if [ ! -f "$ARTIFACT_TMP_FILE" ]; then
        echo "Failed to download $DOWNLOAD_URL"
        exit 1
    fi
    
    # Download the checksum file
    echo "Downloading checksum file..."
    checksum_parser=". | map(select(.tag_name == \"$LATEST_RELEASE_TAG\"))[0].assets | map(select(.name == \"$SPICE_CLI_CHECKSUM\"))[0].id"
    checksum_asset_id=`gh_curl_vnd -s https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases | jq "$checksum_parser"`
    
    if [ "$checksum_asset_id" != "null" ]; then
        if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
            gh_curl -H "Accept:application/octet-stream" -SsL \
                https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$checksum_asset_id \
                -o "$CHECKSUM_TMP_FILE"
        else
            gh_wget -q --auth-no-challenge --header='Accept:application/octet-stream' \
                https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$checksum_asset_id \
                -O "$CHECKSUM_TMP_FILE"
        fi
    else
        echo "Warning: Checksum file not found for this release, skipping verification"
        CHECKSUM_TMP_FILE=""
    fi
    
    # Verify checksum if available
    if [ -n "$CHECKSUM_TMP_FILE" ] && [ -f "$CHECKSUM_TMP_FILE" ]; then
        echo "Verifying checksum..."
        
        # Detect available checksum tool
        if command -v sha256sum >/dev/null 2>&1; then
            CHECKSUM_CMD="sha256sum"
        elif command -v shasum >/dev/null 2>&1; then
            CHECKSUM_CMD="shasum -a 256"
        else
            echo "Warning: No SHA256 checksum tool found (sha256sum or shasum), skipping verification"
            CHECKSUM_CMD=""
        fi
        
        if [ -n "$CHECKSUM_CMD" ]; then
            # Calculate actual checksum
            actual_checksum=$($CHECKSUM_CMD "$ARTIFACT_TMP_FILE" | awk '{print $1}')
            
            # Read expected checksum from file
            expected_checksum=$(cat "$CHECKSUM_TMP_FILE" | awk '{print $1}')
            
            if [ "$actual_checksum" = "$expected_checksum" ]; then
                echo "✓ Checksum verified successfully"
            else
                echo "ERROR: Checksum verification failed!"
                echo "Expected: $expected_checksum"
                echo "Actual:   $actual_checksum"
                echo "This could indicate a corrupted download or security issue."
                exit 1
            fi
        fi
    fi
    
    # Verify the downloaded file is a valid gzip archive
    if ! file "$ARTIFACT_TMP_FILE" | grep -q "gzip compressed"; then
        echo "Downloaded file is not a valid gzip archive"
        echo "File type: $(file "$ARTIFACT_TMP_FILE")"
        exit 1
    fi
    
    echo "Download successful ($(du -h "$ARTIFACT_TMP_FILE" | cut -f1))"
}

installFile() {
    echo "Extracting archive..."
    if ! tar xf "$ARTIFACT_TMP_FILE" -C "$SPICE_TMP_ROOT" 2>/dev/null; then
        echo "Failed to extract archive"
        exit 1
    fi
    
    local tmp_root_spice_cli="$SPICE_TMP_ROOT/$SPICE_CLI_FILENAME"

    if [ ! -f "$tmp_root_spice_cli" ]; then
        echo "Failed to unpack Spice CLI executable."
        echo "Expected file: $tmp_root_spice_cli"
        echo "Archive contents:"
        tar tzf "$ARTIFACT_TMP_FILE" 2>/dev/null | head -10
        exit 1
    fi
    
    echo "Extracted: $SPICE_CLI_FILENAME ($(du -h "$tmp_root_spice_cli" | cut -f1))"

    chmod o+x $tmp_root_spice_cli
    
    # Create directory if it doesn't exist
    if [ ! -d "$SPICE_CLI_INSTALL_DIR" ]; then
        if [ "$USE_SUDO" == "true" ]; then
            runAsRoot mkdir -p "$SPICE_CLI_INSTALL_DIR"
        else
            mkdir -p "$SPICE_CLI_INSTALL_DIR"
        fi
    fi
    
    # Copy the file with sudo if needed
    if [ "$USE_SUDO" == "true" ]; then
        runAsRoot cp "$tmp_root_spice_cli" "$SPICE_CLI_INSTALL_DIR"
    else
        cp "$tmp_root_spice_cli" "$SPICE_CLI_INSTALL_DIR"
    fi

    if [ -f "$SPICE_CLI_FILE" ]; then
        echo "$SPICE_CLI_FILENAME installed into $SPICE_CLI_INSTALL_DIR successfully."
        
        # Verify the binary is executable and works
        if [ -x "$SPICE_CLI_FILE" ]; then
            # Test that the binary can at least print version/help
            if "$SPICE_CLI_FILE" version >/dev/null 2>&1 || "$SPICE_CLI_FILE" --version >/dev/null 2>&1 || "$SPICE_CLI_FILE" --help >/dev/null 2>&1; then
                echo "Verified: $SPICE_CLI_FILENAME binary is working correctly."
            else
                echo -e "${yellow}Warning:${reset} Binary installed but may not be working correctly."
                echo "Try running '$SPICE_CLI_FILE --help' to test."
            fi
        else
            echo -e "${yellow}Warning:${reset} Binary installed but is not executable."
            echo "You may need to run: chmod +x $SPICE_CLI_FILE"
        fi
    else
        echo "Failed to install $SPICE_CLI_FILENAME"
        exit 1
    fi
}

fail_trap() {
    result=$?
    if [ "$result" != "0" ]; then
        echo "Failed to install Spice CLI"
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

SHELL_TO_USE=null
MOST_RECENT_MODIFIED=0

detectShell() {
    # First, try to detect from SHELL environment variable
    if [[ -n "$SHELL" ]]; then
        case "$SHELL" in
            */bash)
                DETECTED_SHELL="bash"
                ;;
            */zsh)
                DETECTED_SHELL="zsh"
                ;;
            */fish)
                DETECTED_SHELL="fish"
                ;;
            */ksh)
                DETECTED_SHELL="ksh"
                ;;
            */tcsh|*/csh)
                DETECTED_SHELL="csh"
                ;;
            *)
                DETECTED_SHELL="unknown"
                ;;
        esac
    else
        DETECTED_SHELL="unknown"
    fi
}

checkShell() {
    local shell_file="$HOME/$1"
    if [[ -f "$shell_file" ]]; then
        local modified_time
        if [[ "$OS" == "linux" ]]; then
            modified_time=$(date +%s -r "$shell_file" 2>/dev/null || echo 0)
        elif [[ "$OS" == "darwin" ]]; then
            modified_time=$(/usr/bin/stat -f%c "$shell_file" 2>/dev/null || echo 0)
        else
            modified_time=0
        fi

        if (( modified_time > MOST_RECENT_MODIFIED )); then
            SHELL_TO_USE="$shell_file"
            MOST_RECENT_MODIFIED=$modified_time
        fi
    fi
}

getShellPathCommand() {
    local shell_type="$1"
    local path_line=""
    
    case "$shell_type" in
        bash|ksh)
            path_line="export PATH=\"\$HOME/$SPICE_BIN:\$PATH\""
            ;;
        zsh)
            path_line="export PATH=\"\$HOME/$SPICE_BIN:\$PATH\""
            ;;
        fish)
            path_line="fish_add_path \$HOME/.spice/bin"
            ;;
        csh)
            path_line="setenv PATH \"\$HOME/$SPICE_BIN:\$PATH\""
            ;;
        *)
            path_line="export PATH=\"\$HOME/$SPICE_BIN:\$PATH\""
            ;;
    esac
    
    echo "$path_line"
}

addToProfile() {
    local path_cmd="$1"
    echo -e "Adding the line:\n"
    echo -e "  ${white}$path_cmd${reset}\n"
    echo -e "to your shell profile at '${blue}$SHELL_TO_USE${reset}'\n"
    echo -e "$path_cmd" >> "$SHELL_TO_USE"
    echo "Added! You may need to restart your shell or run 'source $SHELL_TO_USE' to use 'spice'"
}

installCompleted() {
    echo -e "\nTo get started with Spice.ai, visit https://spiceai.org/docs"
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
trap "fail_trap" EXIT

getSystemInfo
verifySupported
checkHttpRequestCLI
checkJqInstalled

# Create install directory before checking sudo requirements
if [ ! -d "$SPICE_CLI_INSTALL_DIR" ]; then
    # Try to create without sudo first
    if ! mkdir -p "$SPICE_CLI_INSTALL_DIR" 2>/dev/null; then
        USE_SUDO="true"
    fi
fi

# Re-check sudo requirements after attempting to create directory
getSystemInfo

if [ -z "$1" ]; then
    echo "Getting the latest Spice.ai CLI..."
    getLatestRelease
else
    ret_val=v$1
fi

echo "Installing Spice CLI $ret_val ..."

downloadFile $ret_val
installFile
cleanup

# Skip PATH configuration when installing to system directories
if [[ "$SPICE_CLI_INSTALL_DIR" == "/usr/local/bin" ]] || [[ "$SPICE_CLI_INSTALL_DIR" == "/usr/bin" ]]; then
    echo -e "\nInstalled to system directory. The 'spice' command should be available immediately."
    installCompleted
    exit 0
fi

# Detect the current shell
detectShell

# Check shell profile files based on detected shell and common locations
if [[ "$DETECTED_SHELL" == "bash" ]]; then
    SHELLS_TO_CHECK=(".bashrc" ".bash_profile" ".profile")
elif [[ "$DETECTED_SHELL" == "zsh" ]]; then
    SHELLS_TO_CHECK=(".zshrc" ".zprofile" ".zshenv")
elif [[ "$DETECTED_SHELL" == "fish" ]]; then
    SHELLS_TO_CHECK=(".config/fish/config.fish")
elif [[ "$DETECTED_SHELL" == "ksh" ]]; then
    SHELLS_TO_CHECK=(".kshrc" ".profile")
elif [[ "$DETECTED_SHELL" == "csh" ]]; then
    SHELLS_TO_CHECK=(".cshrc" ".tcshrc")
else
    # Unknown shell, check all common profiles
    SHELLS_TO_CHECK=(".bashrc" ".bash_profile" ".zshrc" ".zprofile" ".profile" ".config/fish/config.fish" ".kshrc")
fi

for shell_file in "${SHELLS_TO_CHECK[@]}"; do 
    checkShell "$shell_file"
done

if [[ "$SHELL_TO_USE" == "null" ]]; then
    echo -e "${yellow}Unable to detect shell profile automatically.${reset}"
    echo "Manually add one of the following to your shell profile:"
    echo ""
    echo "  For bash/zsh/ksh: export PATH=\"\$HOME/$SPICE_BIN:\$PATH\""
    echo "  For fish:         fish_add_path \$HOME/.spice/bin"
    echo "  For csh/tcsh:     setenv PATH \"\$HOME/$SPICE_BIN:\$PATH\""
    echo ""
else
    echo "Detected shell profile: $SHELL_TO_USE"
    
    # Check if PATH is already configured properly (look for actual export/setenv/fish_add_path commands)
    PATH_ALREADY_SET=false
    if grep -E "(export PATH=.*\.spice/bin|fish_add_path.*\.spice/bin|setenv PATH.*\.spice/bin)" "$SHELL_TO_USE" >/dev/null 2>&1; then
        PATH_ALREADY_SET=true
    fi
    
    if [ "$PATH_ALREADY_SET" = true ]; then
        echo -e "${yellow}Note:${reset} The Spice CLI PATH configuration is already present in $SHELL_TO_USE"
        echo ""
        echo "If 'spice' command is not found, run one of the following:"
        echo "  source $SHELL_TO_USE"
        echo "  OR restart your terminal"
        echo ""
    else
        echo -e "${yellow}Adding Spice CLI to your PATH${reset}\n"

        # Determine shell type from file path
        shell_type="unknown"
        case "$SHELL_TO_USE" in
            *fish/config.fish)
                shell_type="fish"
                ;;
            *.zshrc|*.zprofile|*.zshenv)
                shell_type="zsh"
                ;;
            *.bashrc|*.bash_profile)
                shell_type="bash"
                ;;
            *.kshrc)
                shell_type="ksh"
                ;;
            *.cshrc|*.tcshrc)
                shell_type="csh"
                ;;
            *.profile)
                # .profile could be bash, ksh, or sh - use bash syntax as most compatible
                shell_type="bash"
                ;;
        esac

        path_command=$(getShellPathCommand "$shell_type")
        addToProfile "$path_command"
    fi
fi

installCompleted
