#!/usr/bin/env bash

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

SPICED_FILE="${SPICED_INSTALL_DIR}/${SPICED_FILENAME}"

getSystemInfo() {
    ARCH=$(uname -m)
    case $ARCH in
        armv7*) ARCH="arm";;
        arm64) ARCH="aarch64";;
        amd64) ARCH="x86_64";;
    esac

    OS=$(echo `uname`|tr '[:upper:]' '[:lower:]')

    # Most linux distro needs root permission to copy the file to /usr/local/bin
    if [[ "$OS" == "linux" || "$OS" == "darwin" ]] && [ "$SPICED_INSTALL_DIR" == "/usr/local/bin" ]; then
        USE_SUDO="true"
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

    echo "No prebuilt binary for ${current_osarch}"
    exit 1
}

runAsRoot() {
    local CMD="$*"

    if [ $EUID -ne 0 -a $USE_SUDO = "true" ]; then
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
        latest_release=$(curl -s $spiceReleaseUrl | grep \"tag_name\" | awk 'NR==1{print $2}' |  sed -n 's/\"\(.*\)\",/\1/p')
    else
        latest_release=$(wget -q --header="Accept: application/json" -O - $spiceReleaseUrl | grep \"tag_name\" | awk 'NR==1{print $2}' |  sed -n 's/\"\(.*\)\",/\1/p')
    fi

    ret_val=$latest_release
}

downloadFile() {
    LATEST_RELEASE_TAG=$1

    SPICED_ARTIFACT="${SPICED_FILENAME}_${OS}_${ARCH}.tar.gz"
    DOWNLOAD_BASE="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download"
    DOWNLOAD_URL="${DOWNLOAD_BASE}/${LATEST_RELEASE_TAG}/${SPICED_ARTIFACT}"

    # Create the temp directory
    SPICE_TMP_ROOT=$(mktemp -dt spice-install-XXXXXX)
    ARTIFACT_TMP_FILE="$SPICE_TMP_ROOT/$SPICED_ARTIFACT"

    echo "Downloading $DOWNLOAD_URL ..."

    function gh_curl() {
      curl -H "Accept: application/vnd.github.v3.raw" \
          $@
    }

    parser=". | map(select(.tag_name == \"$LATEST_RELEASE_TAG\"))[0].assets | map(select(.name == \"$SPICED_ARTIFACT\"))[0].id"

    asset_id=`gh_curl -s https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases | jq "$parser"`
    if [ "$asset_id" = "null" ]; then
      echo "ERROR: version not found $VERSION"
      exit 1
    fi;

    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        curl -H "Accept:application/octet-stream" -SsL \
            https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$asset_id \
            -o "$ARTIFACT_TMP_FILE"
    else
        wget -q --auth-no-challenge --header='Accept:application/octet-stream' \
            https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$asset_id \
            -O "$ARTIFACT_TMP_FILE"
    fi

    if [ ! -f "$ARTIFACT_TMP_FILE" ]; then
        echo "failed to download $DOWNLOAD_URL ..."
        exit 1
    fi
}

installFile() {
    tar xf "$ARTIFACT_TMP_FILE" -C "$SPICE_TMP_ROOT"
    local tmp_root_spiced="$SPICE_TMP_ROOT/$SPICED_FILENAME"

    if [ ! -f "$tmp_root_spiced" ]; then
        echo "Failed to unpack Spice Runtime executable."
        exit 1
    fi

    chmod o+x $tmp_root_spiced
    cp "$tmp_root_spiced" "$SPICED_INSTALL_DIR"

    if [ -f "$SPICED_FILE" ]; then
        echo "$SPICED_FILENAME installed into $SPICED_INSTALL_DIR successfully."
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

mkdir -p $SPICED_INSTALL_DIR

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

downloadFile $ret_val
installFile
cleanup

installCompleted
