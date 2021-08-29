#!/usr/bin/env bash

# Spice CLI location
: ${SPICE_CLI_INSTALL_DIR:="$HOME/.spice/bin/"}

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
        aarch64) ARCH="arm64";;
        x86_64) ARCH="amd64";;
    esac

    OS=$(echo `uname`|tr '[:upper:]' '[:lower:]')

    # Most linux distro needs root permission to copy the file to /usr/local/bin
    if [[ "$OS" == "linux" || "$OS" == "darwin" ]] && [ "$SPICE_CLI_INSTALL_DIR" == "/usr/local/bin" ]; then
        USE_SUDO="true"
    fi
}

verifySupported() {
    local supported=(darwin-amd64 linux-amd64 linux-arm linux-arm64)
    local current_osarch="${OS}-${ARCH}"

    for osarch in "${supported[@]}"; do
        if [ "$osarch" == "$current_osarch" ]; then
            return
        fi
    done

    if [ "$current_osarch" == "darwin-arm64" ]; then
        echo "The darwin_arm64 arch has no native binary, however you can use the amd64 version so long as you have rosetta installed"
        echo "Use 'softwareupdate --install-rosetta' to install rosetta if you don't already have it"
        ARCH="amd64"
        return
    fi


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
    local spiceReleaseUrl="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases"
    local latest_release=""

    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        latest_release=$(curl -H "Authorization: token ${SPICE_GH_TOKEN}" -s $spiceReleaseUrl | grep \"tag_name\" | grep -v rc | grep spice\" | awk 'NR==1{print $2}' |  sed -n 's/\"\(.*\)\",/\1/p')
    else
        latest_release=$(wget -q --header="Accept: application/json" --header="Authorization: token ${SPICE_GH_TOKEN}" -O - $spiceReleaseUrl | grep \"tag_name\" | grep -v rc | grep spice\" | awk 'NR==1{print $2}' |  sed -n 's/\"\(.*\)\",/\1/p')
    fi

    ret_val=$latest_release
}

downloadFile() {
    LATEST_RELEASE_TAG=$1

    SPICE_CLI_ARTIFACT="${SPICE_CLI_FILENAME}_${OS}_${ARCH}.tar.gz"
    DOWNLOAD_BASE="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download"
    DOWNLOAD_URL="${DOWNLOAD_BASE}/${LATEST_RELEASE_TAG}/${SPICE_CLI_ARTIFACT}"

    # Create the temp directory
    SPICE_TMP_ROOT=$(mktemp -dt spice-install-XXXXXX)
    ARTIFACT_TMP_FILE="$SPICE_TMP_ROOT/$SPICE_CLI_ARTIFACT"

    echo "Downloading $DOWNLOAD_URL ..."

    function gh_curl() {
      curl -H "Authorization: token $SPICE_GH_TOKEN" \
          -H "Accept: application/vnd.github.v3.raw" \
          $@
    }

    parser=". | map(select(.tag_name == \"$LATEST_RELEASE_TAG\"))[0].assets | map(select(.name == \"$SPICE_CLI_ARTIFACT\"))[0].id"

    asset_id=`gh_curl -s https://api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases | jq "$parser"`
    if [ "$asset_id" = "null" ]; then
      echo "ERROR: version not found $VERSION"
      exit 1
    fi;

    if [ "$SPICE_HTTP_REQUEST_CLI" == "curl" ]; then
        curl -H "Accept:application/octet-stream" -SsL \
            https://$SPICE_GH_TOKEN:@api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$asset_id \
            -o "$ARTIFACT_TMP_FILE"
    else
        wget -q --auth-no-challenge --header='Accept:application/octet-stream' \
            https://$SPICE_GH_TOKEN:@api.github.com/repos/$GITHUB_ORG/$GITHUB_REPO/releases/assets/$asset_id \
            -O "$ARTIFACT_TMP_FILE"
    fi

    if [ ! -f "$ARTIFACT_TMP_FILE" ]; then
        echo "failed to download $DOWNLOAD_URL ..."
        exit 1
    fi
}

installFile() {
    tar xf "$ARTIFACT_TMP_FILE" -C "$SPICE_TMP_ROOT"
    local tmp_root_spice_cli="$SPICE_TMP_ROOT/$SPICE_CLI_FILENAME"

    if [ ! -f "$tmp_root_spice_cli" ]; then
        echo "Failed to unpack Spice CLI executable."
        exit 1
    fi

    chmod o+x $tmp_root_spice_cli
    cp "$tmp_root_spice_cli" "$SPICE_CLI_INSTALL_DIR"

    if [ -f "$SPICE_CLI_FILE" ]; then
        echo "$SPICE_CLI_FILENAME installed into $SPICE_CLI_INSTALL_DIR successfully."
    else 
        echo "Failed to install $SPICE_CLI_FILENAME"
        exit 1
    fi
}

fail_trap() {
    result=$?
    if [ "$result" != "0" ]; then
        echo "Failed to install Spice CLI"
        echo "For support, see https://docs.spiceai.org"
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
    echo -e "\nTo get started with Spice.ai, visit https://docs.spiceai.org"
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
trap "fail_trap" EXIT

if [[ -z "$SPICE_GH_TOKEN" ]]; then
    if [[ -z "$GITHUB_TOKEN" ]]; then
        echo "ERROR: Please set env var SPICE_GH_TOKEN to your GitHub Token!"
        exit 1
    else
        echo "Using $GITHUB_TOKEN as $SPICE_GH_TOKEN"
        export SPICE_GH_TOKEN="$GITHUB_TOKEN"
    fi
fi

mkdir -p $SPICE_CLI_INSTALL_DIR

getSystemInfo
verifySupported
checkHttpRequestCLI
checkJqInstalled

if [ -z "$1" ]; then
    echo "Getting the latest Spice.ai CLI..."
    getLatestRelease
else
    ret_val=v$1
fi

FRIENDLY_VERSION=$(echo $ret_val | sed 's/-spice//')
echo "Installing Spice CLI $FRIENDLY_VERSION ..."

downloadFile $ret_val
installFile
cleanup

installCompleted
