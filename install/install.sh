#!/usr/bin/env bash

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

    # Most linux distro needs root permission to copy the file to /usr/local/bin
    if [[ "$OS" == "linux" || "$OS" == "darwin" ]] && [ "$SPICE_CLI_INSTALL_DIR" == "/usr/local/bin" ]; then
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
    DOWNLOAD_BASE="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download"
    DOWNLOAD_URL="${DOWNLOAD_BASE}/${LATEST_RELEASE_TAG}/${SPICE_CLI_ARTIFACT}"

    # Create the temp directory
    SPICE_TMP_ROOT=$(mktemp -dt spice-install-XXXXXX)
    ARTIFACT_TMP_FILE="$SPICE_TMP_ROOT/$SPICE_CLI_ARTIFACT"

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

checkShell() {
  SHELL=$HOME/$1
  if [[ -f "$SHELL" ]]; then
    if [[ "$OS" == "linux" ]]; then
      MODIFIED_TIME=`date +%s -r "$SHELL"`
    elif [[ "$OS" == "darwin" ]]; then
      MODIFIED_TIME=`/usr/bin/stat -f%c "$SHELL"`
    fi

    if (( $MODIFIED_TIME > $MOST_RECENT_MODIFIED )); then
      SHELL_TO_USE=$SHELL
      MOST_RECENT_MODIFIED=$MODIFIED_TIME
    fi
  fi
}

addToProfile() {
  echo -e "Adding the line:\n"
  echo -e "  ${white}$1${reset}\n"
  echo -e "to your shell profile at '${blue}$SHELL_TO_USE${reset}'\n"
  echo -e "$1" >> $SHELL_TO_USE
  echo "Added! You may need to restart your shell to be able to run 'spice'"
}

installCompleted() {
    echo -e "\nTo get started with Spice.ai, visit https://spiceai.org/docs"
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
trap "fail_trap" EXIT

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

echo "Installing Spice CLI $ret_val ..."

downloadFile $ret_val
installFile
cleanup

SHELLS_TO_CHECK=(".bashrc" ".bash_profile" ".zshrc" ".zprofile" ".config/fish/config.fish")

for i in "${SHELLS_TO_CHECK[@]}"; do checkShell $i; done

if [[ "$SHELL_TO_USE" == "null" ]]; then
  echo "Unable to find the shell profile, not adding the Spice CLI to PATH"
  echo "Manually add 'export PATH=$HOME/$SPICE_BIN:\$PATH' to your shell profile"
else
  if grep -Fq $SPICE_BIN $SHELL_TO_USE
  then
    echo "The Spice CLI is already in your PATH!"
  else
    echo -e "${yellow}The Spice CLI is not in your PATH${reset}\n"

    if [[ "$SHELL_TO_USE" == "$HOME/.config/fish/config.fish" ]]; then
      addToProfile "fish_add_path \$HOME/.spice/bin"
    else
      addToProfile "export PATH=$HOME/$SPICE_BIN:\$PATH"
    fi
  fi
fi

installCompleted
