#!/bin/bash

if [ -z "$SPICE_BUILD_ARTIFACTS_DIR" ]; then
    echo "Error: SPICE_BUILD_ARTIFACTS_DIR environment variable is not set."
    exit 1
fi

echo "Installing Spice from local folder: $SPICE_BUILD_ARTIFACTS_DIR"

chmod +x "$SPICE_BUILD_ARTIFACTS_DIR/spice"
chmod +x "$SPICE_BUILD_ARTIFACTS_DIR/spiced"

mkdir -p "$HOME/.spice/bin"

mv "$SPICE_BUILD_ARTIFACTS_DIR/spice" "$HOME/.spice/bin"
mv "$SPICE_BUILD_ARTIFACTS_DIR/spiced" "$HOME/.spice/bin"

# Update PATH for the current and future shell sessions
export PATH="$HOME/.spice/bin:$PATH"
echo "$HOME/.spice/bin" >> $GITHUB_PATH

spice version