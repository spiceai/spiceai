#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="$REPO_ROOT/build-spidapter"
IMAGE_NAME="${1:-spidapter:latest}"
SPIDAPTER_BIN="$HOME/.spice/bin/spidapter"

if [ ! -f "$SPIDAPTER_BIN" ]; then
  echo "Error: spidapter binary not found at $SPIDAPTER_BIN"
  exit 1
fi

echo "Preparing Docker build context..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cp "$SPIDAPTER_BIN" "$BUILD_DIR/"
cp "$REPO_ROOT/tools/spidapter/Dockerfile" "$BUILD_DIR/"

echo "Building Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" "$BUILD_DIR"

echo "Cleaning up build context..."
rm -rf "$BUILD_DIR"

echo "Done. Image: $IMAGE_NAME"
