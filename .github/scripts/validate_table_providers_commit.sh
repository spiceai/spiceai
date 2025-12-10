#!/usr/bin/env bash
# Copyright 2025 The Spice.ai OSS Authors
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

# Validates that the datafusion-table-providers commit in Cargo.toml exists on the spiceai branch

set -euo pipefail

CARGO_TOML="${1:-Cargo.toml}"
REPO_URL="https://github.com/datafusion-contrib/datafusion-table-providers.git"
BRANCH="spiceai"

# Extract the commit hash from the datafusion-table-providers line in [patch.crates-io]
COMMIT=$(grep -E '^datafusion-table-providers\s*=' "$CARGO_TOML" | grep 'datafusion-contrib/datafusion-table-providers' | sed -n 's/.*rev\s*=\s*"\([^"]*\)".*/\1/p' | head -1)

if [[ -z "$COMMIT" ]]; then
    echo "Error: Could not find datafusion-table-providers commit in $CARGO_TOML"
    exit 1
fi

echo "Found datafusion-table-providers commit: $COMMIT"
echo "Checking if commit exists on '$BRANCH' branch of $REPO_URL..."

# Use git ls-remote to check if the commit is an ancestor of the spiceai branch
# First, get the SHA of the spiceai branch
BRANCH_SHA=$(git ls-remote "$REPO_URL" "refs/heads/$BRANCH" | cut -f1)

if [[ -z "$BRANCH_SHA" ]]; then
    echo "Error: Could not find '$BRANCH' branch in $REPO_URL"
    exit 1
fi

echo "Branch '$BRANCH' is at commit: $BRANCH_SHA"

# Clone just enough to check ancestry (shallow clone with the specific commit)
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

echo "Cloning repository to verify commit ancestry..."
git clone --bare --filter=blob:none "$REPO_URL" "$TEMP_DIR/repo" 2>/dev/null

cd "$TEMP_DIR/repo"

# Check if the commit exists and is reachable from the spiceai branch
if git merge-base --is-ancestor "$COMMIT" "$BRANCH_SHA" 2>/dev/null; then
    echo "✓ Commit $COMMIT exists on the '$BRANCH' branch"
    exit 0
else
    echo "✗ Error: Commit $COMMIT is NOT on the '$BRANCH' branch"
    echo ""
    echo "The datafusion-table-providers commit in Cargo.toml must be on the 'spiceai' branch."
    echo "Please ensure your changes are merged to the spiceai branch before updating the commit."
    exit 1
fi
