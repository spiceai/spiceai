#!/bin/bash

# Ensure the script is run with a tag argument
if [ -z "$1" ]; then
  echo "Usage: $0 <tag> <base-branch>"
  exit 1
fi

# Ensure the `gh` CLI is installed and authenticated
if ! command -v gh &> /dev/null; then
  echo "gh could not be found. Install the GitHub CLI to generate a changelog."
  exit 1
fi

# Ensure the `gh` CLI is authenticated
if ! gh auth status &> /dev/null; then
  echo "gh is not authenticated. Please authenticate with 'gh auth login'."
  exit 1
fi

TAG="$1"
BASE_BRANCH="${2:-trunk}"

# Update the local repository to fetch all remote commits
git fetch --all

# Get the commit range between the tag and the base branch
COMMIT_RANGE="$(git rev-parse $TAG)..$(git rev-parse $BASE_BRANCH)"

# Temporary file to store cherry-picked commits
TEMP_FILE=$(mktemp)

# Populate the temporary file with cherry-picked commits (if applicable)
git log "$COMMIT_RANGE" --cherry-pick --pretty=format:"%H" > "$TEMP_FILE"

# Generate changelog
echo "### Changelog"
echo ""

# Process git log output, handling tabs and special characters
git log "$COMMIT_RANGE" --pretty=format:"%an	%s" | while IFS=$'\t' read -r author message; do
  # Skip empty lines or lines with missing fields
  if [ -z "$author" ] || [ -z "$message" ]; then
    continue
  fi

  # Check if the commit is in the cherry-picked list
  commit_hash=$(git log -1 --pretty=format:"%H" --grep="$message")
  if grep -q "$commit_hash" "$TEMP_FILE"; then
    echo "- $message by $author"
  else
    echo "- [$author] $message"
  fi
done

# Clean up temporary file
rm -f "$TEMP_FILE"