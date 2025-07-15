#!/bin/bash

tag=$1
release_branch=$2
owner=spiceai
repo=spiceai

if [ -z "$tag" ] || [ -z "$release_branch" ]; then
    echo "Usage: $0 <previous_release_tag> <release_branch>"
    echo "Example: $0 v1.4.0 release/1.5"
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

# Fetch PR data
gh pr list --state merged --base "trunk" --limit 10000 --json number,mergeCommit,author --repo "$owner/$repo" > pr_data_trunk.json
gh pr list --state merged --base "$release_branch" --limit 10000 --json number,mergeCommit,author --repo "$owner/$repo" > pr_data_branch.json

# Create mapping file: commit_hash pr_number username
jq -r '.[] | .mergeCommit.oid + " " + (.number | tostring) + " " + .author.login' pr_data_trunk.json > pr_mapping_trunk.txt
jq -r '.[] | .mergeCommit.oid + " " + (.number | tostring) + " " + .author.login' pr_data_branch.json > pr_mapping_branch.txt

# Get commits in the release branch
git cherry "origin/$release_branch" origin/trunk "$tag" | grep '^-' | awk '{print $2}' > cherry_commits_in_release_branch.txt
git cherry "origin/$release_branch" origin/trunk "$tag" | grep '^+' | awk '{print $2}' > commits_in_trunk_not_in_release_branch.txt

# Generate changelog
echo "### Changelog"
echo ""

while read -r hash; do
    mapping=$(grep "^$hash " pr_mapping_trunk.txt)
    if [ -z "$mapping" ]; then
        mapping=$(grep "^$hash " pr_mapping_branch.txt)
    fi

    if [ -n "$mapping" ]; then
        pr_number=$(echo "$mapping" | cut -d' ' -f2)
        username=$(echo "$mapping" | cut -d' ' -f3)
        subject=$(git log --format=%s -n 1 "$hash")
        echo "- $subject by [@$username](https://github.com/$username) in [#$pr_number](https://github.com/$owner/$repo/pull/$pr_number)"
    else
        echo "Warning: No PR found for commit $hash" >&2
    fi
done < cherry_commits_in_release_branch.txt

echo ""
echo "### Commits in trunk not in release branch (may not be accurate, please review and include if needed)"
echo ""

while read -r hash; do
    mapping=$(grep "^$hash " pr_mapping_trunk.txt)
    if [ -z "$mapping" ]; then
        mapping=$(grep "^$hash " pr_mapping_branch.txt)
    fi

    if [ -n "$mapping" ]; then
        pr_number=$(echo "$mapping" | cut -d' ' -f2)
        username=$(echo "$mapping" | cut -d' ' -f3)
        subject=$(git log --format=%s -n 1 "$hash")
        echo "- $subject by [@$username](https://github.com/$username) in [#$pr_number](https://github.com/$owner/$repo/pull/$pr_number)"
    else
        echo "Warning: No PR found for commit $hash" >&2
    fi
done < commits_in_trunk_not_in_release_branch.txt

# Clean up
rm pr_data_trunk.json pr_data_branch.json pr_mapping_trunk.txt pr_mapping_branch.txt cherry_commits_in_release_branch.txt commits_in_trunk_not_in_release_branch.txt
