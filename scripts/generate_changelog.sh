#!/bin/bash

# Usage: ./generate_changelog.sh <tag> <branch>
# Example: ./generate_changelog.sh v1.1.2 trunk

tag=$1
branch=$2
owner=spiceai
repo=spiceai

# Check if all arguments are provided
if [ -z "$tag" ] || [ -z "$branch" ]; then
    echo "Error: Missing arguments. Usage: $0 <tag> <branch>" >&2
    exit 1
fi

# Step 1: Find the common ancestor of the tag and branch
common_ancestor=$(git merge-base "$tag" "$branch")
if [ -z "$common_ancestor" ]; then
    echo "Error: Could not find common ancestor between $tag and $branch" >&2
    exit 1
fi

# Step 2: Build a set of (author, message) from commits in the tag's history (cherry-picked commits)
declare -A cherry_picked_commits
git log "$common_ancestor".."$tag" --format="%an\t%s" | while read -r line; do
    cherry_picked_commits["$line"]=1
done

# Step 3: Build a map of (author, message) to (PR number, GitHub username) from branch PRs
declare -A commit_to_pr
declare -A commit_to_username
gh pr list --state merged --base "$branch" --json number --jq '.[] | .number' | while read -r pr_number; do
    gh api repos/"$owner"/"$repo"/pulls/"$pr_number"/commits --jq '.[] | .sha + "\t" + (.author.login // "")' | while read -r line; do
        sha=$(echo "$line" | cut -f1)
        github_username=$(echo "$line" | cut -f2)
        author=$(git show -s --format=%an "$sha")
        message=$(git show -s --format=%s "$sha")
        key="$author\t$message"
        commit_to_pr["$key"]="$pr_number"
        commit_to_username["$key"]="$github_username"
    done
done

# Step 4: Generate the changelog header
echo "### Changelog"
echo ""

# Step 5: List commits on branch since common ancestor, filter out cherry-picked ones, and match to PRs
git log "$common_ancestor".."$branch" --format="%an\t%s" | while read -r commit_line; do
    if [ -z "${cherry_picked_commits[$commit_line]}" ]; then
        # Commit is not cherry-picked; check for a matching PR
        if [ -n "${commit_to_pr[$commit_line]}" ]; then
            pr_number="${commit_to_pr[$commit_line]}"
            github_username="${commit_to_username[$commit_line]}"
            author=$(echo "$commit_line" | cut -f1)
            message=$(echo "$commit_line" | cut -f2-)
            # Use GitHub username if available, otherwise fall back to Git author name
            if [ -n "$github_username" ]; then
                display_author="@$github_username"
            else
                display_author="$author"
            fi
            echo "- $message by $display_author in https://github.com/$owner/$repo/pull/$pr_number"
        else
            # Log commits without a matching PR to stderr
            echo "Warning: No matching PR found for commit: $commit_line" >&2
        fi
    fi
done