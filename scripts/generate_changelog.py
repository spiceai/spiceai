#!/usr/bin/env python3
import subprocess
import json
import sys
import re

def run_git(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
    return result.stdout.strip()

"""
The idea behind this script is to:
* Find commits that are in current release branch but not in the previous tag
* Match them to trunk commits using patch-id
* Find relevant PR
"""

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 generate_changelog.py <previous_release_tag> <release_branch>")
        print("Example: python3 generate_changelog.py v1.4.0 release/1.5")
        sys.exit(1)

    tag = sys.argv[1]
    release_branch = sys.argv[2]
    owner = "spiceai"
    repo = "spiceai"

    print("Fetching PR data...", file=sys.stderr)

    trunk_prs = json.loads(subprocess.check_output([
        "gh", "pr", "list", "--state", "merged", "--base", "trunk",
        "--limit", "10000", "--json", "number,mergeCommit,author,title",
        "--repo", f"{owner}/{repo}"
    ]))

    # Build PR mappings: commit_hash -> (pr_number, username, title)
    pr_mapping = {}
    for pr in trunk_prs:
        commit_hash = pr['mergeCommit']['oid']
        pr_mapping[commit_hash] = (pr['number'], pr['author']['login'], pr['title'])

    print("Generating patch-ids for trunk commits...", file=sys.stderr)

    trunk_commits = run_git("git log origin/trunk --format=%H -n 1000").split('\n')
    trunk_patch_to_hash = {}

    # Build mapping: patch-id -> commit_hash for trunk commits
    for i, commit_hash in enumerate(trunk_commits):
        if i % 100 == 0:
            print(f"  Processing trunk commit {i}/{len(trunk_commits)}", file=sys.stderr)

        patch_id_output = run_git(f"git show {commit_hash} | git patch-id --stable").split()
        if not patch_id_output:
            continue
        patch_id = patch_id_output[0]
        trunk_patch_to_hash[patch_id] = commit_hash

    # Get release branch commits since tag
    release_commits = run_git(f"git log {tag}..origin/{release_branch} --format=\"%H-%s\"").split('\n')

    print()
    print("### Changelog")
    print()

    for commit in reversed(release_commits):
        commit_hash, commit_title = commit.split("-", 1)

        # Calculate its patch-id
        patch_id_output = run_git(f"git show {commit_hash} | git patch-id --stable").split()
        if not patch_id_output:
            continue
        patch_id = patch_id_output[0]

        # Find PR from patch-id
        trunk_hash = trunk_patch_to_hash.get(patch_id)
        pr_info = pr_mapping.get(trunk_hash)

        # Print PR info if found
        if pr_info:
            pr_number, username, title = pr_info
            print(f"- {title} by [@{username}](https://github.com/{username}) in [#{pr_number}](https://github.com/{owner}/{repo}/pull/{pr_number})")
        else:
            # Try to find PR number in commit message
            pr_num_result = re.search(r"\(#(\d+)\)", commit_title)
            if pr_num_result:
                pr_number = pr_num_result.group(1)
                commit_title_clean = re.sub(r"\(#\d+\)", "", commit_title).strip()
                try:
                    pr_info = json.loads(subprocess.check_output([
                            "gh", "pr", "view", str(pr_number),
                            "--json", "number,author",
                            "--repo", f"{owner}/{repo}"
                        ]))
                except:
                    print(f"- !!!! (PR NOT FOUND): {commit_title} - https://github.com/{owner}/{repo}/commit/{commit_hash}/")
                    continue

                username = pr_info["author"]["login"]
                print(f"- {commit_title_clean} by [@{username}](https://github.com/{username}) in [#{pr_number}](https://github.com/{owner}/{repo}/pull/{pr_number})")
            else:
                # If everything else fails, print commit title with the link to github
                print(f"- !!!! (PR NOT FOUND): {commit_title} - https://github.com/{owner}/{repo}/commit/{commit_hash}/")

if __name__ == "__main__":
    main()
