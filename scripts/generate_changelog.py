#!/usr/bin/env python3
import subprocess
import json
import sys

def run_git(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
    return result.stdout.strip()

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 changelog.py <previous_release_tag> <release_branch>")
        print("Example: python3 changelog.py v1.4.0 release/1.5")
        sys.exit(1)

    tag = sys.argv[1]
    release_branch = sys.argv[2]
    owner = "spiceai"
    repo = "spiceai"

    print("Fetching PR data...", file=sys.stderr)

    trunk_prs = json.loads(subprocess.check_output([
        "gh", "pr", "list", "--state", "merged", "--base", "trunk",
        "--limit", "10000", "--json", "number,mergeCommit,author",
        "--repo", f"{owner}/{repo}"
    ]))

    # Build PR mappings: commit_hash -> (pr_number, username)
    pr_mapping = {}
    for pr in trunk_prs:
        commit_hash = pr['mergeCommit']['oid']
        pr_mapping[commit_hash] = (pr['number'], pr['author']['login'])

    print("Generating patch-ids for trunk commits...", file=sys.stderr)

    trunk_commits = run_git("git log origin/trunk --format=%H -n 1000").split('\n')
    trunk_patch_to_hash = {}

    for i, commit_hash in enumerate(trunk_commits):
        if i % 100 == 0:
            print(f"  Processing trunk commit {i}/{len(trunk_commits)}", file=sys.stderr)

        patch_id_output = run_git(f"git show {commit_hash} | git patch-id --stable").split()
        if not patch_id_output:
            continue
        patch_id = patch_id_output[0]
        trunk_patch_to_hash[patch_id] = commit_hash

    print("Generating patch-ids for release commits...", file=sys.stderr)

    # Get release branch commits since tag
    release_commits = run_git(f"git log {tag}..origin/{release_branch} --format=%H").split('\n')

    print()
    print("### Changelog")
    print()

    for i, release_hash in enumerate(reversed(release_commits)):
        try:
            patch_id = run_git(f"git show {release_hash} | git patch-id --stable").split()[0]
        except IndexError:
           continue

        trunk_hash = trunk_patch_to_hash.get(patch_id)

        pr_info = None
        if trunk_hash:
            pr_info = pr_mapping.get(trunk_hash)

        subject = run_git(f"git log --format=%s -n 1 {release_hash}")

        if pr_info:
            pr_number, username = pr_info
            print(f"- {subject} by [@{username}](https://github.com/{username}) in [#{pr_number}](https://github.com/{owner}/{repo}/pull/{pr_number})")

if __name__ == "__main__":
    main()
