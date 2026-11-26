#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
import subprocess
import json
import sys
import re

# Both arguments come from the inputs of a manually dispatched workflow
# (`.github/workflows/generate_changelog.yml`), so they are untrusted. Each one is
# checked as a Git ref name and resolved to a commit before anything else runs,
# later commands receive only the resolved commit IDs, and every command runs from
# an argument list rather than through a shell, so no input is ever read as shell
# syntax or as a Git option.

# The characters a release tag or branch name may contain: every name Spice has
# used for one (`v1.9.0-rc.4`, `release/1.8`, `pease/release-1.8-prep`) fits, and
# none of the characters a shell or Git's revision syntax gives a meaning to
# does. A name may not start with `-`, which Git would read as an option.
REF_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._/-]*")


class InputError(Exception):
    """An argument that does not name a usable ref. The message names the input and the fix."""


def run_git(*args):
    """Run git with `args` as its argument list and return its stripped stdout."""
    result = subprocess.run(["git", *args], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def git_records(*args):
    """Run git with `args` and return its non-empty output lines.

    Splits on `\n` only: `git log` ends each record with `\n`, while a commit
    subject may itself contain U+2028, U+0085 or another separator that
    `str.splitlines()` would also split on.
    """
    return [record for record in run_git(*args).split("\n") if record]


def resolve_commit(value, input_name, namespace, kind, example):
    """Return the commit ID that the ref `<namespace><value>` names.

    Raises `InputError` when `value` is not a valid ref name or names no commit.
    Only the full ref is passed to Git, and it starts with `refs/`, so Git can
    never read it as an option.
    """
    fix = f"Pass the name of the {kind}, for example '{example}'."
    if not REF_NAME.fullmatch(value):
        raise InputError(
            f"{input_name} {value!r} is not a valid {kind} name: use only letters, digits, "
            f"'.', '_', '-' and '/', starting with a letter, digit or '_'. {fix}"
        )
    ref = namespace + value
    if subprocess.run(["git", "check-ref-format", ref], capture_output=True).returncode != 0:
        raise InputError(
            f"{input_name} {value!r} is not a valid {kind} name: Git does not accept {ref!r} "
            f"as a ref name. {fix}"
        )
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise InputError(
            f"{input_name} {value!r} does not name a {kind} in this checkout: {ref!r} does not "
            f"exist. {fix} If it exists on GitHub, fetch it first (`git fetch --tags origin`)."
        )
    return result.stdout.strip()


def patch_id(commit):
    """Return the stable patch-id of `commit`, or None when it changes no file.

    Equivalent to `git show <commit> | git patch-id --stable`, with the two
    processes connected by a pipe instead of a shell.
    """
    show = subprocess.Popen(["git", "show", commit], stdout=subprocess.PIPE)
    try:
        output = subprocess.run(
            ["git", "patch-id", "--stable"], stdin=show.stdout, capture_output=True, check=True
        ).stdout
    finally:
        show.stdout.close()
        show_status = show.wait()
    if show_status != 0:
        raise subprocess.CalledProcessError(show_status, show.args)
    fields = output.split()
    return fields[0].decode() if fields else None

"""
The idea behind this script is to:
* Find commits that are in current release branch but not in the previous tag
* Match them to trunk commits using patch-id
* Find relevant PR
"""

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 generate_changelog.py <previous_release_tag> <release_branch>", file=sys.stderr)
        print("Example: python3 generate_changelog.py v1.4.0 release/1.5", file=sys.stderr)
        sys.exit(2)

    owner = "spiceai"
    repo = "spiceai"

    # Validate both inputs before any other command runs.
    try:
        tag_commit = resolve_commit(sys.argv[1], "previous_release_tag", "refs/tags/", "tag", "v1.4.0")
        release_commit = resolve_commit(sys.argv[2], "release_branch", "refs/remotes/origin/", "branch", "release/1.5")
    except InputError as error:
        print(f"error: Failed to generate the changelog: {error}", file=sys.stderr)
        sys.exit(2)

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

    trunk_commits = git_records("log", "refs/remotes/origin/trunk", "--format=%H", "-n", "1000")
    trunk_patch_to_hash = {}

    # Build mapping: patch-id -> commit_hash for trunk commits
    for i, commit_hash in enumerate(trunk_commits):
        if i % 100 == 0:
            print(f"  Processing trunk commit {i}/{len(trunk_commits)}", file=sys.stderr)

        trunk_patch_id = patch_id(commit_hash)
        if trunk_patch_id is None:
            continue
        trunk_patch_to_hash[trunk_patch_id] = commit_hash

    # Get release branch commits since tag. Both ends are the commit IDs
    # resolved above, and `--` ends the revision list.
    release_commits = git_records("log", f"{tag_commit}..{release_commit}", "--format=%H-%s", "--")

    print()
    print("### Changelog")
    print()

    for commit in reversed(release_commits):
        commit_hash, commit_title = commit.split("-", 1)

        # Calculate its patch-id
        release_patch_id = patch_id(commit_hash)
        if release_patch_id is None:
            continue

        # Find PR from patch-id
        trunk_hash = trunk_patch_to_hash.get(release_patch_id)
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
                except subprocess.CalledProcessError:
                    # `gh pr view` exits non-zero for a number it cannot resolve.
                    print(f"- !!!! (PR NOT FOUND): {commit_title} - https://github.com/{owner}/{repo}/commit/{commit_hash}/")
                    continue

                username = pr_info["author"]["login"]
                print(f"- {commit_title_clean} by [@{username}](https://github.com/{username}) in [#{pr_number}](https://github.com/{owner}/{repo}/pull/{pr_number})")
            else:
                # If everything else fails, print commit title with the link to github
                print(f"- !!!! (PR NOT FOUND): {commit_title} - https://github.com/{owner}/{repo}/commit/{commit_hash}/")

if __name__ == "__main__":
    main()
