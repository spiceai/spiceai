#!/usr/bin/env python3
"""Unit tests for the CI git-push guard's parser.

The live-tree scan only covers the shapes today's `.github/` happens to contain, so a
parser that stopped recognising `git push` at all would report a clean tree and pass
unnoticed -- the guard would be silently dead. These pin both directions on fixed inputs.
"""

import sys

from check_ci_git_push_noninteractive import steps, violations

FAILURES = []


def check(name, cond):
    if not cond:
        FAILURES.append(name)


# The exact pre-fix shape of `.github/actions/push-snap-changes/action.yml`. This is the
# regression case: the guard MUST reject it, or it would not have caught the hang.
PRE_FIX = """\
runs:
  using: 'composite'
  steps:
    - name: Upload snapshots to branch
      shell: bash
      run: |
        git remote set-url origin "https://x-access-token:${GH_TOKEN}@github.com/${R}.git"
        git push origin "HEAD:${BRANCH_NAME}"
      env:
        GH_TOKEN: ${{ inputs.token }}
"""

POST_FIX = """\
runs:
  using: 'composite'
  steps:
    - name: Upload snapshots to branch
      shell: bash
      run: |
        git remote set-url origin "https://x-access-token:${GH_TOKEN}@github.com/${R}.git"
        git -c credential.helper='' -c credential.interactive=false \\
          push origin "HEAD:${BRANCH_NAME}"
      env:
        GH_TOKEN: ${{ inputs.token }}
        GIT_TERMINAL_PROMPT: '0'
"""

pre = violations("a.yml", PRE_FIX)
check("pre-fix shape is rejected", len(pre) == 2)
check("pre-fix names the missing helper flag", any("credential.helper" in v for v in pre))
check("pre-fix names the missing prompt env", any("GIT_TERMINAL_PROMPT" in v for v in pre))
check("post-fix shape is accepted", violations("a.yml", POST_FIX) == [])

# Half a fix is still a hang: each half must be required on its own.
HELPER_ONLY = POST_FIX.replace("        GIT_TERMINAL_PROMPT: '0'\n", "")
check("helper flag alone is rejected", len(violations("a.yml", HELPER_ONLY)) == 1)

PROMPT_ONLY = POST_FIX.replace(
    "git -c credential.helper='' -c credential.interactive=false \\\n          push",
    "git push",
)
check("prompt env alone is rejected", len(violations("a.yml", PROMPT_ONLY)) == 1)

# A NON-empty helper names a helper to consult -- the opposite of the fix -- so it must
# not satisfy the check. Without the empty-value assertion this passed.
NAMED_HELPER = POST_FIX.replace("credential.helper=''", "credential.helper=osxkeychain")
check("a named helper is rejected", len(violations("a.yml", NAMED_HELPER)) == 1)

# `"" ` is the same empty value spelled the other way.
DQUOTED = POST_FIX.replace("credential.helper=''", 'credential.helper=""')
check("double-quoted empty helper is accepted", violations("a.yml", DQUOTED) == [])

# A step with no push is not this guard's business, however it spells its git calls.
NO_PUSH = """\
jobs:
  b:
    steps:
      - name: Fetch
        run: |
          git fetch --unshallow || true
          git status -s
"""
check("a step with no push is ignored", violations("a.yml", NO_PUSH) == [])

# `gh` is not `git`, and a word ending in `git` is not `git` either.
NOT_GIT = """\
jobs:
  b:
    steps:
      - name: Not a git push
        run: |
          gh push-something
          echo "legit push origin main"
"""
check("gh / substring matches are not treated as git push", violations("a.yml", NOT_GIT) == [])

# Several steps in one file: the guard must attribute a violation to the offending step and
# leave a compliant sibling alone. A parser that fused steps would report one or zero.
TWO_STEPS = POST_FIX + """\
    - name: Second, unguarded
      shell: bash
      run: |
        git push origin other
"""
two = violations("a.yml", TWO_STEPS)
check("only the offending sibling step is reported", len(two) == 2)

# Step splitting itself: a parser that found no steps would return no violations and the
# whole guard would pass vacuously on every file.
check("steps() splits the two-step document", len(list(steps(TWO_STEPS))) == 2)
check("steps() splits a workflow job's steps", len(list(steps(NOT_GIT))) == 1)

if FAILURES:
    print("FAILED:", file=sys.stderr)
    for f in FAILURES:
        print(f"  - {f}", file=sys.stderr)
    sys.exit(1)

print("CI git-push guard parser tests OK")
