---
name: writeReleaseNotes
description: Write release notes for a new version based on git history and previous release notes style.
argument-hint: The new version tag and the previous version tag to diff against (e.g. "v2.0.0-rc.1 since v1.11.2")
---

Write release notes for the specified version based on all changes since the specified previous release.

Follow these steps:

1. **Study previous release notes** in the `docs/release_notes/` directory to understand the structure, tone, and style used by the project. Pay attention to:
   - Header format and date conventions
   - How features are grouped and described
   - Level of technical detail per feature
   - Breaking changes section format
   - Contributors section format
   - Upgrading instructions format
   - Changelog format with PR links and author attribution

2. **Gather changes** using `git log` between the two version tags:
   - Get all non-merge commits with `git log <prev-tag>..HEAD --oneline --no-merges`
   - Identify contributors with `git log <prev-tag>..HEAD --format="%an <%ae>" --no-merges | sort -u`
   - Map contributor names to GitHub usernames by cross-referencing previous release notes and email addresses
   - Read commit messages for major PRs to understand feature scope and details

3. **Categorize changes** into:
   - Major new features (deserve their own subsection with description, key points, and examples)
   - Dependency upgrades (presented in a table)
   - Other improvements (bullet list of smaller features and fixes)
   - Breaking changes (with migration guidance)
   - Bug fixes (grouped by area)

4. **Write the release notes** matching the established style:
   - Opening summary paragraph highlighting the most important features
   - "What's New" section with subsections for each major feature
   - Contributors list with GitHub profile links
   - Breaking Changes section
   - Cookbook Updates section
   - Upgrading section with CLI, Homebrew, Docker, Helm, and marketplace instructions
   - Changelog section with PR links and author attribution

5. **Filter noise** from the changelog: exclude CI fixes, test snapshot updates, dependabot bumps, internal refactors, and other non-user-facing changes from the summary sections (but include significant ones in the detailed changelog).

Save the release notes as a new markdown file in the release notes directory.
