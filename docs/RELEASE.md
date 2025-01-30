# Spice.ai Release Process

Spice.ai releases once per week.

## Release Branch Management

### Branch Structure

The `trunk` branch serves as the primary development branch containing the latest code. Changes to release branches must originate from `trunk` through cherry-picking when direct fast-forwarding isn't possible. Release branches accept only version number updates as direct commits. The release DRI is responsible to ensure all required changes are present on the release branch before the release is cut.

### Major and Minor Releases

1. Create the release branch from trunk one day before the scheduled release (or use an existing branch from a pre-release)
2. Name the branch `release/X.Y` (excluding patch version)
3. Execute all testing procedures on this branch
4. Apply fixes/changes:
   - First commit to `trunk`
   - Fast-forward/cherry-pick to release branch

### Pre-releases (Release Candidates)

Pre-releases follow the format `vX.Y.0-rc.N` where `N` is the RC number (e.g., `v1.0.0-rc.1`).

1. Create/use the corresponding minor release branch (e.g., `release/1.0` for `v1.0.0-rc.1`)
2. The latest release branch should always be fast-forwarded from `trunk`.
3. Older release branches should have changes cherry-picked from `trunk` as needed.
4. Update version number in `version.txt` and `Cargo.toml` to include the new RC version (e.g., `v1.0.0-rc.1`). For the latest release branch, this is done in trunk and fast-forwarded to the release branch. Older release branches should have the version number updated directly.
5. All RC releases for a given version (`vX.Y.0`) happen on the same branch.

Example:

- RC releases for `v1.0.0` (e.g., `v1.0.0-rc.1`, `v1.0.0-rc.2`, etc.) → `release/1.0` branch.
- If `release/1.1` exists, changes to `release/1.0` must be cherry-picked from `trunk`.

### Patch Releases

For version `vX.Y.Z`:

1. Use existing minor release branch (`release/X.Y`)
2. Cherry-pick required fixes from `trunk`. No new features are allowed in patch releases, only bug/security fixes.
3. Update patch version number.

## Endgame issue

Create a [Milestone Endgame](https://github.com/spiceai/spiceai/issues/new?assignees=&labels=endgame&projects=&template=end_game.md&title=v0.x.x-alpha+Endgame) issue to track the checklist needed for a specific milestone release.

## Version update

- Major and minor updates can drop the patch in documentation and release notes. i.e. `v0.3` not `v0.3.0`. The actual version number in version.txt and Cargo.toml needs to be the full semver.
- Create a PR updating version.txt to the next planned version number.
  - i.e. `0.2.1-alpha` -> `0.3.0-alpha`
- Ensure the release notes at `docs/release_notes/v{version}.md` exist
- Create a new pre-release [GitHub release](https://github.com/spiceai/spiceai/releases/new) with placeholder information. Create a tag that matches the version to be released. The details of the release will be filled in by the automation.
  - Alternatively push a new tag via the git command line to the GitHub repository, and the release automation will be triggered.

## Acknowledgements update

- Run the `generate-acknowledgements` target and commit the result
