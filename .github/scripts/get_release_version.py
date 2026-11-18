import os
import re
import sys

with open("version.txt") as f:
    version = f.read()
releaseVersion = version.strip()

# Release jobs publish to tag 'v$REL_VERSION'. On a tag run, a version.txt that
# disagrees with the tag would publish this build into a different release, so
# stop before anything reaches $GITHUB_ENV.
if os.getenv("GITHUB_REF_TYPE") == "tag":
    tag = os.getenv("GITHUB_REF_NAME", "")
    expectedTag = "v{}".format(releaseVersion)
    if tag != expectedTag:
        print(
            "::error title=Tag does not match version.txt::Tag '{tag}' does not match "
            "version.txt '{version}' (expected tag '{expected}'), so a release from "
            "this tag would publish to '{expected}' instead of '{tag}'. Bump "
            "version.txt to '{tagVersion}' and move tag '{tag}' to that commit, or "
            "move the tag to a commit whose version.txt is '{tagVersion}'.".format(
                tag=tag,
                version=releaseVersion,
                expected=expectedTag,
                tagVersion=tag[1:] if tag.startswith("v") else tag,
            )
        )
        sys.exit(1)

# A release build is dispatched with the version it releases. It builds the
# commit whose version.txt says so, or nothing.
requestedVersion = os.getenv("RELEASE_VERSION", "")
if requestedVersion and requestedVersion != releaseVersion:
    print(
        "::error title=Release version does not match version.txt::Release version "
        "'{requested}' does not match version.txt '{version}' at this commit, so this "
        "build would release the wrong version. Start the build on the branch whose "
        "version.txt is '{requested}', or bump version.txt to '{requested}' first.".format(
            requested=requestedVersion, version=releaseVersion
        )
    )
    sys.exit(1)

with open(os.getenv("GITHUB_ENV"), "a") as githubEnv:
    releaseNotePath = "docs/release_notes/v{}.md".format(releaseVersion)

    print("Checking if {} exists".format(releaseNotePath))
    has_release_notes = os.path.exists(releaseNotePath)
    if has_release_notes:
        print("Found {}".format(releaseNotePath))
        githubEnv.write("HAS_RELEASE_NOTES=true\n")
    else:
        print("{} is not found".format(releaseNotePath))

    # A GitHub pre-release is never made 'Latest', so the installers
    # (releases/latest) and the Docker ':latest' tag keep pointing at the last
    # stable release. An RC/beta/alpha (2.4.0-rc.1, 2.4.0-rc.1-enterprise) is a
    # pre-release even when it ships release notes; any other version is a full
    # release only once its notes exist ('2.4.0-enterprise' is stable).
    is_rc = re.search(r"-(rc|beta|alpha)\.\d+", releaseVersion) is not None
    is_prerelease = is_rc or not has_release_notes
    print("Release build from v{} (pre-release: {})...".format(releaseVersion, is_prerelease))

    if is_prerelease:
        githubEnv.write("PRE_RELEASE=true\n")
    githubEnv.write("REL_VERSION={}\n".format(releaseVersion))
