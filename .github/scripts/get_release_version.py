import os
import re

with open(os.getenv("GITHUB_ENV"), "a") as githubEnv:
    with open("version.txt") as f:
        version = f.read()
    releaseVersion = version.strip()

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
