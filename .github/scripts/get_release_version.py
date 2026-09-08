import os

with open(os.getenv("GITHUB_ENV"), "a") as githubEnv:
    with open("version.txt") as f:
        version = f.read()
    releaseVersion = version.strip()
    is_prerelease = True

    releaseNotePath = "docs/release_notes/v{}.md".format(releaseVersion)

    print("Checking if {} exists".format(releaseNotePath))
    if os.path.exists(releaseNotePath):
        print("Found {}".format(releaseNotePath))
        # Set LATEST_RELEASE to true
        githubEnv.write("LATEST_RELEASE=true\n")
        is_prerelease = False
    else:
        print("{} is not found".format(releaseNotePath))
    print("Release build from v{}...".format(releaseVersion))

    if is_prerelease:
        githubEnv.write("PRE_RELEASE=true\n")
    githubEnv.write("REL_VERSION={}\n".format(releaseVersion))
