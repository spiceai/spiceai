import os

with open(os.getenv("GITHUB_ENV"), "a") as githubEnv:
  with open("version.txt") as f:
    version = f.read()
  releaseVersion = version
  githubEnv.write("REL_VERSION={}\n".format(releaseVersion))