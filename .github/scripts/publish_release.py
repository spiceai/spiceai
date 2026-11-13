"""Publish a release from the artifacts of its build_release run.

Used by publish_release.yml:

  find-build  Prints the id of the latest successful release build of a
              version, dispatched on a branch at its head or at another
              commit of the branch, and the commit it built.
  publish     Attaches the files listed in SHA256SUMS, and SHA256SUMS itself,
              to the tag's draft release (created when there is none), checks
              every asset against its sha256, and publishes the release at the
              built commit, which creates the tag there.

The tag is created only by publishing.
A published release is never changed. An asset that already exists is accepted
only when its sha256 matches, so re-running a failed job resumes the upload.
"""

import argparse
import hashlib
import os
import re
import sys

import requests

API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"

# (connect, read) seconds. An upload of the largest binary must fit in read.
TIMEOUT = (10, 60)
UPLOAD_TIMEOUT = (10, 900)

# GitHub rejects a release body longer than this with a 422.
MAX_BODY_CHARS = 125000

# A stable release tag: v2.4.1 or v2.4.1-enterprise. Release candidates and
# other pre-releases never become the latest release.
STABLE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(-enterprise)?$")

SHA256_LINE = re.compile(r"^([0-9a-f]{64}) [ *](\S+)$")

FULL_SHA = re.compile(r"^[0-9a-f]{40}$")

# The manifest's own asset name. Installers and users check downloads against it.
SUMS_ASSET = "SHA256SUMS"


class PublishError(Exception):
    pass


class Api:
    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    def _check(self, resp, what):
        if not resp.ok:
            raise PublishError(f"Failed to {what}: HTTP {resp.status_code}: {resp.text[:500]}")
        return resp

    def get_all(self, path, params=None, key=None):
        """Every item of a paginated list. `key` names the list inside an object response."""
        url = f"{API}/{path}"
        params = dict(params or {}, per_page=100)
        items = []
        while url:
            resp = self._check(self.session.get(url, params=params, timeout=TIMEOUT), f"list {path}")
            body = resp.json()
            items.extend(body[key] if key else body)
            url = resp.links.get("next", {}).get("url")
            params = None
        return items

    def get(self, path):
        """The object at path, or None when it does not exist."""
        resp = self.session.get(f"{API}/{path}", timeout=TIMEOUT)
        if resp.status_code == 404:
            return None
        return self._check(resp, f"read {path}").json()

    def post(self, path, payload):
        return self._check(self.session.post(f"{API}/{path}", json=payload, timeout=TIMEOUT), f"create {path}").json()

    def patch(self, path, payload):
        return self._check(self.session.patch(f"{API}/{path}", json=payload, timeout=TIMEOUT), f"update {path}").json()

    def delete(self, path):
        self._check(self.session.delete(f"{API}/{path}", timeout=TIMEOUT), f"delete {path}")

    def upload(self, repo, release_id, name, path):
        with open(path, "rb") as f:
            resp = self.session.post(
                f"{UPLOADS}/repos/{repo}/releases/{release_id}/assets",
                params={"name": name},
                headers={"Content-Type": "application/octet-stream"},
                data=f,
                timeout=UPLOAD_TIMEOUT,
            )
        return self._check(resp, f"upload {name}").json()


def build_artifact_name(version):
    """Name of the SHA256SUMS artifact of a release build, which build_release.yml uploads."""
    return f"release-v{version}-sha256sums"


def find_build_run(api, repo, workflow, version, sha, branch):
    """The latest successful release build of version, dispatched on branch at sha."""
    runs = api.get_all(
        f"repos/{repo}/actions/workflows/{workflow}/runs",
        {"head_sha": sha, "status": "success", "event": "workflow_dispatch", "branch": branch},
        key="workflow_runs",
    )
    # Only a release build of this version uploads its manifest; a branch build
    # of the same commit does not.
    name = build_artifact_name(version)
    matches = [
        r
        for r in runs
        if r["event"] == "workflow_dispatch"
        and r["head_branch"] == branch
        and any(
            a["name"] == name and not a.get("expired")
            for a in api.get_all(f"repos/{repo}/actions/runs/{r['id']}/artifacts", key="artifacts")
        )
    ]
    if not matches:
        raise PublishError(
            f"No successful {workflow} release build of {version} on {branch} at {sha}. "
            f"Start {workflow} on {branch} with release_version {version} and wait for it to succeed."
        )
    latest = max(matches, key=lambda r: (r["created_at"], r["id"]))
    if len(matches) > 1:
        others = ", ".join(str(r["id"]) for r in matches if r is not latest)
        print(f"Publishing the latest release build of {sha}, run {latest['id']}, not the earlier runs {others}.")
    return latest["id"]


def require_on_branch(api, repo, sha, branch, head):
    """Fail unless sha is a commit of branch: head, its head, is sha or a descendant of it."""
    if not FULL_SHA.match(sha):
        raise PublishError(f"Commit '{sha}' is not a full 40-character commit SHA.")
    comparison = api.get(f"repos/{repo}/compare/{sha}...{head}")
    if comparison is None:
        raise PublishError(f"Commit {sha} does not exist in {repo}.")
    if comparison["status"] not in ("ahead", "identical"):
        raise PublishError(
            f"Commit {sha} is not on {branch}: the head of {branch}, {head}, does not contain it. "
            f"Publish a commit of {branch}, or leave commit empty to publish its head."
        )


def tag_commit(api, repo, tag):
    """The commit tag points at, or None when the tag does not exist."""
    ref = api.get(f"repos/{repo}/git/ref/tags/{tag}")
    if ref is None:
        return None
    obj = ref["object"]
    # An annotated tag points at a tag object; peel it to the commit.
    while obj["type"] == "tag":
        obj = api.get(f"repos/{repo}/git/tags/{obj['sha']}")["object"]
    return obj["sha"]


def require_tag_free_or_at(api, repo, tag, sha):
    """Fail when tag already exists at a commit other than sha, the commit being released."""
    existing = tag_commit(api, repo, tag)
    if existing is not None and existing != sha:
        raise PublishError(
            f"Tag {tag} already points at {existing}, not at {sha}, the commit this run releases. "
            f"A release tag is never moved."
        )


def release_files(directory, sums_path):
    """{asset name: (path, sha256)} for every file sums_path lists, and for the manifest itself.

    Every listed file must be in directory with the sha256 the manifest records.
    """
    sums = read_sums(sums_path)
    if SUMS_ASSET in sums:
        raise PublishError(f"{sums_path} lists {SUMS_ASSET}, its own asset name")
    files = {}
    for name, sha in sums.items():
        path = os.path.join(directory, name)
        if not os.path.isfile(path):
            raise PublishError(f"{name} is listed in {sums_path} but was not downloaded")
        actual = sha256_file(path)
        if actual != sha:
            raise PublishError(f"{name} has sha256 {actual}, but the build recorded {sha}")
        files[name] = (path, sha)
    files[SUMS_ASSET] = (sums_path, sha256_file(sums_path))
    return files


def read_sums(path):
    """Map each file name in a sha256sum manifest to its sha256."""
    sums = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            m = SHA256_LINE.match(line)
            if not m:
                raise PublishError(f"Unreadable line in {path}: '{line}'")
            sha, name = m.groups()
            if os.path.basename(name) != name or name in sums:
                raise PublishError(f"Invalid file name '{name}' in {path}")
            sums[name] = sha
    if not sums:
        raise PublishError(f"{path} lists no files")
    return sums


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def asset_sha256(asset):
    digest = asset.get("digest") or ""
    return digest[len("sha256:"):] if digest.startswith("sha256:") else None


def truncate_body(body, repo, tag):
    """Trim an oversized body to fit GitHub's limit, linking to the full notes."""
    if body is None or len(body) <= MAX_BODY_CHARS:
        return body
    notice = (
        "\n\n---\n\n"
        f"Release notes truncated to fit GitHub's {MAX_BODY_CHARS} character limit. "
        f"Full notes: https://github.com/{repo}/blob/{tag}/docs/release_notes/{tag}.md\n"
    )
    kept = body[: MAX_BODY_CHARS - len(notice)]
    boundary = kept.rfind("\n")
    if boundary <= 0:
        boundary = kept.rfind(" ")
    if boundary > 0:
        kept = kept[:boundary]
    return kept + notice


def title_and_body(body, fallback):
    """Use a leading '# ' heading of the notes as the release title."""
    lines = (body or "").splitlines()
    if lines and lines[0].startswith("# "):
        return lines[0][2:].strip(), "\n".join(lines[1:]).lstrip("\n")
    return fallback, body


def find_release(api, repo, tag):
    # The by-tag endpoint does not return drafts, so search the list.
    releases = [r for r in api.get_all(f"repos/{repo}/releases") if r["tag_name"] == tag]
    if len(releases) > 1:
        ids = ", ".join(str(r["id"]) for r in releases)
        raise PublishError(f"More than one release has tag {tag} (ids {ids}). Delete the extra drafts.")
    return releases[0] if releases else None


def is_latest(api, repo, tag, prerelease):
    """True when tag is a stable version at least as high as every published stable release."""
    m = STABLE_TAG.match(tag)
    if prerelease or not m:
        return False
    version = tuple(int(x) for x in m.groups()[:3])
    for release in api.get_all(f"repos/{repo}/releases"):
        if release["draft"] or release["prerelease"]:
            continue
        other = STABLE_TAG.match(release["tag_name"])
        if other and other.group(4) == m.group(4) and tuple(int(x) for x in other.groups()[:3]) > version:
            return False
    return True


def publish(api, repo, tag, sha, files, notes, prerelease):
    """Attach `files` ({name: (path, sha256)}) to the tag's release and publish it at sha.

    Publishing the draft creates the tag. Returns whether the release was made
    the latest release.
    """
    release = find_release(api, repo, tag)
    title, body = title_and_body(truncate_body(notes, repo, tag), tag)
    if release is None:
        print(f"Creating draft release {tag}")
        release = api.post(
            f"repos/{repo}/releases",
            {"tag_name": tag, "target_commitish": sha, "name": title, "body": body, "draft": True, "prerelease": prerelease},
        )

    assets = {a["name"]: a for a in api.get_all(f"repos/{repo}/releases/{release['id']}/assets")}
    unexpected = sorted(set(assets) - set(files))
    if unexpected:
        raise PublishError(
            f"Release {tag} has assets that are not in this build: {', '.join(unexpected)}. "
            f"A release holds only the files of its build_release run; delete them from the draft."
        )

    # Check every attached asset before uploading anything, so a conflict
    # leaves the draft as it was.
    to_upload = []
    for name, (path, digest) in sorted(files.items()):
        asset = assets.get(name)
        if asset is not None and asset.get("state") != "uploaded" and release["draft"]:
            # An upload that broke off leaves an asset with no content. The
            # release is still a draft, so nobody has downloaded it.
            print(f"Removing incomplete upload of {name}")
            api.delete(f"repos/{repo}/releases/assets/{asset['id']}")
            asset = None
        if asset is not None:
            existing = asset_sha256(asset)
            if existing != digest:
                raise PublishError(
                    f"Release {tag} already has {name} with sha256 {existing}, but this build's is {digest}. "
                    f"Release assets are never replaced. If the draft holds assets of another build, "
                    f"delete them and re-run this job."
                )
            print(f"{name} is already attached with sha256 {digest}")
        elif not release["draft"]:
            raise PublishError(
                f"Release {tag} is already published without {name}. A published release is never changed."
            )
        else:
            to_upload.append((name, path, digest))

    for name, path, digest in to_upload:
        print(f"Uploading {name}")
        uploaded = api.upload(repo, release["id"], name, path)
        if asset_sha256(uploaded) != digest:
            raise PublishError(
                f"GitHub reports sha256 {asset_sha256(uploaded)} for the uploaded {name}, but the file's is {digest}."
            )

    # Read the assets back before publishing, so a release is only published
    # when it holds exactly this build.
    attached = {a["name"]: asset_sha256(a) for a in api.get_all(f"repos/{repo}/releases/{release['id']}/assets")}
    expected = {name: digest for name, (_, digest) in files.items()}
    if attached != expected:
        raise PublishError(f"Release {tag} assets do not match the build: attached {attached}, expected {expected}")

    if not release["draft"]:
        if tag_commit(api, repo, tag) != sha:
            raise PublishError(f"Release {tag} is already published, but not at the built commit {sha}.")
        print(f"Release {tag} is already published with every asset of this build.")
        return is_latest(api, repo, tag, release["prerelease"])

    latest = is_latest(api, repo, tag, prerelease)
    # Publishing creates the tag at target_commitish, the built commit. A draft
    # written by hand may target the branch instead, whose head can move.
    update = {
        "draft": False,
        "tag_name": tag,
        "target_commitish": sha,
        "prerelease": prerelease,
        "make_latest": "true" if latest else "false",
    }
    # A draft written during the endgame keeps its own title and notes.
    if not (release.get("body") or "").strip():
        update["body"] = body
    if not (release.get("name") or "").strip():
        update["name"] = title
    api.patch(f"repos/{repo}/releases/{release['id']}", update)

    created = tag_commit(api, repo, tag)
    if created != sha:
        raise PublishError(f"Published release {tag}, but the tag points at {created}, not at the built commit {sha}.")
    print(f"Published release {tag} at {sha} (pre-release: {prerelease}, latest: {latest})")
    return latest


def write_output(name, value):
    path = os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
    print(f"{name}={value}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--tag", required=True)
    sub = parser.add_subparsers(dest="action", required=True)

    find = sub.add_parser("find-build")
    find.add_argument("--workflow", required=True)
    find.add_argument("--branch", required=True, help="Branch the release build was dispatched on")
    find.add_argument("--head", required=True, help="Commit at the head of the branch")
    find.add_argument("--commit", default="", help="Commit of the branch to publish, instead of its head")

    pub = sub.add_parser("publish")
    pub.add_argument("--dir", required=True, help="Directory holding the downloaded artifacts")
    pub.add_argument("--sums", required=True, help="SHA256SUMS manifest of the build_release run")
    pub.add_argument("--notes-file", help="Release notes, used when the draft has none")
    pub.add_argument("--prerelease", choices=["true", "false"], required=True)
    pub.add_argument("--sha", required=True, help="Commit the build was made from, where the tag is created")

    args = parser.parse_args(argv)
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("::error::GITHUB_TOKEN is not set")
        return 1
    api = Api(token)

    try:
        if args.action == "find-build":
            version = args.tag[1:]
            commit = args.commit or args.head
            if commit != args.head:
                require_on_branch(api, args.repo, commit, args.branch, args.head)
            write_output("build_run_id", find_build_run(api, args.repo, args.workflow, version, commit, args.branch))
            write_output("commit", commit)
            return 0

        files = release_files(args.dir, args.sums)
        notes = None
        if args.notes_file and os.path.isfile(args.notes_file):
            with open(args.notes_file, encoding="utf-8") as f:
                notes = f.read()
        if not notes:
            notes = f"This is the release candidate {args.tag[1:]}"
        require_tag_free_or_at(api, args.repo, args.tag, args.sha)
        latest = publish(api, args.repo, args.tag, args.sha, files, notes, args.prerelease == "true")
        write_output("make_latest", "true" if latest else "false")
        return 0
    except PublishError as e:
        print(f"::error::{e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
