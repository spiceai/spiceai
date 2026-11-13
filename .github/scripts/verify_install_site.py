#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""Verify that install.spiceai.org serves the install scripts of a spiceai/spiceai release.

install.spiceai.org is a Worker that serves byte-for-byte copies of one
release's install scripts, pinned in the Worker's repository and moved there by
a reviewed pull request. Every response says what it is:

  X-Spice-Install-Release        {repo}@{tag}
  X-Spice-Install-Commit         the commit the tag points at
  ETag                           "{sha256 of the body, hex}"
  Content-Digest, Repr-Digest    sha-256=:{sha256 of the body, base64}:

This fetches every path the way `curl https://install.spiceai.org` does and
checks each claim against GitHub, not against the other headers:

  - the response is a 200 with no redirect;
  - ETag, Content-Digest and Repr-Digest are the sha256 of the body received;
  - every path names the same release and commit, the release is published
    and not a pre-release, and the commit is the one its tag points at;
  - the body is byte-identical to install/{script} at that commit;
  - for a release that publishes SHA256SUMS (v2.5.0 on), the body's sha256 is
    the one SHA256SUMS lists for the script and the one GitHub reports for the
    release asset of the same name;
  - the release is --release (default: the latest release), or with
    --allow-earlier-release an earlier stable release, which is reported as a
    pin still to make.

--out-dir writes each verified script there, so a caller runs exactly the bytes
that were checked. Exits 1 when any check fails, listing every failure.

GITHUB_TOKEN, when set, is sent to the GitHub API only.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

SITE = "https://install.spiceai.org"
API = "https://api.github.com"
REPO = "spiceai/spiceai"

# Each path the site serves, and the script under install/ it serves.
# `curl https://install.spiceai.org | /bin/bash` fetches `/`.
PATHS = {
    "/": "install.sh",
    "/install.sh": "install.sh",
    "/install-spiced.sh": "install-spiced.sh",
    "/Install.ps1": "Install.ps1",
}

RELEASE_HEADER = "X-Spice-Install-Release"
COMMIT_HEADER = "X-Spice-Install-Commit"

SUMS_ASSET = "SHA256SUMS"

# The first release whose build lists the install scripts in SHA256SUMS
# (#16341). release/2.4 predates it, so v2.4.x releases publish neither.
FIRST_SIGNED_RELEASE = (2, 5, 0)

# A stable release tag. The site never serves a pre-release.
STABLE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
STRONG_ETAG = re.compile(r'^"([0-9a-f]{64})"$')
SHA256_LINE = re.compile(r"^([0-9a-f]{64}) [ *](\S+)$")

TIMEOUT = 30
# Attempts for a connection failure or a response that says to come back later.
# A 500 is not retried: the Worker answers 500 when a script fails its own
# integrity check, and that is an answer.
ATTEMPTS = 3
RETRY_STATUS = frozenset({429, 502, 503, 504})
USER_AGENT = "spiceai-verify-install-site"


class VerifyError(Exception):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102
        return None


@dataclass
class Response:
    status: int
    headers: dict  # lowercase name -> value
    body: bytes


class Http:
    def __init__(self, api, token):
        self.api = api.rstrip("/")
        self.token = token
        self._follow = urllib.request.build_opener()
        self._no_follow = urllib.request.build_opener(_NoRedirect)

    def get(self, url, headers=None, follow_redirects=True):
        """GET url. Any HTTP status is returned; only a failure to connect raises."""
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
        opener = self._follow if follow_redirects else self._no_follow
        for attempt in range(1, ATTEMPTS + 1):
            try:
                with opener.open(req, timeout=TIMEOUT) as resp:
                    result = Response(resp.status, _lower(resp.headers), resp.read())
            except urllib.error.HTTPError as e:
                result = Response(e.code, _lower(e.headers), e.read())
            except (urllib.error.URLError, OSError) as e:
                if attempt == ATTEMPTS:
                    raise VerifyError(f"Failed to fetch {url}: {e}") from e
                time.sleep(2 * attempt)
                continue
            if result.status in RETRY_STATUS and attempt < ATTEMPTS:
                time.sleep(2 * attempt)
                continue
            return result
        raise AssertionError("unreachable")

    def api_get(self, path, accept="application/vnd.github+json"):
        headers = {"Accept": accept, "X-GitHub-Api-Version": "2022-11-28"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        # urllib sends Authorization on to wherever a redirect points, so the
        # token's requests never follow one.
        return self.get(f"{self.api}/{path}", headers, follow_redirects=False)

    def api_json(self, path):
        """The object at path, or None when GitHub answers 404."""
        resp = self.api_get(path)
        if resp.status == 404:
            return None
        if resp.status != 200:
            raise VerifyError(f"Failed to read {self.api}/{path}: HTTP {resp.status}: {_snippet(resp.body)}")
        return json.loads(resp.body)


def _lower(headers):
    """Header names lowercased. A field sent more than once is combined with ', ', as RFC 9110 does."""
    combined = {}
    for k, val in (headers.items() if headers else ()):
        k = k.lower()
        combined[k] = f"{combined[k]}, {val}" if k in combined else val
    return combined


def _snippet(body):
    return body[:200].decode("utf-8", "replace").strip()


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def stable_version(tag):
    m = STABLE_TAG.match(tag)
    return tuple(int(x) for x in m.groups()) if m else None


def digest_header_sha256(value):
    """The sha-256 member of an RFC 9530 digest field, as hex.

    None when it is absent, malformed, or given more than once.
    """
    found = []
    for member in value.split(","):
        name, sep, rest = member.strip().partition("=")
        if sep and name.strip().lower() == "sha-256":
            found.append(rest.strip())
    if len(found) != 1 or len(found[0]) < 3 or not found[0][0] == found[0][-1] == ":":
        return None
    try:
        raw = base64.b64decode(found[0][1:-1], validate=True)
    except (binascii.Error, ValueError):
        return None
    return raw.hex() if len(raw) == 32 else None


def resolve_release(http, repo, tag):
    """The published release `tag`, or the latest release when tag is 'latest'."""
    if tag == "latest":
        release = http.api_json(f"repos/{repo}/releases/latest")
        if release is None:
            raise VerifyError(f"{repo} has no latest release.")
    else:
        release = http.api_json(f"repos/{repo}/releases/tags/{urllib.parse.quote(tag, safe='')}")
        if release is None:
            raise VerifyError(f"{repo} has no published release {tag}.")
    if release.get("draft") or release.get("prerelease"):
        raise VerifyError(f"{repo} {release['tag_name']} is a draft or a pre-release, which install.spiceai.org never serves.")
    if stable_version(release["tag_name"]) is None:
        raise VerifyError(f"{repo} {release['tag_name']} is not a stable release tag such as v2.4.2.")
    return release


def tag_commit(http, repo, tag):
    """The commit the tag points at, peeling an annotated tag."""
    ref = http.api_json(f"repos/{repo}/git/ref/tags/{urllib.parse.quote(tag, safe='')}")
    if ref is None:
        raise VerifyError(f"{repo} has no tag {tag}.")
    obj = ref["object"]
    while obj["type"] == "tag":
        tag_obj = http.api_json(f"repos/{repo}/git/tags/{obj['sha']}")
        if tag_obj is None:
            raise VerifyError(f"Tag {tag} of {repo} points at tag object {obj['sha']}, which does not exist.")
        obj = tag_obj["object"]
    if obj["type"] != "commit":
        raise VerifyError(f"Tag {tag} of {repo} points at a {obj['type']}, not a commit.")
    return obj["sha"]


def file_at_commit(http, repo, path, commit):
    resp = http.api_get(
        f"repos/{repo}/contents/{urllib.parse.quote(path)}?ref={commit}", accept="application/vnd.github.raw"
    )
    if resp.status == 404:
        return None
    if resp.status != 200:
        raise VerifyError(f"Failed to read {path} at {commit} from {repo}: HTTP {resp.status}: {_snippet(resp.body)}")
    return resp.body


def read_sums(text):
    """Map each file name in a sha256sum manifest to its sha256."""
    sums = {}
    for line in text.splitlines():
        if not line:
            continue
        m = SHA256_LINE.match(line)
        if not m:
            raise VerifyError(f"Unreadable line in {SUMS_ASSET}: '{line}'")
        sha, name = m.groups()
        # `sha256sum --check` checks every line, so a second line for a name
        # must not replace the first, which may be the one that fails.
        if name in sums:
            raise VerifyError(f"{SUMS_ASSET} lists {name} more than once.")
        if os.path.basename(name) != name:
            raise VerifyError(f"{SUMS_ASSET} lists '{name}', which is not a file name.")
        sums[name] = sha
    return sums


@dataclass
class Served:
    path: str
    script: str
    body: bytes
    sha256: str
    release: str  # the X-Spice-Install-Release value
    commit: str  # the X-Spice-Install-Commit value


@dataclass
class Result:
    served: list
    expected_tag: str
    served_tag: str | None
    commit: str | None
    signed: bool | None
    problems: list
    notes: list
    # Set when the site serves an earlier release than expected and that is allowed.
    pending_pin: str | None = None


def fetch_site(http, site, problems):
    """GET every path. Records a problem for each response that is not a 200 whose headers describe its body."""
    served = []
    for path, script in PATHS.items():
        url = site.rstrip("/") + path
        resp = http.get(url, follow_redirects=False)
        if resp.status != 200:
            where = f" to {resp.headers['location']}" if "location" in resp.headers else ""
            problems.append(f"{url} answered HTTP {resp.status}{where}, not 200: {_snippet(resp.body)}")
            continue
        digest = sha256_hex(resp.body)
        etag = STRONG_ETAG.match(resp.headers.get("etag", ""))
        if etag is None:
            problems.append(f"{url} has ETag {resp.headers.get('etag')!r}, not the quoted sha256 of its body, {digest}.")
        elif etag.group(1) != digest:
            problems.append(f"{url} has ETag {etag.group(1)}, but its body's sha256 is {digest}.")
        for name in ("content-digest", "repr-digest"):
            value = resp.headers.get(name)
            claimed = digest_header_sha256(value) if value else None
            if claimed is None:
                problems.append(f"{url} has {name} {value!r}, not a sha-256 digest of its body, {digest}.")
            elif claimed != digest:
                problems.append(f"{url} has a {name} of sha256 {claimed}, but its body's sha256 is {digest}.")
        served.append(
            Served(
                path=path,
                script=script,
                body=resp.body,
                sha256=digest,
                release=resp.headers.get(RELEASE_HEADER.lower(), ""),
                commit=resp.headers.get(COMMIT_HEADER.lower(), ""),
            )
        )
    return served


def verify(http, site, repo, release="latest", allow_earlier=False):
    problems = []
    notes = []
    expected = resolve_release(http, repo, release)
    expected_tag = expected["tag_name"]
    served = fetch_site(http, site, problems)
    result = Result(served, expected_tag, None, None, None, problems, notes)
    if not served:
        return result

    claims = sorted({(s.release, s.commit) for s in served})
    if len(claims) > 1:
        listed = "; ".join(f"{s.path}: {s.release or '(none)'} at {s.commit or '(none)'}" for s in served)
        problems.append(f"{site} serves different releases on different paths: {listed}.")
        return result
    claimed_release, claimed_commit = claims[0]
    served_repo, sep, served_tag = claimed_release.rpartition("@")
    if not sep or served_repo != repo or not served_tag:
        problems.append(f"{site} has {RELEASE_HEADER} {claimed_release!r}, not {repo}@<tag>.")
        return result
    result.served_tag = served_tag

    # Check what is served against its own release first, so a site that serves
    # the wrong release still says whether what it serves is intact.
    if served_tag == expected_tag:
        release_obj = expected
    else:
        try:
            release_obj = resolve_release(http, repo, served_tag)
        except VerifyError as e:
            problems.append(f"{site} serves {served_tag}: {e}")
            return result

    commit = tag_commit(http, repo, served_tag)
    result.commit = commit
    if not FULL_SHA.match(claimed_commit) or claimed_commit != commit:
        problems.append(
            f"{site} has {COMMIT_HEADER} {claimed_commit!r}, but {repo} tag {served_tag} points at {commit}."
        )

    by_script = {}
    for s in served:
        by_script.setdefault(s.script, []).append(s)
    for script, copies in by_script.items():
        source = f"install/{script}"
        expected_body = file_at_commit(http, repo, source, commit)
        if expected_body is None:
            problems.append(f"{repo} has no {source} at {commit}, the commit of {served_tag}.")
            continue
        expected_sha = sha256_hex(expected_body)
        for s in copies:
            if s.sha256 != expected_sha:
                problems.append(
                    f"{site}{s.path} has sha256 {s.sha256}, but {source} at {commit} ({served_tag}) has {expected_sha}."
                )

    check_signed(http, release_obj, by_script, site, result)

    if served_tag != expected_tag:
        latest = ", the latest release" if release == "latest" else ""
        if stable_version(served_tag) < stable_version(expected_tag):
            message = (
                f"{site} serves {served_tag}, not {expected_tag}{latest}. Pin {expected_tag} on "
                f"install.spiceai.org ('Updating the scripts' in the Worker's README), "
                f"then run E2E Test Release Installation."
            )
            if allow_earlier:
                result.pending_pin = message
            else:
                problems.append(message)
        else:
            problems.append(f"{site} serves {served_tag}, which is later than {expected_tag}{latest}.")
    return result


def check_signed(http, release, by_script, site, result):
    """For a release that publishes SHA256SUMS, require each served script to be its listed, attached asset."""
    tag = release["tag_name"]
    assets = {a["name"]: a for a in release.get("assets", [])}
    sums_asset = assets.get(SUMS_ASSET)
    if sums_asset is None:
        result.signed = False
        if stable_version(tag) >= FIRST_SIGNED_RELEASE:
            result.problems.append(
                f"{tag} publishes no {SUMS_ASSET}, but every release from "
                f"v{'.'.join(map(str, FIRST_SIGNED_RELEASE))} on lists its install scripts in one."
            )
        else:
            result.notes.append(
                f"{tag} publishes no {SUMS_ASSET} (it predates v{'.'.join(map(str, FIRST_SIGNED_RELEASE))}), "
                f"so its scripts are checked against its tagged commit only."
            )
        return
    result.signed = True
    resp = http.get(sums_asset["browser_download_url"])
    if resp.status != 200:
        result.problems.append(f"Failed to download {SUMS_ASSET} of {tag}: HTTP {resp.status}.")
        return
    try:
        sums = read_sums(resp.body.decode("utf-8"))
    except (VerifyError, UnicodeDecodeError) as e:
        result.problems.append(f"{SUMS_ASSET} of {tag} is unreadable: {e}")
        return
    for script, copies in by_script.items():
        listed = sums.get(script)
        asset = assets.get(script)
        attached = (asset.get("digest") or "") if asset else ""
        for s in copies:
            if listed is None:
                result.problems.append(f"{SUMS_ASSET} of {tag} does not list {script}, which {site}{s.path} serves.")
            elif listed != s.sha256:
                result.problems.append(
                    f"{site}{s.path} has sha256 {s.sha256}, but {SUMS_ASSET} of {tag} lists {script} with {listed}."
                )
            if asset is None:
                result.problems.append(f"{tag} has no {script} asset, which {site}{s.path} serves.")
            elif attached != f"sha256:{s.sha256}":
                result.problems.append(
                    f"{site}{s.path} has sha256 {s.sha256}, but GitHub reports {attached or 'no digest'} for {tag}'s {script} asset."
                )


def report(result, site, repo, out=None):
    out = out or sys.stdout
    by_path = {s.path: s for s in result.served}
    commit = result.commit or "(unresolved)"
    if result.problems:
        verdict = "FAILED"
    elif result.pending_pin:
        verdict = f"verified, but serves an earlier release than {result.expected_tag}"
    else:
        verdict = "verified"
    print(f"{site}: {verdict}", file=out)
    print(f"  expected {repo}@{result.expected_tag}; serves {repo}@{result.served_tag or '?'} at {commit}", file=out)
    for path, script in PATHS.items():
        s = by_path.get(path)
        print(f"  {path:<20} {script:<18} " + (f"sha256 {s.sha256}" if s else "(no script)"), file=out)
    for note in result.notes:
        print(f"  {note}", file=out)

    in_actions = os.getenv("GITHUB_ACTIONS") == "true"
    for problem in result.problems:
        print(f"::error::{problem}" if in_actions else f"error: {problem}", file=out)
    if result.pending_pin:
        print(f"::warning::{result.pending_pin}" if in_actions else f"warning: {result.pending_pin}", file=out)

    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"### {site}: {verdict}\n\n")
            f.write(
                f"Expected `{repo}@{result.expected_tag}`; serves `{repo}@{result.served_tag or '?'}` at `{commit}`.\n\n"
            )
            f.write("| Path | Script | sha256 |\n| --- | --- | --- |\n")
            for path, script in PATHS.items():
                s = by_path.get(path)
                f.write(f"| `{path}` | `{script}` | {'`' + s.sha256 + '`' if s else 'not served'} |\n")
            f.write("\n")
            for line in result.problems + ([result.pending_pin] if result.pending_pin else []) + result.notes:
                f.write(f"- {line}\n")
            f.write("\n")


def write_scripts(result, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    for s in result.served:
        if s.path == "/":
            continue  # the same script as /install.sh, checked against the same file
        with open(os.path.join(out_dir, s.script), "wb") as f:
            f.write(s.body)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--site", default=SITE)
    parser.add_argument("--repo", default=REPO)
    parser.add_argument("--api-url", default=API)
    parser.add_argument(
        "--release",
        default="latest",
        help="Tag of the release the site must serve, such as v2.5.0, or 'latest' (the default) for the latest release",
    )
    parser.add_argument(
        "--allow-earlier-release",
        action="store_true",
        help="Accept an earlier stable release than --release, still fully verified, and warn that it is to be pinned",
    )
    parser.add_argument("--out-dir", help="Write each verified script here, under its name in install/")
    args = parser.parse_args(argv)

    http = Http(args.api_url, os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN"))
    try:
        result = verify(http, args.site, args.repo, args.release, args.allow_earlier_release)
    except VerifyError as e:
        print(f"::error::{e}" if os.getenv("GITHUB_ACTIONS") == "true" else f"error: {e}")
        return 1
    report(result, args.site, args.repo)
    if result.problems:
        return 1
    if args.out_dir:
        write_scripts(result, args.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
