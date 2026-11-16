"""Tests for publish_release.py, against an in-memory GitHub releases API."""

import hashlib
import os
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import publish_release  # noqa: E402

REPO = "spiceai/spiceai"
SHA = "c" * 40


def sha(data):
    return hashlib.sha256(data).hexdigest()


class FakeApi:
    """The subset of the GitHub API publish_release.py calls."""

    def __init__(self, releases=(), runs=()):
        self.releases = [dict(r) for r in releases]
        for r in self.releases:
            r.setdefault("assets", [])
            r.setdefault("body", "")
            r.setdefault("name", "")
        self.runs = list(runs)
        # Git refs and tag objects, by path below repos/{owner}/{repo}/.
        self.objects = {}
        self.next_id = 1000
        self.patches = []
        self.uploads = []
        self.deleted = []
        # Set to make GitHub report a different digest for an upload.
        self.corrupt_uploads = False

    def _release(self, release_id):
        return next(r for r in self.releases if r["id"] == release_id)

    def get_all(self, path, params=None, key=None):
        parts = path.split("/")
        if parts[3:5] == ["actions", "workflows"]:
            return [
                r
                for r in self.runs
                if r["head_sha"] == params["head_sha"]
                and r["conclusion"] == params["status"]
                and r["event"] == params.get("event", r["event"])
                and r["head_branch"] == params.get("branch", r["head_branch"])
            ]
        if parts[3:5] == ["actions", "runs"]:
            run = next(r for r in self.runs if r["id"] == int(parts[5]))
            return [{"name": n, "expired": False} for n in run.get("artifacts", [])]
        if path == f"repos/{REPO}/releases":
            return [dict(r) for r in self.releases]
        if parts[3] == "releases" and parts[5] == "assets":
            return [dict(a) for a in self._release(int(parts[4]))["assets"]]
        raise AssertionError(f"unexpected GET {path}")

    def get(self, path):
        return self.objects.get(path.split("/", 3)[3])

    def post(self, path, payload):
        assert path == f"repos/{REPO}/releases", path
        self.next_id += 1
        release = dict(payload, id=self.next_id, assets=[])
        self.releases.append(release)
        return dict(release)

    def patch(self, path, payload):
        release = self._release(int(path.split("/")[-1]))
        release.update(payload)
        self.patches.append(payload)
        # Publishing a release whose tag does not exist creates it at target_commitish.
        ref = f"git/ref/tags/{release['tag_name']}"
        if not release["draft"] and ref not in self.objects:
            self.objects[ref] = {"object": {"type": "commit", "sha": release["target_commitish"]}}
        return dict(release)

    def delete(self, path):
        asset_id = int(path.split("/")[-1])
        for r in self.releases:
            r["assets"] = [a for a in r["assets"] if a["id"] != asset_id]
        self.deleted.append(asset_id)

    def upload(self, repo, release_id, name, path):
        with open(path, "rb") as f:
            data = f.read()
        self.next_id += 1
        digest = sha(data + b"x" if self.corrupt_uploads else data)
        asset = {"id": self.next_id, "name": name, "state": "uploaded", "digest": f"sha256:{digest}"}
        self._release(release_id)["assets"].append(asset)
        self.uploads.append(name)
        return dict(asset)


class PublishTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.files = {}
        for name in ("spiced_linux_x86_64.tar.gz", "spice_linux_x86_64.tar.gz"):
            data = name.encode()
            path = os.path.join(self.dir.name, name)
            with open(path, "wb") as f:
                f.write(data)
            self.files[name] = (path, sha(data))

    def tearDown(self):
        self.dir.cleanup()

    def asset(self, name, digest=None, state="uploaded", asset_id=1):
        return {"id": asset_id, "name": name, "state": state, "digest": f"sha256:{digest or self.files[name][1]}"}

    def test_creates_and_publishes_a_release_when_there_is_no_draft(self):
        api = FakeApi(releases=[{"id": 1, "tag_name": "v2.4.1", "draft": False, "prerelease": False}])
        latest = publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "# Spice v2.4.2\n\nNotes", False)

        self.assertTrue(latest)
        release = api.releases[-1]
        self.assertEqual(release["tag_name"], "v2.4.2")
        self.assertEqual(release["name"], "Spice v2.4.2")
        self.assertEqual(release["body"], "Notes")
        self.assertEqual(sorted(api.uploads), sorted(self.files))
        self.assertEqual(api.patches, [{"draft": False, "tag_name": "v2.4.2", "target_commitish": SHA, "prerelease": False, "make_latest": "true"}])

    def test_keeps_the_title_and_notes_of_an_endgame_draft(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "name": "Draft title", "body": "Draft notes"}])
        publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "# Checked-in\n\nOther", False)

        self.assertEqual(api.patches, [{"draft": False, "tag_name": "v2.4.2", "target_commitish": SHA, "prerelease": False, "make_latest": "true"}])
        self.assertEqual(api.releases[0]["name"], "Draft title")
        self.assertEqual(api.releases[0]["body"], "Draft notes")

    def test_fills_an_empty_draft_from_the_notes(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False}])
        publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "# Spice v2.4.2\n\nNotes", True)

        self.assertEqual(
            api.patches,
            [{"draft": False, "tag_name": "v2.4.2", "target_commitish": SHA, "prerelease": True, "make_latest": "false", "body": "Notes", "name": "Spice v2.4.2"}],
        )

    def test_resumes_an_upload_and_skips_assets_already_attached(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "assets": [self.asset("spice_linux_x86_64.tar.gz")]}])
        publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertEqual(api.uploads, ["spiced_linux_x86_64.tar.gz"])
        self.assertEqual(len(api.patches), 1)

    def test_replaces_an_incomplete_upload_on_a_draft(self):
        broken = {"id": 55, "name": "spiced_linux_x86_64.tar.gz", "state": "starter", "digest": None}
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "assets": [broken]}])
        publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertEqual(api.deleted, [55])
        self.assertEqual(sorted(api.uploads), sorted(self.files))

    def test_never_replaces_an_asset_with_a_different_sha256(self):
        other = "0" * 64
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "assets": [self.asset("spiced_linux_x86_64.tar.gz", other)]}])
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertIn(f"already has spiced_linux_x86_64.tar.gz with sha256 {other}", str(ctx.exception))
        self.assertEqual(api.uploads, [])
        self.assertEqual(api.patches, [])

    def test_refuses_a_draft_with_assets_from_elsewhere(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "assets": [{"id": 3, "name": "extra.tar.gz", "state": "uploaded", "digest": "sha256:" + "1" * 64}]}])
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertIn("assets that are not in this build: extra.tar.gz", str(ctx.exception))
        self.assertEqual(api.patches, [])

    def test_does_not_publish_when_github_reports_another_digest(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False}])
        api.corrupt_uploads = True
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertIn("GitHub reports sha256", str(ctx.exception))
        self.assertEqual(api.patches, [])

    def test_never_adds_to_a_published_release(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": False, "prerelease": False, "assets": [self.asset("spice_linux_x86_64.tar.gz")]}])
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertIn("already published without spiced_linux_x86_64.tar.gz", str(ctx.exception))
        self.assertEqual(api.uploads, [])

    def test_accepts_a_published_release_that_holds_this_build(self):
        assets = [self.asset(name, asset_id=i) for i, name in enumerate(self.files)]
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": False, "prerelease": False, "assets": assets}])
        api.objects["git/ref/tags/v2.4.2"] = {"object": {"type": "commit", "sha": SHA}}
        latest = publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertTrue(latest)
        self.assertEqual(api.uploads, [])
        self.assertEqual(api.patches, [])

    def test_publishing_creates_the_tag_at_the_built_commit(self):
        # A draft written by hand targets the branch, whose head can move.
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "target_commitish": "release/2.4", "name": "T", "body": "B"}])
        publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertEqual(api.patches, [{"draft": False, "tag_name": "v2.4.2", "target_commitish": SHA, "prerelease": False, "make_latest": "true"}])
        self.assertEqual(api.objects["git/ref/tags/v2.4.2"], {"object": {"type": "commit", "sha": SHA}})

    def test_fails_when_the_published_tag_is_not_at_the_built_commit(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False}])
        # The tag appeared at another commit after the pre-check, so publishing did not create it.
        api.objects["git/ref/tags/v2.4.2"] = {"object": {"type": "commit", "sha": "d" * 40}}
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertIn(f"the tag points at {'d' * 40}, not at the built commit {SHA}", str(ctx.exception))

    def test_refuses_two_releases_with_the_tag(self):
        api = FakeApi(releases=[{"id": 1, "tag_name": "v2.4.2", "draft": True, "prerelease": False}, {"id": 2, "tag_name": "v2.4.2", "draft": True, "prerelease": False}])
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", SHA, self.files, "notes", False)

        self.assertIn("More than one release has tag v2.4.2 (ids 1, 2)", str(ctx.exception))


class LatestTests(unittest.TestCase):
    def api(self, *tags):
        return FakeApi(releases=[{"id": i, "tag_name": t, "draft": False, "prerelease": "-rc." in t} for i, t in enumerate(tags)])

    def test_highest_stable_version_is_latest(self):
        self.assertTrue(publish_release.is_latest(self.api("v2.4.1", "v2.3.6", "v2.5.0-rc.1"), REPO, "v2.4.2", False))

    def test_patch_of_an_older_line_is_not_latest(self):
        self.assertFalse(publish_release.is_latest(self.api("v2.4.1"), REPO, "v2.3.6", False))

    def test_pre_release_is_never_latest(self):
        self.assertFalse(publish_release.is_latest(self.api("v2.4.1"), REPO, "v2.5.0-rc.1", True))
        self.assertFalse(publish_release.is_latest(self.api("v2.4.1"), REPO, "v2.5.0", True))

    def test_enterprise_tags_compare_only_with_enterprise_tags(self):
        api = self.api("v2.5.0", "v2.4.1-enterprise", "v1.11.9-enterprise")
        self.assertTrue(publish_release.is_latest(api, REPO, "v2.4.2-enterprise", False))
        self.assertFalse(publish_release.is_latest(api, REPO, "v1.11.10-enterprise", False))


class FindBuildTests(unittest.TestCase):
    SHA = "a" * 40
    BRANCH = "release/2.4"
    MANIFEST = "release-v2.4.2-sha256sums"

    def run_(self, run_id, artifacts=(), conclusion="success", head_sha=SHA, event="workflow_dispatch", head_branch=BRANCH, created_at=None):
        return {
            "id": run_id,
            "head_sha": head_sha,
            "conclusion": conclusion,
            "event": event,
            "head_branch": head_branch,
            "created_at": created_at or f"2026-11-0{run_id}T00:00:00Z",
            "artifacts": list(artifacts),
        }

    def find(self, api):
        return publish_release.find_build_run(api, REPO, "build_release.yml", "2.4.2", self.SHA, self.BRANCH)

    def test_finds_the_release_build_and_ignores_a_branch_push_of_the_same_commit(self):
        api = FakeApi(runs=[
            self.run_(1, ["spiced_linux_x86_64"], event="push"),
            self.run_(2, [self.MANIFEST, "spiced_linux_x86_64"]),
            self.run_(3, ["release-v2.4.1-sha256sums"]),
            self.run_(4, [self.MANIFEST], conclusion="cancelled"),
            self.run_(5, [self.MANIFEST], head_sha="b" * 40),
        ])
        self.assertEqual(self.find(api), 2)

    def test_picks_the_latest_of_two_release_builds_of_the_commit(self):
        api = FakeApi(runs=[self.run_(4, [self.MANIFEST]), self.run_(6, [self.MANIFEST]), self.run_(5, [self.MANIFEST])])
        self.assertEqual(self.find(api), 6)

    def test_ignores_a_release_build_of_the_commit_from_another_branch(self):
        api = FakeApi(runs=[
            self.run_(2, [self.MANIFEST]),
            self.run_(3, [self.MANIFEST], head_branch="peasee/test"),
        ])
        self.assertEqual(self.find(api), 2)

    def test_fails_without_a_release_build_of_the_commit(self):
        api = FakeApi(runs=[
            self.run_(1, ["spiced_linux_x86_64"], event="push"),
            self.run_(2, [self.MANIFEST], head_branch="peasee/test"),
            self.run_(3, [self.MANIFEST], conclusion="failure"),
        ])
        with self.assertRaises(publish_release.PublishError) as ctx:
            self.find(api)
        self.assertIn(f"No successful build_release.yml release build of 2.4.2 on {self.BRANCH} at {self.SHA}", str(ctx.exception))


class CommitOverrideTests(unittest.TestCase):
    HEAD = "e" * 40
    BUILT = "a" * 40
    BRANCH = "release/2.4"

    def api_with_comparison(self, status):
        api = FakeApi()
        api.objects[f"compare/{self.BUILT}...{self.HEAD}"] = {"status": status}
        return api

    def test_accepts_a_commit_the_branch_head_contains(self):
        for status in ("ahead", "identical"):
            publish_release.require_on_branch(self.api_with_comparison(status), REPO, self.BUILT, self.BRANCH, self.HEAD)

    def test_refuses_a_commit_that_is_not_on_the_branch(self):
        for status in ("behind", "diverged"):
            with self.assertRaises(publish_release.PublishError) as ctx:
                publish_release.require_on_branch(self.api_with_comparison(status), REPO, self.BUILT, self.BRANCH, self.HEAD)
            self.assertIn(f"Commit {self.BUILT} is not on {self.BRANCH}", str(ctx.exception))

    def test_refuses_an_unknown_or_abbreviated_commit(self):
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.require_on_branch(FakeApi(), REPO, self.BUILT, self.BRANCH, self.HEAD)
        self.assertIn(f"Commit {self.BUILT} does not exist", str(ctx.exception))
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.require_on_branch(FakeApi(), REPO, "aaaaaaa", self.BRANCH, self.HEAD)
        self.assertIn("is not a full 40-character commit SHA", str(ctx.exception))

    def find_build(self, api, commit):
        with tempfile.TemporaryDirectory() as d:
            output = os.path.join(d, "output")
            env = {"GITHUB_TOKEN": "t", "GITHUB_OUTPUT": output}
            with unittest.mock.patch.dict(os.environ, env), unittest.mock.patch.object(publish_release, "Api", lambda token: api):
                code = publish_release.main([
                    "--repo", REPO, "--tag", "v2.4.2", "find-build", "--workflow", "build_release.yml",
                    "--branch", self.BRANCH, "--head", self.HEAD, "--commit", commit,
                ])
            with open(output) as f:
                return code, dict(line.split("=", 1) for line in f.read().splitlines())

    def run_at(self, run_id, sha):
        return {"id": run_id, "head_sha": sha, "conclusion": "success", "event": "workflow_dispatch",
                "head_branch": self.BRANCH, "created_at": f"2026-11-0{run_id}T00:00:00Z",
                "artifacts": ["release-v2.4.2-sha256sums"]}

    def test_find_build_publishes_the_given_commit_of_the_branch(self):
        api = self.api_with_comparison("ahead")
        api.runs = [self.run_at(1, self.BUILT), self.run_at(2, self.HEAD)]
        self.assertEqual(self.find_build(api, self.BUILT), (0, {"build_run_id": "1", "commit": self.BUILT}))

    def test_find_build_defaults_to_the_branch_head(self):
        api = FakeApi(runs=[self.run_at(1, self.BUILT), self.run_at(2, self.HEAD)])
        self.assertEqual(self.find_build(api, ""), (0, {"build_run_id": "2", "commit": self.HEAD}))


class TagTests(unittest.TestCase):
    SHA = "c" * 40

    def test_accepts_a_missing_tag_and_a_tag_at_the_commit(self):
        api = FakeApi()
        publish_release.require_tag_free_or_at(api, REPO, "v2.4.2", self.SHA)
        api.objects["git/ref/tags/v2.4.2"] = {"object": {"type": "commit", "sha": self.SHA}}
        publish_release.require_tag_free_or_at(api, REPO, "v2.4.2", self.SHA)
        api.objects["git/ref/tags/v2.4.2"] = {"object": {"type": "tag", "sha": "t" * 40}}
        api.objects["git/tags/" + "t" * 40] = {"object": {"type": "commit", "sha": self.SHA}}
        publish_release.require_tag_free_or_at(api, REPO, "v2.4.2", self.SHA)

    def test_fails_when_the_tag_is_at_another_commit(self):
        api = FakeApi()
        api.objects["git/ref/tags/v2.4.2"] = {"object": {"type": "commit", "sha": "d" * 40}}
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.require_tag_free_or_at(api, REPO, "v2.4.2", self.SHA)
        self.assertIn(f"already points at {'d' * 40}, not at {self.SHA}", str(ctx.exception))


class ReleaseFilesTests(unittest.TestCase):
    """The files a release publishes: those SHA256SUMS lists, and SHA256SUMS itself."""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.sums = os.path.join(self.dir.name, "SHA256SUMS")
        self.lines = []
        for name in ("spice_linux_x86_64.tar.gz", "install.sh", "Install.ps1"):
            data = name.encode()
            with open(os.path.join(self.dir.name, name), "wb") as f:
                f.write(data)
            self.lines.append(f"{sha(data)}  {name}\n")

    def tearDown(self):
        self.dir.cleanup()

    def write_sums(self, lines):
        with open(self.sums, "w") as f:
            f.writelines(lines)
        with open(self.sums, "rb") as f:
            return sha(f.read())

    def test_publishes_every_listed_file_and_the_manifest(self):
        manifest = self.write_sums(self.lines)
        files = publish_release.release_files(self.dir.name, self.sums)
        self.assertEqual(sorted(files), ["Install.ps1", "SHA256SUMS", "install.sh", "spice_linux_x86_64.tar.gz"])
        self.assertEqual(files["SHA256SUMS"], (self.sums, manifest))
        self.assertEqual(files["install.sh"], (os.path.join(self.dir.name, "install.sh"), sha(b"install.sh")))

    def test_refuses_a_manifest_that_lists_its_own_name(self):
        self.write_sums(self.lines + [f"{'1' * 64}  SHA256SUMS\n"])
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.release_files(self.dir.name, self.sums)
        self.assertIn("lists SHA256SUMS, its own asset name", str(ctx.exception))

    def test_refuses_a_listed_file_that_is_missing_or_differs(self):
        self.write_sums(self.lines + [f"{'1' * 64}  install-spiced.sh\n"])
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.release_files(self.dir.name, self.sums)
        self.assertIn("install-spiced.sh is listed", str(ctx.exception))
        with open(os.path.join(self.dir.name, "install-spiced.sh"), "wb") as f:
            f.write(b"changed")
        with self.assertRaises(publish_release.PublishError) as ctx:
            publish_release.release_files(self.dir.name, self.sums)
        self.assertIn(f"install-spiced.sh has sha256 {sha(b'changed')}, but the build recorded {'1' * 64}", str(ctx.exception))

    def test_publish_uploads_the_manifest_with_the_files(self):
        self.write_sums(self.lines)
        api = FakeApi()
        env = {"GITHUB_TOKEN": "t"}
        with unittest.mock.patch.dict(os.environ, env), unittest.mock.patch.object(publish_release, "Api", lambda token: api):
            code = publish_release.main([
                "--repo", REPO, "--tag", "v2.4.2", "publish", "--dir", self.dir.name, "--sums", self.sums,
                "--prerelease", "false", "--sha", SHA,
            ])
        self.assertEqual(code, 0)
        self.assertEqual(sorted(api.uploads), ["Install.ps1", "SHA256SUMS", "install.sh", "spice_linux_x86_64.tar.gz"])
        release = api.releases[0]
        self.assertFalse(release["draft"])
        self.assertEqual({a["name"] for a in release["assets"]}, set(api.uploads))


class ReadSumsTests(unittest.TestCase):
    def read(self, text):
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write(text)
        try:
            return publish_release.read_sums(f.name)
        finally:
            os.unlink(f.name)

    def test_reads_sha256sum_output(self):
        a, b = "1" * 64, "2" * 64
        self.assertEqual(self.read(f"{a}  spiced_linux_x86_64.tar.gz\n{b} *spice.exe_windows_x86_64.tar.gz\n"), {"spiced_linux_x86_64.tar.gz": a, "spice.exe_windows_x86_64.tar.gz": b})

    def test_rejects_paths_and_malformed_lines(self):
        for text in (f"{'1' * 64}  ../spiced.tar.gz\n", "not a checksum line\n", ""):
            with self.assertRaises(publish_release.PublishError):
                self.read(text)


if __name__ == "__main__":
    unittest.main()
