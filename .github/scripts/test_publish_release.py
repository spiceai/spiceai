"""Tests for publish_release.py, against an in-memory GitHub releases API."""

import hashlib
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import publish_release  # noqa: E402
from publish_release import PublishError  # noqa: E402

REPO = "spiceai/spiceai"


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
            return [r for r in self.runs if r["head_sha"] == params["head_sha"] and r["conclusion"] == params["status"]]
        if path == f"repos/{REPO}/releases":
            return [dict(r) for r in self.releases]
        if parts[3] == "releases" and parts[5] == "assets":
            return [dict(a) for a in self._release(int(parts[4]))["assets"]]
        raise AssertionError(f"unexpected GET {path}")

    def get(self, path):
        return self.objects[path.split("/", 3)[3]]

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
        latest = publish_release.publish(api, REPO, "v2.4.2", self.files, "# Spice v2.4.2\n\nNotes", False)

        self.assertTrue(latest)
        release = api.releases[-1]
        self.assertEqual(release["tag_name"], "v2.4.2")
        self.assertEqual(release["name"], "Spice v2.4.2")
        self.assertEqual(release["body"], "Notes")
        self.assertEqual(sorted(api.uploads), sorted(self.files))
        self.assertEqual(api.patches, [{"draft": False, "prerelease": False, "make_latest": "true"}])

    def test_keeps_the_title_and_notes_of_an_endgame_draft(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "name": "Draft title", "body": "Draft notes"}])
        publish_release.publish(api, REPO, "v2.4.2", self.files, "# Checked-in\n\nOther", False)

        self.assertEqual(api.patches, [{"draft": False, "prerelease": False, "make_latest": "true"}])
        self.assertEqual(api.releases[0]["name"], "Draft title")
        self.assertEqual(api.releases[0]["body"], "Draft notes")

    def test_fills_an_empty_draft_from_the_notes(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False}])
        publish_release.publish(api, REPO, "v2.4.2", self.files, "# Spice v2.4.2\n\nNotes", True)

        self.assertEqual(
            api.patches,
            [{"draft": False, "prerelease": True, "make_latest": "false", "body": "Notes", "name": "Spice v2.4.2"}],
        )

    def test_resumes_an_upload_and_skips_assets_already_attached(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "assets": [self.asset("spice_linux_x86_64.tar.gz")]}])
        publish_release.publish(api, REPO, "v2.4.2", self.files, "notes", False)

        self.assertEqual(api.uploads, ["spiced_linux_x86_64.tar.gz"])
        self.assertEqual(len(api.patches), 1)

    def test_replaces_an_incomplete_upload_on_a_draft(self):
        broken = {"id": 55, "name": "spiced_linux_x86_64.tar.gz", "state": "starter", "digest": None}
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "assets": [broken]}])
        publish_release.publish(api, REPO, "v2.4.2", self.files, "notes", False)

        self.assertEqual(api.deleted, [55])
        self.assertEqual(sorted(api.uploads), sorted(self.files))

    def test_never_replaces_an_asset_with_a_different_sha256(self):
        other = "0" * 64
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "assets": [self.asset("spiced_linux_x86_64.tar.gz", other)]}])
        with self.assertRaises(PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", self.files, "notes", False)

        self.assertIn(f"already has spiced_linux_x86_64.tar.gz with sha256 {other}", str(ctx.exception))
        self.assertEqual(api.uploads, [])
        self.assertEqual(api.patches, [])

    def test_refuses_a_draft_with_assets_from_elsewhere(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False, "assets": [{"id": 3, "name": "extra.tar.gz", "state": "uploaded", "digest": "sha256:" + "1" * 64}]}])
        with self.assertRaises(PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", self.files, "notes", False)

        self.assertIn("assets that are not in this build: extra.tar.gz", str(ctx.exception))
        self.assertEqual(api.patches, [])

    def test_does_not_publish_when_github_reports_another_digest(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": True, "prerelease": False}])
        api.corrupt_uploads = True
        with self.assertRaises(PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", self.files, "notes", False)

        self.assertIn("GitHub reports sha256", str(ctx.exception))
        self.assertEqual(api.patches, [])

    def test_never_adds_to_a_published_release(self):
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": False, "prerelease": False, "assets": [self.asset("spice_linux_x86_64.tar.gz")]}])
        with self.assertRaises(PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", self.files, "notes", False)

        self.assertIn("already published without spiced_linux_x86_64.tar.gz", str(ctx.exception))
        self.assertEqual(api.uploads, [])

    def test_accepts_a_published_release_that_holds_this_build(self):
        assets = [self.asset(name, asset_id=i) for i, name in enumerate(self.files)]
        api = FakeApi(releases=[{"id": 7, "tag_name": "v2.4.2", "draft": False, "prerelease": False, "assets": assets}])
        latest = publish_release.publish(api, REPO, "v2.4.2", self.files, "notes", False)

        self.assertTrue(latest)
        self.assertEqual(api.uploads, [])
        self.assertEqual(api.patches, [])

    def test_refuses_two_releases_with_the_tag(self):
        api = FakeApi(releases=[{"id": 1, "tag_name": "v2.4.2", "draft": True, "prerelease": False}, {"id": 2, "tag_name": "v2.4.2", "draft": True, "prerelease": False}])
        with self.assertRaises(PublishError) as ctx:
            publish_release.publish(api, REPO, "v2.4.2", self.files, "notes", False)

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

    def run_(self, run_id, head_branch, conclusion="success", head_sha=SHA):
        return {"id": run_id, "head_branch": head_branch, "head_sha": head_sha, "conclusion": conclusion}

    def test_finds_the_tag_run_and_ignores_a_branch_run_of_the_same_commit(self):
        api = FakeApi(runs=[self.run_(1, "release/2.4"), self.run_(2, "v2.4.2"), self.run_(3, "v2.4.2", "cancelled")])
        self.assertEqual(publish_release.find_build_run(api, REPO, "build_release.yml", "v2.4.2", self.SHA), 2)

    def test_fails_without_a_successful_tag_run(self):
        api = FakeApi(runs=[self.run_(1, "release/2.4"), self.run_(3, "v2.4.2", "failure")])
        with self.assertRaises(PublishError) as ctx:
            publish_release.find_build_run(api, REPO, "build_release.yml", "v2.4.2", self.SHA)
        self.assertIn("found none", str(ctx.exception))

    def test_fails_with_two_successful_tag_runs(self):
        api = FakeApi(runs=[self.run_(4, "v2.4.2"), self.run_(5, "v2.4.2")])
        with self.assertRaises(PublishError) as ctx:
            publish_release.find_build_run(api, REPO, "build_release.yml", "v2.4.2", self.SHA)
        self.assertIn("found 4, 5", str(ctx.exception))


class TagTests(unittest.TestCase):
    SHA = "c" * 40

    def test_accepts_a_lightweight_and_an_annotated_tag_at_the_commit(self):
        api = FakeApi()
        api.objects["git/ref/tags/v2.4.2"] = {"object": {"type": "commit", "sha": self.SHA}}
        publish_release.require_tag_at(api, REPO, "v2.4.2", self.SHA)
        api.objects["git/ref/tags/v2.4.2"] = {"object": {"type": "tag", "sha": "t" * 40}}
        api.objects["git/tags/" + "t" * 40] = {"object": {"type": "commit", "sha": self.SHA}}
        publish_release.require_tag_at(api, REPO, "v2.4.2", self.SHA)

    def test_fails_when_the_tag_moved(self):
        api = FakeApi()
        api.objects["git/ref/tags/v2.4.2"] = {"object": {"type": "commit", "sha": "d" * 40}}
        with self.assertRaises(PublishError) as ctx:
            publish_release.require_tag_at(api, REPO, "v2.4.2", self.SHA)
        self.assertIn(f"now points at {'d' * 40}, not at {self.SHA}", str(ctx.exception))


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
            with self.assertRaises(PublishError):
                self.read(text)


if __name__ == "__main__":
    unittest.main()
