"""Unit tests for github_release.py."""
import os
import sys
import tempfile
import unittest
import unittest.mock

# github_release exits at import time if GITHUB_TOKEN is missing.
os.environ.setdefault("GITHUB_TOKEN", "test-token")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import github_release  # noqa: E402


class FakeResponse:
    def __init__(self, status_code, json_data=None, text=""):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._json_data = json_data
        self.text = text

    def json(self):
        if self._json_data is None:
            raise ValueError("No JSON body")
        return self._json_data


class GetReleaseByTagTests(unittest.TestCase):
    @unittest.mock.patch("github_release.requests.get")
    def test_returns_release_dict_on_200(self, mock_get):
        release = {"id": 1, "assets": [], "upload_url": "https://example/{?name,label}"}
        mock_get.return_value = FakeResponse(200, release)

        self.assertEqual(github_release.getReleaseByTag("o", "r", "v1.0"), release)

    @unittest.mock.patch("github_release.requests.get")
    def test_returns_none_on_404(self, mock_get):
        mock_get.return_value = FakeResponse(404, {"message": "Not Found"})

        self.assertIsNone(github_release.getReleaseByTag("o", "r", "v1.0"))

    @unittest.mock.patch("github_release.requests.get")
    def test_returns_none_on_5xx(self, mock_get):
        # Regression for #6566: a 5xx response previously fell through to
        # callers, which would then crash on KeyError("assets").
        mock_get.return_value = FakeResponse(500, {"message": "Internal Server Error"})

        self.assertIsNone(github_release.getReleaseByTag("o", "r", "v1.0"))

    @unittest.mock.patch("github_release.requests.get")
    def test_returns_none_on_rate_limit(self, mock_get):
        mock_get.return_value = FakeResponse(403, {"message": "rate limit exceeded"})

        self.assertIsNone(github_release.getReleaseByTag("o", "r", "v1.0"))

    @unittest.mock.patch("github_release.requests.get")
    def test_returns_none_when_error_body_is_not_json(self, mock_get):
        mock_get.return_value = FakeResponse(502, json_data=None, text="Bad Gateway")

        self.assertIsNone(github_release.getReleaseByTag("o", "r", "v1.0"))


class ActionDeleteTests(unittest.TestCase):
    def _args(self, artifacts=None):
        return unittest.mock.Mock(
            owner="o",
            repo="r",
            tag="v1.0",
            artifact=artifacts or ["./release/x.tar.gz"],
        )

    @unittest.mock.patch("github_release.deleteAsset")
    @unittest.mock.patch("github_release.getReleaseByTag")
    def test_no_release_skips_silently(self, mock_get, mock_delete):
        mock_get.return_value = None

        github_release.actionDelete(self._args())

        mock_delete.assert_not_called()

    @unittest.mock.patch("github_release.deleteAsset")
    @unittest.mock.patch("github_release.getReleaseByTag")
    def test_release_without_assets_key_does_not_raise(self, mock_get, mock_delete):
        # Regression for #6566: the release payload occasionally arrives
        # without an "assets" key, which previously raised KeyError mid-loop
        # and aborted the release workflow.
        mock_get.return_value = {"id": 1, "tag_name": "v1.0"}

        github_release.actionDelete(self._args())

        mock_delete.assert_not_called()

    @unittest.mock.patch("github_release.deleteAsset")
    @unittest.mock.patch("github_release.getReleaseByTag")
    def test_release_with_empty_assets(self, mock_get, mock_delete):
        mock_get.return_value = {"id": 1, "assets": []}

        github_release.actionDelete(self._args())

        mock_delete.assert_not_called()

    @unittest.mock.patch("github_release.deleteAsset")
    @unittest.mock.patch("github_release.getReleaseByTag")
    def test_release_with_matching_asset_is_deleted(self, mock_get, mock_delete):
        mock_get.return_value = {
            "id": 1,
            "assets": [
                {"id": 100, "name": "x.tar.gz"},
                {"id": 101, "name": "y.tar.gz"},
            ],
        }

        github_release.actionDelete(self._args(["./release/x.tar.gz"]))

        mock_delete.assert_called_once_with("o", "r", 100)

    @unittest.mock.patch("github_release.deleteAsset")
    @unittest.mock.patch("github_release.getReleaseByTag")
    def test_release_with_no_matching_asset_does_not_delete(self, mock_get, mock_delete):
        mock_get.return_value = {
            "id": 1,
            "assets": [{"id": 101, "name": "y.tar.gz"}],
        }

        github_release.actionDelete(self._args(["./release/x.tar.gz"]))

        mock_delete.assert_not_called()


class ReadBodyTests(unittest.TestCase):
    def _notes_file(self, contents):
        with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as f:
            f.write(contents)
        self.addCleanup(os.remove, f.name)
        return f.name

    def test_body_file_is_preferred_over_body(self):
        path = self._notes_file("# From file\n\nnotes\n")

        args = unittest.mock.Mock(body_file=path, body="from argv")

        self.assertEqual(github_release.readBody(args), "# From file\n\nnotes\n")

    def test_falls_back_to_body_when_no_file_given(self):
        args = unittest.mock.Mock(body_file=None, body="from argv")

        self.assertEqual(github_release.readBody(args), "from argv")


class TruncateBodyTests(unittest.TestCase):
    def _oversized(self):
        body = "".join("- change {}\n".format(i) for i in range(20000))
        self.assertGreater(len(body), github_release.MAX_BODY_CHARS)
        return body

    def test_short_body_is_unchanged(self):
        body = "# v1.0\n\nA short set of notes.\n"

        self.assertEqual(github_release.truncateBody(body, "o", "r", "v1.0"), body)

    def test_missing_body_is_unchanged(self):
        self.assertIsNone(github_release.truncateBody(None, "o", "r", "v1.0"))

    def test_body_exactly_at_the_limit_is_unchanged(self):
        body = "x" * github_release.MAX_BODY_CHARS

        self.assertEqual(github_release.truncateBody(body, "o", "r", "v1.0"), body)

    def test_oversized_body_fits_within_the_limit(self):
        # Regression for the v2.2.0 release: 154,517 characters of notes both
        # blew MAX_ARG_STRLEN at exec time and would have been rejected by
        # GitHub with "body is too long (maximum is 125000 characters)".
        result = github_release.truncateBody(self._oversized(), "spiceai", "spiceai", "v2.2.0")

        self.assertLessEqual(len(result), github_release.MAX_BODY_CHARS)

    def test_oversized_body_links_to_the_full_notes(self):
        result = github_release.truncateBody(self._oversized(), "spiceai", "spiceai", "v2.2.0")

        self.assertIn(
            "https://github.com/spiceai/spiceai/blob/v2.2.0/docs/release_notes/v2.2.0.md",
            result,
        )

    def test_single_line_body_falls_back_to_a_word_break(self):
        # rfind("\n") returns -1 when the retained prefix holds no line break,
        # which previously left the final partial token -- a URL among them --
        # in the published body.
        body = "word " * github_release.MAX_BODY_CHARS

        result = github_release.truncateBody(body, "spiceai", "spiceai", "v2.2.0")
        kept = result.split("\n\n---\n\n")[0]

        self.assertLessEqual(len(result), github_release.MAX_BODY_CHARS)
        self.assertTrue(body.startswith(kept))
        self.assertEqual(body[len(kept)], " ")

    def test_leading_newline_body_keeps_its_content(self):
        # rfind("\n") returns 0 here; cutting at it would leave an empty body.
        body = "\n" + "word " * github_release.MAX_BODY_CHARS

        result = github_release.truncateBody(body, "spiceai", "spiceai", "v2.2.0")
        kept = result.split("\n\n---\n\n")[0]

        self.assertLessEqual(len(result), github_release.MAX_BODY_CHARS)
        self.assertGreater(len(kept), github_release.MAX_BODY_CHARS // 2)

    def test_oversized_body_is_cut_on_a_line_boundary(self):
        result = github_release.truncateBody(self._oversized(), "spiceai", "spiceai", "v2.2.0")
        kept = result.split("\n\n---\n\n")[0]

        self.assertRegex(kept.rstrip("\n").rsplit("\n", 1)[-1], r"^- change \d+$")


class ActionUploadBodyTests(unittest.TestCase):
    @unittest.mock.patch("github_release.uploadArtifact")
    @unittest.mock.patch("github_release.updateRelease")
    @unittest.mock.patch("github_release.getReleaseByTag")
    def test_oversized_notes_file_is_truncated_before_update(self, mock_get, mock_update, mock_upload):
        mock_get.return_value = {
            "id": 1,
            "assets": [],
            "upload_url": "https://example/{?name,label}",
        }

        with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as f:
            f.write("# Spice v2.2.0\n")
            f.write("".join("- change {}\n".format(i) for i in range(20000)))
        self.addCleanup(os.remove, f.name)

        args = unittest.mock.Mock(
            owner="spiceai",
            repo="spiceai",
            tag="v2.2.0",
            release_name="v2.2.0",
            body=None,
            body_file=f.name,
            prerelease="false",
            artifact=["./release/x.tar.gz"],
        )

        github_release.actionUpload(args)

        _, _, _, release_name, body, is_prerelease = mock_update.call_args[0]

        self.assertEqual(release_name, "Spice v2.2.0")
        self.assertLessEqual(len(body), github_release.MAX_BODY_CHARS)
        self.assertFalse(is_prerelease)
        mock_upload.assert_called_once()


if __name__ == "__main__":
    unittest.main()
