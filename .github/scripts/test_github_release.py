"""Unit tests for github_release.py."""
import os
import sys
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


if __name__ == "__main__":
    unittest.main()
