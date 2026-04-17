# Copyright 2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Authentication integration tests for the Spice runtime.

Supports three modes (--mode):
    oidc      - OIDC Bearer token authentication (Entra client-credentials)
    api_key   - API key authentication (x-api-key header)
    combined  - Both OIDC and API key enabled simultaneously

Required environment variables vary by mode:
    OIDC / combined:
        AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, OIDC_SCOPE
    API key / combined:
        SPICE_API_KEY
    All modes:
        SPICE_HTTP_PORT (optional, default: 8090)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

HTTP_PORT = os.environ.get("SPICE_HTTP_PORT", "8090")
BASE_URL = f"http://localhost:{HTTP_PORT}"

failures: list[str] = []


# ── Helpers ───────────────────────────────────────────────────────────


def assert_status(
    description: str,
    expected: int,
    target: urllib.request.Request | str,
) -> None:
    """Send a request and assert the HTTP status code."""
    try:
        req = (
            urllib.request.Request(target)
            if isinstance(target, str)
            else target
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            actual = resp.status
    except urllib.error.HTTPError as e:
        actual = e.code
    except urllib.error.URLError as e:
        msg = f"{description} — request failed: {e.reason}"
        print(f"FAIL: {msg}")
        failures.append(msg)
        return
    except TimeoutError:
        msg = f"{description} — request timed out"
        print(f"FAIL: {msg}")
        failures.append(msg)
        return

    if actual == expected:
        print(f"PASS: {description} (HTTP {actual})")
    else:
        msg = f"{description} — expected HTTP {expected}, got HTTP {actual}"
        print(f"FAIL: {msg}")
        failures.append(msg)


def req(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: str | None = None,
) -> urllib.request.Request:
    """Build a urllib Request."""
    r = urllib.request.Request(url, method=method, headers=headers or {})
    if data is not None:
        r.data = data.encode()
    return r


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: Required environment variable {name} is not set.")
        sys.exit(1)
    return value


def acquire_entra_token() -> str:
    """Acquire a JWT from Entra via the OAuth2 client-credentials flow."""
    tenant_id = require_env("AZURE_TENANT_ID")
    client_id = require_env("AZURE_CLIENT_ID")
    client_secret = require_env("AZURE_CLIENT_SECRET")
    scope = require_env("OIDC_SCOPE")

    print("==> Acquiring Entra access token via client-credentials flow …")

    token_url = (
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    )
    body = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
            "grant_type": "client_credentials",
        }
    ).encode()

    r = urllib.request.Request(
        token_url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    with urllib.request.urlopen(r, timeout=30) as resp:
        token_response = json.loads(resp.read())

    access_token = token_response.get("access_token")
    if not access_token:
        print(
            "ERROR: Failed to acquire access token from Entra."
            f"\nResponse: {token_response}"
        )
        sys.exit(1)

    print(f"==> Access token acquired ({len(access_token)} chars).")
    return access_token


def summarize() -> None:
    """Print summary and exit with appropriate code."""
    print()
    if not failures:
        print("==> All authentication integration tests passed.")
    else:
        print(f"==> {len(failures)} test(s) FAILED:")
        for f in failures:
            print(f"    - {f}")
        sys.exit(1)


# ── Common assertions ────────────────────────────────────────────────


def test_unauthenticated_endpoints_pass() -> None:
    """Health and ready endpoints never require auth."""
    print("\n==> Testing unauthenticated endpoints …")
    assert_status("GET /health (no auth)", 200, f"{BASE_URL}/health")
    assert_status("GET /v1/ready (no auth)", 200, f"{BASE_URL}/v1/ready")


def test_no_credentials_rejected() -> None:
    """Authenticated endpoints must reject requests without credentials."""
    print("\n==> Testing authenticated endpoints without credentials …")
    assert_status("GET /v1/status (no auth)", 401, f"{BASE_URL}/v1/status")
    assert_status(
        "POST /v1/sql (no auth)",
        401,
        req(f"{BASE_URL}/v1/sql", method="POST", data="SELECT 1"),
    )


# ── OIDC tests ───────────────────────────────────────────────────────


def test_oidc_valid_token(token: str) -> None:
    print("\n==> Testing OIDC: valid Bearer token …")
    hdr = {"Authorization": f"Bearer {token}"}
    assert_status(
        "GET /v1/status (OIDC valid)",
        200,
        req(f"{BASE_URL}/v1/status", headers=hdr),
    )
    assert_status(
        "POST /v1/sql (OIDC valid)",
        200,
        req(f"{BASE_URL}/v1/sql", method="POST", headers=hdr, data="SELECT 1"),
    )


def test_oidc_invalid_token() -> None:
    print("\n==> Testing OIDC: invalid Bearer token …")
    hdr = {"Authorization": "Bearer invalid.jwt.token"}
    assert_status(
        "GET /v1/status (OIDC invalid)",
        401,
        req(f"{BASE_URL}/v1/status", headers=hdr),
    )


# ── API key tests ────────────────────────────────────────────────────


def test_api_key_valid(api_key: str) -> None:
    print("\n==> Testing API key: valid key …")
    hdr = {"x-api-key": api_key}
    assert_status(
        "GET /v1/status (API key valid)",
        200,
        req(f"{BASE_URL}/v1/status", headers=hdr),
    )
    assert_status(
        "POST /v1/sql (API key valid)",
        200,
        req(f"{BASE_URL}/v1/sql", method="POST", headers=hdr, data="SELECT 1"),
    )


def test_api_key_invalid() -> None:
    print("\n==> Testing API key: invalid key …")
    hdr = {"x-api-key": "wrong-key-value"}
    assert_status(
        "GET /v1/status (API key invalid)",
        401,
        req(f"{BASE_URL}/v1/status", headers=hdr),
    )


# ── Mode runners ─────────────────────────────────────────────────────


def run_oidc() -> None:
    """Test OIDC-only authentication."""
    print("=" * 60)
    print("Mode: OIDC")
    print("=" * 60)

    token = acquire_entra_token()

    test_unauthenticated_endpoints_pass()
    test_no_credentials_rejected()
    test_oidc_valid_token(token)
    test_oidc_invalid_token()


def run_api_key() -> None:
    """Test API-key-only authentication."""
    print("=" * 60)
    print("Mode: API Key")
    print("=" * 60)

    api_key = require_env("SPICE_API_KEY")

    test_unauthenticated_endpoints_pass()
    test_no_credentials_rejected()
    test_api_key_valid(api_key)
    test_api_key_invalid()


def run_combined() -> None:
    """Test OIDC + API key authentication (either credential accepted)."""
    print("=" * 60)
    print("Mode: Combined (OIDC + API Key)")
    print("=" * 60)

    token = acquire_entra_token()
    api_key = require_env("SPICE_API_KEY")

    test_unauthenticated_endpoints_pass()
    test_no_credentials_rejected()

    # Both credential types should be accepted independently
    test_oidc_valid_token(token)
    test_api_key_valid(api_key)

    # Invalid credentials should still be rejected
    test_oidc_invalid_token()
    test_api_key_invalid()


# ── Entry point ──────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spice runtime authentication integration tests"
    )
    parser.add_argument(
        "--mode",
        choices=["oidc", "api_key", "combined"],
        required=True,
        help="Authentication mode to test",
    )
    args = parser.parse_args()

    runners = {
        "oidc": run_oidc,
        "api_key": run_api_key,
        "combined": run_combined,
    }
    runners[args.mode]()
    summarize()


if __name__ == "__main__":
    main()
