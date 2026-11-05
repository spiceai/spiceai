"""Writes a GitHub App installation token to GITHUB_OUTPUT. Key Vault signs the app JWT."""

import base64
import hashlib
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5
TIMEOUT_SECONDS = 30


def fail(message):
    print(f"::error::{message}", flush=True)
    sys.exit(1)


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def request(service, url, body=None, headers=None):
    """Returns the parsed JSON response. Retries only network, rate-limit and server failures."""
    for attempt in range(1, ATTEMPTS + 1):
        req = urllib.request.Request(url, data=body, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as response:
                return json.load(response)
        except urllib.error.HTTPError as err:
            if err.code != 429 and err.code < 500:
                fail(f"{service} refused the request: HTTP {err.code}: {error_detail(err)}")
            failure = f"HTTP {err.code}"
        except (OSError, http.client.HTTPException) as err:
            failure = str(err)
        print(f"{service} attempt {attempt} of {ATTEMPTS} failed: {failure}", flush=True)
        if attempt < ATTEMPTS:
            time.sleep(RETRY_DELAY_SECONDS)
    fail(f"{service} did not answer after {ATTEMPTS} attempts.")


def error_detail(err):
    try:
        body = json.load(err)
    except ValueError:
        return "no details"
    detail = body.get("error_description") or body.get("message") or body.get("error")
    if isinstance(detail, dict):
        detail = detail.get("message")
    return str(detail).splitlines()[0] if detail else "no details"


def github_oidc_token():
    url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
    bearer = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if not url or not bearer:
        fail(
            "No GitHub OIDC token is available. The job needs 'permissions: id-token: write'; "
            "pull requests from forks never get one."
        )
    query = urllib.parse.urlencode({"audience": "api://AzureADTokenExchange"})
    response = request("GitHub OIDC", f"{url}&{query}", headers={"Authorization": f"Bearer {bearer}"})
    return response["value"]


def vault_access_token():
    tenant = os.environ["AZURE_TENANT_ID"]
    form = urllib.parse.urlencode(
        {
            "client_id": os.environ["AZURE_CLIENT_ID"],
            "scope": "https://vault.azure.net/.default",
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": github_oidc_token(),
        }
    ).encode()
    url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    return request("Entra", url, body=form)["access_token"]


def app_jwt():
    now = int(time.time())
    header = b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    claims = {"iat": now - 60, "exp": now + 540, "iss": os.environ["APP_ID"]}
    signing_input = f"{header}.{b64url(json.dumps(claims).encode())}"
    digest = b64url(hashlib.sha256(signing_input.encode()).digest())
    response = request(
        "Key Vault",
        f"{os.environ['KEY_ID'].rstrip('/')}/sign?api-version=7.4",
        body=json.dumps({"alg": "RS256", "value": digest}).encode(),
        headers={
            "Authorization": f"Bearer {vault_access_token()}",
            "Content-Type": "application/json",
        },
    )
    return f"{signing_input}.{response['value'].rstrip('=')}"


def installation_token():
    installation = os.environ["INSTALLATION_ID"]
    body = {
        "repositories": os.environ["REPOSITORIES"].split(),
        "permissions": json.loads(os.environ["PERMISSIONS"]),
    }
    response = request(
        "GitHub",
        f"https://api.github.com/app/installations/{installation}/access_tokens",
        body=json.dumps(body).encode(),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {app_jwt()}",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    return response["token"]


def main():
    token = installation_token()
    print(f"::add-mask::{token}", flush=True)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"token={token}\n")


if __name__ == "__main__":
    main()
