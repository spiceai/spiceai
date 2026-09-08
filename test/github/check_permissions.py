#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# SPDX-License-Identifier: Apache-2.0
"""Check the live GitHub token against the connector's stargazers selection."""

import argparse
from email.utils import parsedate_to_datetime
import json
import math
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# Registration requests every field, even when the later SQL projects fewer.
QUERY = """{
  repository(owner: "spiceai", name: "spiceai") {
    stargazers(first: 1) {
      edges {
        starredAt
        node { login name avatarUrl bio location company email twitterUsername }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}"""
HELP = (
    "Check GITHUB_TOKEN and the job's explicit contents: write permission. "
    "OAuth/classic personal tokens also need read:user or user:email. "
    "See https://github.com/spiceai/spiceai/blob/trunk/test/github/README.md"
)


def check(endpoint: str, token: str, artifacts: Path):
    artifacts.mkdir(parents=True, exist_ok=True)
    observations = []
    deadline = time.monotonic() + 60
    token = token.strip()
    try:
        if not token:
            raise RuntimeError(f"GITHUB_TOKEN is missing. {HELP}")
        if not token.isascii() or any(
            char.isspace() or not char.isprintable() for char in token
        ):
            raise RuntimeError(
                "GITHUB_TOKEN contains invalid whitespace or control characters"
            )
        for attempt in range(3):
            request = Request(
                endpoint,
                data=json.dumps({"query": QUERY}).encode(),
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "User-Agent": "spice-github-connector-test",
                },
            )
            retryable = False
            rate_limited = False
            headers = {}
            try:
                with urlopen(
                    request, timeout=min(15, deadline - time.monotonic())
                ) as response:
                    headers = response.headers
                    body = json.load(response)
                errors = json.loads(
                    json.dumps(body.get("errors") or []).replace(token, "[REDACTED]")
                )
                repository = (body.get("data") or {}).get("repository") or {}
                edges = (repository.get("stargazers") or {}).get("edges") or []
                observation = {
                    "attempt": attempt + 1,
                    "errors": errors,
                    "rows": len(edges),
                }
                observations.append(observation)
                if (
                    not errors
                    and len(edges) == 1
                    and edges[0].get("node", {}).get("login")
                ):
                    print(
                        "PASS GITHUB_TOKEN: full stargazers selection returned one row"
                    )
                    return
                rate_limited = bool(errors) and all(
                    error.get("type") == "RATE_LIMITED" for error in errors
                )
                retryable = rate_limited
            except HTTPError as error:
                headers = error.headers
                observation = {"attempt": attempt + 1, "http_status": error.code}
                try:
                    payload = json.loads(error.read(65536))
                    message = (
                        payload.get("message", "") if isinstance(payload, dict) else ""
                    )
                    observation["message"] = str(message).replace(token, "[REDACTED]")
                except (ValueError, TimeoutError):
                    observation["message"] = "GitHub returned no readable JSON error"
                observations.append(observation)
                error.close()
                rate_limited = error.code == 429 or (
                    error.code == 403
                    and (
                        "Retry-After" in headers
                        or headers.get("x-ratelimit-remaining") == "0"
                        or "rate limit" in observation["message"].lower()
                    )
                )
                retryable = rate_limited or error.code in (500, 502, 503, 504)
            except (URLError, TimeoutError) as error:
                observation = {
                    "attempt": attempt + 1,
                    "transport_error": str(error.reason)
                    if isinstance(error, URLError)
                    else str(error),
                }
                observations.append(observation)
                retryable = True
            except (ValueError, AttributeError, TypeError) as error:
                observations.append(
                    {"attempt": attempt + 1, "invalid_response": str(error)}
                )
                raise RuntimeError(f"Invalid GitHub API response: {error}") from error

            if not retryable or attempt == 2:
                raise RuntimeError(
                    f"GitHub permission check failed: {observation}. {HELP}"
                )
            delay = 2**attempt
            # GitHub requires at least a minute when a secondary limit gives
            # neither Retry-After nor an exhausted primary quota's reset time.
            # That exceeds this preflight's budget, so fail without retrying early.
            if (
                rate_limited
                and "Retry-After" not in headers
                and headers.get("x-ratelimit-remaining") != "0"
            ):
                delay = 60
            try:
                retry_after = headers.get("Retry-After", "0")
                try:
                    requested_delay = float(retry_after)
                except ValueError:
                    requested_delay = (
                        parsedate_to_datetime(retry_after).timestamp() - time.time()
                    )
                if not math.isfinite(requested_delay):
                    raise ValueError("non-finite Retry-After")
                delay = max(delay, requested_delay)
                if headers.get("x-ratelimit-remaining") == "0":
                    reset = float(headers["x-ratelimit-reset"])
                    if not math.isfinite(reset):
                        raise ValueError("non-finite x-ratelimit-reset")
                    delay = max(delay, reset - time.time())
            except (ValueError, TypeError, KeyError, OverflowError) as error:
                raise RuntimeError(
                    f"Invalid GitHub rate-limit headers: {error}"
                ) from error
            if delay + 15 >= deadline - time.monotonic():
                raise RuntimeError(
                    f"GitHub rate limit exceeds the 60-second permission-check budget: {observation}"
                )
            time.sleep(delay)
    except RuntimeError as error:
        message = str(error).replace(token, "[REDACTED]") if token else str(error)
        raise RuntimeError(message) from None
    finally:
        # Record errors and counts, never tokens or users' profile fields.
        diagnostic = json.dumps(observations, indent=2)
        if token:
            diagnostic = diagnostic.replace(token, "[REDACTED]")
        (artifacts / "permissions.json").write_text(diagnostic)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="https://api.github.com/graphql")
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    try:
        check(args.endpoint, os.environ.get("GITHUB_TOKEN", ""), args.artifacts)
    except RuntimeError as error:
        parser.exit(1, f"{error}\n")
