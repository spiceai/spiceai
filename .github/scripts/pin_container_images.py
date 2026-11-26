#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""Read the digest each container image tag names, and record it in test/container_images.json.

The integration tests and spicescript start a container only from an image pinned in
test/container_images.json, which maps each `registry/repository:tag` they use to the
digest that tag named when it was recorded. This reads that digest from the image's
registry, anonymously, through the registry API (`GET /v2/<repository>/manifests/<tag>`),
so no credential on this machine decides it. For a multi-platform image the digest is the
image index's, so each host still pulls its own platform's image.

Usage:
  .github/scripts/pin_container_images.py IMAGE [IMAGE...]          # print each digest
  .github/scripts/pin_container_images.py --write IMAGE [IMAGE...]  # and record it
  .github/scripts/pin_container_images.py --refresh                 # report pins whose tag moved

A digest is checked against the bytes the registry served before it is printed or
recorded. Pure stdlib; no third-party deps.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PINS = ROOT / "test" / "container_images.json"

INDEX_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)
MANIFEST_TYPES = (
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)
ACCEPT = ", ".join(INDEX_TYPES + MANIFEST_TYPES)

# The reference grammar Docker uses (distribution/reference).
PATH = re.compile(r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*")
DOMAIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*(?::[0-9]+)?")
TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
# Docker Hub serves its registry API from a host other than its name.
API_HOSTS = {"docker.io": "registry-1.docker.io"}


class Error(Exception):
    pass


def canonical(image: str) -> str:
    """`image` as `registry/repository:tag`, the form test/container_images.json keys on:
    `postgres:18` is `docker.io/library/postgres:18`."""
    first, slash, rest = image.partition("/")
    # A first component that is not a host is part of a Docker Hub repository name.
    if slash and ("." in first or ":" in first or first == "localhost"):
        domain, name = first, rest
    else:
        domain, name = "docker.io", image
    path, colon, tag = name.rpartition(":") if ":" in name else (name, "", "")
    if "@" in image or not DOMAIN.fullmatch(domain) or not PATH.fullmatch(path):
        raise Error(f"{image!r} is not an image reference with a tag")
    if not colon or not TAG.fullmatch(tag):
        raise Error(f"{image} names no tag; name a release tag to pin")
    # As crates/container-images refuses it: `latest`, or a tag with it as a word (`2022-latest`).
    if "latest" in re.split(r"[-._]", tag.lower()):
        raise Error(f"{image} names `{tag}`, a tag that moves to each new release; name a release tag to pin")
    if domain in ("index.docker.io", "registry-1.docker.io"):
        domain = "docker.io"
    if domain == "docker.io" and "/" not in path:
        path = f"library/{path}"
    return f"{domain}/{path}:{tag}"


def bearer_challenge(header: str) -> dict[str, str]:
    """The parameters of a `WWW-Authenticate: Bearer realm=...,service=...,scope=...`."""
    scheme, _, params = header.partition(" ")
    if scheme.lower() != "bearer":
        raise Error(f"the registry asks for {scheme} authentication, not an anonymous token")
    return dict(re.findall(r'(\w+)="([^"]*)"', params))


def http_get(url: str, headers: dict[str, str], attempts: int = 5):
    """`url`'s response, retried with backoff while the registry answers 429 or 5xx."""
    for attempt in range(1, attempts + 1):
        try:
            return urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60)
        except urllib.error.HTTPError as e:
            if attempt == attempts or not (e.code == 429 or e.code >= 500):
                raise
            time.sleep(min(2**attempt, 30))
    raise AssertionError("unreachable")


def anonymous_token(challenge: dict[str, str], repository: str) -> str:
    realm = challenge.get("realm")
    if not realm or not realm.startswith("https://"):
        raise Error(f"the registry's token realm {realm!r} is not an https URL")
    query = {"scope": challenge.get("scope") or f"repository:{repository}:pull"}
    if "service" in challenge:
        query["service"] = challenge["service"]
    with http_get(f"{realm}?{urllib.parse.urlencode(query)}", {}) as response:
        body = json.load(response)
    token = body.get("token") or body.get("access_token")
    if not token:
        raise Error(f"{realm} issued no anonymous token")
    return token


def read_manifest(reference: str) -> tuple[str, str, bytes]:
    """The digest, media type and bytes of the manifest `reference` (canonical) names."""
    name, tag = reference.rsplit(":", 1)
    domain, repository = name.split("/", 1)
    url = f"https://{API_HOSTS.get(domain, domain)}/v2/{repository}/manifests/{tag}"
    headers = {"Accept": ACCEPT}
    try:
        response = http_get(url, headers)
    except urllib.error.HTTPError as e:
        if e.code != 401 or "WWW-Authenticate" not in e.headers:
            raise Error(f"{url} answered {e.code} {e.reason}") from e
        token = anonymous_token(bearer_challenge(e.headers["WWW-Authenticate"]), repository)
        try:
            response = http_get(url, {**headers, "Authorization": f"Bearer {token}"})
        except urllib.error.HTTPError as e2:
            raise Error(f"{url} answered {e2.code} {e2.reason} to an anonymous token") from e2
    with response:
        body = response.read()
        media_type = response.headers.get("Content-Type", "").split(";")[0].strip()
        served = response.headers.get("Docker-Content-Digest")
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    if served and served != digest:
        raise Error(f"{url} says its digest is {served}, but the bytes it served hash to {digest}")
    if not media_type:
        media_type = json.loads(body).get("mediaType", "")
    if media_type not in INDEX_TYPES + MANIFEST_TYPES:
        raise Error(f"{url} served a {media_type or 'untyped'} document, not an image manifest or index")
    return digest, media_type, body


def platforms(media_type: str, body: bytes) -> list[str]:
    if media_type not in INDEX_TYPES:
        return ["single image (not an index)"]
    found = []
    for manifest in json.loads(body).get("manifests", []):
        platform = manifest.get("platform") or {}
        if platform.get("os") in (None, "unknown"):
            continue  # an attestation, not an image
        found.append("/".join(filter(None, (platform.get("os"), platform.get("architecture"), platform.get("variant")))))
    return found


def load_pins() -> dict:
    return json.loads(PINS.read_text())


def write_pins(pins: dict) -> None:
    pins["images"] = dict(sorted(pins["images"].items()))
    PINS.write_text(json.dumps(pins, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("images", nargs="*")
    parser.add_argument("--write", action="store_true", help=f"record each digest in {PINS.relative_to(ROOT)}")
    parser.add_argument("--refresh", action="store_true", help="re-read every pinned tag and report the ones that moved")
    args = parser.parse_args()
    if args.refresh == bool(args.images):
        parser.error("name images, or pass --refresh")
    pins = load_pins()
    failed = False
    references = list(pins["images"]) if args.refresh else args.images
    for image in references:
        try:
            reference = canonical(image)
            digest, media_type, body = read_manifest(reference)
        except (Error, OSError, ValueError) as e:
            print(f"error: {image}: {e}", file=sys.stderr)
            failed = True
            continue
        recorded = pins["images"].get(reference)
        if args.refresh:
            if recorded != digest:
                print(f"{reference}: pinned {recorded}, the tag now names {digest}")
            continue
        print(f"{reference} {digest} ({', '.join(platforms(media_type, body))})")
        if args.write:
            pins["images"][reference] = digest
    if args.write:
        write_pins(pins)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
