import os
import argparse
import requests

apiHost = "https://api.github.com"
token = os.getenv("GITHUB_TOKEN")

if token == None:
    print("ERROR: Specify GITHUB_TOKEN environment variable")
    exit(1)

authHeader = { "Authorization": f"token {token}"}

# GitHub rejects a release body longer than this with a 422. Release notes are
# also read from a file rather than taken as an argument: a body this large
# exceeds MAX_ARG_STRLEN (128 KiB), so the exec fails with E2BIG before Python
# ever starts.
MAX_BODY_CHARS = 125000

parser = argparse.ArgumentParser()
parser.add_argument('--owner')
parser.add_argument('--repo')
parser.add_argument('--tag')
parser.add_argument('--release-name')
parser.add_argument('--body')
parser.add_argument('--body-file')
parser.add_argument('--prerelease')
parser.add_argument('action')
parser.add_argument('artifact', nargs='+')

def getReleaseByTag(owner, repo, tag):
    releaseInfo = requests.get(f"{apiHost}/repos/{owner}/{repo}/releases/tags/{tag}", headers=authHeader)

    if releaseInfo.status_code == 404:
        return None

    if not releaseInfo.ok:
        # Non-2xx responses (e.g. transient 5xx, 403 rate limit) return an
        # error payload that lacks the keys callers expect ("id", "assets",
        # "upload_url"). Treat as missing so callers can retry/skip cleanly
        # instead of crashing on KeyError downstream.
        print(f"Failed to fetch release for tag {tag}: HTTP {releaseInfo.status_code}")
        try:
            print(releaseInfo.json())
        except ValueError:
            print(releaseInfo.text)
        return None

    return releaseInfo.json()

def getUploadUrl(artifactPath, originalUploadUrl):
    uploadUrl = originalUploadUrl.replace("{?name,label}", "")
    filename = os.path.basename(artifactPath)
    uploadUrl = f"{uploadUrl}?name={filename}"
    return uploadUrl

def getUploadHeader(artifactPath):
    file_size = os.path.getsize(artifactPath)
    uploadHeader = dict(**authHeader)
    uploadHeader["Content-Type"] = "application/octet-stream"
    uploadHeader["Content-Length"] = str(file_size)
    return uploadHeader

def uploadArtifact(artifactPath, uploadUrl):
    uploadHeader = getUploadHeader(artifactPath)

    with open(artifactPath, 'rb') as f:
        data = f.read()

        resp = requests.post(uploadUrl, headers=uploadHeader, data=data)
        if resp.status_code != 201:
            print(resp.status_code)
            print(resp.json())
        else:
            print("Uploaded successfully!")

def createRelease(owner, repo, tag, release_name, body, prerelease):
    url = f"{apiHost}/repos/{owner}/{repo}/releases"
    data = {
        "tag_name": tag,
        "name": release_name,
        "body": body,
        "prerelease": prerelease
    }

    resp = requests.post(url, headers=authHeader, json=data)
    if not resp.ok:
        # In our matrix publish example, it is possible for a race condition in happen when creating the release - so handle it by retrying to get the release
        releaseInfo = getReleaseByTag(owner, repo, tag)
        if releaseInfo is not None:
            return releaseInfo
        
        # If we still aren't able to get the release, then print the creation error
        print(resp.status_code)
        print(resp.json())
    else:
        return resp.json()

def updateRelease(id, owner, repo, release_name, body, prerelease):
    url = f"{apiHost}/repos/{owner}/{repo}/releases/{id}"

    data = {
        "name": release_name,
        "body": body,
        "prerelease": prerelease
    }

    resp = requests.patch(url, headers=authHeader, json=data)
    if not resp.ok:
        print(resp.status_code)
        print(resp.json())


def readBody(args):
    """Resolve the release body, preferring --body-file over --body."""
    if args.body_file:
        with open(args.body_file, encoding="utf-8") as f:
            return f.read()

    return args.body

def truncateBody(body, owner, repo, tag):
    """Trim an oversized body to fit GitHub's limit, linking to the full notes."""
    if body is None or len(body) <= MAX_BODY_CHARS:
        return body

    notice = (
        "\n\n---\n\n"
        f"Release notes truncated to fit GitHub's {MAX_BODY_CHARS} character limit. "
        f"Full notes: https://github.com/{owner}/{repo}/blob/{tag}/docs/release_notes/{tag}.md\n"
    )

    kept = body[:MAX_BODY_CHARS - len(notice)]

    # Cut on a line boundary so the body never ends mid-sentence or mid-link.
    # A body with no line break in the retained prefix falls back to a word
    # break, which still drops a partial URL whole. Dropping the prefix
    # entirely would discard the whole body to satisfy the boundary.
    boundary = kept.rfind("\n")
    if boundary <= 0:
        boundary = kept.rfind(" ")
    if boundary > 0:
        kept = kept[:boundary]

    print(f"Body is {len(body)} characters, over the {MAX_BODY_CHARS} limit; truncating to {len(kept) + len(notice)}")

    return kept + notice

def extract_title_from_body(body, fallback_name):
    """Extract the first # heading from body as the release title, returning (title, remaining_body)."""
    if body:
        lines = body.splitlines()
        if lines and lines[0].startswith("# "):
            title = lines[0][2:].strip()
            remaining = "\n".join(lines[1:]).lstrip("\n")
            return title, remaining
    return fallback_name, body

def actionUpload(args):
    # Step 1: Get the release
    releaseInfo = getReleaseByTag(args.owner, args.repo, args.tag)

    is_prerelease = args.prerelease == "true"

    body = truncateBody(readBody(args), args.owner, args.repo, args.tag)
    release_name, body = extract_title_from_body(body, args.release_name)

    # Step 2: Create the release if it doesn't exist
    if releaseInfo == None:
        print("Creating release")
        releaseInfo = createRelease(args.owner, args.repo, args.tag, release_name, body, is_prerelease)
        print("Release created!")
    else:
        # Step 2.5: Update the release with the latest info
        print("Updating release")
        updateRelease(releaseInfo["id"], args.owner, args.repo, release_name, body, is_prerelease)
        print("Release updated!")

    # Step 3: Upload the release asset for each artifact
    for artifact in args.artifact:
        print(f"Uploading -> {artifact}")
        uploadUrl = getUploadUrl(artifact, releaseInfo['upload_url'])
        print(uploadUrl)

        uploadArtifact(artifact, uploadUrl)

def deleteAsset(owner, repo, id):
    url = f"{apiHost}/repos/{owner}/{repo}/releases/assets/{id}"
    resp = requests.delete(url, headers=authHeader)
    if not resp.ok:
        print(resp.status_code)
        print(resp.json())

def actionDelete(args):
    # Step 1: Get the release
    releaseInfo = getReleaseByTag(args.owner, args.repo, args.tag)

    if releaseInfo == None:
        print(f"Unable to find a release with tag: {args.tag}")
        return

    # Step 2: For each artifact, see if it already exists as an asset on the release
    assets = releaseInfo.get("assets", [])
    for artifact in args.artifact:
        filename = os.path.basename(artifact)
        print(f"Deleting -> {filename}")

        for asset in assets:
            if asset["name"] == filename:
                deleteAsset(args.owner, args.repo, asset["id"])

def main():
    args = parser.parse_args()
    print(args)

    if args.action == "upload":
        actionUpload(args)
        return
    
    if args.action == "delete":
        actionDelete(args)
        return

    print(f"Unknown action {args.action}!")
    exit(1)

if __name__ == "__main__":
    main()