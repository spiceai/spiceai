# Controlled two-bucket replication for snapshots-cross-bucket-replication.yaml.
# The metadata and archives retain the writer's URIs, as with mc mirror.
cat > /tmp/${sources.control.namespace}-snapshot-mirror.py <<'PY'
import datetime
import hashlib
import hmac
import json
from pathlib import Path
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError

endpoint = '${sources.lake.endpoint}'.replace('127.0.0.1', '${sources.control.host_from_container}')
origin = '${sources.lake.shared_bucket}'
target = '${sources.lake.bucket}'
key = '${sources.lake.key}'
secret = '${sources.lake.secret}'
metadata_key = 'state/snapshots/metadata.json'
marker = Path('/tmp/${sources.control.namespace}-mirror-digest')
import sys
operation = sys.argv[1] if len(sys.argv) > 1 else 'copy'

# AWS Signature Version 4, using only the Python image's standard library.
# Explicit path-style requests keep this independent of the engine's S3 client.
def request(method, bucket, object_key, body=b''):
    now = datetime.datetime.now(datetime.timezone.utc)
    timestamp = now.strftime('%Y%m%dT%H%M%SZ')
    day = now.strftime('%Y%m%d')
    path = '/' + quote(bucket + '/' + object_key, safe='/')
    digest = hashlib.sha256(body).hexdigest()
    host = urlsplit(endpoint).netloc
    headers = {'host': host, 'x-amz-content-sha256': digest, 'x-amz-date': timestamp}
    signed = ';'.join(sorted(headers))
    canonical_headers = ''.join(name + ':' + headers[name] + '\n' for name in sorted(headers))
    canonical = '\n'.join([method, path, '', canonical_headers, signed, digest])
    scope = day + '/us-east-1/s3/aws4_request'
    to_sign = '\n'.join(['AWS4-HMAC-SHA256', timestamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    signing_key = ('AWS4' + secret).encode()
    for part in [day, 'us-east-1', 's3', 'aws4_request']:
        signing_key = hmac.new(signing_key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing_key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers['Authorization'] = 'AWS4-HMAC-SHA256 Credential=' + key + '/' + scope + ', SignedHeaders=' + signed + ', Signature=' + signature
    req = Request(endpoint + path, data=body if method == 'PUT' else None, headers=headers, method=method)
    with urlopen(req, timeout=30) as response:
        return response.read()

metadata = request('GET', target if operation == 'remove-origin' else origin, metadata_key)
parsed = json.loads(metadata)
base = 's3://' + origin + '/'
archives = []
for name in ['modules', 'configuration_addresses']:
    table = parsed[name]
    current = table['current-snapshot-id']
    entry = next(e for e in table['snapshots'] if e['snapshot-id'] == current)
    uri = entry['snapshot']
    if not uri.startswith(base):
        raise ValueError('snapshot does not belong to the isolated writer bucket')
    archives.append((uri[len(base):], entry['snapshot-checksum']))

if operation == 'remove-origin':
    for path, _ in archives:
        request('DELETE', origin, path)
        try:
            request('GET', origin, path)
        except HTTPError as error:
            if error.code != 404:
                raise
        else:
            raise ValueError('origin archive still exists after deletion: ' + path)
    print('removed current writer archives; reader must restore its target copies')
else:
    digest = hashlib.sha256(metadata).hexdigest()
    if '${matrix.arrival}' == 'metadata-first' and (not marker.exists() or marker.read_text() != digest):
        # Copy the exact bytes: locations deliberately still name the writer bucket.
        request('PUT', target, metadata_key, metadata)
        marker.write_text(digest)
        print('replicated metadata before its archives')
    else:
        for path, expected in archives:
            data = request('GET', origin, path)
            actual = hashlib.sha256(data).hexdigest()
            if actual != expected:
                raise ValueError('writer archive checksum differs: ' + path)
            request('PUT', target, path, data)
            if hashlib.sha256(request('GET', target, path)).hexdigest() != actual:
                raise ValueError('replicated archive checksum differs: ' + path)
        request('PUT', target, metadata_key, metadata)
        print('replicated both current archives and byte-identical metadata')
PY
python3 /tmp/${sources.control.namespace}-snapshot-mirror.py
