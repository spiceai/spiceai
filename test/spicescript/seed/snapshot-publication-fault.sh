# Fault injection for snapshots-reader-publication-failures.yaml. Runs inside
# its Python control container. Only the test's isolated snapshot metadata is
# changed; the writer must be stopped first. A second invocation repairs it.
python3 - <<'PY'
import copy
import datetime
import hashlib
import hmac
import json
from pathlib import Path
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

endpoint = '${sources.lake.endpoint}'.replace('127.0.0.1', '${sources.control.host_from_container}')
bucket = '${sources.lake.shared_bucket}'
key = '${sources.lake.key}'
secret = '${sources.lake.secret}'
metadata_key = 'state/snapshots/metadata.json'
backup = Path('/tmp/${sources.control.namespace}.json')
fault = '${matrix.fault}'

# AWS Signature Version 4, using only the Python image's standard library.
# Explicit path-style requests keep this independent of the engine's S3 client.
def request(method, object_key, body=b''):
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

if backup.exists():
    request('PUT', metadata_key, backup.read_bytes())
    backup.unlink()
    print('restored snapshot metadata')
else:
    original = request('GET', metadata_key)
    metadata = json.loads(original)
    dataset = metadata['t']
    current = dataset['current-snapshot-id']
    entry = copy.deepcopy(next(e for e in dataset['snapshots'] if e['snapshot-id'] == current))
    if fault == 'rollback':
        older = [e['snapshot-id'] for e in dataset['snapshots'] if e['snapshot-id'] < current]
        if not older:
            raise RuntimeError('rollback requires a previously loaded newer snapshot')
        dataset['current-snapshot-id'] = max(older)
    else:
        entry['snapshot-id'] = current + 1
        if fault == 'checksum':
            entry['snapshot-checksum'] = '0' * 64
        elif fault == 'missing':
            entry['snapshot'] += '.missing'
        else:
            raise ValueError('unknown fault: ' + fault)
        dataset['snapshots'].append(entry)
        dataset['current-snapshot-id'] = entry['snapshot-id']
    backup.write_bytes(original)
    request('PUT', metadata_key, json.dumps(metadata).encode())
    print('injected ' + fault + ' after snapshot ' + str(current))
PY
