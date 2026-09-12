"""Local teaching service. Deploy behind a production HTTP/authentication stack."""
import hmac
import json
import os
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from app_client import sales, search

keys={os.environ['NORTHSTAR_NORTH_TOKEN']:'north',
      os.environ['NORTHSTAR_SOUTH_TOKEN']:'south'}
if len(keys) != 2 or any(len(key) < 16 for key in keys):
    raise ValueError('Supply two different demo tokens of at least 16 characters')

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # Do not place user questions or credentials in the teaching server log.
        return

    def reply(self, status, body):
        data=json.dumps(body).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(data)))
        self.send_header('Cache-Control','no-store')
        self.end_headers()
        self.wfile.write(data)

    def tenant(self):
        header=self.headers.get('Authorization','')
        if not header.startswith('Bearer '):
            return None
        candidate=header[7:].encode('utf-8')
        for key, tenant in keys.items():
            if hmac.compare_digest(candidate,key.encode('utf-8')):
                return tenant
        return None

    def do_GET(self):
        tenant=self.tenant()
        if tenant is None:
            self.reply(401,{'error':'unauthorized'}); return
        parsed=urlsplit(self.path)
        if parsed.path != '/sales' or parsed.query:
            self.reply(404,{'error':'not_found'}); return
        try:
            self.reply(200,sales(tenant))
        except (OSError,ValueError,urllib.error.URLError):
            self.reply(502,{'error':'data_unavailable'})

    def do_POST(self):
        tenant=self.tenant()
        if tenant is None:
            self.reply(401,{'error':'unauthorized'}); return
        if self.path != '/search':
            self.reply(404,{'error':'not_found'}); return
        try:
            length=int(self.headers.get('Content-Length','0'))
            if not 0 < length <= 8192:
                self.reply(413,{'error':'invalid_body_size'}); return
            if self.headers.get_content_type() != 'application/json':
                self.reply(415,{'error':'expected_json'}); return
            body=json.loads(self.rfile.read(length))
            if not isinstance(body,dict) or set(body) != {'question'}:
                raise ValueError('Expected only question')
            question=body['question']
            if not isinstance(question,str) or not 1 <= len(question.strip()) <= 1000:
                raise ValueError('Invalid question')
        except (ValueError,UnicodeError):
            self.reply(400,{'error':'invalid_request'}); return
        try:
            self.reply(200,search(tenant,question))
        except (OSError,ValueError,urllib.error.URLError):
            self.reply(502,{'error':'search_unavailable'})

if __name__ == '__main__':
    server=ThreadingHTTPServer(('127.0.0.1',int(os.environ.get('NORTHSTAR_PORT','8088'))),Handler)
    print('Northstar teaching service listening on',server.server_address,flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
