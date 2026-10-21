"""Integration checks against the running local Northstar teaching service."""
import argparse,json,urllib.request,urllib.error
from pathlib import Path

def call(base,path,token=None,body=None):
    headers={}
    if token: headers['Authorization']='Bearer '+token
    data=None
    if body is not None:
        headers['Content-Type']='application/json'
        data=json.dumps(body).encode()
    req=urllib.request.Request(base+path,data,headers)
    try:
        with urllib.request.urlopen(req,timeout=15) as response:
            return response.status,json.load(response)
    except urllib.error.HTTPError as error:
        return error.code,json.load(error)

if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--url',default='http://127.0.0.1:8088')
    p.add_argument('--north-token',default='local-north-token-1234')
    p.add_argument('--south-token',default='local-south-token-5678')
    p.add_argument('--output',default='service-verification.json')
    a=p.parse_args()
    cases=[('unauthenticated','/sales',None,None,401),
           ('north-sales','/sales',a.north_token,None,200),
           ('south-sales','/sales',a.south_token,None,200),
           ('tenant-query-rejected','/sales?tenant_id=south',a.north_token,None,404),
           ('north-search','/search',a.north_token,{'question':'shipping'},200),
           ('south-search','/search',a.south_token,{'question':'shipping'},200),
           ('tenant-body-rejected','/search',a.north_token,{'question':'shipping','tenant_id':'south'},400),
           ('empty-question','/search',a.north_token,{'question':''},400)]
    records=[]
    for name,path,token,body,expected in cases:
        status,result=call(a.url,path,token,body)
        record={'name':name,'status':status,'body':result}
        records.append(record)
        Path(a.output).write_text(json.dumps(records,indent=2))
        if status != expected: raise AssertionError(record)
        if name=='north-sales' and result.get('gross_cents') != 22200: raise AssertionError(record)
        if name=='south-sales' and result.get('gross_cents') != 24900: raise AssertionError(record)
        if name=='north-search' and [x['article_id'] for x in result['evidence']] != [3]: raise AssertionError(record)
        if name=='south-search' and [x['article_id'] for x in result['evidence']] != [6]: raise AssertionError(record)
        print(json.dumps(record))
