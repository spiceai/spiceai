"""Bounded Northstar client; callers must supply an authorized tenant context."""
import argparse
import json
import os
import urllib.request

TENANTS = frozenset({'north', 'south'})

def execute(sql, parameters, base_url=None):
    base = base_url or os.environ.get('SPICE_URL', 'http://127.0.0.1:8090')
    headers = {'Content-Type':'application/json', 'Accept':'application/json',
               'Cache-Control':'no-cache'}
    key = os.environ.get('SPICE_API_KEY')
    if key:
        headers['X-API-Key'] = key
    request = urllib.request.Request(base + '/v1/sql',
        json.dumps({'sql':sql, 'parameters':parameters}).encode('utf-8'), headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        rows = json.load(response)
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError('Unexpected SQL response shape')
    return rows

def sales(tenant, base_url=None):
    if tenant not in TENANTS:
        raise ValueError('Invalid authorized tenant context')
    rows = execute('''SELECT tenant_id, COUNT(*) AS paid_orders,
        SUM(total_cents) AS gross_cents FROM paid_orders
        WHERE tenant_id = $1 GROUP BY tenant_id''', [tenant], base_url)
    if len(rows) > 1 or any(row.get('tenant_id') != tenant for row in rows):
        raise ValueError('SQL result violated tenant contract')
    if not rows:
        return {'tenant_id':tenant, 'paid_orders':0, 'gross_cents':0,
                'currency':'USD', 'definition':'paid-gross-v1'}
    row = rows[0]
    if type(row.get('paid_orders')) is not int or type(row.get('gross_cents')) is not int:
        raise ValueError('SQL result violated numeric contract')
    return dict(row, currency='USD', definition='paid-gross-v1')

def search(tenant, question, base_url=None):
    if tenant not in TENANTS:
        raise ValueError('Invalid authorized tenant context')
    if not isinstance(question, str) or not 1 <= len(question.strip()) <= 1000:
        raise ValueError('Question must contain 1 to 1000 characters')
    base = base_url or os.environ.get('SPICE_URL', 'http://127.0.0.1:8090')
    # The predicate is chosen from trusted constants, never supplied by a caller.
    predicates = {'north': "tenant_id = 'north'", 'south': "tenant_id = 'south'"}
    payload = {'datasets':['articles'], 'text':question,
        'where':predicates[tenant], 'additional_columns':['tenant_id','title','body'],
        'limit':3}
    headers = {'Content-Type':'application/json', 'Accept':'application/json',
               'Cache-Control':'no-cache'}
    key = os.environ.get('SPICE_API_KEY')
    if key:
        headers['X-API-Key'] = key
    request = urllib.request.Request(base + '/v1/search',
        json.dumps(payload).encode('utf-8'), headers)
    with urllib.request.urlopen(request,timeout=10) as response:
        result = json.load(response)
    rows = result.get('results')
    if not isinstance(rows,list) or len(rows) > 3:
        raise ValueError('Search result violated row contract')
    evidence=[]
    for row in rows:
        data=row.get('data',{})
        article_id=row.get('primary_key',{}).get('article_id')
        if data.get('tenant_id') != tenant or row.get('dataset') != 'articles':
            raise ValueError('Search result violated tenant contract')
        if type(article_id) is not int or not isinstance(data.get('body'),str):
            raise ValueError('Search result violated evidence contract')
        evidence.append({'citation_id':'policy-'+str(article_id),
            'article_id':article_id, 'title':data['title'],
            'source_version':'fixture-2026-08', 'text':data['body']})
    return {'tenant_id':tenant, 'question':question, 'evidence':evidence}

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('tenant',choices=sorted(TENANTS))
    parser.add_argument('--url',default=None)
    parser.add_argument('--search',default=None)
    args=parser.parse_args()
    result=sales(args.tenant,args.url) if args.search is None else search(args.tenant,args.search,args.url)
    print(json.dumps(result,indent=2))
