"""Run the book's SQL examples against an already running Spice runtime."""
import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

CASES = [
    ('first-query', "SELECT tenant_id, COUNT(*) AS paid_orders, SUM(total_cents) AS gross_cents FROM paid_orders GROUP BY tenant_id ORDER BY tenant_id", [{'tenant_id':'north','paid_orders':4,'gross_cents':22200},{'tenant_id':'south','paid_orders':2,'gross_cents':24900}]),
    ('null-counts', 'SELECT COUNT(*) AS rows, COUNT(customer_id) AS known_customers, COUNT(DISTINCT customer_id) AS distinct_customers FROM orders', [{'rows':8,'known_customers':7,'distinct_customers':4}]),
    ('empty-aggregate', "SELECT COUNT(*) AS n, SUM(total_cents) AS total FROM orders WHERE status = 'missing'", [{'n':0,'total':None}]),
    ('wrong-join', "SELECT SUM(o.total_cents) AS gross_cents FROM paid_orders o JOIN order_items i ON o.order_id = i.order_id", [{'gross_cents':74600}]),
    ('fixed-join', "WITH line_totals AS (SELECT order_id, SUM(quantity * unit_price_cents) AS line_cents FROM order_items GROUP BY order_id) SELECT SUM(o.total_cents) AS gross_cents, SUM(i.line_cents) AS line_cents FROM paid_orders o JOIN line_totals i ON o.order_id = i.order_id", [{'gross_cents':47100,'line_cents':47100}]),
    ('net-revenue', "WITH refunds AS (SELECT order_id, SUM(refund_cents) AS refund_cents FROM returns GROUP BY order_id) SELECT o.tenant_id, SUM(o.total_cents) AS gross_cents, SUM(COALESCE(r.refund_cents, 0)) AS refund_cents, SUM(o.total_cents - COALESCE(r.refund_cents, 0)) AS net_cents FROM paid_orders o LEFT JOIN refunds r ON o.order_id = r.order_id GROUP BY o.tenant_id ORDER BY o.tenant_id", [{'tenant_id':'north','gross_cents':22200,'refund_cents':11200,'net_cents':11000},{'tenant_id':'south','gross_cents':24900,'refund_cents':3300,'net_cents':21600}]),
    ('customers-without-orders', 'SELECT c.customer_id FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id AND o.tenant_id = c.tenant_id) ORDER BY c.customer_id', [{'customer_id':5}]),
    ('wrong-not-in', 'SELECT customer_id FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders)', []),
    ('window', "SELECT order_id, SUM(total_cents) OVER (PARTITION BY tenant_id ORDER BY ordered_at, order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cents FROM paid_orders WHERE tenant_id = 'north' ORDER BY order_id", [{'order_id':1001,'running_cents':12500},{'order_id':1002,'running_cents':19700},{'order_id':1005,'running_cents':22200},{'order_id':1007,'running_cents':22200}]),
    ('decimal', 'SELECT CAST(SUM(total_cents) AS DECIMAL(18,2)) / 100 AS gross_dollars FROM paid_orders', None),
    ('schema', "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' ORDER BY ordinal_position", None),
    ('plan', "EXPLAIN SELECT order_id FROM orders WHERE status = 'paid' AND total_cents > 5000", None),
    ('analyze', 'EXPLAIN ANALYZE SELECT tenant_id, SUM(total_cents) FROM paid_orders GROUP BY tenant_id', None),
]

def query(base, sql, parameters=None):
    body = sql.encode() if parameters is None else json.dumps({'sql':sql,'parameters':parameters}).encode()
    req = urllib.request.Request(base + '/v1/sql', body, headers={
        'Content-Type':'text/plain' if parameters is None else 'application/json',
        'Accept':'application/json', 'Cache-Control':'no-cache'})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)

def wait_ready(base, timeout=60):
    deadline = time.monotonic() + timeout
    last = 'no response'
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(base + '/v1/ready', timeout=2) as response:
                last = response.read().decode()
                if response.status == 200:
                    query(base, 'SELECT COUNT(*) FROM daily_sales')
                    return
        except (OSError, urllib.error.URLError) as error:
            last = str(error)
        time.sleep(0.2)
    raise TimeoutError('Runtime did not become ready: ' + last)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8090')
    parser.add_argument('--output', default='verification.json')
    args = parser.parse_args()
    wait_ready(args.url)
    records = []
    for name, sql, expected in CASES:
        actual = query(args.url, sql)
        record = {'name':name, 'sql':sql, 'actual':actual, 'expected':expected}
        records.append(record)
        if expected is not None and actual != expected:
            Path(args.output).write_text(json.dumps(records, indent=2))
            raise AssertionError(record)
        print(name + ': ' + json.dumps(actual))
    try:
        actual = query(args.url, 'SELECT SUM(total_cents) AS gross_cents FROM paid_orders WHERE tenant_id = $1', ['north'])
        if actual != [{'gross_cents':22200}]:
            raise AssertionError(actual)
        records.append({'name':'parameters','actual':actual})
        print('parameters: ' + json.dumps(actual))
    except urllib.error.HTTPError as error:
        detail = error.read().decode()
        records.append({'name':'parameters','http_status':error.code,'response':detail})
        print('parameters: HTTP ' + str(error.code) + ' ' + detail)
        Path(args.output).write_text(json.dumps(records, indent=2))
        raise
    Path(args.output).write_text(json.dumps(records, indent=2))
