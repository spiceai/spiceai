-- Queries a cluster distributes: scans, filters, aggregates, joins and sorts.
SELECT id, i, d, s, dt FROM t;
SELECT count(*) AS n, sum(i) AS si, sum(d) AS sd, min(dt) AS lo, max(dt) AS hi FROM t;
SELECT s, count(*) AS n, avg(d) AS ad FROM t GROUP BY s;
SELECT id FROM t WHERE i > 40 AND s = 'apple';
SELECT t.id, u.label FROM t JOIN u ON t.id = u.t_id WHERE u.label = 'red';
SELECT u.label, count(*) AS n, sum(t.d) AS sd FROM t JOIN u ON t.id = u.t_id GROUP BY u.label;
SELECT id, d FROM t ORDER BY d DESC NULLS LAST, id LIMIT 10;
SELECT dt, count(*) AS n FROM t GROUP BY dt ORDER BY n DESC, dt LIMIT 5;
SELECT count(DISTINCT s) AS ns, count(DISTINCT dt) AS nd FROM t;
SELECT id FROM t WHERE id IN (SELECT t_id FROM u WHERE label = 'blue');
