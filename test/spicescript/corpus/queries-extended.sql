-- Queries beyond the basic pushdown corpus, over the same tables t and u, that
-- mean the same in DataFusion and PostgreSQL: every ORDER BY that decides the
-- answer ends on a unique key with NULL placement stated, and no string is
-- ordered or compared in a way that depends on collation. Where Spice's
-- semantics differ from PostgreSQL's by design (Spark's NULL-propagating
-- concat, truncating casts) the oracle is written to Spice's. A query known to
-- fail somewhere names those places (a variant, `key=value` on the matrix,
-- joined by `&`); the finding behind each is in the PR or issue tracker.

-- Window functions
SELECT id, i, row_number() OVER (ORDER BY id) AS rn FROM t;
SELECT id, sum(d) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving FROM t;
SELECT id, b, rank() OVER (PARTITION BY b ORDER BY i NULLS LAST, id) AS r FROM t;
SELECT id, dense_rank() OVER (ORDER BY i % 5 NULLS FIRST) AS dr FROM t;
SELECT id, lag(i) OVER (ORDER BY id) AS prev, lead(d, 2) OVER (ORDER BY id) AS next2 FROM t;
SELECT id, b, first_value(d) OVER (PARTITION BY b ORDER BY id) AS first_d, last_value(d) OVER (PARTITION BY b ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_d FROM t;
SELECT id, count(*) OVER (PARTITION BY i % 4) AS n, avg(f) OVER (PARTITION BY i % 4) AS mean FROM t;
SELECT id, ntile(4) OVER (ORDER BY id) AS q FROM t;

-- Common table expressions
WITH x AS (SELECT i % 3 AS k, d FROM t WHERE i IS NOT NULL) SELECT k, count(*), sum(d) FROM x GROUP BY k;
WITH a AS (SELECT id, i FROM t WHERE i > 0), c AS (SELECT t_id, count(*) AS n FROM u GROUP BY t_id) SELECT a.id, a.i, c.n FROM a JOIN c ON a.id = c.t_id;

-- Set operations
SELECT id FROM t WHERE i > 30 UNION SELECT t_id FROM u WHERE label = 'red';
SELECT id FROM t WHERE i > 30 UNION ALL SELECT t_id FROM u WHERE label = 'red';
SELECT id FROM t WHERE i > 0 INTERSECT SELECT t_id FROM u;
SELECT id FROM t WHERE id < 100 EXCEPT SELECT t_id FROM u;

-- Joins
SELECT t.id, u.label FROM t LEFT JOIN u ON t.id = u.t_id WHERE t.id < 60;
SELECT t.id, u.id AS uid, u.label FROM u RIGHT JOIN t ON t.id = u.t_id WHERE t.id < 40;
SELECT t.id, u.id AS uid FROM t FULL OUTER JOIN u ON t.id = u.t_id WHERE coalesce(t.id, 0) < 30 OR u.id < 10;
SELECT a.id, b.id AS other FROM t a JOIN t b ON a.i = b.i AND a.id < b.id WHERE a.id < 40;
SELECT x.label, x.n, y.total FROM (SELECT label, count(*) AS n FROM u GROUP BY label) x JOIN (SELECT u.label, sum(t.d) AS total FROM t JOIN u ON t.id = u.t_id GROUP BY u.label) y ON x.label = y.label;
SELECT t.id, u.label FROM t JOIN u ON t.id = u.t_id AND u.label <> 'blue' WHERE t.b;
SELECT v.x, v.y, t.d FROM (VALUES (3, 'a'), (17, 'b'), (999, 'c')) AS v(x, y) LEFT JOIN t ON t.id = v.x;

-- Grouping
SELECT b, count(*) FILTER (WHERE i > 0) AS pos, count(*) FILTER (WHERE i < 0) AS neg FROM t GROUP BY b;
SELECT b, i % 2 AS parity, count(*) FROM t GROUP BY ROLLUP (b, i % 2);
SELECT b, i % 3 AS k, count(*), sum(d) FROM t GROUP BY GROUPING SETS ((b), (i % 3), ());
SELECT dt, count(*) FROM t GROUP BY dt HAVING count(*) > 1;

-- Casts
SELECT id, CAST(i AS VARCHAR) AS si, CAST(dt AS VARCHAR) AS sdt FROM t WHERE id < 30;
SELECT id, CAST(ts AS DATE) AS day FROM t WHERE id < 30;
SELECT id, CAST(f AS DECIMAL(10, 2)) AS fd FROM t WHERE id < 30;
-- oracle-sql: SELECT id, CAST(trunc(d) AS INTEGER) AS di FROM t WHERE id < 50
SELECT id, CAST(d AS INTEGER) AS di FROM t WHERE id < 50;
SELECT id FROM t WHERE dt = CAST('2024-05-01' AS DATE) OR dt = DATE '2024-06-15';
SELECT id, CAST(big AS DOUBLE PRECISION) / 3 AS third FROM t WHERE id < 20;

-- Dates and times
SELECT id, date_trunc('month', ts) AS month FROM t WHERE id < 40;
SELECT date_trunc('week', ts) AS week, count(*) FROM t GROUP BY date_trunc('week', ts);
SELECT id, dt + INTERVAL '1 day' AS next_day, ts - INTERVAL '2 hours' AS earlier FROM t WHERE id < 30;
SELECT id, EXTRACT(DOW FROM dt) AS dow, EXTRACT(DOY FROM dt) AS doy, EXTRACT(QUARTER FROM dt) AS q FROM t WHERE id < 40;
SELECT id FROM t WHERE ts >= TIMESTAMP '2024-06-01 00:00:00' AND ts < TIMESTAMP '2024-06-01 00:00:00' + INTERVAL '10 days';
SELECT EXTRACT(MONTH FROM dt) AS m, count(*), min(ts), max(ts) FROM t GROUP BY EXTRACT(MONTH FROM dt);

-- Strings
-- oracle-sql: SELECT id, s || '-' || id AS c1, s || '!' AS c2 FROM t WHERE id < 40
SELECT id, concat(s, '-', id) AS c1, s || '!' AS c2 FROM t WHERE id < 40;
SELECT id, char_length(s) AS chars, octet_length(s) AS bytes FROM t WHERE s IS NOT NULL;
SELECT id, trim(BOTH 'a' FROM s) AS t1, replace(s, 'a', 'A') AS r1 FROM t WHERE s IS NOT NULL;
SELECT id, position('a' IN s) AS p, left(s, 2) AS l2, right(s, 2) AS r2 FROM t WHERE s IS NOT NULL;
SELECT id, lpad(s, 8, '*') AS padded, rpad(s, 8, '.') AS rpadded FROM t WHERE s IS NOT NULL AND char_length(s) < 8;
SELECT id FROM t WHERE s ILIKE 'apple';
SELECT id FROM t WHERE s NOT LIKE '%a%';
SELECT id, upper(s) = 'APPLE' AS is_apple FROM t WHERE s IS NOT NULL;
SELECT id, starts_with(s, 'ap') AS sw FROM t WHERE s IS NOT NULL;

-- NULL handling
SELECT id, nullif(i, 0) AS ni, coalesce(s, 'none') AS cs FROM t;
SELECT id FROM t WHERE i IS DISTINCT FROM 5;
SELECT id FROM t WHERE i IS NOT DISTINCT FROM NULL;
SELECT id, greatest(i, 0) AS g, least(i, 0) AS l FROM t;
SELECT count(*), count(i), count(DISTINCT i), sum(CASE WHEN i IS NULL THEN 1 ELSE 0 END) FROM t;

-- Aggregates
SELECT b, stddev_samp(f), var_pop(d) FROM t GROUP BY b;
SELECT bool_and(b), bool_or(b), bool_and(i > -100) FROM t;
SELECT label, string_agg(CAST(t_id AS VARCHAR), ',' ORDER BY id) FROM u GROUP BY label;
SELECT b, min(s), max(s) FROM t WHERE s IN ('apple', 'banana') GROUP BY b;
SELECT count(*), sum(big), avg(big), min(dt), max(dt), min(ts), max(ts) FROM t;

-- Ordering and limits
SELECT id, i FROM t ORDER BY i NULLS FIRST, id LIMIT 15;
SELECT id, d FROM t ORDER BY d DESC, id LIMIT 5 OFFSET 10;
SELECT id, i FROM t ORDER BY i DESC NULLS LAST, id DESC LIMIT 12;

-- Arithmetic
SELECT id, i / 3 AS q, i % 7 AS m, -i % 7 AS nm FROM t WHERE i IS NOT NULL;
SELECT id, 1.0 * i / 7 AS ratio FROM t WHERE i IS NOT NULL AND id < 50;
SELECT id, d * d AS sq, d / 4 AS quarter FROM t WHERE id < 50;
SELECT id, big * 2 AS doubled, big - id AS diff FROM t WHERE id < 50;
SELECT id, abs(d), round(d, 1), floor(f), ceil(f) FROM t WHERE id < 50;

-- Subqueries
SELECT id, (SELECT count(*) FROM u WHERE u.t_id = t.id) AS refs FROM t WHERE id < 40;
SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.t_id = t.id) AND id < 60;
SELECT id FROM t WHERE i IN (SELECT i FROM t WHERE b) AND id < 100;
-- expect-empty
SELECT id FROM t WHERE id NOT IN (SELECT i FROM t);
SELECT id FROM t WHERE id NOT IN (SELECT i FROM t WHERE i IS NOT NULL) AND id < 80;
SELECT id, d FROM t WHERE d > (SELECT avg(d) FROM t WHERE b) AND id < 100;
SELECT k, n FROM (SELECT i % 5 AS k, count(*) AS n FROM t GROUP BY i % 5) AS g WHERE n > 10;

-- Subquery shapes the optimizer prunes or rewrites before the SQL is sent
SELECT count(*) FROM (SELECT b, count(*) AS n FROM t GROUP BY b) g;
SELECT count(*) FROM (SELECT i % 5 AS k FROM t GROUP BY i % 5) g;
SELECT count(*) FROM (SELECT DISTINCT b FROM t) g;
SELECT count(*) FROM (SELECT id FROM t ORDER BY id LIMIT 7) g;
SELECT count(*) FROM (SELECT id FROM t WHERE i > 0 UNION ALL SELECT t_id FROM u) g;
SELECT count(*) FROM (SELECT id FROM t UNION SELECT t_id FROM u) g;
SELECT count(*) FROM (SELECT t.id FROM t JOIN u ON t.id = u.t_id) g;
SELECT count(*) FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM t) g WHERE rn <= 10;
SELECT k FROM (SELECT 1 AS k, sum(d) AS s FROM t) g;
SELECT id FROM t WHERE EXISTS (SELECT max(t_id) FROM u WHERE u.label = 'none') AND id < 5;
SELECT id, (SELECT max(t_id) FROM u) AS top FROM t WHERE id < 5;
SELECT g.b, g.n FROM (SELECT b, count(*) AS n, sum(d) AS s FROM t GROUP BY b) g WHERE g.n > 0;
SELECT count(*) FROM (SELECT b FROM (SELECT b, i FROM t WHERE i > 0) x GROUP BY b) y;
SELECT count(*), sum(n) FROM (SELECT t_id, count(*) AS n FROM u GROUP BY t_id HAVING count(*) > 1) g;
