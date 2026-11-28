-- Queries that mean the same thing in DataFusion, PostgreSQL and MySQL:
-- no string ordering or comparison that depends on collation (see
-- collation.sql), no integer division, no backslash in string literals (MySQL
-- reads it as an escape, and DataFusion's LIKE accepts no other ESCAPE), and
-- ORDER BY only on non-null unique keys.

SELECT * FROM t WHERE id = 7;
SELECT id, i FROM t WHERE i > 40;
SELECT id FROM t WHERE i <> 5;
SELECT id FROM t WHERE i BETWEEN -3 AND 3;
SELECT id FROM t WHERE i IN (1, 2, 3, 99);
SELECT id FROM t WHERE i NOT IN (1, 2, 3);
-- expect-empty
SELECT id FROM t WHERE i NOT IN (1, 2, NULL);
SELECT id FROM t WHERE i IS NULL;
SELECT id FROM t WHERE i IS NOT NULL AND d < 0;
SELECT id FROM t WHERE s IS NULL OR s = '';
SELECT id FROM t WHERE NOT (i > 10 OR i < -10);
SELECT id, i + 1, i * 2, i - big FROM t WHERE i + 1 > 40;
SELECT id, d * 2, d + 0.5, -d FROM t WHERE d * 2 > 100;
SELECT id, abs(i), i % 7 FROM t WHERE abs(i) > 45;
SELECT id, f FROM t WHERE f > 12.5;
SELECT id FROM t WHERE big > 3000000000;
SELECT id, coalesce(i, -1) FROM t WHERE coalesce(i, -1) < 0;
SELECT id, CASE WHEN i > 0 THEN 'pos' WHEN i < 0 THEN 'neg' ELSE 'zero-or-null' END FROM t;
SELECT id, upper(s), lower(s) FROM t WHERE s IS NOT NULL;
SELECT id, substring(s, 1, 3) FROM t WHERE s IS NOT NULL;
SELECT id, char_length(s) FROM t WHERE char_length(s) > 4;
SELECT id FROM t WHERE s LIKE 'O''%';
SELECT id, s FROM t WHERE s = 'O''Brien';
SELECT id FROM t WHERE dt >= DATE '2024-03-01' AND dt < DATE '2024-04-01';
SELECT id, EXTRACT(YEAR FROM dt), EXTRACT(MONTH FROM dt) FROM t WHERE id < 20;
SELECT id FROM t WHERE ts < TIMESTAMP '2024-01-05 00:00:00';
SELECT id, ts FROM t WHERE ts BETWEEN TIMESTAMP '2024-01-02 12:00:00' AND TIMESTAMP '2024-01-03 12:00:00';
SELECT count(*), count(i), count(s), sum(i), min(i), max(i) FROM t;
SELECT sum(d), min(d), max(d), sum(f), sum(big) FROM t;
SELECT avg(i), avg(d) FROM t;
SELECT count(DISTINCT i), count(DISTINCT b) FROM t;
SELECT i % 5, count(*), sum(d) FROM t GROUP BY i % 5;
SELECT b, count(*), min(dt), max(ts) FROM t GROUP BY b;
SELECT i, count(*) FROM t GROUP BY i HAVING count(*) > 2;
SELECT DISTINCT i % 4 FROM t;
SELECT id, i FROM t ORDER BY id LIMIT 10;
SELECT id, i FROM t WHERE i IS NOT NULL ORDER BY id DESC LIMIT 7 OFFSET 3;
SELECT t.id, u.label FROM t JOIN u ON t.id = u.t_id WHERE t.i > 10;
SELECT u.label, count(*), sum(t.d) FROM t JOIN u ON t.id = u.t_id GROUP BY u.label;
SELECT id FROM t WHERE id IN (SELECT t_id FROM u WHERE label = 'red');
SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.id AND u.label = 'blue');
SELECT id FROM t WHERE i > (SELECT avg(i) FROM t);
SELECT x.k, x.n FROM (SELECT i % 3 AS k, count(*) AS n FROM t GROUP BY i % 3) AS x WHERE x.n > 1;

-- Duplicate names preserve the nullability of each outer-join side.
SELECT t.id, u.id FROM t LEFT JOIN u ON t.id = u.t_id WHERE t.id < 30;
