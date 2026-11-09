-- Queries over a Kafka topic and the same rows in PostgreSQL.
SELECT id FROM t WHERE i = 3;
SELECT id FROM t WHERE i > 5 AND s = 'apple';
SELECT id FROM t WHERE i < -5 OR s = 'cherry';
SELECT id FROM t WHERE s IN ('apple', 'banana');
SELECT id FROM t WHERE s IS NULL;
SELECT id FROM t WHERE f BETWEEN 10 AND 20.5;
SELECT id FROM t WHERE NOT (i = 0);
SELECT id, f FROM t ORDER BY f DESC, id LIMIT 7;
SELECT s, count(*) AS n, sum(i) AS si, min(f) AS lo, max(f) AS hi FROM t GROUP BY s;
SELECT count(*) AS n, count(s) AS ns, sum(f) AS sf FROM t;
SELECT DISTINCT s FROM t WHERE s IS NOT NULL;
SELECT id FROM t WHERE s LIKE 'b%';
