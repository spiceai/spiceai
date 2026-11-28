-- Queries over a MongoDB collection and the same rows in PostgreSQL.
SELECT _id, i, s, f, b FROM t;
SELECT _id FROM t WHERE i = 3;
SELECT _id FROM t WHERE i > 5 AND s = 'apple';
SELECT _id FROM t WHERE i < -5 OR s = 'cherry';
SELECT _id FROM t WHERE s IN ('apple', 'banana');
SELECT _id FROM t WHERE s IS NULL;
SELECT _id FROM t WHERE s IS NOT NULL AND b;
SELECT _id FROM t WHERE f BETWEEN 10 AND 20.5;
SELECT _id FROM t WHERE NOT (i = 0);
SELECT _id, f FROM t ORDER BY f DESC, _id LIMIT 7;
SELECT s, count(*) AS n, sum(i) AS si, min(f) AS lo, max(f) AS hi FROM t GROUP BY s;
SELECT count(*) AS n, count(s) AS ns, sum(f) AS sf FROM t;
SELECT DISTINCT s FROM t WHERE s IS NOT NULL;
SELECT _id FROM t WHERE s LIKE 'b%';
