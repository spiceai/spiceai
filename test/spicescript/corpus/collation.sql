-- String comparison, grouping and ordering where a source's default collation
-- decides the answer: MySQL's default (utf8mb4_0900_ai_ci) is case- and
-- accent-insensitive, the postgres image's default (en_US.utf8) orders
-- linguistically, and DataFusion compares bytes.

SELECT id FROM t WHERE s = 'apple';
SELECT id FROM t WHERE s LIKE 'a%';
SELECT id FROM t WHERE s IN ('APPLE', 'banana');
SELECT s, count(*) FROM t WHERE s IS NOT NULL GROUP BY s;
SELECT count(DISTINCT s) FROM t;
SELECT min(s), max(s) FROM t;
SELECT id FROM t WHERE s > 'b';
