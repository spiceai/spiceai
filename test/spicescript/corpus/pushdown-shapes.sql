-- Query shapes a federated SQL source answers whole: each must reach the
-- source as one statement carrying the shape (the fragments leave out
-- identifier quoting, which differs by source), and answer as the oracle.

-- A filter, and only the columns read.
-- sends federated: > 10
-- keeps federated: big
SELECT id, i FROM t WHERE i > 10;
-- keeps federated: WHERE
-- keeps federated: big
SELECT id FROM t;
-- A limit, alone and under ORDER BY, with OFFSET.
-- sends federated: LIMIT 5
SELECT count(*) AS n FROM (SELECT id, s FROM t LIMIT 5) AS l;
-- sends federated: ORDER BY
-- sends federated: LIMIT 7
SELECT id, f FROM t ORDER BY f DESC, id LIMIT 7;
-- sends federated: LIMIT 5 OFFSET 10
SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10;
-- Aggregates.
-- sends federated: GROUP BY
-- sends federated: sum(
SELECT s, count(*) AS n, sum(i) AS si FROM t GROUP BY s;
-- sends federated: count(1)
-- keeps federated: id
SELECT count(*) AS n FROM t;
-- sends federated: DISTINCT
SELECT DISTINCT s FROM t;
-- A join of two tables of the same source, and a subquery.
-- sends federated: INNER JOIN
-- sends federated: = 'red'
SELECT t.id, u.label FROM t JOIN u ON t.id = u.t_id WHERE u.label = 'red';
-- sends federated: IN (SELECT
SELECT id FROM t WHERE id IN (SELECT t_id FROM u WHERE label = 'blue');
-- Expressions.
-- sends federated: CASE WHEN
SELECT id, CASE WHEN i > 0 THEN 'pos' WHEN i < 0 THEN 'neg' ELSE 'zero' END AS sign FROM t;
-- sends federated: LIKE 'a%'
SELECT id FROM t WHERE s LIKE 'a%';
-- sends federated: 2024-07-01
SELECT id FROM t WHERE dt >= DATE '2024-06-01' AND dt < DATE '2024-07-01';
-- sends federated: IS NULL
SELECT id FROM t WHERE i IS NULL;
-- sends federated: BETWEEN -5 AND 5
SELECT id FROM t WHERE i BETWEEN -5 AND 5 AND s IS NOT NULL;
-- A window function.
-- sends federated: row_number() OVER (PARTITION BY
SELECT id, row_number() OVER (PARTITION BY s ORDER BY id) AS rn FROM t;
