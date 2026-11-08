-- Aggregate queries over a Cayenne acceleration with maintained aggregates;
-- every answer is PostgreSQL's. (Whether a matching query is served from
-- maintained state: maintained-aggregates-plans.sql.)

SELECT customer_id, MIN(amount) AS min_a, MAX(amount) AS max_a, SUM(amount) AS sum_a, COUNT(*) AS cnt FROM orders GROUP BY customer_id;
SELECT customer_id, MAX(amount) AS max_a FROM orders GROUP BY customer_id;
SELECT customer_id, AVG(amount) AS avg_a FROM orders GROUP BY customer_id;
-- The filtered grouping, with its identical predicate.
SELECT region, SUM(amount) AS sum_a, COUNT(*) AS cnt FROM orders WHERE amount > 50 GROUP BY region;
-- Groupings nothing maintains are planned as usual and answer the same.
SELECT region, MIN(amount) AS min_a FROM orders GROUP BY region;
SELECT region, SUM(amount) AS sum_a FROM orders WHERE amount > 10 GROUP BY region;
SELECT customer_id, MIN(amount) AS min_a FROM orders WHERE region = 'eu' GROUP BY customer_id;
