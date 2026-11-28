-- Aggregate queries over a Cayenne acceleration with maintained aggregates.
-- Those matching a maintained grouping are answered from maintained state;
-- every answer is PostgreSQL's.

-- plan-has full: MaintainedAggregateExec
-- plan-has changes: MaintainedAggregateExec
SELECT customer_id, MIN(amount) AS min_a, MAX(amount) AS max_a, SUM(amount) AS sum_a, COUNT(*) AS cnt FROM orders GROUP BY customer_id;
-- plan-has full: MaintainedAggregateExec
-- plan-has changes: MaintainedAggregateExec
SELECT customer_id, MAX(amount) AS max_a FROM orders GROUP BY customer_id;
-- plan-has full: MaintainedAggregateExec
-- plan-has changes: MaintainedAggregateExec
SELECT customer_id, AVG(amount) AS avg_a FROM orders GROUP BY customer_id;
-- The filtered grouping, with its identical predicate.
-- plan-has full: MaintainedAggregateExec
-- plan-has changes: MaintainedAggregateExec
SELECT region, SUM(amount) AS sum_a, COUNT(*) AS cnt FROM orders WHERE amount > 50 GROUP BY region;
-- Groupings nothing maintains are planned as usual and answer the same.
-- plan-lacks full: MaintainedAggregateExec
-- plan-lacks changes: MaintainedAggregateExec
SELECT region, MIN(amount) AS min_a FROM orders GROUP BY region;
-- plan-lacks full: MaintainedAggregateExec
-- plan-lacks changes: MaintainedAggregateExec
SELECT region, SUM(amount) AS sum_a FROM orders WHERE amount > 10 GROUP BY region;
SELECT customer_id, MIN(amount) AS min_a FROM orders WHERE region = 'eu' GROUP BY customer_id;
