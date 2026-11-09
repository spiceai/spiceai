-- Queries an application sends its sidecar, answered over PostgreSQL through
-- the sidecar's upstream. sidecar-cluster.yaml runs them one at a time and
-- under load; each must read data that does not change while it runs.

-- Point lookups and filters, some returning the column they filter on, and a
-- miss.
SELECT customer_id, amount, status, placed FROM orders WHERE id = 1234;
SELECT id, amount FROM orders WHERE id = 1234;
SELECT id, customer_id, amount, status FROM orders WHERE id = 1234;
SELECT customer_id, amount FROM orders WHERE customer_id = 7;
SELECT id, amount FROM orders WHERE amount > 995;
-- expect-empty
SELECT amount FROM orders WHERE id = -1;
SELECT name, region, tier FROM customers WHERE id = 42;
SELECT id, amount, status FROM orders WHERE customer_id IN (3, 7, 11) AND amount BETWEEN 100 AND 600;

-- Aggregates.
SELECT count(*) AS n, count(customer_id) AS with_customer, sum(amount) AS total, min(placed) AS first_placed, max(placed) AS last_placed FROM orders;
SELECT status, count(*) AS n, sum(amount) AS total, max(amount) AS largest FROM orders GROUP BY status;

-- Joins: unmatched and NULL keys drop out of an inner join and stay in a left join.
SELECT c.region, count(*) AS n, sum(o.amount) AS total FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.region;
SELECT count(*) AS n, count(c.id) AS matched FROM orders o LEFT JOIN customers c ON o.customer_id = c.id;

-- Top-k, with every tie broken so the order is fixed.
SELECT id, amount FROM orders ORDER BY amount DESC, id ASC LIMIT 10;
SELECT id, region FROM customers ORDER BY region ASC NULLS FIRST, id DESC LIMIT 8;

-- NULLs: counted, grouped, and false in a comparison.
SELECT count(*) AS n FROM orders WHERE customer_id IS NULL;
SELECT region, count(*) AS n FROM customers GROUP BY region;
SELECT count(*) AS n FROM customers WHERE tier <> 2;
SELECT id, coalesce(region, 'none') AS region FROM customers WHERE tier IS NULL ORDER BY id LIMIT 5;
