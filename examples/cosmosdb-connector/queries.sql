-- Azure Cosmos DB connector: example queries for the spicepod in this directory.
-- Replace column names to match your container's schema.

-- Count rows across the full container.
SELECT COUNT(*) AS total FROM products;

-- Preview the first few documents.
SELECT * FROM products LIMIT 10;

-- Project a single column across all rows.
SELECT id, name, price FROM products ORDER BY price DESC LIMIT 10;

-- Simple aggregation.
SELECT
  category,
  COUNT(*)      AS count,
  AVG(price)    AS avg_price,
  MAX(price)    AS max_price
FROM products
GROUP BY category
ORDER BY count DESC;

-- Custom-query dataset.
SELECT COUNT(*) AS active_orders FROM active_orders;

-- Join across two Cosmos-backed datasets. Spice federates the join in the
-- local DataFusion engine — Cosmos DB itself does not support joins across
-- containers.
SELECT
  o.id     AS order_id,
  p.name   AS product_name,
  p.price  AS unit_price
FROM active_orders o
JOIN products p ON o.product_id = p.id
LIMIT 50;
