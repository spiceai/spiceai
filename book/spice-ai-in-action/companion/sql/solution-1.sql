SELECT c.customer_id, c.customer_name,
       COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id
 AND c.tenant_id = o.tenant_id
GROUP BY c.customer_id, c.customer_name
ORDER BY c.customer_id;
