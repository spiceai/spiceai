SELECT tenant_id, COUNT(*) AS paid_orders,
       SUM(total_cents) AS gross_cents
FROM paid_orders
GROUP BY tenant_id
ORDER BY tenant_id;
