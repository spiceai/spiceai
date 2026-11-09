WITH events AS (
  SELECT tenant_id,
         CAST(ordered_at AS DATE) AS event_date,
         total_cents AS signed_cents
  FROM paid_orders
  UNION ALL
  SELECT o.tenant_id,
         CAST(r.returned_at AS DATE) AS event_date,
         -r.refund_cents AS signed_cents
  FROM returns r
  JOIN orders o ON r.order_id = o.order_id
  WHERE o.status = 'paid'
)
SELECT tenant_id, event_date,
       SUM(signed_cents) AS net_event_cents
FROM events
GROUP BY tenant_id, event_date
ORDER BY tenant_id, event_date;
