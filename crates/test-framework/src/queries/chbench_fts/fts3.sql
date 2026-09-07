-- Full-text search over the CDC-mutated customer.c_data. The payment
-- transaction prepends a space-separated prefix
--   | {c_id} {c_d_id} {c_w_id} {d_id} {w_id} ${h_amount} {unix_secs}
-- to bad-credit customers, so the searchable tokens are the numeric ids/amounts
-- it adds. '5' matches the district / warehouse id fields of that prefix. This
-- query returns rows only as the OLTP workload rewrites c_data over the CDC
-- changes stream, so it exercises full-text index freshness under mutation.
SELECT
    c_w_id,
    c_d_id,
    c_id,
    _score
FROM text_search(customer, '5', c_data)
ORDER BY _score DESC, c_w_id, c_d_id, c_id
LIMIT 20;
