SELECT
    sum(ol_amount) AS revenue
FROM
    order_line
WHERE
    ol_delivery_d >= '1997-01-01 00:00:00'
    AND ol_delivery_d < '2030-01-01 00:00:00'
    AND ol_quantity BETWEEN 1 AND 100000;
