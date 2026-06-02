SELECT
    sum(ol_amount) / 2.0 AS avg_yearly
FROM
    order_line,
    (SELECT   i_id, avg(ol_quantity) AS a
     FROM     item, order_line
     WHERE    i_data LIKE '%b'
       AND ol_i_id = i_id
     GROUP BY i_id) t
WHERE
    ol_i_id = t.i_id
    AND ol_quantity < t.a;
