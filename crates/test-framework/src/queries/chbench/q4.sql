SELECT
    o_ol_cnt,
    count(*) as order_count
FROM
    oorder
WHERE exists (SELECT *
              FROM order_line
              WHERE o_id = ol_o_id
                AND o_w_id = ol_w_id
                AND o_d_id = ol_d_id
                AND ol_delivery_d >= o_entry_d)
GROUP BY o_ol_cnt
ORDER BY o_ol_cnt;