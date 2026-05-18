SELECT
    c_id,
    c_last,
    sum(ol_amount) AS revenue,
    c_city,
    c_phone,
    n_name
FROM
    customer, orders, order_line, nation
WHERE
    c_id = o_c_id
    AND c_w_id = o_w_id
    AND c_d_id = o_d_id
    AND ol_w_id = o_w_id
    AND ol_d_id = o_d_id
    AND ol_o_id = o_id
    AND o_entry_d >= '2007-01-02 00:00:00.000000'
    AND o_entry_d <= ol_delivery_d
    AND n_nationkey = ascii(substr(c_state,1,1)) - 65
GROUP BY
    c_id, c_last, c_city, c_phone, n_name
ORDER BY
    revenue DESC;
