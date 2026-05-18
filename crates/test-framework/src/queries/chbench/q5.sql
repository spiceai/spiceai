SELECT
    n_name,
    sum(ol_amount) AS revenue
FROM
    customer, 
    orders,
    order_line,
    stock,
    supplier,
    nation,
    region
WHERE
    c_id = o_c_id
    AND c_w_id = o_w_id
    AND c_d_id = o_d_id
    AND ol_o_id = o_id
    AND ol_w_id = o_w_id
    AND ol_d_id = o_d_id
    AND ol_w_id = s_w_id
    AND ol_i_id = s_i_id
    AND mod((s_w_id * s_i_id),10000) = su_suppkey
    AND ascii(substr(c_state,1,1)) - 65 = su_nationkey
    AND su_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND o_entry_d >= '2007-01-02 00:00:00.000000'
GROUP BY n_name
ORDER BY revenue DESC;
