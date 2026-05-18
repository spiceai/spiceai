SELECT
    su_nationkey AS supp_nation,
    substr(c_state,1,1) AS cust_nation,
    extract(year FROM o_entry_d) AS l_year,
    sum(ol_amount) AS revenue
FROM
    supplier, stock, order_line, orders, customer, nation n1, nation n2
WHERE
    ol_supply_w_id = s_w_id
    AND ol_i_id = s_i_id
    AND mod((s_w_id * s_i_id), 10000) = su_suppkey
    AND ol_w_id = o_w_id
    AND ol_d_id = o_d_id
    AND ol_o_id = o_id
    AND c_id = o_c_id
    AND c_w_id = o_w_id
    AND c_d_id = o_d_id
    AND su_nationkey = n1.n_nationkey
    AND ascii(substr(c_state,1,1)) - 65 = n2.n_nationkey
    AND (
        (n1.n_name = 'JAPAN' AND n2.n_name = 'CHINA')
        OR
        (n1.n_name = 'CHINA' AND n2.n_name = 'JAPAN')
    )
GROUP BY
    su_nationkey, cust_nation, l_year
ORDER BY
    su_nationkey, cust_nation, l_year;
