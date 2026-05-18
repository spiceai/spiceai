SELECT
    su_name,
    su_address
FROM
    supplier,
    nation
WHERE
    su_suppkey IN
    (SELECT mod(s_i_id * s_w_id, 10000)
     FROM stock
     INNER JOIN item ON i_id = s_i_id
     INNER JOIN order_line ON ol_i_id = s_i_id
     WHERE ol_delivery_d > '2010-05-23 12:00:00'
       AND i_data LIKE 'co%'
     GROUP BY s_i_id, s_w_id, s_quantity
     HAVING 2*s_quantity > sum(ol_quantity))
    AND su_nationkey = n_nationkey
    AND n_name = 'CHINA'
ORDER BY
    su_name;
