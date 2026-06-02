SELECT
    s_i_id, sum(s_order_cnt) AS ordercount
FROM
    stock, supplier, nation
WHERE
    mod((s_w_id * s_i_id),10000) = su_suppkey
    AND su_nationkey = n_nationkey
    AND n_name = 'CHINA'
GROUP BY
    s_i_id
HAVING
    sum(s_order_cnt) >
    (SELECT sum(s_order_cnt) * .005
     FROM stock, supplier, nation
     WHERE mod((s_w_id * s_i_id),10000) = su_suppkey
       AND su_nationkey = n_nationkey
       AND n_name = 'CHINA')
ORDER BY
    ordercount DESC;
