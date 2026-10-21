SELECT
    c_count, count(*) AS custdist
FROM
    (SELECT c_id, count(o_id) AS c_count
     FROM customer LEFT OUTER JOIN oorder ON (
         c_w_id = o_w_id
         AND c_d_id = o_d_id
         AND c_id = o_c_id
         AND o_carrier_id > 8)
     GROUP BY c_id) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC, c_count DESC;
