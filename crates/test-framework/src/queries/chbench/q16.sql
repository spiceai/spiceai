SELECT
    i_name,
    substr(i_data, 1, 3) AS brand,
    i_price,
    count(DISTINCT (mod((s_w_id * s_i_id),10000))) AS supplier_cnt
FROM
    stock, item
WHERE
    i_id = s_i_id
    AND i_data NOT LIKE 'zz%'
    AND (mod((s_w_id * s_i_id),10000) NOT IN
         (SELECT su_suppkey
          FROM supplier
          WHERE su_comment LIKE '%bad%'))
GROUP BY
    i_name, substr(i_data, 1, 3), i_price
ORDER BY
    supplier_cnt DESC;
