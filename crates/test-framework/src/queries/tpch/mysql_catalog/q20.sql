SELECT
    s_name,
    s_address
FROM
    mysql.public.supplier,
    mysql.public.nation
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            mysql.public.partsupp
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    mysql.public.part
                WHERE
                    p_name LIKE 'forest%'
            )
            AND ps_availqty > (
                SELECT
                    0.5 * SUM(l_quantity)
                FROM
                    mysql.public.lineitem
                WHERE
                    l_partkey = ps_partkey
                    AND l_suppkey = ps_suppkey
                    AND l_shipdate >= DATE '1994-01-01'
                    AND l_shipdate < DATE '1995-01-01'
            )
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY
    s_name;
