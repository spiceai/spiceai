SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM
    mysql.public.partsupp,
    mysql.public.supplier,
    mysql.public.nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY
    ps_partkey
HAVING
    SUM(ps_supplycost * ps_availqty) > (
        SELECT
            SUM(ps_supplycost * ps_availqty) * 0.0001
        FROM
            mysql.public.partsupp,
            mysql.public.supplier,
            mysql.public.nation
        WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY'
    )
ORDER BY
    value DESC;
