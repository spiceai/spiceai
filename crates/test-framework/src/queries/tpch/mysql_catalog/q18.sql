SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity)
FROM
    mysql.tpch_sf1.customer,
    mysql.tpch_sf1.orders,
    mysql.tpch_sf1.lineitem
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            mysql.tpch_sf1.lineitem
        GROUP BY
            l_orderkey
        HAVING
            SUM(l_quantity) > 300
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
