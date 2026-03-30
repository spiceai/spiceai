SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    mysql.public.orders
WHERE
    o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-10-01'
    AND EXISTS (
        SELECT
            *
        FROM
            mysql.public.lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;
