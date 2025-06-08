SELECT
    SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM
    glue.tpch.lineitem,
    glue.tpch.part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * AVG(l_quantity)
        FROM
            glue.tpch.lineitem
        WHERE
            l_partkey = p_partkey
    );
