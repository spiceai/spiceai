select
    c_count,
    count(*) as custdist
from
    (
        select
            "C_CUSTKEY" as c_custkey,
            count("O_ORDERKEY") as c_count
        from
            oracle_tpch."TPCH_SF1"."CUSTOMER"
            left outer join oracle_tpch."TPCH_SF1"."ORDERS" on
                        "C_CUSTKEY" = "O_CUSTKEY"
                    and "O_COMMENT" not like '%special%requests%'
        group by
            "C_CUSTKEY"
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;