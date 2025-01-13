select
    c_count,
    count(*) as custdist
from
    (
        select
            "C_CUSTKEY" as c_custkey,
            count("O_ORDERKEY") as c_count
        from
            customer
            left outer join orders on
                        "C_CUSTKEY" = "O_CUSTKEY"
                    and "O_COMMENT" not like '%"SPECIAL"%"REQUESTS"%'
        group by
            "C_CUSTKEY"
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;