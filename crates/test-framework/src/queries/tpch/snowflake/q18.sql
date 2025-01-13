select
    "C_NAME",
    "C_CUSTKEY",
    "O_ORDERKEY",
    "O_ORDERDATE",
    "O_TOTALPRICE",
    sum("L_QUANTITY")
from
    customer,
    orders,
    lineitem
where
        "O_ORDERKEY" in (
        select
            "L_ORDERKEY"
        from
            lineitem
        group by
            "L_ORDERKEY" having
                sum("L_QUANTITY") > 300
    )
  and "C_CUSTKEY" = "O_CUSTKEY"
  and "O_ORDERKEY" = "L_ORDERKEY"
group by
    "C_NAME",
    "C_CUSTKEY",
    "O_ORDERKEY",
    "O_ORDERDATE",
    "O_TOTALPRICE"
order by
    "O_TOTALPRICE" desc,
    "O_ORDERDATE";