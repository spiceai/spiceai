select
    "L_ORDERKEY",
    sum("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) as "REVENUE",
    "O_ORDERDATE",
    "O_SHIPPRIORITY"
from
    customer,
    orders,
    lineitem
where
      "C_MKTSEGMENT" = 'BUILDING'
  and "C_CUSTKEY" = "O_CUSTKEY"
  and "L_ORDERKEY" = "O_ORDERKEY"
  and "O_ORDERDATE" < date '1995-03-15'
  and "L_SHIPDATE" > date '1995-03-15'
group by
    "L_ORDERKEY",
    "O_ORDERDATE",
    "O_SHIPPRIORITY"
order by
    "REVENUE" desc,
    "O_ORDERDATE";