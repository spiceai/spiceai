select
    "C_CUSTKEY",
    "C_NAME",
    sum("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) as "REVENUE",
    "C_ACCTBAL",
    "N_NAME",
    "C_ADDRESS",
    "C_PHONE",
    "C_COMMENT"
from
    customer,
    orders,
    lineitem,
    nation
where
        "C_CUSTKEY" = "O_CUSTKEY"
  and "L_ORDERKEY" = "O_ORDERKEY"
  and "O_ORDERDATE" >= date '1993-10-01'
  and "O_ORDERDATE" < date '1994-01-01'
  and "L_RETURNFLAG" = 'R'
  and "C_NATIONKEY" = "N_NATIONKEY"
group by
    "C_CUSTKEY",
    "C_NAME",
    "C_ACCTBAL",
    "C_PHONE",
    "N_NAME",
    "C_ADDRESS",
    "C_COMMENT"
order by
    "REVENUE" desc;