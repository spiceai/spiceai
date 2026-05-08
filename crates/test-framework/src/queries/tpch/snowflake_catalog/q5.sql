select
    "N_NAME",
    sum("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) as "REVENUE"
from
    snowflake.TPCH_SF1.CUSTOMER,
    snowflake.TPCH_SF1.ORDERS,
    snowflake.TPCH_SF1.LINEITEM,
    snowflake.TPCH_SF1.SUPPLIER,
    snowflake.TPCH_SF1.NATION,
    snowflake.TPCH_SF1.REGION
where
        "C_CUSTKEY" = "O_CUSTKEY"
  and "L_ORDERKEY" = "O_ORDERKEY"
  and "L_SUPPKEY" = "S_SUPPKEY"
  and "C_NATIONKEY" = "S_NATIONKEY"
  and "S_NATIONKEY" = "N_NATIONKEY"
  and "N_REGIONKEY" = "R_REGIONKEY"
  and "R_NAME" = 'ASIA'
  and "O_ORDERDATE" >= date '1994-01-01'
  and "O_ORDERDATE" < date '1995-01-01'
group by
    "N_NAME"
order by
    "REVENUE" desc;