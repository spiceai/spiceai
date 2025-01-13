select
    "NATION",
    "O_YEAR",
    sum("AMOUNT") as "SUM_PROFIT"
from
    (
        select
            "N_NAME" as "NATION",
            EXTRACT(YEAR from "O_ORDERDATE") as "O_YEAR",
            "L_EXTENDEDPRICE" * (1 - "L_DISCOUNT") - "PS_SUPPLYCOST" * "L_QUANTITY" as "AMOUNT"
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
                "S_SUPPKEY" = "L_SUPPKEY"
          and "PS_SUPPKEY" = "L_SUPPKEY"
          and "PS_PARTKEY" = "L_PARTKEY"
          and "P_PARTKEY" = "L_PARTKEY"
          and "O_ORDERKEY" = "L_ORDERKEY"
          and "S_NATIONKEY" = "N_NATIONKEY"
          and "P_NAME" like '%"GREEN"%'
    ) as "PROFIT"
group by
    "NATION",
    "O_YEAR"
order by
    "NATION",
    "O_YEAR" desc;