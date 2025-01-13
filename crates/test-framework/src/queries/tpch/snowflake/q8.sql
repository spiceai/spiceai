select
    "O_YEAR",
    sum(CASE
            WHEN "NATION" = 'BRAZIL' THEN "VOLUME"
            ELSE 0
        END) / sum("VOLUME") as "MKT_SHARE"
from
    (
        select
            EXTRACT(YEAR from "O_ORDERDATE") as "O_YEAR",
            "L_EXTENDEDPRICE" * (1 - "L_DISCOUNT") as "VOLUME",
            n2."N_NAME" as "NATION"
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
                "P_PARTKEY" = "L_PARTKEY"
          and "S_SUPPKEY" = "L_SUPPKEY"
          and "L_ORDERKEY" = "O_ORDERKEY"
          and "O_CUSTKEY" = "C_CUSTKEY"
          and "C_NATIONKEY" = n1."N_NATIONKEY"
          and n1."N_REGIONKEY" = "R_REGIONKEY"
          and "R_NAME" = 'AMERICA'
          and "S_NATIONKEY" = n2."N_NATIONKEY"
          and "O_ORDERDATE" between date '1995-01-01' and date '1996-12-31'
          and "P_TYPE" = 'ECONOMY "ANODIZED" STEEL'
    ) as "ALL_NATIONS"
group by
    "O_YEAR"
order by
    "O_YEAR";