select
    "SUPP_NATION",
    "CUST_NATION",
    "L_YEAR",
    sum("VOLUME") as "REVENUE"
from
    (
        select
            n1."N_NAME" as "SUPP_NATION",
            n2."N_NAME" as "CUST_NATION",
            EXTRACT(YEAR from "L_SHIPDATE") as "L_YEAR",
            "L_EXTENDEDPRICE" * (1 - "L_DISCOUNT") as "VOLUME"
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
                "S_SUPPKEY" = "L_SUPPKEY"
          and "O_ORDERKEY" = "L_ORDERKEY"
          and "C_CUSTKEY" = "O_CUSTKEY"
          and "S_NATIONKEY" = n1."N_NATIONKEY"
          and "C_NATIONKEY" = n2."N_NATIONKEY"
          and (
                (n1."N_NAME" = 'FRANCE' and n2."N_NAME" = 'GERMANY')
                or (n1."N_NAME" = 'GERMANY' and n2."N_NAME" = 'FRANCE')
            )
          and "L_SHIPDATE" between date '1995-01-01' and date '1996-12-31'
    ) as "SHIPPING"
group by
    "SUPP_NATION",
    "CUST_NATION",
    "L_YEAR"
order by
    "SUPP_NATION",
    "CUST_NATION",
    "L_YEAR";
