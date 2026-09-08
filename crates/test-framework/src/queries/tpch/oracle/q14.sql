select
    100.00 * sum(CASE
                    WHEN "P_TYPE" like 'PROMO%'
                        THEN "L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")
                    ELSE 0
                END) / sum("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) as promo_revenue
from
    lineitem,
    part
where
        "L_PARTKEY" = "P_PARTKEY"
  and "L_SHIPDATE" >= date '1995-09-01'
  and "L_SHIPDATE" < date '1995-10-01';