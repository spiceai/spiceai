select
    sum("L_EXTENDEDPRICE" * "L_DISCOUNT") as "REVENUE"
from
    lineitem
where
        "L_SHIPDATE" >= date '1994-01-01'
  and "L_SHIPDATE" < date '1995-01-01'
  and "L_DISCOUNT" between 0.06 - 0.01 and 0.06 + 0.01
  and "L_QUANTITY" < 24;