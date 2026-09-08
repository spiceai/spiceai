select
        sum("L_EXTENDEDPRICE") / 7.0 as "AVG_YEARLY"
from
    lineitem,
    part
where
        "P_PARTKEY" = "L_PARTKEY"
  and "P_BRAND" = 'Brand#23'
  and "P_CONTAINER" = 'MED BOX'
  and "L_QUANTITY" < (
    select
            0.2 * avg("L_QUANTITY")
    from
        lineitem
    where
            "L_PARTKEY" = part."P_PARTKEY"
);