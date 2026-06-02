select
        sum("L_EXTENDEDPRICE") / 7.0 as "AVG_YEARLY"
from
    oracle_tpch."TPCH_SF1"."LINEITEM",
    oracle_tpch."TPCH_SF1"."PART"
where
        "P_PARTKEY" = "L_PARTKEY"
  and "P_BRAND" = 'Brand#23'
  and "P_CONTAINER" = 'MED BOX'
  and "L_QUANTITY" < (
    select
            0.2 * avg("L_QUANTITY")
    from
        oracle_tpch."TPCH_SF1"."LINEITEM"
    where
            "L_PARTKEY" = oracle_tpch."TPCH_SF1"."PART"."P_PARTKEY"
);