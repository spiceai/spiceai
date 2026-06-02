select
    "PS_PARTKEY",
    sum("PS_SUPPLYCOST" * "PS_AVAILQTY") as "VALUE"
from
    oracle_tpch."TPCH_SF1"."PARTSUPP",
    oracle_tpch."TPCH_SF1"."SUPPLIER",
    oracle_tpch."TPCH_SF1"."NATION"
where
    "PS_SUPPKEY" = "S_SUPPKEY"
  and "S_NATIONKEY" = "N_NATIONKEY"
  and "N_NAME" = 'GERMANY'
group by
    "PS_PARTKEY" having
    sum("PS_SUPPLYCOST" * "PS_AVAILQTY") > (
    select
    sum("PS_SUPPLYCOST" * "PS_AVAILQTY") * 0.0001
    from
    oracle_tpch."TPCH_SF1"."PARTSUPP",
    oracle_tpch."TPCH_SF1"."SUPPLIER",
    oracle_tpch."TPCH_SF1"."NATION"
    where
    "PS_SUPPKEY" = "S_SUPPKEY"
                  and "S_NATIONKEY" = "N_NATIONKEY"
                  and "N_NAME" = 'GERMANY'
    )
order by
    "VALUE" desc;