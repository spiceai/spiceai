select
    "S_ACCTBAL",
    "S_NAME",
    "N_NAME",
    "P_PARTKEY",
    "P_MFGR",
    "S_ADDRESS",
    "S_PHONE",
    "S_COMMENT"
from
    snowflake.TPCH_SF1.PART,
    snowflake.TPCH_SF1.SUPPLIER,
    snowflake.TPCH_SF1.PARTSUPP,
    snowflake.TPCH_SF1.NATION,
    snowflake.TPCH_SF1.REGION
where
        "P_PARTKEY" = "PS_PARTKEY"
  and "S_SUPPKEY" = "PS_SUPPKEY"
  and "P_SIZE" = 15
  and "P_TYPE" like '%BRASS'
  and "S_NATIONKEY" = "N_NATIONKEY"
  and "N_REGIONKEY" = "R_REGIONKEY"
  and "R_NAME" = 'EUROPE'
  and "PS_SUPPLYCOST" = (
    select
        min("PS_SUPPLYCOST")
    from
        snowflake.TPCH_SF1.PARTSUPP,
        snowflake.TPCH_SF1.SUPPLIER,
        snowflake.TPCH_SF1.NATION,
        snowflake.TPCH_SF1.REGION
    where
            "P_PARTKEY" = "PS_PARTKEY"
      and "S_SUPPKEY" = "PS_SUPPKEY"
      and "S_NATIONKEY" = "N_NATIONKEY"
      and "N_REGIONKEY" = "R_REGIONKEY"
      and "R_NAME" = 'EUROPE'
)
order by
    "S_ACCTBAL" desc,
    "N_NAME",
    "S_NAME",
    "P_PARTKEY"
limit 100;