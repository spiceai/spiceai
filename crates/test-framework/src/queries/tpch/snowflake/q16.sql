select
    "P_BRAND",
    "P_TYPE",
    "P_SIZE",
    count(distinct "PS_SUPPKEY") as "SUPPLIER_CNT"
from
    partsupp,
    part
where
        "P_PARTKEY" = "PS_PARTKEY"
  and "P_BRAND" <> 'Brand#45'
  and "P_TYPE" not like 'MEDIUM "POLISHED"%'
  and "P_SIZE" in (49, 14, 23, 45, 19, 3, 36, 9)
  and "PS_SUPPKEY" not in (
    select
        "S_SUPPKEY"
    from
        supplier
    where
        "S_COMMENT" like '%"CUSTOMER"%"COMPLAINTS"%'
)
group by
    "P_BRAND",
    "P_TYPE",
    "P_SIZE"
order by
    "SUPPLIER_CNT" desc,
    "P_BRAND",
    "P_TYPE",
    "P_SIZE";