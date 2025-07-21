select
    "S_NAME",
    "S_ADDRESS"
from
    supplier,
    nation
where
        "S_SUPPKEY" in (
        select
            "PS_SUPPKEY"
        from
            partsupp
        where
                "PS_PARTKEY" in (
                select
                    "P_PARTKEY"
                from
                    part
                where
                        "P_NAME" like 'forest%'
            )
          and "PS_AVAILQTY" > (
            select
                    0.5 * sum("L_QUANTITY")
            from
                lineitem
            where
                    "L_PARTKEY" = "PS_PARTKEY"
              and "L_SUPPKEY" = "PS_SUPPKEY"
              and "L_SHIPDATE" >= date '1994-01-01'
              and "L_SHIPDATE" < date '1994-01-01' + interval '1' year
        )
    )
  and "S_NATIONKEY" = "N_NATIONKEY"
  and "N_NAME" = 'CANADA'
order by
    "S_NAME";