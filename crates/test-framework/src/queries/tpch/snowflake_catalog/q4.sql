select
    "O_ORDERPRIORITY",
    count(*) as "ORDER_COUNT"
from
    snowflake.TPCH_SF1.ORDERS
where
        "O_ORDERDATE" >= '1993-07-01'
  and "O_ORDERDATE" < date '1993-07-01' + interval '3' month
  and exists (
        select
            *
        from
            snowflake.TPCH_SF1.LINEITEM
        where
                "L_ORDERKEY" = "O_ORDERKEY"
          and "L_COMMITDATE" < "L_RECEIPTDATE"
    )
group by
    "O_ORDERPRIORITY"
order by
    "O_ORDERPRIORITY";