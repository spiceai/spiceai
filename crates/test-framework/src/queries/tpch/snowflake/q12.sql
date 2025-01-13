select
    "L_SHIPMODE",
    sum(CASE
            WHEN "O_ORDERPRIORITY" = '1-URGENT'
                or "O_ORDERPRIORITY" = '2-HIGH'
                THEN 1
            ELSE 0
        END) as "HIGH_LINE_COUNT",
    sum(CASE
            WHEN "O_ORDERPRIORITY" <> '1-URGENT'
                and "O_ORDERPRIORITY" <> '2-HIGH'
                THEN 1
            ELSE 0
        END) as "LOW_LINE_COUNT"
from
    lineitem
        join
    orders
    on
            "L_ORDERKEY" = "O_ORDERKEY"
where
        "L_SHIPMODE" in ('MAIL', 'SHIP')
  and "L_COMMITDATE" < "L_RECEIPTDATE"
  and "L_SHIPDATE" < "L_COMMITDATE"
  and "L_RECEIPTDATE" >= date '1994-01-01'
  and "L_RECEIPTDATE" < date '1995-01-01'
group by
    "L_SHIPMODE"
order by
    "L_SHIPMODE";