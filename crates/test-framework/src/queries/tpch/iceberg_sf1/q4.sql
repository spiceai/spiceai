select
    o_orderpriority,
    count(*) as order_count
from
    ice.tpch_sf1.orders
where
        o_orderdate >= '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
        select
            *
        from
            ice.tpch_sf1.lineitem
        where
                l_orderkey = o_orderkey
          and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;