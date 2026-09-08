select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    hadoop.tpch.customer,
    hadoop.tpch.orders,
    hadoop.tpch.lineitem
where
        o_orderkey in (
        select
            l_orderkey
        from
            hadoop.tpch.lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 300
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate;