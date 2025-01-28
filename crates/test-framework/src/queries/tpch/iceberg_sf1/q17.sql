select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
    ice.tpch_sf1.lineitem,
    ice.tpch_sf1.part
where
        p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
            0.2 * avg(l_quantity)
    from
        ice.tpch_sf1.lineitem
    where
            l_partkey = p_partkey
);