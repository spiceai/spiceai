select
        sum(l_extendedprice) / $1 as avg_yearly
from
    lineitem,
    part
where
        p_partkey = l_partkey
  and p_brand = $2
  and p_container = $3
  and l_quantity < (
    select
            $4 * avg(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
);