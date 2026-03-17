select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
    mssql.public.lineitem,
    mssql.public.part
where
        p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
            0.2 * avg(l_quantity)
    from
        mssql.public.lineitem
    where
            l_partkey = p_partkey
);