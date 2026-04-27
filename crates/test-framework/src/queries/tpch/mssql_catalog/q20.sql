select
    s_name,
    s_address
from
    mssql.dbo.supplier,
    mssql.dbo.nation
where
        s_suppkey in (
        select
            ps_suppkey
        from
            mssql.dbo.partsupp
        where
                ps_partkey in (
                select
                    p_partkey
                from
                    mssql.dbo.part
                where
                        p_name like 'forest%'
            )
          and ps_availqty > (
            select
                    0.5 * sum(l_quantity)
            from
                mssql.dbo.lineitem
            where
                    l_partkey = ps_partkey
              and l_suppkey = ps_suppkey
              and l_shipdate >= date '1994-01-01'
              and l_shipdate < date '1994-01-01' + interval '1' year
        )
    )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
    s_name;