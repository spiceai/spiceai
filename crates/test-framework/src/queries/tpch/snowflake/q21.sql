select
    "S_NAME",
    count(*) as "NUMWAIT"
from
    supplier,
    lineitem l1,
    orders,
    nation
where
        "S_SUPPKEY" = l1."L_SUPPKEY"
  and "O_ORDERKEY" = l1."L_ORDERKEY"
  and "O_ORDERSTATUS" = 'F'
  and l1."L_RECEIPTDATE" > l1."L_COMMITDATE"
  and exists (
        select
            *
        from
            lineitem l2
        where
                l2."L_ORDERKEY" = l1."L_ORDERKEY"
          and l2."L_SUPPKEY" <> l1."L_SUPPKEY"
    )
  and not exists (
        select
            *
        from
            lineitem l3
        where
                l3."L_ORDERKEY" = l1."L_ORDERKEY"
          and l3."L_SUPPKEY" <> l1."L_SUPPKEY"
          and l3."L_RECEIPTDATE" > l3."L_COMMITDATE"
    )
  and "S_NATIONKEY" = "N_NATIONKEY"
  and "N_NAME" = 'SAUDI ARABIA'
group by
    "S_NAME"
order by
    "NUMWAIT" desc,
    "S_NAME";
