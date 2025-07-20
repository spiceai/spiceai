select
    "CNTRYCODE",
    count(*) as "NUMCUST",
    sum("C_ACCTBAL") as "TOTACCTBAL"
from
    (
        select
            SUBSTRING("C_PHONE" from 1 for 2) as "CNTRYCODE",
            "C_ACCTBAL"
        from
            customer
        where
                SUBSTRING("C_PHONE" from 1 for 2) in
                ('13', '31', '23', '29', '30', '18', '17')
          and "C_ACCTBAL" > (
            select
                avg("C_ACCTBAL")
            from
                customer
            where
                    "C_ACCTBAL" > 0.00
              and SUBSTRING("C_PHONE" from 1 for 2) in
                  ('13', '31', '23', '29', '30', '18', '17')
        )
          and not exists (
                select
                    *
                from
                    orders
                where
                        "O_CUSTKEY" = "C_CUSTKEY"
            )
    ) as "CUSTSALE"
group by
    "CNTRYCODE"
order by
    "CNTRYCODE";