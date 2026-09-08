select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
                p_partkey = l_partkey
            and p_brand = $1
            and p_container in ($2, $3, $4, $5)
            and l_quantity >= $6 and l_quantity <= $7 + $8
            and p_size between $9 and $10
            and l_shipmode in ($11, $12)
            and l_shipinstruct = $13
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = $14
            and p_container in ($15, $16, $17, $18)
            and l_quantity >= $19 and l_quantity <= $20 + $21
            and p_size between $22 and $23
            and l_shipmode in ($24, $25)
            and l_shipinstruct = $26
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = $27
            and p_container in ($28, $29, $30, $31)
            and l_quantity >= $32 and l_quantity <= $33 + $34
            and p_size between $35 and $36
            and l_shipmode in ($37, $38)
            and l_shipinstruct = $39
        );