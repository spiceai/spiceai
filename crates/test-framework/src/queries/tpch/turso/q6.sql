select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1995-01-01'
  -- `between 0.06 - 0.01 and 0.06 + 0.01` rewritten as explicit values to avoid float arithmetic precision issue
  -- https://github.com/spiceai/spiceai/issues/9872
  and l_discount between 0.05 and 0.07
  and l_quantity < 24;
