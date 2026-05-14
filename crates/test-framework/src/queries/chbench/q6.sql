select
    sum(ol_amount) as revenue
from
    order_line
where
    ol_delivery_d >= '1997-01-01 00:00:00'
    and ol_delivery_d < '2030-01-01 00:00:00'
    and ol_quantity between 1 and 100000;
