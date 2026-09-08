with sales_ext_discount_amt as (
   select
      ws_item_sk as sd_item_sk,
      1.3 * avg(ws_ext_discount_amt) as sd_ext_discount_amt
   from
      web_sales
      ,date_dim
      ,item
   where
         ws_item_sk = i_item_sk
      and d_date between '2001-01-25' and
                        (cast('2001-01-25' as date) + INTERVAL '90 days')
      and d_date_sk = ws_sold_date_sk
   group by ws_item_sk
)
select
   sum(ws_ext_discount_amt)  as "Excess Discount Amount"
from
    web_sales
   ,item
   ,date_dim
   ,sales_ext_discount_amt
where
i_manufact_id = 914
and i_item_sk = ws_item_sk
and sd_item_sk = i_item_sk
and d_date between '2001-01-25' and
        (cast('2001-01-25' as date) + INTERVAL '90 days')
and d_date_sk = ws_sold_date_sk
and ws_ext_discount_amt
     > sd_ext_discount_amt
order by sum(ws_ext_discount_amt)
 LIMIT 100;
