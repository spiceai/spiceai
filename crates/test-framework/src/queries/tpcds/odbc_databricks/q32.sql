with sales_ext_discount_amt as (
   select   
      cs_item_sk as sd_item_sk,
         1.3 * avg(cs_ext_discount_amt) as sd_ext_discount_amt
   from
      catalog_sales
      ,date_dim
      ,item
   where
         cs_item_sk = i_item_sk
      and d_date between '1999-02-22' and
                        (cast('1999-02-22' as date) + INTERVAL '90 days')
      and d_date_sk = cs_sold_date_sk
   group by cs_item_sk
)
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
   ,sales_ext_discount_amt
where
i_manufact_id = 283
and i_item_sk = cs_item_sk
and sd_item_sk = i_item_sk
and d_date between '1999-02-22' and
        (cast('1999-02-22' as date) + INTERVAL '90 days')
and d_date_sk = cs_sold_date_sk
and cs_ext_discount_amt
     > sd_ext_discount_amt
 LIMIT 100;
