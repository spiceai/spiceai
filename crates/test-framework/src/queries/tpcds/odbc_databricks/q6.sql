with category_averages as (
	select i_category
		  ,avg(i_current_price) as avg_price
	from item
	group by i_category
)
select  a.ca_state state, count(*) cnt
 from customer_address a
     ,customer c
     ,store_sales s
     ,date_dim d
     ,item i
	 ,category_averages ca
 where       a.ca_address_sk = c.c_current_addr_sk
 	and c.c_customer_sk = s.ss_customer_sk
 	and s.ss_sold_date_sk = d.d_date_sk
 	and s.ss_item_sk = i.i_item_sk
	and i.i_category = ca.i_category
 	and d.d_month_seq =
 	     (select distinct (d_month_seq)
 	      from date_dim
               where d_year = 1998
 	        and d_moy = 3 )
 	and i.i_current_price > 1.2 * ca.avg_price
 group by a.ca_state
 having count(*) >= 10
 order by cnt, a.ca_state
  LIMIT 100;
