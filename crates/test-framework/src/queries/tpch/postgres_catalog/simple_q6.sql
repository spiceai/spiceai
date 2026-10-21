SELECT * FROM (SELECT o_orderkey + 1 FROM pg.public.orders) AS c(key) LIMIT 10;
