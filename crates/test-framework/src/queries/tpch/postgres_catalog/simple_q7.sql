SELECT * FROM (SELECT o_orderkey FROM pg.public.orders LIMIT 10) AS c(key) LIMIT 10;
