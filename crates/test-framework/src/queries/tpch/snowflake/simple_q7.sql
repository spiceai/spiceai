SELECT * FROM (SELECT "O_ORDERKEY" as "key" FROM orders limit 10) AS c("KEY") limit 10;
