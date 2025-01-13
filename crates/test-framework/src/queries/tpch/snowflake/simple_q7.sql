SELECT * FROM (SELECT "O_ORDERKEY" FROM orders limit 10) AS c("KEY") limit 10;
