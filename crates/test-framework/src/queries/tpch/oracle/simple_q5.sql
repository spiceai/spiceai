select SUM("O"."O_TOTALPRICE") as "TOTAL_PRICE", "L"."L_LINESTATUS" from orders "O" JOIN lineitem "L" ON "O"."O_ORDERKEY" = "L"."L_ORDERKEY" group by "L"."L_LINESTATUS" order by "TOTAL_PRICE" desc;
