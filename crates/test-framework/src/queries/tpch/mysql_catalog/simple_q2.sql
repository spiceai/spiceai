SELECT l_linenumber FROM mysql.tpch_sf1.lineitem WHERE l_linenumber = (SELECT MAX(l_linenumber) FROM mysql.tpch_sf1.lineitem);
