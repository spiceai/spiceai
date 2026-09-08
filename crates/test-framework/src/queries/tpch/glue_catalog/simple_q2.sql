SELECT l_linenumber FROM glue.tpch.lineitem WHERE l_linenumber = (SELECT MAX(l_linenumber) FROM glue.tpch.lineitem);
