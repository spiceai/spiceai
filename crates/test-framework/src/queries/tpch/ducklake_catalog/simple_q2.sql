SELECT l_linenumber FROM ducklake.main.lineitem WHERE l_linenumber = (SELECT MAX(l_linenumber) FROM ducklake.main.lineitem);
