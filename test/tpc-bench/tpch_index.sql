-- Indexes for TPC-H queries optimization

-- Primary keys
ALTER TABLE region ADD PRIMARY KEY (r_regionkey);
ALTER TABLE nation ADD PRIMARY KEY (n_nationkey);
ALTER TABLE part ADD PRIMARY KEY (p_partkey);
ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);
ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE customer ADD PRIMARY KEY (c_custkey);
ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber);
ALTER TABLE orders ADD PRIMARY KEY (o_orderkey);

-- Indexes
CREATE INDEX idx_region_name ON region(r_name);

CREATE INDEX idx_nation_region ON nation(n_regionkey);

CREATE INDEX idx_supplier_nation ON supplier(s_nationkey);
CREATE INDEX idx_supplier_suppkey ON supplier(s_suppkey);

CREATE INDEX idx_customer_nation ON customer(c_nationkey);
CREATE INDEX idx_customer_custkey ON customer(c_custkey);

CREATE INDEX idx_orders_custkey_orderdate ON orders(o_custkey, o_orderdate);
CREATE INDEX idx_orders_orderkey ON orders(o_orderkey);

CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);

CREATE INDEX idx_partsupp_supplier ON partsupp(ps_suppkey);
CREATE INDEX idx_partsupp_part ON partsupp(ps_partkey);