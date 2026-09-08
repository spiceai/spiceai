-- Create the region table
CREATE TABLE region (
    r_regionkey INT PRIMARY KEY,
    r_name VARCHAR(25) NOT NULL,
    r_comment VARCHAR(152)
);
GO

-- Create the nation table
CREATE TABLE nation (
    n_nationkey INT PRIMARY KEY,
    n_name VARCHAR(25) NOT NULL,
    n_regionkey INT NOT NULL,
    n_comment VARCHAR(152),
    FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey)
);
GO

-- Create the supplier table
CREATE TABLE supplier (
    s_suppkey INT PRIMARY KEY,
    s_name VARCHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey INT NOT NULL,
    s_phone CHAR(15) NOT NULL,
    s_acctbal DECIMAL(15,2) NOT NULL,
    s_comment VARCHAR(101),
    FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)
);
GO

-- Create the customer table
CREATE TABLE customer (
    c_custkey INT PRIMARY KEY,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INT NOT NULL,
    c_phone CHAR(15) NOT NULL,
    c_acctbal DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment VARCHAR(117),
    FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)
);
GO

-- Create the part table
CREATE TABLE part (
    p_partkey INT PRIMARY KEY,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr CHAR(25) NOT NULL,
    p_brand CHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INT NOT NULL,
    p_container CHAR(10) NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment VARCHAR(23)
);
GO

-- Create the partsupp table
CREATE TABLE partsupp (
    ps_partkey INT NOT NULL,
    ps_suppkey INT NOT NULL,
    ps_availqty INT NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment VARCHAR(199),
    PRIMARY KEY (ps_partkey, ps_suppkey),
    FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),
    FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)
);
GO

-- Create the orders table
CREATE TABLE orders (
    o_orderkey INT PRIMARY KEY,
    o_custkey INT NOT NULL,
    o_orderstatus CHAR(1) NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk CHAR(15) NOT NULL,
    o_shippriority INT NOT NULL,
    o_comment VARCHAR(79),
    FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
);
GO

-- Create the lineitem table
CREATE TABLE lineitem (
    l_orderkey INT NOT NULL,
    l_partkey INT NOT NULL,
    l_suppkey INT NOT NULL,
    l_linenumber INT NOT NULL,
    l_quantity DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount DECIMAL(15,2) NOT NULL,
    l_tax DECIMAL(15,2) NOT NULL,
    l_returnflag CHAR(1) NOT NULL,
    l_linestatus CHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL,
    l_shipmode CHAR(10) NOT NULL,
    l_comment VARCHAR(44),
    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),
    FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey)
);
GO

-- Create indexes to improve query performance
CREATE INDEX idx_n_regionkey ON nation (n_regionkey);
CREATE INDEX idx_s_nationkey ON supplier (s_nationkey);
CREATE INDEX idx_c_nationkey ON customer (c_nationkey);
CREATE INDEX idx_ps_partkey ON partsupp (ps_partkey);
CREATE INDEX idx_ps_suppkey ON partsupp (ps_suppkey);
CREATE INDEX idx_o_custkey ON orders (o_custkey);
CREATE INDEX idx_l_partkey ON lineitem (l_partkey);
CREATE INDEX idx_l_suppkey ON lineitem (l_suppkey);
CREATE INDEX idx_l_orderkey ON lineitem (l_orderkey);
GO
