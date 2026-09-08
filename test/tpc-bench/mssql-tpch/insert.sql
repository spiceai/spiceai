-- Bulk Insert for nation table
BULK INSERT nation
FROM '/data/nation.tbl'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);
GO

-- Bulk Insert for region table
BULK INSERT region
FROM '/data/region.tbl'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);
GO

-- Bulk Insert for supplier table
BULK INSERT supplier
FROM '/data/supplier.tbl'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);
GO

-- Bulk Insert for customer table
BULK INSERT customer
FROM '/data/customer.tbl'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);
GO

-- Bulk Insert for part table
BULK INSERT part
FROM '/data/part.tbl'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);
GO

-- Bulk Insert for partsupp table
BULK INSERT partsupp
FROM '/data/partsupp.tbl'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);
GO

-- Bulk Insert for orders table
BULK INSERT orders
FROM '/data/orders.tbl'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);
GO

-- Bulk Insert for lineitem table
BULK INSERT lineitem
FROM '/data/lineitem.tbl'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);
GO
