SELECT 
    (SELECT MAX(DateUpdated) FROM data_upsert) AS max_date_updated1,
    (SELECT MAX(DateUpdated) FROM data_drop) AS max_date_updated2,
    ABS(EXTRACT(EPOCH FROM ( 
        (SELECT MAX(DateUpdated) FROM data_upsert) - 
        (SELECT MAX(DateUpdated) FROM data_drop)
    ))) AS date_difference_seconds;


-- +---------------------+---------------------+-------------------------+
-- | max_date_updated1   | max_date_updated2   | date_difference_seconds |
-- +---------------------+---------------------+-------------------------+
-- | 2025-02-27T17:02:11 | 2025-02-27T16:49:45 | 746.0                   |
-- +---------------------+---------------------+-------------------------+