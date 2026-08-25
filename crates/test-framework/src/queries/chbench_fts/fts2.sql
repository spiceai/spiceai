-- Full-text search over static region names. 'america' matches the 'AMERICA'
-- region row.
SELECT
    r_regionkey,
    r_name,
    _score
FROM text_search(region, 'america', r_name)
ORDER BY _score DESC, r_regionkey
LIMIT 10;
