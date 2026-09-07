-- Full-text search over static nation names. 'united' matches both
-- 'UNITED KINGDOM' and 'UNITED STATES' after lower-casing and stemming.
SELECT
    n_nationkey,
    n_name,
    _score
FROM text_search(nation, 'united', n_name)
ORDER BY _score DESC, n_nationkey
LIMIT 10;
