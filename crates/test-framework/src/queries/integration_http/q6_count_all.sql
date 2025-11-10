-- Test 6: Count all records from multiple endpoints
-- Count all rows across datasets
SELECT count(_path) as row_count 
FROM httpbin_json
