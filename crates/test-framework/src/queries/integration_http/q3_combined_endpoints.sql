-- Multiple status codes combined
SELECT _path, content 
FROM httpbin_status_200
UNION ALL
SELECT _path, content 
FROM httpbin_status_201
