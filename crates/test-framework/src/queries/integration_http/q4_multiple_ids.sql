-- Status code endpoints
SELECT _path, content 
FROM httpbin_status_200
WHERE _path LIKE '/status/%'
