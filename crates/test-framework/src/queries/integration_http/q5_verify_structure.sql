-- Verify JSON content structure
SELECT _path, 
       content
FROM httpbin_json
WHERE content IS NOT NULL
