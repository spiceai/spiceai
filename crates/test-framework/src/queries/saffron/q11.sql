-- Number Lookup by Identity/DID Query
-- Returns number records from number_info table by NumberDid
-- Used for identity-based number resolution

SELECT A.* FROM number_info AS A 
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberDid = ?;