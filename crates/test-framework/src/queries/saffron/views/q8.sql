-- Short Code Selection Query (Views)
-- Returns Short Codes filtered by account, pool, number type, and capability
-- Used for premium messaging services

SELECT A.* FROM number_info_with_cap AS A
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberType = ?
  AND A.NumberSid NOT IN (?)
  AND A.Capability = ?
ORDER BY A.NumberSid, A.Capability
LIMIT ?;