-- Short Code Selection Query
-- Returns Short Codes (NumberType='sc') filtered by account, pool, and capability
-- Used for premium messaging services

SELECT A.* FROM number_info_with_cap AS A
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberType = 'sc'
  AND A.NumberSid NOT IN (?)
  AND A.Capability = ?
LIMIT ?;