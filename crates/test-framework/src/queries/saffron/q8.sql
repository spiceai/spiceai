-- Short Code Selection Query
-- Returns Short Codes (NumberType='sc') filtered by account, pool, and capability
-- Used for premium messaging services

SELECT A.DateCreated, A.DateUpdated, A.AccountSid, A.NumberPoolSid, A.NumberSid, 
  A.MaxRate, A.NumberDid, A.NumberType, A.SupportedDestRegion, A.NumberRegion, 
  A.CurrentRate, A.IsAvailable, A.ProviderSid, A.AreaCodeRegion, 
  A.AvailableForNumberSelection, B.Capability 
FROM number_info AS A
INNER JOIN number_caps AS B ON A.NumberSid = B.NumberSid
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberType = 'sc'
  AND A.NumberSid NOT IN (?)
  AND B.Capability = ?
LIMIT ?;