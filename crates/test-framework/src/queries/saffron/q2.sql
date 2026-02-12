-- Any Capable Random Number Selection Query
-- Executes random number selection queries
-- Filters by AccountSid, NumberPoolSid, NumberType, Capability

SELECT A.DateCreated, A.DateUpdated, A.AccountSid, A.NumberPoolSid, A.NumberSid, 
  A.MaxRate, A.NumberDid, A.NumberType, A.SupportedDestRegion, A.NumberRegion, 
  A.CurrentRate, A.IsAvailable, A.ProviderSid, A.AreaCodeRegion, 
  A.AvailableForNumberSelection, B.Capability
FROM number_info AS A
INNER JOIN number_caps AS B ON A.NumberSid = B.NumberSid
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberType IN (?)
  AND A.MaxRate = ?
  AND A.NumberSid NOT IN (?)
  AND (CASE WHEN ? THEN A.AvailableForNumberSelection = 1 ELSE A.AvailableForNumberSelection IN (1, 0) END)
  AND B.Capability = ?
ORDER BY A.NumberSid, B.Capability
LIMIT 1 OFFSET ?