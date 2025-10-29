-- Any Capable Random Number Selection Query
-- Executes random number selection queries
-- Filters by AccountSid, NumberPoolSid, NumberType, Capability

SELECT DateCreated, DateUpdated, AccountSid, NumberPoolSid, NumberSid, 
       MaxRate, NumberDid, NumberType, SupportedDestRegion, NumberRegion, 
       CurrentRate, IsAvailable, ProviderSid, AreaCodeRegion, 
       AvailableForNumberSelection, Capability 
FROM number_info_with_cap AS A
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberType IN (?)
  AND A.MaxRate = ?
  AND A.NumberSid NOT IN (?)
  AND (CASE WHEN ? THEN A.AvailableForNumberSelection = 1 ELSE A.AvailableForNumberSelection IN (1, 0) END)
  AND A.Capability = ?
ORDER BY A.NumberSid
LIMIT 1 OFFSET ?