-- Region Random Number Selection Query (Views)
-- Executes random number selection by region (less selective than q4)
-- Filters by AccountSid, NumberPoolSid, NumberRegion, NumberType, Capability (no AreaCode)

SELECT DateCreated, DateUpdated, AccountSid, NumberPoolSid, NumberSid, 
       MaxRate, NumberDid, NumberType, SupportedDestRegion, NumberRegion, 
       CurrentRate, IsAvailable, ProviderSid, AreaCodeRegion, 
       AvailableForNumberSelection, Capability 
FROM number_info_with_cap AS A
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberRegion IN (?)
  AND A.NumberType IN (?)
  AND A.MaxRate = ?
  AND A.NumberSid NOT IN (?)
  AND (CASE WHEN ? THEN A.AvailableForNumberSelection = 1 ELSE A.AvailableForNumberSelection IN (1, 0) END)
  AND A.Capability = ?
ORDER BY A.NumberSid, A.Capability
LIMIT 1 OFFSET ?