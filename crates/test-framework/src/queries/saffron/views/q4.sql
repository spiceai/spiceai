-- Area Code Random Number Selection Query (Views)
-- Executes area code random number selection queries (most selective)
-- Filters by AccountSid, NumberPoolSid, NumberRegion, AreaCodeRegion, NumberType, Capability, etc.

SELECT DateCreated, DateUpdated, AccountSid, NumberPoolSid, NumberSid, MaxRate, NumberDid, 
       NumberType, SupportedDestRegion, NumberRegion, CurrentRate, IsAvailable, ProviderSid,
       AreaCodeRegion, AvailableForNumberSelection, Capability
FROM number_info_with_cap AS A
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberRegion IN (?)
  AND A.NumberType IN (?)
  AND A.AreaCodeRegion IN (?)
  AND A.MaxRate = ?
  AND A.NumberSid NOT IN (?)
  AND A.Capability = ?
  AND (CASE WHEN ? THEN A.AvailableForNumberSelection = 1 ELSE A.AvailableForNumberSelection IN (1, 0) END)
ORDER BY A.NumberSid, A.Capability
LIMIT 1 OFFSET ?;