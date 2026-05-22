-- Area Code Random Number Selection Query
-- Executes area code random number selection queries (most selective)
-- Filters by AccountSid, NumberPoolSid, NumberRegion, AreaCodeRegion, NumberType, Capability, etc.

SELECT A.DateCreated, A.DateUpdated, A.AccountSid, A.NumberPoolSid, A.NumberSid, A.MaxRate, A.NumberDid, 
  A.NumberType, A.SupportedDestRegion, A.NumberRegion, A.CurrentRate, A.IsAvailable, A.ProviderSid,
  A.AreaCodeRegion, A.AvailableForNumberSelection, B.Capability
FROM number_info AS A
INNER JOIN number_caps AS B ON A.NumberSid = B.NumberSid
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberRegion IN (?)
  AND A.NumberType IN (?)
  AND A.AreaCodeRegion IN (?)
  AND A.MaxRate = ?
  AND A.NumberSid NOT IN (?)
  AND B.Capability = ?
  AND (CASE WHEN ? THEN A.AvailableForNumberSelection = 1 ELSE A.AvailableForNumberSelection IN (1, 0) END)
ORDER BY A.NumberSid, B.Capability
LIMIT 1 OFFSET ?;