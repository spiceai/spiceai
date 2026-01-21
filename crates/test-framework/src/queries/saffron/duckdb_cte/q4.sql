-- Area Code Random Number Selection Query
-- Executes area code random number selection queries (most selective)
-- Filters by AccountSid, NumberPoolSid, NumberRegion, AreaCodeRegion, NumberType, Capability, etc.

WITH filtered AS (
    SELECT *
    FROM number_info_with_cap AS A
    WHERE A.AccountSid = ?
      AND A.NumberPoolSid = ?
)
SELECT DateCreated, DateUpdated, AccountSid, NumberPoolSid, NumberSid, MaxRate, NumberDid,
       NumberType, SupportedDestRegion, NumberRegion, CurrentRate, IsAvailable, ProviderSid,
       AreaCodeRegion, AvailableForNumberSelection, Capability
FROM filtered
WHERE NumberRegion IN (?)
  AND NumberType IN (?)
  AND AreaCodeRegion IN (?)
  AND MaxRate = ?
  AND NumberSid NOT IN (?)
  AND Capability = ?
  AND (CASE WHEN ? THEN AvailableForNumberSelection = 1 ELSE AvailableForNumberSelection IN (1, 0) END)
ORDER BY RAND()
LIMIT 1 OFFSET ?;