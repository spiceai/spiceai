-- Region Random Number Selection Query
-- Executes random number selection by region (less selective than q4)
-- Filters by AccountSid, NumberPoolSid, NumberRegion, NumberType, Capability (no AreaCode)

WITH filtered AS (
    SELECT *
    FROM number_info_with_cap AS A
    WHERE A.AccountSid = ?
      AND A.NumberPoolSid = ?
)
SELECT DateCreated, DateUpdated, AccountSid, NumberPoolSid, NumberSid,
       MaxRate, NumberDid, NumberType, SupportedDestRegion, NumberRegion,
       CurrentRate, IsAvailable, ProviderSid, AreaCodeRegion,
       AvailableForNumberSelection, Capability
FROM filtered
WHERE NumberRegion IN (?)
  AND NumberType IN (?)
  AND MaxRate = ?
  AND NumberSid NOT IN (?)
  AND (CASE WHEN ? THEN AvailableForNumberSelection = 1 ELSE AvailableForNumberSelection IN (1, 0) END)
  AND Capability = ?
ORDER BY NumberSid
LIMIT 1 OFFSET ?;