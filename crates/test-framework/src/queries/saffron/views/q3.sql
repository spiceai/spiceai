-- Area Code Max Rate Group Query (Views)
-- Returns MaxRate and COUNT(*) grouped by MaxRate
-- Filters by AccountSid, NumberPoolSid, NumberRegion, AreaCodeRegion, NumberType, Capability

SELECT MaxRate, COUNT(*) as Count 
FROM number_info_with_cap AS A
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberRegion IN (?)
  AND A.NumberType IN (?)
  AND A.AreaCodeRegion IN (?)
  AND A.NumberSid NOT IN (?)
  AND A.Capability = ?
  AND (CASE WHEN ? THEN A.AvailableForNumberSelection = 1 ELSE A.AvailableForNumberSelection IN (1, 0) END)
GROUP BY MaxRate
ORDER BY MaxRate ASC;