-- Area Code Max Rate Group Query
-- Returns MaxRate and COUNT(*) grouped by MaxRate
-- Filters by AccountSid, NumberPoolSid, NumberRegion, AreaCodeRegion, NumberType, Capability

SELECT A.MaxRate, COUNT(*) as Count 
FROM number_info AS A
INNER JOIN number_caps AS B ON A.NumberSid = B.NumberSid
WHERE A.AccountSid = ? 
  AND A.NumberPoolSid = ? 
  AND A.NumberRegion IN (?)
  AND A.NumberType IN (?)
  AND A.AreaCodeRegion IN (?)
  AND A.NumberSid NOT IN (?)
  AND B.Capability = ?
  AND (CASE WHEN ? THEN A.AvailableForNumberSelection = 1 ELSE A.AvailableForNumberSelection IN (1, 0) END)
GROUP BY A.MaxRate
ORDER BY A.MaxRate ASC;