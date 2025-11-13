-- Any Capable Max Rate Group Query
-- Returns MaxRate and COUNT(*) grouped by MaxRate
-- Filters by AccountSid, NumberPoolSid, Capability

SELECT MaxRate, COUNT(*) as Count 
FROM number_info AS A
INNER JOIN number_caps AS B ON A.NumberSid = B.NumberSid
WHERE A.AccountSid = ?
  AND A.NumberPoolSid = ?
  AND A.NumberSid NOT IN (?)
  AND B.Capability = ?
  AND (CASE WHEN ? THEN A.AvailableForNumberSelection = 1 ELSE A.AvailableForNumberSelection IN (1, 0) END)
GROUP BY MaxRate
ORDER BY MaxRate ASC;