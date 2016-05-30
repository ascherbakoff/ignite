SELECT * FROM "activity".activity activity0
LEFT OUTER JOIN "activityuseraccountrole".activityuseraccountrole activityuseraccountrole0
ON activityuseraccountrole0.activityId = activity0.activityId

LEFT OUTER JOIN "activityhistory".activityhistory activityhistory0
ON activityhistory0.activityhistoryId = activity0.lastactivityId
AND NOT activityhistory0.activitystateEnumid IN (37, 30, 463, 33, 464)

LEFT OUTER JOIN "activityhistoryuseraccount".activityhistoryuseraccount activityhistoryuseraccount0
ON activityhistoryuseraccount0.activityHistoryId = activityhistory0.activityhistoryId
AND activityuseraccountrole0.useraccountroleId IN (1, 3)

WHERE activity0.kernelId IS NULL
AND activity0.realizationId IS NULL
AND activity0.removefromworklist = 0

UNION ALL

SELECT * FROM "activity".activity activity0
LEFT OUTER JOIN "activityuseraccountrole".activityuseraccountrole activityuseraccountrole0
ON activityuseraccountrole0.activityId = activity0.activityId

LEFT OUTER JOIN "activityhistory".activityhistory activityhistory0
ON activityhistory0.activityhistoryId = activity0.lastactivityId
AND NOT activityhistory0.activitystateEnumid IN (37, 30, 463, 33, 464)

LEFT OUTER JOIN "activityhistoryuseraccount".activityhistoryuseraccount activityhistoryuseraccount0
ON activityhistoryuseraccount0.activityHistoryId = activityhistory0.activityhistoryId
AND activityuseraccountrole0.useraccountroleId IN (1, 3)

WHERE activity0.kernelId IS NULL
AND activity0.realizationId IS NULL
AND activity0.removefromworklist = 0
AND activityhistoryuseraccount0.userAccountId IS NULL

UNION ALL

SELECT * FROM "activity".activity activity0
LEFT OUTER JOIN "activityhistoryuseraccount".activityhistoryuseraccount activityhistoryuseraccount0
ON activityhistoryuseraccount0.activityHistoryId = activity0.activityId
AND activityhistoryuseraccount0.UserAccountId = 600301

LEFT OUTER JOIN "activityuseraccountrole".activityuseraccountrole activityuseraccountrole0
ON activityuseraccountrole0.activityId = activity0.activityId
AND activityuseraccountrole0.useraccountroleId IN (1, 3)

LEFT OUTER JOIN "activityhistory".activityhistory activityhistory0
ON activityhistory0.activityhistoryId = activity0.lastactivityId
AND NOT activityhistory0.activitystateEnumid IN (37, 30, 463, 33, 464)

WHERE activity0.kernelId IS NULL
AND activity0.realizationId IS NULL



