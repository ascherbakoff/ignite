SELECT * FROM activity activity0
LEFT OUTER JOIN "activityuseraccountrole".activityuseraccountrole activityuseraccountrole0
ON activityuseraccountrole0.activityId = activity0.activityId

LEFT OUTER JOIN "activityhistory".activityhistory activityhistory0
ON activityhistory0.activityhistoryId = activity0.lastactivityId
AND NOT activityhistory0.activitystate_enumid IN (37, 30, 463, 33, 464)

LEFT OUTER JOIN "activityhistoryuseraccount".activityhistoryuseraccount activityhistoryuseraccount0
ON activityhistoryuseraccount0.activityHistoryId = activityhistory0.activityhistoryId
AND activityuseraccountrole0.useraccountrole_id IN (1, 3)

WHERE activity0.kernel_id IS NULL
AND activity0.realization_id IS NULL
AND activity0.removefromworklist = 0

UNION ALL

SELECT * FROM activity activity0
LEFT OUTER JOIN "activityuseraccountrole".activityuseraccountrole activityuseraccountrole0
ON activityuseraccountrole0.activityId = activity0.activityId

LEFT OUTER JOIN "activityhistory".activityhistory activityhistory0
ON activityhistory0.activityhistoryId = activity0.lastactivityId
AND NOT activityhistory0.activitystate_enumid IN (37, 30, 463, 33, 464)

LEFT OUTER JOIN "activityhistoryuseraccount".activityhistoryuseraccount activityhistoryuseraccount0
ON activityhistoryuseraccount0.activityHistoryId = activityhistory0.activityhistoryId
AND activityuseraccountrole0.useraccountrole_id IN (1, 3)

WHERE activity0.kernel_id IS NULL
AND activity0.realization_id IS NULL
AND activity0.removefromworklist = 0
AND activityhistoryuseraccount0.UserAccount_id IS NULL

UNION ALL

SELECT * FROM activity activity0
LEFT OUTER JOIN "activityhistoryuseraccount".activityhistoryuseraccount activityhistoryuseraccount0
ON activityhistoryuseraccount0.activityHistoryId = activityhistory0.activityhistoryId AND activityhistoryuseraccount0.UserAccount_id = 600301
AND activityuseraccountrole0.useraccountrole_id IN (1, 3)

LEFT OUTER JOIN "activityuseraccountrole".activityuseraccountrole activityuseraccountrole0
ON activityuseraccountrole0.activityId = activity0.activityId

LEFT OUTER JOIN "activityhistory".activityhistory activityhistory0
ON activityhistory0.activityhistoryId = activity0.lastactivityId
AND NOT activityhistory0.activitystate_enumid IN (37, 30, 463, 33, 464)

WHERE activity0.kernel_id IS NULL
AND activity0.realization_id IS NULL



