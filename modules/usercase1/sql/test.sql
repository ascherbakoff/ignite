SELECT * FROM activity activity0
LEFT OUTER JOIN "activityhistory".activityhistory activityhistory0
ON activityhistory0.activityhistoryId = activity0.lastactivityId
LEFT OUTER JOIN "activityuseraccountrole".activityuseraccountrole activityuseraccountrole0
ON activityuseraccountrole0.activityId = activity0.activityId
LEFT OUTER JOIN "activityhistoryuseraccount".activityhistoryuseraccount activityhistoryuseraccount0
ON activityhistoryuseraccount0.activityHistoryId = activityhistory0.activityhistoryId
WHERE activity0.kernel_id IS NULL
AND activity0.realization_id IS NULL
AND NOT activityhistory0.activitystate_enumid IN (37, 30, 463, 33, 464)
AND (
(activityuseraccountrole0.useraccountrole_id IN (1, 3) AND (activity0.removefromworklist = 0 OR activityhistoryuseraccount0.UserAccount_id IS NULL))
OR activityhistoryuseraccount0.UserAccount_id = 600301
)