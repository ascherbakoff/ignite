/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 *
 */
public class ServerNode {
    public static void main(String[] args) throws InterruptedException {
        try(Ignite ignite = Ignition.start("example-ignite.xml")) {
            IgniteCache<ActivityKey, Activity> activity = ignite.cache("activity");

            SqlFieldsQuery qry = new SqlFieldsQuery("explain SELECT activity0.* FROM activity activity0\n" +
                "LEFT OUTER JOIN \"activityhistory\".activityhistory activityhistory0 ON activityhistory0.activityhistoryId = activity0.lastactivityId\n" +
                "LEFT OUTER JOIN \"activityuseraccountrole\".activityuseraccountrole activityuseraccountrole0 ON activityuseraccountrole0.activityId = activity0.activityId\n" +
                "LEFT OUTER JOIN \"activityhistoryuseraccount\".activityhistoryuseraccount activityhistoryuseraccount0 ON activityhistoryuseraccount0.activityHistoryId = activityhistory0.activityhistoryId\n" +
                "WHERE activity0.kernelId IS NULL\n" +
                "AND activity0.realizationId IS NULL\n" +
                "AND NOT activityhistory0.activitystateEnumid IN (37, 30, 463, 33, 464)\n" +
                "AND (\n" +
                "(activityuseraccountrole0.useraccountroleId IN (1, 3) AND (activity0.removefromworklist = 0 OR activityhistoryuseraccount0.userAccountId IS NULL))\n" +
                "OR activityhistoryuseraccount0.useraccountId = 600301\n" +
                ")");
            qry.setLocal(true);
            QueryCursor<List<?>> query = activity.query(qry);
            System.out.println(query.getAll());
        };
    }

}
