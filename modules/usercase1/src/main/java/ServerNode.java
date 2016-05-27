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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.BinaryNoopMetadataHandler;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;

/**
 *
 */
public class ServerNode {
    public static void main(String[] args) throws InterruptedException, IgniteCheckedException {
        try(Ignite ignite = Ignition.start("example-ignite.xml")) {
            IgniteCache<ActivityKey, Activity> activity = ignite.cache("activity");
            IgniteCache<ActivityuseraccountroleKey, Activityuseraccountrole> role = ignite.cache("activityuseraccountrole");

            for (int i = 1; i <= 100_000; i++ ) {
                ActivityKey key = new ActivityKey();
                key.setActivityId(i);
                Activity val = new Activity();
                val.setActivityId(i);
                val.setDescription("test" + i);
                activity.put(key, val);

                if ( i % 10_000 == 0 )
                    System.out.println("processed: " + i);

                for (int j = 1; j <= 10; j++ ) {
                    Activityuseraccountrole r1 = new Activityuseraccountrole();
                    r1.setActivityId(i);
                    r1.setUseraccountroleId(j);
                    ActivityuseraccountroleKey key1 = new ActivityuseraccountroleKey();
                    key1.setActivityId(i);
                    key1.setUseraccountroleId(j);
                    role.put(key1, r1);
                }
            }


//            SqlFieldsQuery qry = new SqlFieldsQuery("EXPLAIN ANALYZE SELECT DISTINCT * FROM activity activity0\n" +
//                "LEFT OUTER JOIN \"activityuseraccountrole\".activityuseraccountrole activityuseraccountrole0\n" +
//                "ON activityuseraccountrole0.activityId = activity0.activityId\n" +
//                "AND activityuseraccountrole0.useraccountroleId IN (1, 3)\n" +
//                "\n" +
//                "LEFT OUTER JOIN \"activityhistory\".activityhistory activityhistory0\n" +
//                "ON activityhistory0.activityhistoryId = activity0.lastactivityId\n" +
//                "AND activityhistory0.activitystateEnumid NOT IN (37, 30, 463, 33, 464)\n" +
//                "\n" +
//                "LEFT OUTER JOIN \"activityhistoryuseraccount\".activityhistoryuseraccount activityhistoryuseraccount0\n" +
//                "ON activityhistoryuseraccount0.activityHistoryId = activityhistory0.activityhistoryId\n" +
//                "\n" +
//                "WHERE activity0.kernelId IS NULL\n" +
//                "AND activity0.realizationId IS NULL\n" +
//                "AND activity0.removefromworklist = 0");
//            QueryCursor<List<?>> query = activity.query(qry);
//            System.out.println(query.getAll());

            Thread.sleep(1000000000L);
        };
    }

}
