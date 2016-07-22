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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryCancelledException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 * Tests distributed fields query resources cleanup on cancellation by various reasons.
 */
public class IgniteCacheQueryStopSelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** */
    private static final String QUERY_1 = "select a._key, b._key from String a, String b";

    /** */
    private static final String QUERY_2 = "select a._key, count(*) from String a group by a._key";

    /** */
    private static final String QUERY_3 = "select a._val, b._val from String a, String b";

    /** */
    private static final String QUERY_4 = "select a._key from String a";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** */
//    public void testRemoteQueryExecutionTimeout() throws Exception {
//        testQueryTimeout(10_000, 4, QUERY_1, 3);
//    }

    /**
     * Tests stopping two-step long query while result set is being generated on remote nodes.
     */
    public void testRemoteQueryExecutionCancel1() throws Exception {
        testQueryCancel(10_000, 4, QUERY_1, 500);
    }

    /** */
    public void testRemoteQueryExecutionCancel2() throws Exception {
        testQueryCancel(10_000, 4, QUERY_1, 1000);
    }

    /** */
    public void testRemoteQueryExecutionCancel3() throws Exception {
        testQueryCancel(10_000, 4, QUERY_1, 3000);
    }

    /** */
    public void testRemoteQueryWithMergeTableCancel1() throws Exception {
        testQueryCancel(100_000, 4, QUERY_2, 500);
    }

    /** */
    public void testRemoteQueryWithMergeTableCancel2() throws Exception {
        testQueryCancel(100_000, 4, QUERY_2, 1_500);
    }

    /** */
    public void testRemoteQueryWithMergeTableCancel3() throws Exception {
        testQueryCancel(100_000, 4, QUERY_2, 3_000);
    }

    /** */
    public void testRemoteQueryWithoutMergeTableCancel1() throws Exception {
        testQueryCancel(100_000, 512, QUERY_3, 500);
    }

    /** */
    public void testRemoteQueryWithoutMergeTableCancel2() throws Exception {
        testQueryCancel(100_000, 512, QUERY_3, 1_500);
    }

    /** */
    public void testRemoteQueryWithoutMergeTableCancel3() throws Exception {
        testQueryCancel(100_000, 512, QUERY_3, 3000);
    }

    /** */
    public void testRemoteQueryAlreadyFinishedStop() throws Exception {
        testQueryCancel(100, 4, QUERY_4, 3000);
    }

    /**
     * Tests stopping two step query while fetching result set from remote nodes.
     */
    private void testQueryCancel(int keyCnt, int valSize, String sql, long cancelTimeout) throws Exception {
        try (Ignite client = startGrid("client")) {

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            int p = 1;
            for (int i = 0; i < keyCnt; i++) {
                char[] tmp = new char[valSize];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));

                if ((i+1)/(float)keyCnt >= p/10f) {
                    log().info("Loaded " + (i+1) + " of " + keyCnt);

                    p++;
                }

            }

            assertEquals(0, cache.localSize());

            final QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(sql));

            final CountDownLatch l = new CountDownLatch(1);

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    cursor.close();

                    l.countDown();
                }
            }, cancelTimeout, TimeUnit.MILLISECONDS);

            try {
                cursor.iterator();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);

                assertTrue("Must throw correct exception", ex.getCause() instanceof QueryCancelledException);
            }

            l.await();

            // Give some time to clean up after query cancellation.
            Thread.sleep(3000);

            // Validate nodes query result buffer.
            checkCleanState();
        }
    }

    /**
     * Tests stopping two step query on timeout.
     */
    private void testQueryTimeout(int keyCnt, int valSize, String sql, int qryTimeoutSecs) throws Exception {
        try (Ignite client = startGrid("client")) {

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            for (int i = 0; i < keyCnt; i++) {
                char[] tmp = new char[valSize];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));
            }

            assertEquals(0, cache.localSize(ALL));

            SqlFieldsQuery sqlFldQry = new SqlFieldsQuery(sql);
            //sqlFldQry.setTimeout(qryTimeoutSecs, TimeUnit.SECONDS);

            final QueryCursor<?> cursor = cache.query(sqlFldQry);

            try {
                // Trigger distributed execution.
                cursor.iterator();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);
            }

            // Validate nodes query result buffer.
            checkCleanState();
        }
    }

    /**
     * Validates clean state on all participating nodes after query execution stopping.
     */
    private void checkCleanState() {
        int total = gridCount();

        for (int i = 0; i < total; i++) {
            IgniteEx grid = grid(i);

            // Validate everything was cleaned up.
            ConcurrentMap<UUID, ConcurrentMap<Long, ?>> map = U.field(((IgniteH2Indexing)U.field(U.field(
                grid.context(), "qryProc"), "idx")).mapQueryExecutor(), "qryRess");

            String msg = "Executor state is not cleared";

            if (map.size() == 1)
                assertEquals(msg, 0, map.entrySet().iterator().next().getValue().size());
            else
                assertEquals(msg, 0, map.size());
        }
    }
}