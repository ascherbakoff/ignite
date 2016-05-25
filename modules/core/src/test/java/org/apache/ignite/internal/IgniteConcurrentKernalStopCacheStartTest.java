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

package org.apache.ignite.internal;

import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests concurrent grid instance start and stop.
 */
public class IgniteConcurrentKernalStopCacheStartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Tests concurrent instance shutdown.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentAccess() throws Exception {
        final IgniteEx ignite = grid();

        Thread invoker = new Thread(new Runnable() {
            @Override public void run() {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();
                ccfg.setCacheStoreSessionListenerFactories(new Factory<CacheStoreSessionListener>() {
                    @Override public CacheStoreSessionListener create() {
                        return null;
                    }
                });

                final IgniteCache<Object, Object> dfltCache = ignite.getOrCreateCache(ccfg);

                dfltCache.put("1", "1");
            }
        });

        invoker.setName("ConcurrentEntryProcessorActionThread");

        invoker.start();

        stopGrid();

        invoker.join();
    }
}
