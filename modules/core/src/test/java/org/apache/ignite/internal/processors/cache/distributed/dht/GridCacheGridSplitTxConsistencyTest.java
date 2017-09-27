/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests commit consitency in split-brain scenario.
 */
public class GridCacheGridSplitTxConsistencyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Switches grid to segmented state. */
    private volatile boolean segmented;

    /** */
    private Map<Integer, Set<Integer>> segMap = new HashMap<Integer, Set<Integer>>() {{
        put(TcpDiscoverySpi.DFLT_PORT, new HashSet<>(Arrays.asList(TcpDiscoverySpi.DFLT_PORT + 1, TcpDiscoverySpi.DFLT_PORT + 2)));
        put(TcpDiscoverySpi.DFLT_PORT + 1, new HashSet<>(Collections.singletonList(TcpDiscoverySpi.DFLT_PORT)));
        put(TcpDiscoverySpi.DFLT_PORT + 2, new HashSet<>(Collections.singletonList(TcpDiscoverySpi.DFLT_PORT)));
    }};


    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        GridTestUtils.deleteDbFiles();
    }

    /** */
    private final TestTcpDiscoverySpi[] spis = new TestTcpDiscoverySpi[3];

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int idx = getTestIgniteInstanceIndex(gridName);
        spis[idx] = new TestTcpDiscoverySpi();
        spis[idx].setSocketTimeout(5_000);
        cfg.setDiscoverySpi(spis[idx]);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(gridName);

        MemoryConfiguration memCfg = new MemoryConfiguration();
        memCfg.setPageSize(1024);
        memCfg.setDefaultMemoryPolicySize(100 * 1024 * 1024);

        cfg.setMemoryConfiguration(memCfg);

        ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 3));
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Tests if commits are working as expected.
     * @throws Exception
     */
    public void testSplitTxConsistency() throws Exception {
        IgniteEx grid0 = startGrid(0);
        grid0.active(true);

        IgniteEx grid1 = startGrid(1);
        IgniteEx grid2 = startGrid(2);

        int key = 0;

        Affinity<Object> aff = grid0.affinity(DEFAULT_CACHE_NAME);
        assertTrue(aff.isPrimary(grid0.localNode(), key));
        assertTrue(aff.isBackup(grid1.localNode(), key));
        assertTrue(aff.isBackup(grid2.localNode(), key));

        final TestRecordingCommunicationSpi spi0 = (TestRecordingCommunicationSpi) grid0.configuration().getCommunicationSpi();

        spi0.blockMessages(GridDhtTxFinishRequest.class, grid1.name());
        spi0.blockMessages(GridDhtTxFinishRequest.class, grid2.name());

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    spi0.waitForBlocked();

                } catch (InterruptedException e) {
                    fail();
                }

                segmented = true;

                waitForSegmentation();

                spi0.stopBlock(true);
            }
        }, 1, "stop-thread");

        IgniteCache cache = grid0.cache(DEFAULT_CACHE_NAME);

        int val0 = 1;

        cache.put(key, val0);

        fut.get();

        assertTrue("Expecting committed key", cache.containsKey(0));

        // Some actions can be done after successful commit at this point.

        // Now split-brain happens.
        // In real life grid0 and grid1 are separated and do not see each other.
        // Pretend second DC is active.
        stopGrid(0);

        grid1 = startGrid(1);
        grid1.active(true);

        grid2 = startGrid(2);

        // Test if previous commit is not lost.
        assertTrue("Expecting committed key", grid1.cache(DEFAULT_CACHE_NAME).containsKey(0));
        assertTrue("Expecting committed key", grid2.cache(DEFAULT_CACHE_NAME).containsKey(0));
    }

    protected void waitForSegmentation() {
        LockSupport.park();
    }

    /**
     *
     */
    private class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private Exception err;

        protected boolean segmented(Socket socket) {
            if (!segmented)
                return false;

            int port = socket.getPort();

            Set<Integer> blocked = segMap.get(getLocalPort());

            return blocked.contains(port);
        }

        /**  */
        @Override protected void writeToSocket(
                Socket sock,
                TcpDiscoveryAbstractMessage msg,
                byte[] data,
                long timeout
        ) throws IOException {
            if (segmented(sock)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // No-op.
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
            else
                super.writeToSocket(sock, msg, data, timeout);
        }

        /**  */
        @Override protected void writeToSocket(Socket sock,
                                               OutputStream out,
                                               TcpDiscoveryAbstractMessage msg,
                                               long timeout) throws IOException, IgniteCheckedException {
            if (segmented(sock)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // No-op.
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
            else
                super.writeToSocket(sock, out, msg, timeout);
        }

        /**  */
        @Override protected void writeToSocket(
                Socket sock,
                TcpDiscoveryAbstractMessage msg,
                long timeout
        ) throws IOException, IgniteCheckedException {
            if (segmented(sock)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // No-op.
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
            else
                super.writeToSocket(sock, msg, timeout);
        }

        /**  */
        @Override protected void writeToSocket(
                TcpDiscoveryAbstractMessage msg,
                Socket sock,
                int res,
                long timeout
        ) throws IOException {
            if (segmented(sock)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // No-op.
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
            else
                super.writeToSocket(msg, sock, res, timeout);
        }
    }
}