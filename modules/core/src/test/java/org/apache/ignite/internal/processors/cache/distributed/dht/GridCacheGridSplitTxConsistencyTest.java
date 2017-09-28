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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    private Map<Integer, Set<Integer>> blockedMap = new HashMap<Integer, Set<Integer>>() {{
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

        cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

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

        spi0.blockMessages(GridDhtTxPrepareRequest.class, grid1.name());
        spi0.blockMessages(GridDhtTxPrepareRequest.class, grid2.name());

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    spi0.waitForBlocked();

                } catch (InterruptedException e) {
                    fail();
                }

                try {
                    waitForSegmentation();
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }

                spi0.stopBlock(true);
            }
        }, 1, "stop-thread");

        IgniteCache cache = grid0.cache(DEFAULT_CACHE_NAME);

        int val0 = 1;

        cache.put(key, val0);

        fut.get();

        assertEquals("Expected topology size segment 1", 1, grid0.cluster().nodes().size());
        assertEquals("Expected topology size segment 2", 2, grid1.cluster().nodes().size());
        assertEquals("Expected topology size segment 3", 2, grid2.cluster().nodes().size());
        assertFalse(grid1.cluster().nodes().contains(grid0.localNode()));
        assertFalse(grid2.cluster().nodes().contains(grid0.localNode()));

        // Grid is split in two parts now.
        assertTrue("Expecting committed key", cache.containsKey(0));
        assertTrue("Expecting committed key", grid1.cache(DEFAULT_CACHE_NAME).containsKey(0));
        assertTrue("Expecting committed key", grid2.cache(DEFAULT_CACHE_NAME).containsKey(0));
    }

    protected void waitForSegmentation() throws InterruptedException, IgniteCheckedException {
        long topVer = grid(0).cluster().topologyVersion();

        // Trigger segmentation.
        segmented = true;

        // Wait for stable segmented topology.
        IgniteInternalFuture<Long> fut0 = grid(0).context().discovery().topologyFuture(topVer + 2);
        IgniteInternalFuture<Long> fut1 = grid(1).context().discovery().topologyFuture(topVer + 1);
        IgniteInternalFuture<Long> fut2 = grid(2).context().discovery().topologyFuture(topVer + 1);

        fut0.get();
        fut1.get();
        fut2.get();

        grid(0).context().cache().context().exchange().lastTopologyFuture().get();
        grid(1).context().cache().context().exchange().lastTopologyFuture().get();
        grid(2).context().cache().context().exchange().lastTopologyFuture().get();
    }

    /**
     * Discovery SPI which can simulate network split.
     */
    private class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /**
         * @param socket Socket.
         */
        protected boolean segmented(Socket socket) {
            if (!segmented)
                return false;

            int port = socket.getPort();

            Set<Integer> blocked = blockedMap.get(getLocalPort());

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