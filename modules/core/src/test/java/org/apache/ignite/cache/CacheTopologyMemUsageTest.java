package org.apache.ignite.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Created by A.Scherbakov on 11/22/2017.
 */
public class CacheTopologyMemUsageTest extends GridCommonAbstractTest {
    /** */
    private Map<Integer, CacheGroupContext> registeredCacheGrps = new HashMap<>();

    /** */
    public static final String CACHE_NAME_1 = "cache1";

    /** */
    public static final String CACHE_NAME_2 = "cache2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        final CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, CacheConfiguration.MAX_PARTITIONS_COUNT));
        ccfg.setBackups(0);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests partition topology mem usage.
     */
    public void testMemUsage() throws Exception {
        int nodesCnt = 2;

        List<HeapUsage> usages = new ArrayList<>(nodesCnt);

        final IgniteEx ig = startGrid(0);

        usages.add(computeHeapUsage(ig));

        for (int i = 1; i < nodesCnt; i++) {
            startGrid(i);

            awaitPartitionMapExchange();

            usages.add(computeHeapUsage(ig));
        }

        for (HeapUsage usage : usages)
            log.info(usage.toString());
    }

    private HeapUsage computeHeapUsage(IgniteEx ig) throws IgniteCheckedException {
        CacheGroupContext grpCtx = ig.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        GridDhtPartitionTopology top = grpCtx.topology();

        long fullMapSize = ObjectSizeCalculator.getObjectSize(U.field(top, "node2part"));

        long cntrMapSize = ObjectSizeCalculator.getObjectSize(U.field(top, "cntrMap"));

        long diffMapSize = ObjectSizeCalculator.getObjectSize(U.field(top, "diffFromAffinity"));

        final GridCachePartitionExchangeManager<Object, Object> mgr = ig.context().cache().context().exchange();

        final List<GridDhtPartitionsExchangeFuture> futs = mgr.exchangeFutures();

        long exchHistSize = 0;

        final AffinityTopologyVersion topVer = mgr.readyAffinityVersion();

        for (GridDhtPartitionsExchangeFuture fut : futs) {
            GridDhtPartitionsFullMessage msg = U.field(U.field(fut, "finishState"), "msg");

            final GridDhtPartitionsFullMessage cpy = U.invoke(GridDhtPartitionsFullMessage.class, msg, "copy");

            cpy.exchangeId(null);

            exchHistSize += ObjectSizeCalculator.getObjectSize(cpy);
        }

        return new HeapUsage(topVer, diffMapSize, fullMapSize, cntrMapSize, exchHistSize);
    }

    /** */
    private static class HeapUsage {
        final AffinityTopologyVersion topVer;

        /** */
        final long diffMapSize;

        /** */
        final long fullMapSize;

        /** */
        final long cntrMapSize;

        /** */
        final long exchHistorySize;

        /**
         * @param fullMapSize Full map size.
         * @param cntrMapSize Counter map size.
         * @param exchHistSize Exchange history size.
         */
        public HeapUsage(AffinityTopologyVersion topVer, long diffMapSize, long fullMapSize, long cntrMapSize, long exchHistSize) {
            this.topVer = topVer;
            this.diffMapSize = diffMapSize;
            this.fullMapSize = fullMapSize;
            this.cntrMapSize = cntrMapSize;
            this.exchHistorySize = exchHistSize;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "HeapUsage: [" +
                "topVer=" + topVer +
                ", diffMapSize=" + diffMapSize +
                ", fullMapSize=" + fullMapSize +
                ", cntrMapSize=" + cntrMapSize +
                ", exchHistorySize=" + exchHistorySize +
                ']';
        }
    }
}
