package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.concurrent.locks.LockSupport;

@WithSystemProperty(key = "IGNITE_BASELINE_AUTO_ADJUST_ENABLED", value = "false")
public class FastPMETest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);
        cfg.setFailureDetectionTimeout(1000000000L);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
                setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
                setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC).setAffinity(new RendezvousAffinityFunction(false, 8)));

//        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setPageSize(1024).setWalSegmentSize(4 * 1024 * 1024).
//                setCheckpointFrequency(10000000000L).
//                setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    @Test
    public void test() throws Exception {
        try {
            IgniteEx crd = startGrids(3);
            crd.cluster().active(true);

            stopGrid(1, true);

            LockSupport.park();
        }
        finally {
            stopAllGrids();
        }
    }
}
