package org.apache.ignite.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridNodeOrderComparator;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Created by A.Scherbakov on 11/22/2017.
 */
public class CacheTopologyMemUsageTest extends GridCommonAbstractTest {
    /** */
    private Map<Integer, CacheGroupContext> registeredCacheGrps = new HashMap<>();

    /** */
    private DiscoCache discoCache;

    public void test() {
        AffinityFunction aff = new RendezvousAffinityFunction(false, 8192);

        final int nodesCnt = 10;

        final int backups = 3;

        List<ClusterNode> nodes = new ArrayList<>(nodesCnt);

        List<List<ClusterNode>> prev = null;

        GridTestKernalContext kernalCtx = new GridTestKernalContext(log);

        GridCacheSharedContext<Object, Object> ctx = new GridCacheSharedContext<>(kernalCtx, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null);

        AffinityTopologyVersion topVer = new AffinityTopologyVersion(1, 0);

        CacheGroupContext cacheGrpCtx = new CacheGroupContext(ctx, CU.cacheId("testGrp"), UUID.randomUUID(),
            CacheType.USER, new CacheConfiguration().setGroupName("testGrp"), true, null, null, null, null, topVer);

        GridTestNode locNode = new GridTestNode(UUID.randomUUID());

        locNode.order(1);

        locNode.local(true);

        nodes.add(locNode);

        DiscoveryEvent discoEvt = new DiscoveryEvent(locNode, "", EventType.EVT_NODE_JOINED, locNode);

        GridAffinityFunctionContextImpl affCtx =
            new GridAffinityFunctionContextImpl(nodes, null, discoEvt, topVer, 3);

        List<List<ClusterNode>> assignment = aff.assignPartitions(affCtx);

        prev = assignment;

        DiscoveryDataClusterState globalState = DiscoveryDataClusterState.createState(true);

        discoCache = createDiscoCache(topVer, globalState, locNode, nodes);

        for (int i = 1; i < nodesCnt; i++) {
            GridTestNode node = new GridTestNode(UUID.randomUUID());

            node.order(i + 1);

            nodes.add(node);

            discoEvt = new DiscoveryEvent(node, "", EventType.EVT_NODE_JOINED, node);

            topVer = new AffinityTopologyVersion(node.order());

            affCtx = new GridAffinityFunctionContextImpl(nodes, prev, discoEvt, topVer, backups);

            assignment = aff.assignPartitions(affCtx);

            prev = assignment;

            discoCache = createDiscoCache(topVer, globalState, locNode, nodes);
        }

        System.out.println(discoCache);
    }

    @NotNull private DiscoCache createDiscoCache(
        AffinityTopologyVersion topVer,
        DiscoveryDataClusterState state,
        ClusterNode loc,
        Collection<ClusterNode> topSnapshot) {
        assert topSnapshot.contains(loc);

        HashSet<UUID> alives = U.newHashSet(topSnapshot.size());
        HashMap<UUID, ClusterNode> nodeMap = U.newHashMap(topSnapshot.size());

        ArrayList<ClusterNode> daemonNodes = new ArrayList<>(topSnapshot.size());
        ArrayList<ClusterNode> srvNodes = new ArrayList<>(topSnapshot.size());
        ArrayList<ClusterNode> rmtNodes = new ArrayList<>(topSnapshot.size());
        ArrayList<ClusterNode> allNodes = new ArrayList<>(topSnapshot.size());

        IgniteProductVersion minVer = null;

        for (ClusterNode node : topSnapshot) {
            alives.add(node.id());

            if (node.isDaemon())
                daemonNodes.add(node);
            else {
                allNodes.add(node);

                if (!node.isLocal())
                    rmtNodes.add(node);

                if (!CU.clientNode(node))
                    srvNodes.add(node);
            }

            nodeMap.put(node.id(), node);

            if (minVer == null)
                minVer = node.version();
            else if (node.version().compareTo(minVer) < 0)
                minVer = node.version();
        }

        assert !rmtNodes.contains(loc) : "Remote nodes collection shouldn't contain local node" +
            " [rmtNodes=" + rmtNodes + ", loc=" + loc + ']';

        Map<Integer, List<ClusterNode>> allCacheNodes = U.newHashMap(allNodes.size());
        Map<Integer, List<ClusterNode>> cacheGrpAffNodes = U.newHashMap(allNodes.size());
        Set<ClusterNode> rmtNodesWithCaches = new TreeSet<>(GridNodeOrderComparator.INSTANCE);

        fillAffinityNodeCaches(allNodes, allCacheNodes, cacheGrpAffNodes, rmtNodesWithCaches);

        return new DiscoCache(
            topVer,
            state,
            loc,
            Collections.unmodifiableList(rmtNodes),
            Collections.unmodifiableList(allNodes),
            Collections.unmodifiableList(srvNodes),
            Collections.unmodifiableList(daemonNodes),
            U.sealList(rmtNodesWithCaches),
            Collections.unmodifiableMap(allCacheNodes),
            Collections.unmodifiableMap(cacheGrpAffNodes),
            Collections.unmodifiableMap(nodeMap),
            alives,
            minVer);
    }

    /**
     * Fills affinity node caches.
     *
     * @param allNodes All nodes.
     * @param allCacheNodes All cache nodes.
     * @param cacheGrpAffNodes Cache group aff nodes.
     * @param rmtNodesWithCaches Rmt nodes with caches.
     */
    private void fillAffinityNodeCaches(List<ClusterNode> allNodes, Map<Integer, List<ClusterNode>> allCacheNodes,
        Map<Integer, List<ClusterNode>> cacheGrpAffNodes, Set<ClusterNode> rmtNodesWithCaches) {
        for (ClusterNode node : allNodes) {
            for (Map.Entry<Integer, CacheGroupContext> e : registeredCacheGrps.entrySet()) {
                CacheGroupContext grpAff = e.getValue();
                Integer grpId = e.getKey();

                List<ClusterNode> nodes = cacheGrpAffNodes.get(grpId);

                if (nodes == null)
                    cacheGrpAffNodes.put(grpId, nodes = new ArrayList<>());

                nodes.add(node);
            }
        }
    }

    /**
     * Adds node to map.
     *
     * @param cacheMap Map to add to.
     * @param cacheName Cache name.
     * @param rich Node to add
     */
    private void addToMap(Map<Integer, List<ClusterNode>> cacheMap, String cacheName, ClusterNode rich) {
        List<ClusterNode> cacheNodes = cacheMap.get(CU.cacheId(cacheName));

        if (cacheNodes == null) {
            cacheNodes = new ArrayList<>();

            cacheMap.put(CU.cacheId(cacheName), cacheNodes);
        }

        cacheNodes.add(rich);
    }
}
