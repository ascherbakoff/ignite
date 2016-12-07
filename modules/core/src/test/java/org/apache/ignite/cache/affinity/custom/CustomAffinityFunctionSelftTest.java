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

package org.apache.ignite.cache.affinity.custom;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityPrimaryFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * The implementation splits partitions to zones, each zone is split to cells which contain equal fixed number of
 * cluster nodes.
 * Zone may grow or shrink only by cell.
 * Partition data is lost then cell is gone.
 * Partitions must be distributed equally between cells.
 */
public class CustomAffinityFunctionSelftTest extends GridCommonAbstractTest {
    /** Parts count. */
    public static final int PARTS_COUNT = 1024;

    /** Cell size. */
    public static final int CELL_SIZE = 8;

    /** Backups. */
    public static final int BACKUPS = 3;

    /** Zone attribute. */
    public static final String ZONE_ATTR = "zone";

    /** Cell attribute. */
    public static final String CELL_ATTR = "cell";

    /** Data center attribute. */
    public static final String DC_ATTR = "dc";

    /** Zones. */
    public static final Object[] ZONES = new String[] {
        "Zone1", "Zone2", "Zone3", "Zone4", "Zone5"
    };

    /** Map partitions to zones */
    public static final NavigableMap<Object, List<Integer>> ZONE_TO_PART_MAP = new TreeMap<Object, List<Integer>>() {{
        put(ZONES[0], Arrays.asList(0, 10)); // [0,10) in Zone1
        put(ZONES[1], Arrays.asList(10, 90)); // [10,100) in Zone2
        put(ZONES[2], Arrays.asList(100, 300)); // [100,400) in Zone3
        put(ZONES[3], Arrays.asList(400, 600)); // [400,1000) in Zone4
        put(ZONES[4], Arrays.asList(1000, 24)); // [1000,1024) in Zone5

        int sum = 0;
        for (List<Integer> objects : values())
            sum += objects.get(1);

        assertEquals("Illegal part to zone mapping", PARTS_COUNT, sum);
    }};

    /** Ignite. */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param part Partition.
     * TODO use indexed search.
     */
    private Object partToZone(int part) {
        for (Map.Entry<Object, List<Integer>> entry : ZONE_TO_PART_MAP.entrySet()) {
            List<Integer> val = entry.getValue();

            if (val.get(0) <= part && part < val.get(0) + val.get(1))
                return entry.getKey();
        }

        throw new IgniteException("Failed to find zone for partition");
    }

    /**
     * Tests key split between zones.
     */
    public void testKeySplitBetweenZones() {
        AffinityFunction aff = affinityFunction();

        for (Map.Entry<Object, List<Integer>> entry : ZONE_TO_PART_MAP.entrySet()) {
            for (int i = 0; i < 10_000; i++) {
                int part = aff.partition(new TestKey(i, entry.getKey()));

                List<Integer> vals = ZONE_TO_PART_MAP.get(entry.getKey());

                int lower = vals.get(0);
                int upper = vals.get(0) + vals.get(1);

                assertTrue("Invalid assignment", lower <= part && part < upper);
            }
        }
    }

    /**
     * Tests assignment in zone.
     */
    public void testZoneAssignemnt() {
        AffinityFunction aff = affinityFunction();

        List<ClusterNode> nodes = createTopology(ZONES.length, 1, 1, 1);

        ClusterNode last = nodes.get(nodes.size() - 1);

        DiscoveryEvent discoEvt = new DiscoveryEvent(last, "", EventType.EVT_NODE_JOINED, last);

        GridAffinityFunctionContextImpl ctx =
            new GridAffinityFunctionContextImpl(nodes, null, discoEvt, new AffinityTopologyVersion(0), BACKUPS);

        List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

        printAssignment(assignment, BACKUPS, PARTS_COUNT, nodes.size());
    }

    /**
     * @param zones Zones.
     * @param dataCenters Data centers.
     * @param cells Cells per DC.
     * @param nodesPerCell Nodes per cell.
     */
    public List<ClusterNode> createTopology(int zones, int dataCenters, int cells, int nodesPerCell) {
        List<ClusterNode> nodes = new ArrayList<>(zones * cells * nodesPerCell);

        for (int z = 0; z < zones; z++) {
            for (int d = 0; d < dataCenters; d++) {
                for (int c = 0; c < cells; c++) {
                    for (int n = 0; n < nodesPerCell; n++)
                        nodes.add(createNode(ZONES[z], "dc" + d, "cell" + c));
                }
            }
        }

        return nodes;
    }

    /**
     * @param zone Zone.
     * @param dcId Data center id.
     * @param cellId Cell id.
     */
    private ClusterNode createNode(Object zone, Object dcId, Object cellId) {
        GridTestNode node = new GridTestNode(UUID.randomUUID());

        node.setAttribute(ZONE_ATTR, zone);
        node.setAttribute(DC_ATTR, dcId);
        node.setAttribute(CELL_ATTR, cellId);

        return node;
    }

    private void printAssignment(List<List<ClusterNode>> assignment, int backups, int partitions, int topSize) {
        for (int part = 0; part < assignment.size(); part++) {
            for (ClusterNode node : assignment.get(part))
                System.out.println("[Part=" + part + ", Zone=" + node.attribute(ZONE_ATTR) + ", Cell=" + node.attribute(CELL_ATTR));
        }
    }

    public void testZoneResize() {

    }

    /**
     * Affinity function to test.
     */
    protected AffinityFunction affinityFunction() {
        RendezvousAffinityFunction function = new RendezvousAffinityFunction(false, PARTS_COUNT) {
            @Override public int partition(Object key) {
                if (key instanceof TestKey) {
                    TestKey testKey = (TestKey)key;

                    Map.Entry<Object, List<Integer>> entry = ZONE_TO_PART_MAP.floorEntry(testKey.zone);

                    Integer cnt = entry.getValue().get(1);

                    // Align in range.
                    return U.safeAbs(key.hashCode() % cnt) + entry.getValue().get(0);
                }

                return super.partition(key);
            }
        };

        function.setAffinityPrimaryFilter(new AffinityPrimaryFilter() {
            @Override public List<ClusterNode> apply(Integer part, List<ClusterNode> currTopNodes) {
                Object zone = partToZone(part);

                // Prepare the list of nodes belonging to zone.
                List<ClusterNode> zoneNodes = new ArrayList<>();

                List<Object> cells = new ArrayList<>();

                for (ClusterNode node : currTopNodes) {
                    Object nodeZone = node.attribute(ZONE_ATTR);

                    A.notNull(zone, "Zone attribute is missing for node " + node);

                    if ( zone.equals(nodeZone)) {
                        zoneNodes.add(node);

                        Object nodeCell = node.attribute(CELL_ATTR);

                        if (!cells.contains(nodeCell))
                            cells.add(nodeCell);
                    }
                }

                int size = zoneNodes.size();

                if (size == 0)
                    return Collections.emptyList();

                // Get cell for partition
                long hash = Long.MIN_VALUE;

                Object cell = null;

                for (Object testCell : cells) {
                    long cellHash = ((long)part << 32) | testCell.hashCode();

                    if (cellHash > hash)
                        cell = testCell;
                }

                List<ClusterNode> cellNodes = new ArrayList<>();

                // Return nodes belonging to selected cell.
                for (ClusterNode node : zoneNodes) {
                    Object nodeCell = node.attribute(CELL_ATTR);

                    A.notNull(nodeCell, "Cell attribute is missing for node " + node);

                    if (nodeCell.equals(cell))
                        cellNodes.add(node);
                }

                return cellNodes;
            }
        });

        function.setAffinityBackupFilter(new IgniteBiPredicate<ClusterNode, List<ClusterNode>>() {
            @Override public boolean apply(ClusterNode testNode, List<ClusterNode> primaryAndBackupNodes) {
                if (BACKUPS + 1 >= primaryAndBackupNodes.size())
                    return false;

                Object dcAttr = testNode.attribute(DC_ATTR);

                int dcCnt = 0;

                int maxAllowed = CELL_SIZE/2;

                for (ClusterNode clusterNode : primaryAndBackupNodes) {
                    Object nodeDcAttr = clusterNode.attribute(DC_ATTR);

                    A.notNull(nodeDcAttr, "Data center attribute is missing for testNode " + testNode);

                    if (nodeDcAttr.equals(dcAttr)) {
                        dcCnt++;

                        if (dcCnt >= maxAllowed)
                            return false;
                    }
                }

                return true;
            }
        });

        GridTestUtils.setFieldValue(function, RendezvousAffinityFunction.class, "ignite", ignite);

        return function;
    }

    /**
     * Test key.
     */
    public static class TestKey {
        /** Id. */
        public int id;
        /** Zone. */
        public Object zone;

        /**
         * @param id Id.
         * @param zone Zone.
         */
        public TestKey(int id, Object zone) {
            this.id = id;
            this.zone = zone;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestKey key = (TestKey)o;

            if (id != key.id)
                return false;
            return zone.equals(key.zone);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;
            result = 31 * result + zone.hashCode();
            return result;
        }
    }
}
