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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * The implementation splits partitions to zones, each zone is split to cells which contain equal fixed number of
 * cluster nodes. Zone may grow or shrink only by cell. Partition data is lost then cell is gone. Partitions must be
 * distributed equally between cells.
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

    public static final String ATTR_REQ_MSG = "%s attribute is missing for node %s";

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

    private AffinityFunction aff;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        aff = affinityFunction();
    }

    /**
     * @param part Partition. TODO use indexed search.
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
        Assignment assignment = createAssignment(ZONES.length, 1, 1);

        printAssignment(assignment.assignment, BACKUPS, PARTS_COUNT);
    }

    /**
     * @param zones Zones.
     * @param cells Cells per DC.
     * @param nodesPerCell Nodes per cell.
     */
    public Assignment createAssignment(int zones, int cells, int nodesPerCell) {
        int top = 0;

        Assignment a = new Assignment();

        a.topology = new ArrayList<>(zones * cells * nodesPerCell);

        for (int z = 0; z < zones; z++) {
            int dc = 0;

            for (int c = 0; c < cells; c++) {
                for (int n = 0; n < nodesPerCell; n++) {
                    ClusterNode node = createNode(ZONES[z], "dc" + (dc++ % 2), "cell" + c);

                    a.topology.add(node);

                    DiscoveryEvent discoEvt = new DiscoveryEvent(node, "", EventType.EVT_NODE_JOINED, node);

                    GridAffinityFunctionContextImpl ctx =
                        new GridAffinityFunctionContextImpl(a.topology, null, discoEvt, new AffinityTopologyVersion(top++), BACKUPS);

                    a.assignment = aff.assignPartitions(ctx);
                }
            }
        }

        return a;
    }

    /**
     * Assignment info.
     */
    private static class Assignment {
        public List<List<ClusterNode>> assignment;

        public List<ClusterNode> topology;
    }

    /**
     * @param zone Zone.
     * @param dataCenterId Data center id.
     * @param cellId Cell id.
     */
    private ClusterNode createNode(Object zone, Object dataCenterId, Object cellId) {
        GridTestNode node = new GridTestNode(UUID.randomUUID());

        node.setAttribute(ZONE_ATTR, zone);
        node.setAttribute(DC_ATTR, dataCenterId);
        node.setAttribute(CELL_ATTR, cellId);

        return node;
    }

    private void printAssignment(List<List<ClusterNode>> assignment, int backups, int partitions) {
        for (int part = 0; part < assignment.size(); part++) {
            for (ClusterNode node : assignment.get(part))
                System.out.println("[Part=" + part + ", Zone=" + node.attribute(ZONE_ATTR) + ", Cell=" + node.attribute(CELL_ATTR));
        }
    }

    /**
     * TODO
     */
    public void testEmptyZones() {
    }

    /**
     * Tests partition distribution in zone with several cells.
     */
    public void testDistribution() {
        int cells = 2;

        Assignment assignment = createAssignment(ZONES.length, cells, CELL_SIZE);

        assertEquals("Topology size", ZONES.length * cells * CELL_SIZE, assignment.topology.size());

        //for (Object zone : ZONES) {
        Object zone = ZONES[3];

        List<ClusterNode> cellNodes0 = IgniteUtils.arrayList(
            assignment.topology,
            new NodeAttributeFilter(ZONE_ATTR, zone),
            new NodeAttributeFilter(CELL_ATTR, "cell0"));

        validateCellDistribution(zone, cells, cellNodes0, assignment);

        assertEquals("Cell size", CELL_SIZE, cellNodes0.size());

        System.out.println("------------");

        List<ClusterNode> cellNodes1 = IgniteUtils.arrayList(
            assignment.topology,
            new NodeAttributeFilter(ZONE_ATTR, zone),
            new NodeAttributeFilter(CELL_ATTR, "cell1"));

        validateCellDistribution(zone, cells, cellNodes1, assignment);

        assertEquals("Cell size", CELL_SIZE, cellNodes1.size());

        // Cells should have no intersection
        assertTrue("Cells are different", Collections.disjoint(cellNodes0, cellNodes1));

        // Cells should not have common partitions

        // Partitions must be deployed on equal number of nodes in both DCs.
    }

    private  Map<UUID, Collection<Integer>> validateCellDistribution(Object zone, int cellsCnt, List<ClusterNode> cellNodes, Assignment assignment) {
        List<Integer> range = ZONE_TO_PART_MAP.get(zone);

        int partsCnt = range.get(1);

        int topSize = cellNodes.size();

        Map<UUID, Collection<Integer>> mapping = new HashMap<>();

        // Ideal count of partition on cell node.
        int ideal = Math.round((float)partsCnt / cellsCnt / topSize * Math.min(BACKUPS + 1, topSize));

        int start = range.get(0);
        int stop = start + range.get(1);

        for (int part = start; part < stop; part++) {
            for (ClusterNode node : assignment.assignment.get(part)) {
                assert node != null;

                if (cellNodes.contains(node))
                    continue;

                Collection<Integer> parts = mapping.get(node.id());

                if (parts == null) {
                    parts = new HashSet<>();

                    mapping.put(node.id(), parts);
                }

                assertTrue(parts.add(part));
            }
        }

        int max = -1, min = Integer.MAX_VALUE;

        for (Collection<Integer> parts : mapping.values()) {
            max = Math.max(max, parts.size());
            min = Math.min(min, parts.size());
        }

        log().warning("max=" + max + ", min=" + min + ", ideal=" + ideal + ", minDev=" + deviation(min, ideal) + "%, " +
            "maxDev=" + deviation(max, ideal) + "%");

        return mapping;
    }

    private static class NodeAttributeFilter implements IgnitePredicate<ClusterNode> {
        private final String attrName;
        private final Object attrVal;

        public NodeAttributeFilter(String attrName, Object attrVal) {
            this.attrName = attrName;
            this.attrVal = attrVal;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(attrName).equals(attrVal);
        }
    }

    /**
     * @param assignment Assignment to verify.
     */
    private void verifyAssignment(List<List<ClusterNode>> assignment, int keyBackups, int partsCnt, int topSize) {
        Map<UUID, Collection<Integer>> mapping = new HashMap<>();

        int ideal = Math.round((float)partsCnt / topSize * Math.min(keyBackups + 1, topSize));

        for (int part = 0; part < assignment.size(); part++) {
            for (ClusterNode node : assignment.get(part)) {
                assert node != null;

                Collection<Integer> parts = mapping.get(node.id());

                if (parts == null) {
                    parts = new HashSet<>();

                    mapping.put(node.id(), parts);
                }

                assertTrue(parts.add(part));
            }
        }

        int max = -1, min = Integer.MAX_VALUE;

        for (Collection<Integer> parts : mapping.values()) {
            max = Math.max(max, parts.size());
            min = Math.min(min, parts.size());
        }

        log().warning("max=" + max + ", min=" + min + ", ideal=" + ideal + ", minDev=" + deviation(min, ideal) + "%, " +
            "maxDev=" + deviation(max, ideal) + "%");
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

                    A.notNull(zone, String.format(ATTR_REQ_MSG, "Zone", node));

                    if (zone.equals(nodeZone)) {
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
                    long cellHash = hash(part, testCell);

                    if (cellHash > hash) {
                        cell = testCell;

                        hash = cellHash;
                    }
                }

                List<ClusterNode> cellNodes = new ArrayList<>();

                // Return nodes belonging to selected cell.
                for (ClusterNode node : zoneNodes) {
                    Object nodeCell = node.attribute(CELL_ATTR);

                    A.notNull(nodeCell, String.format(ATTR_REQ_MSG, "Cell", node));

                    if (nodeCell.equals(cell))
                        cellNodes.add(node);
                }

                return cellNodes;
            }
        });

        function.setAffinityBackupFilter(new IgniteBiPredicate<ClusterNode, List<ClusterNode>>() {
            @Override public boolean apply(ClusterNode testNode, List<ClusterNode> primaryAndBackupNodes) {
                if (primaryAndBackupNodes.size() >= BACKUPS + 1)
                    return false;

                ClusterNode prim = primaryAndBackupNodes.get(0);

                Object primDc = prim.attribute(DC_ATTR);

                A.notNull(primDc, "Data center attribute is missing for node " + prim);

                Object dcAttr = testNode.attribute(DC_ATTR);

                A.notNull(dcAttr, "Data center attribute is missing for node " + prim);

                // Enforce rule: equal number of nodes hosting partition in each DC
                int dcCnt = primDc.equals(dcAttr) ? 1 : 0;

                int maxAllowed = (BACKUPS + 1) / 2;

                for (ClusterNode clusterNode : primaryAndBackupNodes) {
                    Object nodeDcAttr = clusterNode.attribute(DC_ATTR);

                    A.notNull(nodeDcAttr, "Data center attribute is missing for node " + testNode);

                    if (nodeDcAttr.equals(dcAttr)) {
                        dcCnt++;

                        if (dcCnt > maxAllowed)
                            return false;
                    }
                }

                return true;
            }
        });

        GridTestUtils.setFieldValue(function, RendezvousAffinityFunction.class, "ignite", ignite);

        return function;
    }

    public void testHash() {
        int r1 = 0;
        int r2 = 0;
        for (int i = 400; i < 1000; i++) {
            long h1 = hash(i, "cell0");

            long h2 = hash(i, "cell1");

            if (h1 > h2)
                r1++;
            else
                r2++;
        }

        System.out.println("r1=" + r1 + ", r2=" + r2);
    }

    /**
     * @param part Partition.
     * @param obj Object.
     */
    private long hash(Integer part, Object obj) {
        return xorshift64star(((long)part << 32) | obj.hashCode());
    }

    public static long xorshift64star(long x) {
        x ^= x >>> 12; // a
        x ^= x << 25; // b
        x ^= x >>> 27; // c
        return x * 2685821657736338717L;
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

    /**
     * @param val Value.
     * @param ideal Ideal.
     */
    private static int deviation(int val, int ideal) {
        return Math.round(Math.abs(((float)val - ideal) / ideal * 100));
    }
}
