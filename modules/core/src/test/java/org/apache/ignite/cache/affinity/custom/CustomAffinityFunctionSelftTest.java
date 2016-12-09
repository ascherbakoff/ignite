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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
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

    /** Data centers. */
    public static final int DATA_CENTERS = 2;

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

    /** Affinity. */
    private CustomRendezvousAffinityFunction affinity;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        affinity = affinityFunction();
    }

    /**
     * Tests key split between zones.
     */
    public void testKeySplitBetweenZones() {
        for (Map.Entry<Object, List<Integer>> entry : ZONE_TO_PART_MAP.entrySet()) {
            for (int i = 0; i < 10_000; i++) {
                int part = affinity.partition(new TestKey(i, entry.getKey()));

                List<Integer> vals = ZONE_TO_PART_MAP.get(entry.getKey());

                int lower = vals.get(0);
                int upper = vals.get(0) + vals.get(1);

                assertTrue("Invalid assignment", lower <= part && part < upper);
            }
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
        int cells = 3;

        long t1 = System.nanoTime();

        Assignment assignment = createAssignment(ZONES.length, cells, CELL_SIZE);

        long t2 = System.nanoTime();

        System.out.println("Topology created for " + (t2 - t1) / 1000 / 1000. + " ms");

        assertEquals("Topology size", ZONES.length * cells * CELL_SIZE, assignment.topology.size());

        for (Object zone : ZONES) {
            log().info("Processing zone " + zone);

            List<Map<UUID, Collection<Integer>>> mappings = new ArrayList<>();

            for (int c = 0; c < cells; c++) {
                String cell = "cell" + c;

                List<ClusterNode> nodes = IgniteUtils.arrayList(
                    assignment.topology,
                    new NodeAttributeFilter(CustomPrimaryFilter.ZONE_ATTR, zone),
                    new NodeAttributeFilter(CustomPrimaryFilter.CELL_ATTR, cell));

                validateCell(cell, nodes);

                Map<UUID, Collection<Integer>> mapping = validateCellDistribution(zone, cells, nodes, assignment);

                mappings.add(mapping);

                assertEquals("Cell size", CELL_SIZE, nodes.size());
            }

            validatePartitions(mappings);

            List<Integer> range = ZONE_TO_PART_MAP.get(zone);

            int start = range.get(0);
            int end = start + range.get(1);

            for (int part = start; part < end; part++)
                validatePartition(part, assignment);
        }
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
                    ClusterNode node = createNode(ZONES[z], "dc" + (dc++ % DATA_CENTERS), "cell" + c);

                    a.topology.add(node);

                    DiscoveryEvent discoEvt = new DiscoveryEvent(node, "", EventType.EVT_NODE_JOINED, node);

                    GridAffinityFunctionContextImpl ctx =
                        new GridAffinityFunctionContextImpl(a.topology, null, discoEvt, new AffinityTopologyVersion(top++), BACKUPS);

                    a.assignment = affinity.assignPartitions(ctx);
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

        node.setAttribute(CustomPrimaryFilter.ZONE_ATTR, zone);
        node.setAttribute(CustomPrimaryFilter.CELL_ATTR, cellId);
        node.setAttribute(CustomBackupFilter.DC_ATTR, dataCenterId);

        return node;
    }

    private void printAssignment(List<List<ClusterNode>> assignment, int backups, int partitions) {
        for (int part = 0; part < assignment.size(); part++) {
            for (ClusterNode node : assignment.get(part))
                System.out.println("[Part=" + part + ", Zone=" +
                    node.attribute(CustomPrimaryFilter.ZONE_ATTR) + ", Cell=" +
                    node.attribute(CustomPrimaryFilter.CELL_ATTR));
        }
    }

    /**
     * @param cell Cell.
     * @param nodes Nodes.
     */
    private void validateCell(String cell, List<ClusterNode> nodes) {
        assertEquals("Cell size", CELL_SIZE, nodes.size());

        Map<Object, Integer> cnt = new HashMap<>();

        for (ClusterNode node : nodes) {
            Object cellId = node.attribute(CustomPrimaryFilter.CELL_ATTR);

            assertEquals("Valid cell", cell, cellId);

            Object dcId = node.attribute(CustomBackupFilter.DC_ATTR);

            updateCounter(cnt, dcId, 1);
        }

        Iterator<Object> it = cnt.keySet().iterator();
        Object k1 = it.next();
        Object k2 = it.next();

        boolean equals = cnt.get(k1).equals(cnt.get(k2));
        assertTrue("Nodes distribution", equals);
    }

    /**
     * Validates what mappings have no common partitions
     *
     * @param mappings Mappings.
     */
    private void validatePartitions(List<Map<UUID, Collection<Integer>>> mappings) {
        if (mappings.size() == 1)
            return;

        for (int i = 0; i < mappings.size() - 1; i++) {
            Map<UUID, Collection<Integer>> m1 = mappings.get(i);

            Map<UUID, Collection<Integer>> m2 = mappings.get(i + 1);

            assertTrue("Partitions not intersect", Collections.disjoint(m1.values(), m2.values()));
        }
    }

    /**
     * Validate rule for partitions placement: partition is evenly split between data centers on full topology(every
     * cell contains CELL_SIZE elements)
     *
     * @param part Partition.
     * @param assignment Assignment.
     */
    private void validatePartition(int part, Assignment assignment) {
        List<ClusterNode> nodes = assignment.assignment.get(part);

        assertEquals("Nodes count", BACKUPS + 1, nodes.size());

        Map<Object, Integer> cnt = new HashMap<>();

        for (ClusterNode node : nodes) {
            Object dcId = node.attribute(CustomBackupFilter.DC_ATTR);

            updateCounter(cnt, dcId, 1);
        }

        assertEquals("Data centers count", 2, cnt.size());

        Iterator<Object> it = cnt.keySet().iterator();
        Object k1 = it.next();
        Object k2 = it.next();

        boolean equals = cnt.get(k1).equals(cnt.get(k2));
        assertTrue("Nodes distribution", equals);
    }

    /**
     * @param map Map.
     * @param key Key.
     * @param delta Delta.
     */
    private void updateCounter(Map<Object, Integer> map, Object key, int delta) {
        Integer cnt = map.get(key);

        if (cnt == null)
            cnt = 0;

        map.put(key, cnt + delta);
    }

    /**
     * @param zone Zone.
     * @param cellsCnt Cells count.
     * @param cellNodes Cell nodes.
     * @param assignment Assignment.
     */
    private Map<UUID, Collection<Integer>> validateCellDistribution(Object zone, int cellsCnt,
        List<ClusterNode> cellNodes, Assignment assignment) {
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

                // Skip node from other cell.
                if (!cellNodes.contains(node))
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

        log().info("max=" + max + ", min=" + min + ", ideal=" + ideal + ", minDev=" + deviation(min, ideal) + "%, " +
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

    /** Thread local message digest. */
    private ThreadLocal<MessageDigest> digest = new ThreadLocal<MessageDigest>() {
        @Override protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            }
            catch (NoSuchAlgorithmException e) {
                assert false : "Should have failed in constructor";

                throw new IgniteException("Failed to obtain message digest (digest was available in constructor)", e);
            }
        }
    };

    /**
     * Affinity function to test.
     */
    protected CustomRendezvousAffinityFunction affinityFunction() {
        CustomRendezvousAffinityFunction function = new CustomRendezvousAffinityFunction(false, PARTS_COUNT) {
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

        function.setAffinityPrimaryFilter(new CustomPrimaryFilter() {
            @Override protected Object partToZone(Integer part) {
                for (Map.Entry<Object, List<Integer>> entry : ZONE_TO_PART_MAP.entrySet()) {
                    List<Integer> val = entry.getValue();

                    if (val.get(0) <= part && part < val.get(0) + val.get(1))
                        return entry.getKey();
                }

                throw new IgniteException("Failed to find zone for partition");
            }
        });

        function.setAffinityBackupFilter(new CustomBackupFilter(BACKUPS, DATA_CENTERS));

        GridTestUtils.setFieldValue(function, CustomRendezvousAffinityFunction.class, "ignite", ignite);

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

    /**
     * @param val Value.
     * @param ideal Ideal.
     */
    private static int deviation(int val, int ideal) {
        return Math.round(Math.abs(((float)val - ideal) / ideal * 100));
    }
}
