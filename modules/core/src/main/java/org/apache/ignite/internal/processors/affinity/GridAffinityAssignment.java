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

package org.apache.ignite.internal.processors.affinity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.GridIntSet;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cached affinity calculations.
 */
public class GridAffinityAssignment implements AffinityAssignment, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Collection of calculated affinity nodes. */
    private List<List<ClusterNode>> assignment;

    /** Map of primary node partitions. */
    private final Map<UUID, GridIntSet> primary;

    /** Map of backup node partitions. */
    private final Map<UUID, GridIntSet> backup;

    /** Assignment node IDs */
    private transient volatile List<HashSet<UUID>> assignmentIds;

    /** Nodes having primary partitions assignments. */
    private transient volatile Set<ClusterNode> primaryPartsNodes;

    /** */
    private transient List<List<ClusterNode>> idealAssignment;

    /** */
    private final boolean clientEvtChange;

    /**
     * Constructs cached affinity calculations item.
     *
     * @param topVer Topology version.
     */
    GridAffinityAssignment(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
        primary = new HashMap<>();
        backup = new HashMap<>();
        clientEvtChange = false;
    }

    /**
     * @param topVer Topology version.
     * @param assignment Assignment.
     * @param idealAssignment Ideal assignment.
     */
    GridAffinityAssignment(AffinityTopologyVersion topVer,
        List<List<ClusterNode>> assignment,
        List<List<ClusterNode>> idealAssignment) {
        assert topVer != null;
        assert assignment != null;
        assert idealAssignment != null;

        this.topVer = topVer;
        this.assignment = assignment;
        this.idealAssignment = idealAssignment.equals(assignment) ? assignment : idealAssignment;

        primary = new HashMap<>();
        backup = new HashMap<>();
        clientEvtChange = false;

        initPrimaryBackupMaps();
    }

    /**
     * @param topVer Topology version.
     * @param aff Assignment to copy from.
     */
    GridAffinityAssignment(AffinityTopologyVersion topVer, GridAffinityAssignment aff) {
        this.topVer = topVer;

        assignment = aff.assignment;
        idealAssignment = aff.idealAssignment;
        primary = aff.primary;
        backup = aff.backup;

        clientEvtChange = true;
    }

    /**
     * @return {@code True} if related discovery event did not not cause affinity assignment change and
     *    this assignment is just reference to the previous one.
     */
    public boolean clientEventChange() {
        return clientEvtChange;
    }

    /**
     * @return Affinity assignment computed by affinity function.
     */
    public List<List<ClusterNode>> idealAssignment() {
        return idealAssignment;
    }

    /**
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> assignment() {
        return assignment;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * Get affinity nodes for partition.
     *
     * @param part Partition.
     * @return Affinity nodes.
     */
    public List<ClusterNode> get(int part) {
        assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
            " [part=" + part + ", partitions=" + assignment.size() + ']';

        return assignment.get(part);
    }

    /**
     * Get affinity node IDs for partition.
     *
     * @param part Partition.
     * @return Affinity nodes IDs.
     */
    public HashSet<UUID> getIds(int part) {
        assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
            " [part=" + part + ", partitions=" + assignment.size() + ']';

        List<HashSet<UUID>> assignmentIds0 = assignmentIds;

        if (assignmentIds0 == null) {
            assignmentIds0 = new ArrayList<>();

            for (List<ClusterNode> assignmentPart : assignment) {
                HashSet<UUID> partIds = new HashSet<>();

                for (ClusterNode node : assignmentPart)
                    partIds.add(node.id());

                assignmentIds0.add(partIds);
            }

            assignmentIds = assignmentIds0;
        }

        return assignmentIds0.get(part);
    }

    /**
     * @return Nodes having primary partitions assignments.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public Set<ClusterNode> primaryPartitionNodes() {
        Set<ClusterNode> primaryPartsNodes0 = primaryPartsNodes;

        if (primaryPartsNodes0 == null) {
            int parts = assignment.size();

            primaryPartsNodes0 = new HashSet<>();

            for (int p = 0; p < parts; p++) {
                List<ClusterNode> nodes = assignment.get(p);

                if (nodes.size() > 0)
                    primaryPartsNodes0.add(nodes.get(0));
            }

            primaryPartsNodes = primaryPartsNodes0;
        }

        return primaryPartsNodes0;
    }

    /**
     * Get primary partitions for specified node ID.
     *
     * @param nodeId Node ID to get primary partitions for.
     * @return Primary partitions for specified node ID.
     */
    public GridIntSet primaryPartitions(UUID nodeId) {
        GridIntSet set = primary.get(nodeId);

        return set == null ? GridIntSet.EMPTY : set;
    }

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @return Backup partitions for specified node ID.
     */
    public GridIntSet backupPartitions(UUID nodeId) {
        GridIntSet set = backup.get(nodeId);

        return set == null ? GridIntSet.EMPTY : set;
    }

    /**
     * Initializes primary and backup maps.
     */
    private void initPrimaryBackupMaps() {
        // Temporary mirrors with modifiable partition's collections.
        Map<UUID, GridIntSet> tmpPrm = new HashMap<>();
        Map<UUID, GridIntSet> tmpBkp = new HashMap<>();

        for (int partsCnt = assignment.size(), p = 0; p < partsCnt; p++) {
            // Use the first node as primary, other - backups.
            Map<UUID, GridIntSet> tmp = tmpPrm;
            Map<UUID, GridIntSet> map = primary;

            for (ClusterNode node : assignment.get(p)) {
                UUID id = node.id();

                GridIntSet set = tmp.get(id);

                if (set == null) {
                    tmp.put(id, set = new GridIntSet());
                    map.put(id, set);
                }

                set.add(p);

                // Use the first node as primary, other - backups.
                tmp = tmpBkp;
                map = backup;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return topVer.hashCode();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || !(o instanceof AffinityAssignment))
            return false;

        return topVer.equals(((AffinityAssignment)o).topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAffinityAssignment.class, this, super.toString());
    }
}