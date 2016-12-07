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

package org.apache.ignite.cache.affinity;

import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiClosure;

/**
 * Allows custom primary partitions assignment from subset of nodes.
 */
public interface AffinityPrimaryFilter extends IgniteBiClosure<Integer, List<ClusterNode>, List<ClusterNode>> {
    /**
     * Returns nodes allowed to contain
     * @param partition Partition.
     * @param currentTopologyNodes All nodes from current topology.
     * @return subset of nodes allowed to contain given partition.
     */
    @Override public List<ClusterNode> apply(Integer partition, List<ClusterNode> currentTopologyNodes);
}
