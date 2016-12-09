package org.apache.ignite.cache.affinity.custom;

import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * The filter implements backups nodes filtering based on their Data Center location.
 * Primary and backup nodes are equally distributed between given number of data centers.
 * Implementation is based on setting node attribute.
 *
 * NOTE: The filter doesn't check required node attribute to be set for performance reasons.
 * It's recommended to use {@link TopologyValidator} for preventing misconfigured nodes to enter a topology.
 */
public class CustomBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
    /** Backups. */
    private final int backups;

    /** Data centers. */
    private final int dataCenters;

    /** Data center attribute.
     * TODO: allow to change name.
     * */
    public static final String DC_ATTR = "dc";

    /**
     * @param backups Backups.
     * @param dataCenters Data centers.
     */
    public CustomBackupFilter(int backups, int dataCenters) {
        this.backups = backups;
        this.dataCenters = dataCenters;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode testNode, List<ClusterNode> primaryAndBackupNodes) {
        if (primaryAndBackupNodes.size() >= backups + 1)
            return false;

        Object dcAttr = testNode.attribute(DC_ATTR);

        // Enforce rule: equal number of nodes hosting partition in each DC
        int dcCnt = 0;

        int maxAllowed = (backups + 1) / dataCenters;

        for (ClusterNode clusterNode : primaryAndBackupNodes) {
            Object nodeDcAttr = clusterNode.attribute(DC_ATTR);

            //A.notNull(nodeDcAttr, "Data center attribute is missing for node " + testNode);

            if (nodeDcAttr.equals(dcAttr)) {
                dcCnt++;

                if (dcCnt >= maxAllowed)
                    break;
            }
        }

        return dcCnt < maxAllowed;
    }
}