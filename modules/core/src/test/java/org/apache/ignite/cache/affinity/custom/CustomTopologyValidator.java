package org.apache.ignite.cache.affinity.custom;

import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;

/**
 * Validates a presence of all necessary attributes for both filters.
 * Recommended to use with primary and backup filters.
 */
public class CustomTopologyValidator implements TopologyValidator {
    /** {@inheritDoc} */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        for (ClusterNode node : nodes) {
            if (node.attribute(CustomPrimaryFilter.ZONE_ATTR) == null ||
                node.attribute(CustomPrimaryFilter.CELL_ATTR) == null ||
                node.attribute(CustomBackupFilter.DC_ATTR) == null)
                return false;
        }

        return true;
    }
}
