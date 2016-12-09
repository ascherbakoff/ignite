package org.apache.ignite.cache.affinity.custom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.affinity.AffinityPrimaryFilter;
import org.apache.ignite.cluster.ClusterNode;

/**
 * .
 */
public abstract class CustomPrimaryFilter implements AffinityPrimaryFilter {
    /**
     * Zone attribute.
     * TODO: allow name customization.
     */
    public static final String ZONE_ATTR = "zone";

    /**
     * Cell attribute.
     * TODO: allow name customization.
     */
    public static final String CELL_ATTR = "cell";

    @Override public List<ClusterNode> apply(Integer part, List<ClusterNode> currTopNodes) {
        Object zone = partToZone(part);

        // Prepare the list of nodes belonging to zone.
        List<ClusterNode> zoneNodes = new ArrayList<>();

        List<Object> cells = new ArrayList<>();

        for (ClusterNode node : currTopNodes) {
            Object nodeZone = node.attribute(ZONE_ATTR);

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

        long hash = Long.MIN_VALUE;

        Object cell = null;

        // Selecting hash with max weight.
        for (Object testCell : cells) {
            long cellHash = hash(part, testCell);

            if (cellHash > hash) { // TODO handle collision.
                cell = testCell;

                hash = cellHash;
            }
        }

        List<ClusterNode> cellNodes = new ArrayList<>();

        // Return nodes belonging to selected cell.
        for (ClusterNode node : zoneNodes) {
            Object nodeCell = node.attribute(CELL_ATTR);

            if (nodeCell.equals(cell))
                cellNodes.add(node);
        }

        return cellNodes;
    }

    /**
     * @param part Partition.
     * @param obj Object.
     */
    private long hash(Integer part, Object obj) {
        return xorshift64star(((long)part << 32) | obj.hashCode());

//        MessageDigest d = digest.get();
//
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//
//        out.write(U.intToBytes(part), 0, 4); // Avoid IOException.
//        out.write(U.intToBytes(obj.hashCode()), 0, 4); // Avoid IOException.
//
//        d.reset();
//
//        byte[] bytes = d.digest(out.toByteArray());
//
//        long hash =
//            (bytes[0] & 0xFFL)
//                | ((bytes[1] & 0xFFL) << 8)
//                | ((bytes[2] & 0xFFL) << 16)
//                | ((bytes[3] & 0xFFL) << 24)
//                | ((bytes[4] & 0xFFL) << 32)
//                | ((bytes[5] & 0xFFL) << 40)
//                | ((bytes[6] & 0xFFL) << 48)
//                | ((bytes[7] & 0xFFL) << 56);
//
//        return hash;
    }

    public static long xorshift64star(long x) {
        x ^= x >>> 12; // a
        x ^= x << 25; // b
        x ^= x >>> 27; // c
        return x * 2685821657736338717L;
    }

    protected abstract Object partToZone(Integer part);
}