package org.apache.ignite.internal.processors.query;

import org.apache.ignite.cache.query.IndexQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.util.GridSpinBusyLock;

/**
 * <p> The <code>GridBinaryQueryProcessor</code> </p>
 *
 * @author Alexei Scherbakov
 */
public interface GridBinaryQueryProcessor {
    void start(GridKernalContext ctx, GridSpinBusyLock lock);

    void registerCache(CacheConfiguration<?, ?> ccfg);

    void store(String space, GridQueryTypeDescriptor desc, CacheObject key, CacheObject val, byte[] ver,
        long time);

    <T> IndexQueryCursor<T> query(String space, byte[] seekTo, boolean match, boolean asc);
}
