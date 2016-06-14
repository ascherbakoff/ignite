package org.apache.ignite.internal.processors.query.binary;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.IndexQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.query.GridBinaryQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.jsr166.ConcurrentHashMap8;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * <p> The <code>GridBinaryQueryProcessorImpl</code> </p>
 *
 * @author Alexei Scherbakov
 */
public class GridBinaryQueryProcessorImpl extends GridProcessorAdapter implements GridBinaryQueryProcessor {
    // TODO off-heap indexes
    private ConcurrentMap<String, SnapTreeMap<byte[], ?>> indexes = new ConcurrentHashMap8<>();

    /**
     * @param ctx Context.
     */
    public GridBinaryQueryProcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    @Override public void start(GridKernalContext ctx, GridSpinBusyLock lock) {

    }

    @Override public void registerCache(CacheConfiguration<?, ?> ccfg) {
        // Default ascending order.
        SnapTreeMap<?, ?> map = indexes.putIfAbsent(ccfg.getName(), new SnapTreeMap<>(new Comparator<byte[]>() {
            @Override public int compare(byte[] o1, byte[] o2) {
                return FastByteComparisons.compareTo(o1, 0, o1.length, o2, 0, o2.length);
            }
        }));

        if (map != null)
            throw new IgniteException("Cache already registered");
    }

    @Override
    public void store(String space, GridQueryTypeDescriptor desc, CacheObject key, CacheObject val, byte[] ver,
        long time) {

    }

    @Override public <T> IndexQueryCursor<T> query(final String space, final byte[] seekTo, boolean match, final boolean asc) {
        final SnapTreeMap<byte[], ?> map = indexes.get(space);

        IndexQueryCursor<T> ts = new IndexQueryCursor<T>() {
            private Collection<?> values;

            @Override public List<T> getAll() {
                return null;
            }

            @Override public void seek(byte[] key, boolean match) {
                values = key == null ?
                    asc ? map.values() : map.descendingMap().values() :
                    asc ? map.tailMap(key, match).values() : map.headMap(key, match).values();
            }

            @Override public void close() {
                // No-op.
            }

            @Override public Iterator<T> iterator() {
                final Iterator<?> iter = values.iterator();

                return new GridCloseableIteratorAdapterEx<T>() {
                    @Override protected T onNext() throws IgniteCheckedException {
                        Object key = iter.next();
                        GridCacheAdapter<Object, Object> internalCache = ctx.cache().internalCache(space);
                        return (T)internalCache.get(key);
                    }

                    @Override protected boolean onHasNext() {
                        return iter.hasNext();
                    }
                };
            }
        };

        ts.seek(seekTo, match);

        return ts;
    }
}
