import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class Example {
    public static void main(String[] args) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(new TcpDiscoveryVmIpFinder(true));
        cfg.setDiscoverySpi(spi);
        CacheConfiguration<Integer, Entity> ccfg = new CacheConfiguration<>();
        ccfg.setIndexedTypes(Integer.class, Entity.class);
        cfg.setCacheConfiguration(ccfg);
        try(Ignite ignite = Ignition.getOrStart(cfg)) {
            IgniteCache<Integer, Entity> cache = ignite.cache(null);
            QueryCursor<List<?>> query = cache.query(new SqlFieldsQuery("select name from Entity"));
            System.out.println(query.getAll());
        }
    }

    public static abstract class Id<T> {
        private T id;

        public T getId() {
            return id;
        }

        public void setId(T id) {
            this.id = id;
        }
    }

    public static class Entity extends Id<Integer> {
        private String name;

        @QuerySqlField(index = true)
        @Override public Integer getId() {
            return super.getId();
        }

        @QuerySqlField
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
